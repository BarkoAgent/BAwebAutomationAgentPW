from __future__ import annotations

from typing import Any, Dict, List

from ..models import COVERAGE_AUTOMATED, OUTCOME_FAILED, OUTCOME_PASSED


# WCAG 2.5.8 (AA, WCAG 2.2): Minimum Target Size
# Each target must be at least 24×24 CSS pixels OR have 24px spacing offset
# from all adjacent targets.
# Simplified rule applied here: flag elements where BOTH dimensions < 24px
# AND there is no sufficient adjacent spacing.

TARGET_SIZE_SCRIPT = """
() => {
  function cssPath(el) {
    if (!el || el.nodeType !== 1) return '';
    if (el.id) return '#' + el.id;
    const classes = Array.from(el.classList || []).slice(0, 3).join('.');
    return el.tagName.toLowerCase() + (classes ? '.' + classes : '');
  }

  const selector = [
    'a[href]',
    'button',
    'input:not([type="hidden"])',
    'select',
    'textarea',
    '[role="button"]',
    '[role="link"]',
    '[role="checkbox"]',
    '[role="radio"]',
    '[role="menuitem"]',
    '[role="tab"]',
    '[role="option"]',
    '[tabindex]:not([tabindex="-1"])',
  ].join(', ');

  const MIN = 24;
  const failing = [];
  const passing = [];
  const elements = Array.from(document.querySelectorAll(selector)).slice(0, 200);

  for (const el of elements) {
    const style = window.getComputedStyle(el);
    if (style.display === 'none' || style.visibility === 'hidden') continue;
    const rect = el.getBoundingClientRect();
    if (rect.width === 0 && rect.height === 0) continue;

    const w = rect.width;
    const h = rect.height;

    // Check if either dimension meets the 24px threshold
    if (w >= MIN || h >= MIN) {
      passing.push({ locator: cssPath(el), width: Math.round(w), height: Math.round(h), text: (el.innerText || el.textContent || '').trim().slice(0, 80) });
      continue;
    }

    // Both dimensions < 24px — check spacing offset from nearest sibling targets
    // Simple approach: check margins/padding that create spacing
    const marginTop = parseFloat(style.marginTop) || 0;
    const marginBottom = parseFloat(style.marginBottom) || 0;
    const marginLeft = parseFloat(style.marginLeft) || 0;
    const marginRight = parseFloat(style.marginRight) || 0;

    // Effective size including spacing in each direction
    const effectiveW = w + marginLeft + marginRight;
    const effectiveH = h + marginTop + marginBottom;

    const meetsSpacingOffset = effectiveW >= MIN && effectiveH >= MIN;

    if (!meetsSpacingOffset) {
      failing.push({
        locator: cssPath(el),
        text: (el.innerText || el.textContent || '').trim().slice(0, 80),
        width: Math.round(w),
        height: Math.round(h),
        effectiveWidth: Math.round(effectiveW),
        effectiveHeight: Math.round(effectiveH),
        tag: el.tagName.toLowerCase(),
        type: el.getAttribute('type') || '',
        role: el.getAttribute('role') || '',
      });
    } else {
      passing.push({ locator: cssPath(el), width: Math.round(w), height: Math.round(h), text: (el.innerText || el.textContent || '').trim().slice(0, 80) });
    }
    if (failing.length >= 20) break;
  }

  return { failing, passing_count: passing.length, total: elements.length };
}
"""


async def run_pointer_target_evaluator(page: Any) -> List[Dict[str, Any]]:
    try:
        data = await page.evaluate(TARGET_SIZE_SCRIPT)
    except Exception:
        return []

    failing = data.get("failing", [])
    passing_count = data.get("passing_count", 0)
    total = data.get("total", 0)

    results: List[Dict[str, Any]] = []

    if failing:
        first = failing[0]
        results.append(
            {
                "criterion_id": "2.5.8",
                "source": "custom:pointer_target",
                "coverage_status": COVERAGE_AUTOMATED,
                "outcome": OUTCOME_FAILED,
                "severity": "serious",
                "message": (
                    "{} interactive element(s) have a target size and spacing offset below "
                    "the WCAG 2.5.8 minimum of 24×24 CSS pixels. "
                    "First failing element: {} ({}×{}px).".format(
                        len(failing),
                        first.get("locator", ""),
                        first.get("width", 0),
                        first.get("height", 0),
                    )
                ),
                "locator": first.get("locator", ""),
                "element_text": first.get("text", ""),
                "metadata": {
                    "failing_targets": failing,
                    "passing_count": passing_count,
                    "total_scanned": total,
                    "minimum_px": 24,
                },
            }
        )
    else:
        results.append(
            {
                "criterion_id": "2.5.8",
                "source": "custom:pointer_target",
                "coverage_status": COVERAGE_AUTOMATED,
                "outcome": OUTCOME_PASSED,
                "severity": "moderate",
                "message": (
                    "All {} scanned interactive elements meet or exceed the "
                    "WCAG 2.5.8 minimum 24px target size or spacing offset.".format(total)
                ),
                "locator": "",
                "element_text": "",
                "metadata": {
                    "failing_targets": [],
                    "passing_count": passing_count,
                    "total_scanned": total,
                    "minimum_px": 24,
                },
            }
        )

    return results
