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
    if (el.id) return '#' + CSS.escape(el.id);
    const classes = Array.from(el.classList || []).slice(0, 3).map(c => CSS.escape(c)).join('.');
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

  // Pre-compute visible rects for every interactive element so we can measure
  // real centre-to-centre distance to nearest neighbour (WCAG 2.5.8 spacing
  // exception requires actual neighbour distance, not own margins).
  const rectsAll = [];
  for (const el of elements) {
    const style = window.getComputedStyle(el);
    if (style.display === 'none' || style.visibility === 'hidden') continue;
    const rect = el.getBoundingClientRect();
    if (rect.width === 0 && rect.height === 0) continue;
    rectsAll.push({ el, rect, cx: rect.left + rect.width / 2, cy: rect.top + rect.height / 2 });
  }

  function nearestNeighbourDistance(self) {
    let best = Infinity;
    for (const other of rectsAll) {
      if (other.el === self.el) continue;
      // Edge-to-edge gap on the dominant axis (x and y), then take the smaller.
      const dx = Math.max(0, Math.max(self.rect.left - other.rect.right, other.rect.left - self.rect.right));
      const dy = Math.max(0, Math.max(self.rect.top - other.rect.bottom, other.rect.top - self.rect.bottom));
      // Centre-to-centre Euclidean distance — used for the 24px circle test.
      const ddx = self.cx - other.cx;
      const ddy = self.cy - other.cy;
      const centreDist = Math.sqrt(ddx * ddx + ddy * ddy);
      const gap = Math.max(dx, dy); // either-axis clearance
      const score = Math.min(centreDist, gap === 0 ? 0 : Math.max(centreDist, gap));
      if (centreDist < best) best = centreDist;
    }
    return best;
  }

  for (const item of rectsAll) {
    const el = item.el;
    const rect = item.rect;
    const w = rect.width;
    const h = rect.height;
    const baseInfo = {
      locator: cssPath(el),
      text: (el.innerText || el.textContent || '').trim().slice(0, 80),
      width: Math.round(w),
      height: Math.round(h),
    };

    if (w >= MIN && h >= MIN) {
      passing.push(baseInfo);
      continue;
    }

    // Undersized — apply WCAG 2.5.8 spacing-offset exception.
    // Pass when the nearest interactive neighbour's centre is ≥ 24 CSS-px away
    // (i.e. a 24px-diameter circle on each target's centre does not intersect).
    const neighbourDist = nearestNeighbourDistance(item);
    const meetsSpacing = neighbourDist >= MIN;

    if (!meetsSpacing) {
      if (failing.length < 20) {
        failing.push(Object.assign({}, baseInfo, {
          neighbourDistance: Number.isFinite(neighbourDist) ? Math.round(neighbourDist) : null,
          tag: el.tagName.toLowerCase(),
          type: el.getAttribute('type') || '',
          role: el.getAttribute('role') || '',
        }));
      }
    } else {
      passing.push(Object.assign({}, baseInfo, {
        neighbourDistance: Number.isFinite(neighbourDist) ? Math.round(neighbourDist) : null,
      }));
    }
  }

  return { failing, passing_count: passing.length, total: rectsAll.length };
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
