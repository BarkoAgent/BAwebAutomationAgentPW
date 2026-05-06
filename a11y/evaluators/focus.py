from __future__ import annotations

import asyncio
from typing import Any, Dict, List

from ..models import COVERAGE_SEMI_AUTOMATED, OUTCOME_FAILED, OUTCOME_NEEDS_REVIEW


FOCUS_ACTIVE_SCRIPT = """
() => {
  function cssPath(el) {
    if (!el || el.nodeType !== 1) return '';
    if (el.id) return '#' + el.id;
    const classes = Array.from(el.classList || []).slice(0, 3).join('.');
    return el.tagName.toLowerCase() + (classes ? '.' + classes : '');
  }

  const el = document.activeElement;
  if (!el || !el.matches) return null;
  // Force layout/style flush so :focus-visible-driven styles are committed
  // before we sample computedStyle. Without this, transitions that haven't
  // finished can read as a missing indicator.
  void el.offsetHeight;
  const rect = el.getBoundingClientRect();
  const style = window.getComputedStyle(el);
  // Also sample a synthetic ::before pseudo — some designs render the focus
  // ring on a pseudo-element rather than on the host.
  const beforeStyle = window.getComputedStyle(el, '::before');
  const afterStyle = window.getComputedStyle(el, '::after');
  function pseudoHasIndicator(s) {
    if (!s) return false;
    const ow = parseFloat(s.outlineWidth || '0') || 0;
    const bw = parseFloat(s.borderWidth || '0') || 0;
    return (
      (s.outlineStyle && s.outlineStyle !== 'none' && ow > 0) ||
      (s.boxShadow && s.boxShadow !== 'none') ||
      (s.borderStyle && s.borderStyle !== 'none' && bw > 0)
    );
  }
  const centerX = Math.min(Math.max(rect.left + rect.width / 2, 0), Math.max(window.innerWidth - 1, 0));
  const centerY = Math.min(Math.max(rect.top + rect.height / 2, 0), Math.max(window.innerHeight - 1, 0));
  const topEl = document.elementFromPoint(centerX, centerY);
  const obscured = !!topEl && topEl !== el && !el.contains(topEl);
  const outlineWidth = parseFloat(style.outlineWidth || '0') || 0;
  const borderWidth = parseFloat(style.borderWidth || '0') || 0;
  const hasIndicator =
    (style.outlineStyle && style.outlineStyle !== 'none' && outlineWidth > 0) ||
    (style.boxShadow && style.boxShadow !== 'none') ||
    (style.borderStyle && style.borderStyle !== 'none' && borderWidth > 0) ||
    pseudoHasIndicator(beforeStyle) ||
    pseudoHasIndicator(afterStyle);
  const indicatorChanged =
    window.__a11yPreviousFocusStyle &&
    (
      window.__a11yPreviousFocusStyle.outlineStyle !== style.outlineStyle ||
      window.__a11yPreviousFocusStyle.outlineWidth !== style.outlineWidth ||
      window.__a11yPreviousFocusStyle.boxShadow !== style.boxShadow ||
      window.__a11yPreviousFocusStyle.borderColor !== style.borderColor ||
      window.__a11yPreviousFocusStyle.backgroundColor !== style.backgroundColor
    );
  window.__a11yPreviousFocusStyle = {
    outlineStyle: style.outlineStyle,
    outlineWidth: style.outlineWidth,
    boxShadow: style.boxShadow,
    borderColor: style.borderColor,
    backgroundColor: style.backgroundColor,
  };
  const clipped =
    rect.top < 0 ||
    rect.left < 0 ||
    rect.bottom > window.innerHeight ||
    rect.right > window.innerWidth;
  return {
    locator: cssPath(el),
    text: (el.innerText || el.textContent || '').trim().slice(0, 120),
    tag: el.tagName.toLowerCase(),
    obscured,
    clipped,
    hasIndicator,
    indicatorChanged: !!indicatorChanged,
    matchesFocusVisible: typeof el.matches === 'function' ? el.matches(':focus-visible') : false,
    outlineStyle: style.outlineStyle,
    outlineWidth: style.outlineWidth,
    boxShadow: style.boxShadow,
  };
}
"""


async def run_focus_visibility_evaluator(page: Any) -> List[Dict[str, Any]]:
    await page.evaluate("() => { window.__a11yPreviousFocusStyle = null; }")
    samples: List[Dict[str, Any]] = []
    seen = set()

    for _ in range(8):
        await page.keyboard.press("Tab")
        # Allow :focus-visible-driven styles & focus-ring transitions to settle.
        await asyncio.sleep(0.12)
        sample = await page.evaluate(FOCUS_ACTIVE_SCRIPT)
        if not sample:
            continue
        locator = sample.get("locator") or ""
        if locator and locator in seen:
            continue
        if locator:
            seen.add(locator)
        samples.append(sample)

    if not samples:
        return []

    missing_indicator = next(
        (
            sample for sample in samples
            if not sample.get("hasIndicator") and not sample.get("indicatorChanged") and not sample.get("matchesFocusVisible")
        ),
        None,
    )
    obscured_focus = next((sample for sample in samples if sample.get("obscured")), None)
    clipped_focus = next((sample for sample in samples if sample.get("clipped")), None)

    results: List[Dict[str, Any]] = []
    if missing_indicator:
        results.append(
            {
                "criterion_id": "2.4.7",
                "source": "custom:focus_visibility",
                "coverage_status": COVERAGE_SEMI_AUTOMATED,
                "outcome": OUTCOME_FAILED,
                "severity": "serious",
                "message": "A sampled focusable element did not expose a clear computed focus indicator.",
                "locator": missing_indicator.get("locator", ""),
                "element_text": missing_indicator.get("text", ""),
                "metadata": {
                    "samples": samples,
                    "focus_sample_count": len(samples),
                },
            }
        )
    else:
        results.append(
            {
                "criterion_id": "2.4.7",
                "source": "custom:focus_visibility",
                "coverage_status": COVERAGE_SEMI_AUTOMATED,
                "outcome": OUTCOME_NEEDS_REVIEW,
                "severity": "moderate",
                "message": "Sampled focusable elements exposed computed focus styling; full visual verification is still required.",
                "locator": samples[0].get("locator", ""),
                "element_text": samples[0].get("text", ""),
                "metadata": {
                    "samples": samples,
                    "focus_sample_count": len(samples),
                },
            }
        )

    if obscured_focus:
        results.append(
            {
                "criterion_id": "2.4.11",
                "source": "custom:focus_visibility",
                "coverage_status": COVERAGE_SEMI_AUTOMATED,
                "outcome": OUTCOME_FAILED,
                "severity": "serious",
                "message": "A sampled focused element appeared obscured at its center point.",
                "locator": obscured_focus.get("locator", ""),
                "element_text": obscured_focus.get("text", ""),
                "metadata": {
                    "samples": samples,
                    "focus_sample_count": len(samples),
                },
            }
        )
    else:
        results.append(
            {
                "criterion_id": "2.4.11",
                "source": "custom:focus_visibility",
                "coverage_status": COVERAGE_SEMI_AUTOMATED,
                "outcome": OUTCOME_NEEDS_REVIEW,
                "severity": "moderate",
                "message": "Focus obstruction was not detected in sampled elements, but full obscuration checks remain semi-automated.",
                "locator": samples[0].get("locator", ""),
                "element_text": samples[0].get("text", ""),
                "metadata": {
                    "samples": samples,
                    "focus_sample_count": len(samples),
                },
            }
        )

    if clipped_focus:
        results.append(
            {
                "criterion_id": "2.4.12",
                "source": "custom:focus_visibility",
                "coverage_status": COVERAGE_SEMI_AUTOMATED,
                "outcome": OUTCOME_FAILED,
                "severity": "serious",
                "message": "A sampled focused element extended beyond the current viewport, indicating possible clipping or partial obscuration.",
                "locator": clipped_focus.get("locator", ""),
                "element_text": clipped_focus.get("text", ""),
                "metadata": {
                    "samples": samples,
                    "focus_sample_count": len(samples),
                },
            }
        )
    else:
        results.append(
            {
                "criterion_id": "2.4.12",
                "source": "custom:focus_visibility",
                "coverage_status": COVERAGE_SEMI_AUTOMATED,
                "outcome": OUTCOME_NEEDS_REVIEW,
                "severity": "moderate",
                "message": "No clipped focused element was detected in the sample set, but enhanced obscuration still needs reviewer confirmation.",
                "locator": samples[0].get("locator", ""),
                "element_text": samples[0].get("text", ""),
                "metadata": {
                    "samples": samples,
                    "focus_sample_count": len(samples),
                },
            }
        )

    return results
