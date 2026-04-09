from __future__ import annotations

import asyncio
from typing import Any, Dict, List

from ..models import COVERAGE_AUTOMATED, COVERAGE_SEMI_AUTOMATED, OUTCOME_FAILED, OUTCOME_NEEDS_REVIEW, OUTCOME_PASSED


# WCAG 2.3.3 (AAA): Animation from Interactions
# Motion animation triggered by interaction can be disabled.
# Best practice (and required for many accessibility standards):
# CSS must respect prefers-reduced-motion media query.

ANIMATION_SCAN_SCRIPT = """
() => {
  function cssPath(el) {
    if (!el || el.nodeType !== 1) return '';
    if (el.id) return '#' + el.id;
    const classes = Array.from(el.classList || []).slice(0, 3).join('.');
    return el.tagName.toLowerCase() + (classes ? '.' + classes : '');
  }

  const animating = [];
  const candidates = Array.from(document.querySelectorAll('*')).slice(0, 500);

  for (const el of candidates) {
    const style = window.getComputedStyle(el);
    const animDuration = parseFloat(style.animationDuration) || 0;
    const transDuration = parseFloat(style.transitionDuration) || 0;
    if (animDuration > 0 || transDuration > 0) {
      animating.push({
        locator: cssPath(el),
        text: (el.innerText || el.textContent || '').trim().slice(0, 80),
        animationName: style.animationName,
        animationDuration: style.animationDuration,
        transitionDuration: style.transitionDuration,
        transitionProperty: style.transitionProperty,
      });
      if (animating.length >= 15) break;
    }
  }
  return animating;
}
"""

REDUCED_MOTION_CSS_CHECK = """
() => {
  // Check if any stylesheet contains prefers-reduced-motion rules
  const sheets = Array.from(document.styleSheets);
  let hasReducedMotionRule = false;
  let ruleCount = 0;
  for (const sheet of sheets) {
    try {
      const rules = Array.from(sheet.cssRules || []);
      for (const rule of rules) {
        if (
          rule.conditionText &&
          /prefers-reduced-motion/.test(rule.conditionText)
        ) {
          hasReducedMotionRule = true;
          ruleCount += 1;
        }
      }
    } catch (e) {
      // Cross-origin stylesheet
    }
  }
  return { hasReducedMotionRule, ruleCount };
}
"""


async def run_motion_preference_evaluator(page: Any) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []

    # Check stylesheet for prefers-reduced-motion rules (before emulation)
    try:
        css_check = await page.evaluate(REDUCED_MOTION_CSS_CHECK)
    except Exception:
        css_check = {"hasReducedMotionRule": False, "ruleCount": 0}

    # Emulate prefers-reduced-motion: reduce
    try:
        await page.emulate_media(reduced_motion="reduce")
        await asyncio.sleep(0.3)
        animations_under_reduced = await page.evaluate(ANIMATION_SCAN_SCRIPT)
    except Exception:
        animations_under_reduced = []
    finally:
        try:
            await page.emulate_media(reduced_motion="no-preference")
        except Exception:
            pass

    # Compare: get animations without reduced-motion emulation
    try:
        animations_normal = await page.evaluate(ANIMATION_SCAN_SCRIPT)
    except Exception:
        animations_normal = []

    has_reduced_motion_css = css_check.get("hasReducedMotionRule", False)
    still_animating = len(animations_under_reduced)
    was_animating = len(animations_normal)

    metadata = {
        "has_reduced_motion_css": has_reduced_motion_css,
        "reduced_motion_rule_count": css_check.get("ruleCount", 0),
        "animations_normal_count": was_animating,
        "animations_under_reduced_count": still_animating,
        "animations_under_reduced": animations_under_reduced[:5],
        "animations_normal": animations_normal[:5],
    }

    if was_animating == 0:
        results.append(
            {
                "criterion_id": "2.3.3",
                "source": "custom:motion_preference",
                "coverage_status": COVERAGE_AUTOMATED,
                "outcome": OUTCOME_PASSED,
                "severity": "minor",
                "message": "No CSS animations or transitions detected on the page.",
                "locator": "",
                "element_text": "",
                "metadata": metadata,
            }
        )
        return results

    if not has_reduced_motion_css:
        results.append(
            {
                "criterion_id": "2.3.3",
                "source": "custom:motion_preference",
                "coverage_status": COVERAGE_AUTOMATED,
                "outcome": OUTCOME_FAILED,
                "severity": "serious",
                "message": (
                    "{} animated element(s) detected but no @media (prefers-reduced-motion) "
                    "rule found in any stylesheet. Users who prefer reduced motion will see "
                    "full animations, violating WCAG 2.3.3.".format(was_animating)
                ),
                "locator": animations_normal[0].get("locator", "") if animations_normal else "",
                "element_text": animations_normal[0].get("text", "") if animations_normal else "",
                "metadata": metadata,
            }
        )
        return results

    # Has reduced-motion CSS — check if animations are actually suppressed
    if still_animating > 0 and still_animating >= was_animating:
        first = animations_under_reduced[0] if animations_under_reduced else {}
        results.append(
            {
                "criterion_id": "2.3.3",
                "source": "custom:motion_preference",
                "coverage_status": COVERAGE_SEMI_AUTOMATED,
                "outcome": OUTCOME_NEEDS_REVIEW,
                "severity": "moderate",
                "message": (
                    "prefers-reduced-motion CSS rules exist but {} animated element(s) "
                    "still have non-zero animation/transition durations under reduced-motion emulation. "
                    "Verify these are essential animations or properly suppressed.".format(still_animating)
                ),
                "locator": first.get("locator", ""),
                "element_text": first.get("text", ""),
                "metadata": metadata,
            }
        )
    else:
        reduction = was_animating - still_animating
        results.append(
            {
                "criterion_id": "2.3.3",
                "source": "custom:motion_preference",
                "coverage_status": COVERAGE_AUTOMATED,
                "outcome": OUTCOME_PASSED,
                "severity": "moderate",
                "message": (
                    "prefers-reduced-motion CSS rules are present and reduced {} of {} "
                    "animated element(s) under reduced-motion emulation.".format(reduction, was_animating)
                ),
                "locator": "",
                "element_text": "",
                "metadata": metadata,
            }
        )

    return results
