from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional

from ..models import COVERAGE_AUTOMATED, COVERAGE_SEMI_AUTOMATED, OUTCOME_FAILED, OUTCOME_NEEDS_REVIEW, OUTCOME_PASSED

logger = logging.getLogger(__name__)


# WCAG 1.3.4: Orientation
# Content must not restrict its view/operation to a single orientation
# unless a specific orientation is essential.

ORIENTATION_LOCK_SCRIPT = """
() => {
  // Detect CSS that restricts to one orientation
  const sheets = Array.from(document.styleSheets);
  const locks = [];
  for (const sheet of sheets) {
    try {
      const rules = Array.from(sheet.cssRules || []);
      for (const rule of rules) {
        if (rule.conditionText && /orientation\\s*:\\s*(landscape|portrait)/i.test(rule.conditionText)) {
          // Check if rules inside hide content (display:none, visibility:hidden)
          const innerText = rule.cssText || '';
          if (/display\\s*:\\s*none|visibility\\s*:\\s*hidden/i.test(innerText)) {
            locks.push({
              condition: rule.conditionText,
              cssText: innerText.slice(0, 200),
            });
          }
        }
      }
    } catch (e) {
      // Cross-origin stylesheet — skip
    }
  }
  return locks;
}
"""

OVERFLOW_CHECK_SCRIPT = """
() => {
  const bodyH = document.body.scrollHeight;
  const bodyW = document.body.scrollWidth;
  const winH = window.innerHeight;
  const winW = window.innerWidth;
  return {
    horizontalOverflow: bodyW > winW + 4,
    verticalOverflow: bodyH > winH * 3,
    scrollWidth: bodyW,
    scrollHeight: bodyH,
    windowWidth: winW,
    windowHeight: winH,
  };
}
"""


async def run_orientation_evaluator(page: Any) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []

    # Save original viewport
    try:
        original_viewport = page.viewport_size or {"width": 1280, "height": 800}
    except Exception:
        logger.warning("orientation: viewport_size lookup failed; using default 1280x800", exc_info=True)
        original_viewport = {"width": 1280, "height": 800}

    orig_w = original_viewport.get("width", 1280)
    orig_h = original_viewport.get("height", 800)

    # --- Check for CSS orientation locks ---
    try:
        locks = await page.evaluate(ORIENTATION_LOCK_SCRIPT)
    except Exception:
        logger.warning("orientation: CSS lock detection script failed; treating as no locks found", exc_info=True)
        locks = []

    if locks:
        results.append(
            {
                "criterion_id": "1.3.4",
                "source": "custom:orientation",
                "coverage_status": COVERAGE_AUTOMATED,
                "outcome": OUTCOME_FAILED,
                "severity": "serious",
                "message": (
                    "CSS @media orientation query detected that hides content in one orientation, "
                    "potentially restricting use to a single orientation (WCAG 1.3.4)."
                ),
                "locator": "stylesheet",
                "element_text": locks[0].get("condition", ""),
                "metadata": {"orientation_locks": locks},
            }
        )

    # --- Test portrait viewport (375×812) ---
    portrait_data: Optional[Dict] = None
    landscape_data: Optional[Dict] = None

    try:
        await page.set_viewport_size({"width": 375, "height": 812})
        await asyncio.sleep(0.3)
        portrait_data = await page.evaluate(OVERFLOW_CHECK_SCRIPT)
    except Exception:
        logger.warning("orientation: portrait viewport probe failed", exc_info=True)
        portrait_data = None

    # --- Test landscape viewport (812×375) ---
    try:
        await page.set_viewport_size({"width": 812, "height": 375})
        await asyncio.sleep(0.3)
        landscape_data = await page.evaluate(OVERFLOW_CHECK_SCRIPT)
    except Exception:
        logger.warning("orientation: landscape viewport probe failed", exc_info=True)
        landscape_data = None

    # Restore original viewport
    try:
        await page.set_viewport_size({"width": orig_w, "height": orig_h})
        await asyncio.sleep(0.2)
    except Exception:
        logger.warning("orientation: viewport restore to %dx%d failed", orig_w, orig_h, exc_info=True)

    portrait_overflow = portrait_data.get("horizontalOverflow", False) if portrait_data else False
    landscape_overflow = landscape_data.get("horizontalOverflow", False) if landscape_data else False

    if not locks:
        if portrait_overflow or landscape_overflow:
            failed_orientation = "portrait" if portrait_overflow else "landscape"
            failed_data = portrait_data if portrait_overflow else landscape_data
            results.append(
                {
                    "criterion_id": "1.3.4",
                    "source": "custom:orientation",
                    "coverage_status": COVERAGE_SEMI_AUTOMATED,
                    "outcome": OUTCOME_NEEDS_REVIEW,
                    "severity": "moderate",
                    "message": (
                        "Horizontal overflow detected in {} orientation "
                        "({}×{}px viewport). Content may not reflow correctly — "
                        "verify that all content and functionality remains available.".format(
                            failed_orientation,
                            failed_data.get("windowWidth", 0) if failed_data else 0,
                            failed_data.get("windowHeight", 0) if failed_data else 0,
                        )
                    ),
                    "locator": "body",
                    "element_text": "",
                    "metadata": {
                        "portrait": portrait_data,
                        "landscape": landscape_data,
                        "orientation_locks": locks,
                    },
                }
            )
        else:
            results.append(
                {
                    "criterion_id": "1.3.4",
                    "source": "custom:orientation",
                    "coverage_status": COVERAGE_AUTOMATED,
                    "outcome": OUTCOME_PASSED,
                    "severity": "moderate",
                    "message": (
                        "No CSS orientation locks or horizontal overflow detected in "
                        "portrait (375×812) or landscape (812×375) viewports."
                    ),
                    "locator": "",
                    "element_text": "",
                    "metadata": {
                        "portrait": portrait_data,
                        "landscape": landscape_data,
                        "orientation_locks": [],
                    },
                }
            )

    return results
