from __future__ import annotations

import asyncio
import base64
from typing import Any, Dict, List

from ..models import COVERAGE_AUTOMATED, OUTCOME_FAILED, OUTCOME_PASSED


OVERFLOW_SCRIPT = """
() => {
  const doc = document.scrollingElement || document.documentElement;
  const overflow = Math.max(0, doc.scrollWidth - window.innerWidth);
  const offenders = Array.from(document.querySelectorAll('body *'))
    .filter(el => {
      const rect = el.getBoundingClientRect();
      return rect.width > 0 && rect.right > window.innerWidth + 1;
    })
    .slice(0, 5)
    .map(el => {
      const classes = Array.from(el.classList || []).slice(0, 3).join('.');
      return el.id ? '#' + el.id : el.tagName.toLowerCase() + (classes ? '.' + classes : '');
    });
  return { overflow, offenders };
}
"""


async def run_viewport_reflow_evaluator(page: Any, viewport_profile: str) -> List[Dict[str, Any]]:
    if not viewport_profile or "mobile" not in viewport_profile.lower():
        return []

    original_viewport = getattr(page, "viewport_size", None) or {"width": 800, "height": 800}
    mobile_viewport = {"width": 375, "height": 812}

    screenshot_b64: str = ""
    overflow_result: Dict[str, Any] = {}
    try:
        await page.set_viewport_size(mobile_viewport)
        await asyncio.sleep(0.25)
        overflow_result = await page.evaluate(OVERFLOW_SCRIPT)

        try:
            screenshot_bytes = await page.screenshot(full_page=False, type="jpeg", quality=55)
            screenshot_b64 = base64.b64encode(screenshot_bytes).decode()
        except Exception:
            pass
    finally:
        await page.set_viewport_size(original_viewport)
        await asyncio.sleep(0.1)

    overflow = int(overflow_result.get("overflow") or 0)
    offenders = overflow_result.get("offenders") or []
    if overflow > 1:
        return [
            {
                "criterion_id": "1.4.10",
                "source": "custom:viewport_reflow",
                "coverage_status": COVERAGE_AUTOMATED,
                "outcome": OUTCOME_FAILED,
                "severity": "serious",
                "message": "Horizontal overflow of {}px was detected at a mobile viewport width of {}px.".format(
                    overflow, mobile_viewport["width"]
                ),
                "locator": offenders[0] if offenders else "document",
                "element_text": "",
                "screenshot_b64": screenshot_b64,
                "metadata": {
                    "overflow_pixels": overflow,
                    "offenders": offenders,
                    "viewport": mobile_viewport,
                },
            }
        ]

    return [
        {
            "criterion_id": "1.4.10",
            "source": "custom:viewport_reflow",
            "coverage_status": COVERAGE_AUTOMATED,
            "outcome": OUTCOME_PASSED,
            "severity": "moderate",
            "message": "No horizontal overflow was detected at a mobile viewport width of {}px.".format(
                mobile_viewport["width"]
            ),
            "locator": "document",
            "element_text": "",
            "screenshot_b64": screenshot_b64,
            "metadata": {
                "overflow_pixels": overflow,
                "offenders": offenders,
                "viewport": mobile_viewport,
            },
        }
    ]
