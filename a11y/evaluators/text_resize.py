from __future__ import annotations

import asyncio
import base64
import logging
from typing import Any, Dict, List

from ..models import COVERAGE_AUTOMATED, COVERAGE_SEMI_AUTOMATED, OUTCOME_FAILED, OUTCOME_NEEDS_REVIEW, OUTCOME_PASSED

logger = logging.getLogger(__name__)


# WCAG 1.4.12 mandates these overrides must not cause content/functionality loss
TEXT_SPACING_CSS = """
* {
  line-height: 1.5 !important;
  letter-spacing: 0.12em !important;
  word-spacing: 0.16em !important;
}
p {
  margin-bottom: 2em !important;
}
"""

INJECT_SPACING_SCRIPT = """
(css) => {
  const existing = document.getElementById('__a11y_text_spacing__');
  if (existing) existing.remove();
  const style = document.createElement('style');
  style.id = '__a11y_text_spacing__';
  style.textContent = css;
  document.head.appendChild(style);
}
"""

REMOVE_SPACING_SCRIPT = """
() => {
  const el = document.getElementById('__a11y_text_spacing__');
  if (el) el.remove();
}
"""

OVERFLOW_SCAN_SCRIPT = """
() => {
  function cssPath(el) {
    if (!el || el.nodeType !== 1) return '';
    if (el.id) return '#' + el.id;
    const classes = Array.from(el.classList || []).slice(0, 3).join('.');
    return el.tagName.toLowerCase() + (classes ? '.' + classes : '');
  }

  const overflowing = [];
  const candidates = Array.from(document.querySelectorAll(
    'p, li, td, th, label, span, div, section, article, h1, h2, h3, h4, h5, h6'
  )).slice(0, 300);

  for (const el of candidates) {
    const style = window.getComputedStyle(el);
    if (style.display === 'none' || style.visibility === 'hidden') continue;
    const rect = el.getBoundingClientRect();
    if (rect.width === 0 && rect.height === 0) continue;
    // horizontal overflow: scrollWidth > clientWidth + tolerance
    if (el.scrollWidth > el.clientWidth + 4) {
      overflowing.push({
        locator: cssPath(el),
        text: (el.innerText || el.textContent || '').trim().slice(0, 120),
        scrollWidth: el.scrollWidth,
        clientWidth: el.clientWidth,
        overflow: el.scrollWidth - el.clientWidth,
      });
      if (overflowing.length >= 10) break;
    }
  }

  // Also check body-level horizontal scroll
  const bodyOverflow = document.body.scrollWidth > window.innerWidth + 4;

  return { overflowing, bodyOverflow, bodyScrollWidth: document.body.scrollWidth, windowWidth: window.innerWidth };
}
"""

ZOOM_OVERFLOW_SCRIPT = """
() => {
  // Check for clipped/truncated text after zoom by looking for overflow:hidden containers
  // that now have scrollable content
  function cssPath(el) {
    if (!el || el.nodeType !== 1) return '';
    if (el.id) return '#' + el.id;
    const classes = Array.from(el.classList || []).slice(0, 3).join('.');
    return el.tagName.toLowerCase() + (classes ? '.' + classes : '');
  }

  const clipped = [];
  const candidates = Array.from(document.querySelectorAll('*')).slice(0, 500);
  for (const el of candidates) {
    const style = window.getComputedStyle(el);
    if (style.overflow === 'hidden' || style.overflowX === 'hidden' || style.overflowY === 'hidden') {
      if (el.scrollHeight > el.clientHeight + 4 || el.scrollWidth > el.clientWidth + 4) {
        clipped.push({
          locator: cssPath(el),
          text: (el.innerText || el.textContent || '').trim().slice(0, 80),
          scrollHeight: el.scrollHeight,
          clientHeight: el.clientHeight,
          scrollWidth: el.scrollWidth,
          clientWidth: el.clientWidth,
        });
        if (clipped.length >= 10) break;
      }
    }
  }
  return clipped;
}
"""


def _build_spacing_failure_message(overflowing: List[Dict[str, Any]], body_overflow: bool) -> str:
    if not overflowing and body_overflow:
        return (
            "The page scrolls horizontally when text spacing is increased. "
            "Look for elements with a fixed width that prevents text from wrapping — "
            "replace fixed widths with min-width, or remove overflow:hidden so text "
            "can expand."
        )

    first = overflowing[0]
    text = (first.get("text") or "").strip()
    locator = first.get("locator", "")
    overflow_px = first.get("overflow", 0)

    name = '"{}"'.format(text[:60]) if text else locator or "an element"
    loc_hint = " ({})".format(locator) if locator and text else ""
    overflow_hint = " by {}px".format(overflow_px) if overflow_px else ""

    others = ""
    if len(overflowing) > 1:
        count = len(overflowing) - 1
        others = " {} other element{} also overflow.".format(count, "s" if count != 1 else "")

    return (
        "The element {name}{loc_hint} overflows its container{overflow_hint} when text "
        "spacing is increased (letter-spacing 0.12em, word-spacing 0.16em, line-height 1.5). "
        "Its container likely has a fixed width or overflow:hidden. "
        "Fix: remove or relax the fixed width/max-width so the element can grow, "
        "or allow text to wrap instead of being clipped.{others}"
    ).format(name=name, loc_hint=loc_hint, overflow_hint=overflow_hint, others=others)


async def _screenshot_element(page: Any, locator_str: str) -> str:
    """Screenshot the specific element; fall back to viewport with element scrolled into view."""
    try:
        loc = page.locator(locator_str).first
        await loc.scroll_into_view_if_needed(timeout=2000)
        data = await loc.screenshot(type="jpeg", quality=70)
        return base64.b64encode(data).decode()
    except Exception:
        logger.debug("text_resize: element screenshot failed for %s, falling back to viewport", locator_str, exc_info=True)
    try:
        await page.evaluate(
            "(sel) => { const el = document.querySelector(sel); if (el) el.scrollIntoView({block: 'center'}); }",
            locator_str,
        )
        data = await page.screenshot(full_page=False, type="jpeg", quality=55)
        return base64.b64encode(data).decode()
    except Exception:
        logger.warning("text_resize: viewport-fallback screenshot failed for %s", locator_str, exc_info=True)
        return ""


async def run_text_resize_evaluator(page: Any) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []

    # --- 1.4.12: Text Spacing ---
    spacing_screenshot_b64: str = ""
    try:
        await page.evaluate(INJECT_SPACING_SCRIPT, TEXT_SPACING_CSS)
        await asyncio.sleep(0.2)
        spacing_data = await page.evaluate(OVERFLOW_SCAN_SCRIPT)

        _overflowing_early = spacing_data.get("overflowing", [])
        if _overflowing_early:
            spacing_screenshot_b64 = await _screenshot_element(page, _overflowing_early[0]["locator"])
        if not spacing_screenshot_b64:
            try:
                spacing_screenshot_b64 = base64.b64encode(
                    await page.screenshot(full_page=False, type="jpeg", quality=55)
                ).decode()
            except Exception:
                logger.warning("text_resize: spacing fallback viewport screenshot failed", exc_info=True)

        await page.evaluate(REMOVE_SPACING_SCRIPT)
    except Exception:
        logger.warning("text_resize: spacing override / overflow scan pipeline failed", exc_info=True)
        spacing_data = {"overflowing": [], "bodyOverflow": False}

    overflowing = spacing_data.get("overflowing", [])
    body_overflow = spacing_data.get("bodyOverflow", False)

    if overflowing or body_overflow:
        first = overflowing[0] if overflowing else {}
        results.append(
            {
                "criterion_id": "1.4.12",
                "source": "custom:text_resize",
                "coverage_status": COVERAGE_AUTOMATED,
                "outcome": OUTCOME_FAILED,
                "severity": "serious",
                "message": _build_spacing_failure_message(overflowing, body_overflow),
                "locator": first.get("locator", "body"),
                "element_text": first.get("text", ""),
                "screenshot_b64": spacing_screenshot_b64,
                "metadata": {
                    "overflowing_elements": overflowing,
                    "body_overflow": body_overflow,
                    "body_scroll_width": spacing_data.get("bodyScrollWidth"),
                    "window_width": spacing_data.get("windowWidth"),
                },
            }
        )
    else:
        results.append(
            {
                "criterion_id": "1.4.12",
                "source": "custom:text_resize",
                "coverage_status": COVERAGE_AUTOMATED,
                "outcome": OUTCOME_PASSED,
                "severity": "moderate",
                "message": (
                    "WCAG 1.4.12 text-spacing overrides did not trigger measurable "
                    "horizontal overflow in sampled elements."
                ),
                "locator": "body",
                "element_text": "",
                "screenshot_b64": spacing_screenshot_b64,
                "metadata": {
                    "overflowing_elements": [],
                    "body_overflow": False,
                },
            }
        )

    # --- 1.4.4: Resize Text (200% zoom via CSS zoom) ---
    zoom_screenshot_b64: str = ""
    try:
        await page.evaluate("""
            () => {
              const isFirefox = navigator.userAgent.toLowerCase().includes('firefox');
              if (isFirefox) {
                document.documentElement.style.transform = 'scale(2)';
                document.documentElement.style.transformOrigin = '0 0';
              } else {
                document.documentElement.style.zoom = '2';
              }
            }
        """)
        await asyncio.sleep(0.2)
        zoom_clipped = await page.evaluate(ZOOM_OVERFLOW_SCRIPT)
        try:
            zoom_screenshot_b64 = base64.b64encode(
                await page.screenshot(full_page=False, type="jpeg", quality=55)
            ).decode()
        except Exception:
            logger.warning("text_resize: zoom screenshot capture failed", exc_info=True)
        await page.evaluate("""
            () => {
              document.documentElement.style.zoom = '';
              document.documentElement.style.transform = '';
              document.documentElement.style.transformOrigin = '';
            }
        """)
    except Exception:
        logger.warning("text_resize: 200%% zoom probe pipeline failed", exc_info=True)
        zoom_clipped = []

    if zoom_clipped:
        first_clipped = zoom_clipped[0]
        results.append(
            {
                "criterion_id": "1.4.4",
                "source": "custom:text_resize",
                "coverage_status": COVERAGE_SEMI_AUTOMATED,
                "outcome": OUTCOME_FAILED,
                "severity": "serious",
                "message": (
                    "At 200% zoom, {} element(s) with overflow:hidden have content taller "
                    "or wider than their container, indicating clipped/truncated text.".format(len(zoom_clipped))
                ),
                "locator": first_clipped.get("locator", ""),
                "element_text": first_clipped.get("text", ""),
                "screenshot_b64": zoom_screenshot_b64,
                "metadata": {"clipped_elements": zoom_clipped},
            }
        )
    else:
        results.append(
            {
                "criterion_id": "1.4.4",
                "source": "custom:text_resize",
                "coverage_status": COVERAGE_SEMI_AUTOMATED,
                "outcome": OUTCOME_NEEDS_REVIEW,
                "severity": "moderate",
                "message": (
                    "No hidden-overflow clipping detected at 200% zoom; "
                    "visual review at browser zoom level still recommended for WCAG 1.4.4."
                ),
                "locator": "body",
                "element_text": "",
                "screenshot_b64": zoom_screenshot_b64,
                "metadata": {"clipped_elements": []},
            }
        )

    return results
