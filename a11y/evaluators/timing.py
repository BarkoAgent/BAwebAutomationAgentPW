from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List

from ..models import COVERAGE_AUTOMATED, COVERAGE_SEMI_AUTOMATED, OUTCOME_FAILED, OUTCOME_NEEDS_REVIEW, OUTCOME_PASSED

logger = logging.getLogger(__name__)


# WCAG 2.2.1: Timing Adjustable — user can extend/disable time limits
# WCAG 2.2.2: Pause, Stop, Hide — auto-updating content has pause/stop/hide control

TIMING_SCAN_SCRIPT = """
() => {
  function cssPath(el) {
    if (!el || el.nodeType !== 1) return '';
    if (el.id) return '#' + el.id;
    const classes = Array.from(el.classList || []).slice(0, 3).join('.');
    return el.tagName.toLowerCase() + (classes ? '.' + classes : '');
  }

  // Detect carousels / sliders
  const carouselSelectors = [
    '[aria-roledescription="carousel"]',
    '[class*="carousel"]',
    '[class*="slider"]',
    '[class*="slideshow"]',
    '[data-ride="carousel"]',
    '.swiper',
    '.slick-slider',
    '.owl-carousel',
  ];
  const carousels = [];
  const carouselSeen = new Set();
  for (const sel of carouselSelectors) {
    for (const el of Array.from(document.querySelectorAll(sel)).slice(0, 5)) {
      const path = cssPath(el);
      if (carouselSeen.has(path)) continue;
      carouselSeen.add(path);
      // Check for pause/stop control nearby
      const hasPauseControl = !!(
        el.querySelector('[aria-label*="pause" i], [aria-label*="stop" i], [title*="pause" i], button[class*="pause" i]') ||
        el.previousElementSibling?.querySelector('button') ||
        el.nextElementSibling?.querySelector('button')
      );
      carousels.push({
        locator: path,
        text: (el.innerText || '').trim().slice(0, 80),
        hasPauseControl,
      });
    }
  }

  // Detect countdown timers (elements matching time patterns)
  const timerElements = [];
  const timeSelectors = ['[role="timer"]', 'time', '[class*="timer"]', '[class*="countdown"]', '[id*="timer"]', '[id*="countdown"]'];
  for (const sel of timeSelectors) {
    for (const el of Array.from(document.querySelectorAll(sel)).slice(0, 5)) {
      const text = (el.innerText || el.textContent || '').trim();
      // Match patterns like "3:59", "01:23:45", "59s", "30 seconds"
      if (/\d+:\d{2}|\d+\s*(s|sec|seconds?)\b/i.test(text)) {
        timerElements.push({
          locator: cssPath(el),
          text: text.slice(0, 80),
        });
      }
    }
  }

  // Detect auto-updating content via aria-live regions that aren't just status
  const autoUpdating = Array.from(document.querySelectorAll(
    '[aria-live="polite"], [aria-live="assertive"], [role="marquee"], [role="log"]'
  )).slice(0, 10).map(el => ({
    locator: cssPath(el),
    text: (el.innerText || '').trim().slice(0, 80),
    role: el.getAttribute('role') || '',
    ariaLive: el.getAttribute('aria-live') || '',
  }));

  // Check for marquee elements (deprecated but still found)
  const marquees = Array.from(document.querySelectorAll('marquee, [role="marquee"]')).map(el => ({
    locator: cssPath(el),
    text: (el.innerText || '').trim().slice(0, 80),
  }));

  return { carousels, timerElements, autoUpdating, marquees };
}
"""

INTERVAL_INTERCEPT_SCRIPT = """
() => {
  if (window.__a11yIntervalCount !== undefined) return window.__a11yIntervalCount;
  window.__a11yIntervalCount = 0;
  const origSetInterval = window.setInterval;
  window.setInterval = function(fn, delay, ...args) {
    window.__a11yIntervalCount += 1;
    return origSetInterval.call(this, fn, delay, ...args);
  };
  return 0;
}
"""

GET_INTERVAL_COUNT_SCRIPT = "() => window.__a11yIntervalCount || 0"


# Captures per-carousel state fingerprint (active slide index, inner-track
# transform, scrollLeft, and aria-current). Polled twice across an interval
# so we only flag carousels that actually auto-advance.
CAROUSEL_FINGERPRINT_SCRIPT = """
(locators) => {
  function findByPath(path) {
    if (!path) return null;
    if (path.startsWith('#')) return document.getElementById(path.slice(1));
    try { return document.querySelector(path); } catch (e) { return null; }
  }
  function fingerprint(el) {
    if (!el) return '';
    const parts = [];
    const active = el.querySelector('[aria-current="true"], .is-active, .active, .swiper-slide-active, .slick-active, .owl-item.active');
    if (active) {
      const idx = Array.prototype.indexOf.call(active.parentElement ? active.parentElement.children : [], active);
      parts.push('active:' + idx);
    }
    // Inner-track transform (Swiper / Slick / generic)
    const track = el.querySelector('.swiper-wrapper, .slick-track, [class*="track"], [class*="slides"]');
    if (track) {
      const t = window.getComputedStyle(track).transform || '';
      parts.push('t:' + t);
      parts.push('sl:' + Math.round(track.scrollLeft || 0));
    }
    parts.push('elsl:' + Math.round(el.scrollLeft || 0));
    return parts.join('|');
  }
  return locators.map(loc => fingerprint(findByPath(loc)));
}
"""


async def run_timing_evaluator(page: Any) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []

    try:
        # Intercept setInterval calls during a short observation window
        await page.evaluate(INTERVAL_INTERCEPT_SCRIPT)
        await asyncio.sleep(1.0)
        interval_count = await page.evaluate(GET_INTERVAL_COUNT_SCRIPT)
        timing_data = await page.evaluate(TIMING_SCAN_SCRIPT)
    except Exception:
        logger.warning("timing: interval intercept / timing scan failed", exc_info=True)
        interval_count = 0
        timing_data = {"carousels": [], "timerElements": [], "autoUpdating": [], "marquees": []}

    carousels = timing_data.get("carousels", [])

    # Confirm auto-advance by polling each detected carousel's slide-state
    # fingerprint across an interval. Only carousels whose fingerprint changes
    # without user interaction are treated as actually auto-advancing.
    auto_advancing_locators: set = set()
    if carousels:
        try:
            locators = [c.get("locator", "") for c in carousels]
            fp_before = await page.evaluate(CAROUSEL_FINGERPRINT_SCRIPT, locators)
            await asyncio.sleep(1.6)
            fp_after = await page.evaluate(CAROUSEL_FINGERPRINT_SCRIPT, locators)
            for i, loc in enumerate(locators):
                before = fp_before[i] if i < len(fp_before) else ""
                after = fp_after[i] if i < len(fp_after) else ""
                if before and after and before != after:
                    auto_advancing_locators.add(loc)
            for c in carousels:
                c["autoAdvancing"] = c.get("locator", "") in auto_advancing_locators
        except Exception:
            logger.warning("timing: carousel auto-advance fingerprint probe failed", exc_info=True)
            for c in carousels:
                c["autoAdvancing"] = None
    # A static carousel (no auto-advance observed) is not a 2.2.2 issue.
    carousels = [c for c in carousels if c.get("autoAdvancing") is not False]
    timer_elements = timing_data.get("timerElements", [])
    auto_updating = timing_data.get("autoUpdating", [])
    marquees = timing_data.get("marquees", [])

    metadata = {
        "interval_count": interval_count,
        "carousels": carousels,
        "timer_elements": timer_elements,
        "auto_updating": auto_updating,
        "marquees": marquees,
    }

    # --- WCAG 2.2.2: Carousels without pause control ---
    carousels_without_pause = [c for c in carousels if not c.get("hasPauseControl")]

    if marquees:
        results.append(
            {
                "criterion_id": "2.2.2",
                "source": "custom:timing",
                "coverage_status": COVERAGE_AUTOMATED,
                "outcome": OUTCOME_FAILED,
                "severity": "serious",
                "message": (
                    "A <marquee> or role='marquee' element was detected. Marquee elements "
                    "auto-scroll content without a built-in pause mechanism, violating WCAG 2.2.2."
                ),
                "locator": marquees[0].get("locator", ""),
                "element_text": marquees[0].get("text", ""),
                "metadata": {**metadata, "failing_marquees": marquees},
            }
        )
    elif carousels_without_pause:
        first = carousels_without_pause[0]
        results.append(
            {
                "criterion_id": "2.2.2",
                "source": "custom:timing",
                "coverage_status": COVERAGE_SEMI_AUTOMATED,
                "outcome": OUTCOME_FAILED,
                "severity": "serious",
                "message": (
                    "An auto-advancing carousel/slider was detected without a visible "
                    "pause or stop control nearby, violating WCAG 2.2.2."
                ),
                "locator": first.get("locator", ""),
                "element_text": first.get("text", ""),
                "metadata": {**metadata, "failing_carousels": carousels_without_pause},
            }
        )
    elif carousels:
        first = carousels[0]
        results.append(
            {
                "criterion_id": "2.2.2",
                "source": "custom:timing",
                "coverage_status": COVERAGE_SEMI_AUTOMATED,
                "outcome": OUTCOME_NEEDS_REVIEW,
                "severity": "moderate",
                "message": (
                    "A carousel/slider was detected with what appears to be a pause control. "
                    "Verify the pause control is keyboard-accessible and actually stops auto-advance."
                ),
                "locator": first.get("locator", ""),
                "element_text": first.get("text", ""),
                "metadata": metadata,
            }
        )
    elif interval_count > 0 and auto_updating:
        results.append(
            {
                "criterion_id": "2.2.2",
                "source": "custom:timing",
                "coverage_status": COVERAGE_SEMI_AUTOMATED,
                "outcome": OUTCOME_NEEDS_REVIEW,
                "severity": "moderate",
                "message": (
                    "{} setInterval call(s) detected alongside {} aria-live/auto-updating "
                    "region(s). Verify moving/blinking/scrolling content has pause controls.".format(
                        interval_count, len(auto_updating)
                    )
                ),
                "locator": auto_updating[0].get("locator", "") if auto_updating else "",
                "element_text": auto_updating[0].get("text", "") if auto_updating else "",
                "metadata": metadata,
            }
        )
    else:
        results.append(
            {
                "criterion_id": "2.2.2",
                "source": "custom:timing",
                "coverage_status": COVERAGE_AUTOMATED,
                "outcome": OUTCOME_PASSED,
                "severity": "minor",
                "message": "No auto-advancing carousels, marquees, or auto-updating content detected.",
                "locator": "",
                "element_text": "",
                "metadata": metadata,
            }
        )

    # --- WCAG 2.2.1: Time limits with countdown timers ---
    if timer_elements:
        first = timer_elements[0]
        results.append(
            {
                "criterion_id": "2.2.1",
                "source": "custom:timing",
                "coverage_status": COVERAGE_SEMI_AUTOMATED,
                "outcome": OUTCOME_NEEDS_REVIEW,
                "severity": "serious",
                "message": (
                    "A countdown timer was detected ({}). Verify the user can extend, "
                    "adjust, or disable this time limit per WCAG 2.2.1.".format(first.get("text", ""))
                ),
                "locator": first.get("locator", ""),
                "element_text": first.get("text", ""),
                "metadata": {**metadata, "timer_details": timer_elements},
            }
        )
    else:
        results.append(
            {
                "criterion_id": "2.2.1",
                "source": "custom:timing",
                "coverage_status": COVERAGE_AUTOMATED,
                "outcome": OUTCOME_PASSED,
                "severity": "minor",
                "message": "No countdown timer elements detected on the page.",
                "locator": "",
                "element_text": "",
                "metadata": metadata,
            }
        )

    return results
