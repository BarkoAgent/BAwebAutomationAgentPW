from __future__ import annotations

import asyncio
import base64
import logging
import time
from typing import Any, Callable, Dict, List, Optional

from ..models import (
    COVERAGE_SEMI_AUTOMATED,
    OUTCOME_ERROR,
    OUTCOME_FAILED,
    OUTCOME_NEEDS_REVIEW,
    OUTCOME_NOT_APPLICABLE,
    OUTCOME_PASSED,
)

logger = logging.getLogger(__name__)

# WCAG 1.4.13: Content on Hover or Focus
# Tooltip/popover content that appears on hover must be:
#   1. Dismissible — user can dismiss without moving pointer (e.g. Escape key)
#   2. Hoverable — pointer can move to the new content without it disappearing
#   3. Persistent — content stays visible until dismissed, pointer leaves, or focus moves

# ── Tuning constants ──────────────────────────────────────────────────────────
_MAX_PROBES = 3         # candidates actually probed with mouse interaction
_HOVER_SETTLE_MS = 800  # max wait for tooltip to appear after mouse.move
_ESCAPE_SETTLE_MS = 600 # max wait for tooltip to disappear after Escape
_POLL_INTERVAL_MS = 100 # polling tick — 100ms halves browser round-trips vs 50ms
_HOVERABILITY_STEPS = 8 # incremental mouse steps from trigger → tooltip

# ── JavaScript scripts ────────────────────────────────────────────────────────

# Dedicated scan for native title attribute elements.
# These produce browser-chrome tooltips — not DOM elements, not detectable via
# MutationObserver. Reported informational only; never passed to the probe loop.
TITLE_ELEMENTS_SCRIPT = """
() => {
  function cssPath(el) {
    if (!el || el.nodeType !== 1) return '';
    if (el.id) return '#' + CSS.escape(el.id);
    const classes = Array.from(el.classList || []).slice(0, 3).map(c => CSS.escape(c)).join('.');
    return el.tagName.toLowerCase() + (classes ? '.' + classes : '');
  }
  const results = [];
  const seen = new WeakSet();
  for (const el of document.querySelectorAll('[title]:not([title=""])')) {
    if (seen.has(el)) continue;
    seen.add(el);
    const rect = el.getBoundingClientRect();
    const style = window.getComputedStyle(el);
    if (style.display === 'none' || style.visibility === 'hidden' || rect.width === 0 || rect.height === 0) continue;
    results.push({
      locator: cssPath(el),
      text: (el.innerText || el.textContent || '').trim().slice(0, 120),
      tag: el.tagName.toLowerCase(),
      titleValue: el.getAttribute('title'),
      centerX: rect.left + rect.width / 2,
      centerY: rect.top + rect.height / 2,
      rect: { top: rect.top, left: rect.left, width: rect.width, height: rect.height },
    });
    if (results.length >= 5) break;
  }
  return results;
}
"""

# Non-title hover triggers only — elements that trigger author-created tooltip content.
# [role="tooltip"] is a tooltip container, not a trigger — excluded from this script.
HOVER_TRIGGERS_SCRIPT = """
() => {
  function cssPath(el) {
    if (!el || el.nodeType !== 1) return '';
    if (el.id) return '#' + CSS.escape(el.id);
    const classes = Array.from(el.classList || []).slice(0, 3).map(c => CSS.escape(c)).join('.');
    return el.tagName.toLowerCase() + (classes ? '.' + classes : '');
  }
  const selectors = [
    '[aria-describedby]',
    '[data-tooltip]',
    '[data-tippy-content]',
    '[data-bs-toggle="tooltip"]',
    '[data-bs-toggle="popover"]',
  ];
  const seenEls = new WeakSet();
  const candidates = [];
  for (const sel of selectors) {
    for (const el of document.querySelectorAll(sel)) {
      if (seenEls.has(el)) continue;
      seenEls.add(el);
      const rect = el.getBoundingClientRect();
      const style = window.getComputedStyle(el);
      if (style.display === 'none' || style.visibility === 'hidden' || rect.width === 0 || rect.height === 0) continue;
      candidates.push({
        locator: cssPath(el),
        text: (el.innerText || el.textContent || '').trim().slice(0, 120),
        tag: el.tagName.toLowerCase(),
        trigger: sel,
        centerX: rect.left + rect.width / 2,
        centerY: rect.top + rect.height / 2,
        rect: { top: rect.top, left: rect.left, width: rect.width, height: rect.height },
      });
      if (candidates.length >= 8) break;
    }
    if (candidates.length >= 8) break;
  }
  return candidates;
}
"""

# Captures a visibility snapshot of all tooltip-like elements.
# Each element is assigned a stable, per-session `data-a11y-probe-id` attribute on
# first encounter (skipped on subsequent snapshots to preserve the same ID).
# `stableKey` is a DOM path (tag + nth-child chain, max 6 levels) that is stable
# even when class names change between before/after snapshots — used as the diff key.
TOOLTIP_SNAPSHOT_SCRIPT = """
() => {
  // Stable DOM path — class-name-independent, up to 6 ancestor levels.
  function domPath(el) {
    const parts = [];
    let cur = el;
    let depth = 0;
    while (cur && cur.parentElement && depth < 6) {
      const parent = cur.parentElement;
      const idx = Array.prototype.indexOf.call(parent.children, cur);
      parts.unshift(cur.tagName.toLowerCase() + '[' + idx + ']');
      cur = parent;
      depth++;
    }
    return parts.join('>');
  }

  if (!window.__a11yProbeCounter) window.__a11yProbeCounter = 0;

  const selectors = [
    '[role="tooltip"]',
    '[data-tooltip]',
    '[data-tippy-content]',
    '[data-bs-toggle="tooltip"]',
    '[data-bs-toggle="popover"]',
    '[class*="tooltip"]',
    '[class*="popover"]',
    '[class*="tippy"]',
  ];
  const seenEls = new WeakSet();
  const snapshot = [];
  for (const sel of selectors) {
    for (const el of document.querySelectorAll(sel)) {
      if (seenEls.has(el)) continue;
      seenEls.add(el);

      // Assign a stable probe ID on first encounter; preserve on subsequent snapshots.
      if (!el.hasAttribute('data-a11y-probe-id')) {
        el.setAttribute('data-a11y-probe-id', 'a11yp-' + (window.__a11yProbeCounter++));
      }
      const probeId = el.getAttribute('data-a11y-probe-id');
      const stableKey = domPath(el);

      const style = window.getComputedStyle(el);
      const rect = el.getBoundingClientRect();
      const opacity = parseFloat(style.opacity) || 0;
      const isVisible = (
        style.display !== 'none' &&
        style.visibility !== 'hidden' &&
        style.visibility !== 'collapse' &&
        rect.width > 0 &&
        rect.height > 0 &&
        opacity > 0.05
      );
      snapshot.push({
        tag: el.tagName.toLowerCase(),
        role: el.getAttribute('role') || '',
        id: el.id || '',
        classes: el.className || '',
        probeId,
        stableKey,
        isVisible,
        display: style.display,
        visibility: style.visibility,
        opacity: style.opacity,
        rect: { top: Math.round(rect.top), left: Math.round(rect.left), width: Math.round(rect.width), height: Math.round(rect.height) },
        text: (el.innerText || el.textContent || '').trim().slice(0, 120),
      });
    }
  }
  return snapshot;
}
"""

# Checks whether specific appeared tooltip elements (identified by probe ID) are
# still visible. Takes a plain array of probe ID strings — no broad class search,
# no querySelectorAll('div') over the entire document.
TOOLTIP_GONE_SCRIPT = """
(probeIds) => {
  let stillVisible = 0;
  for (const probeId of probeIds) {
    const el = document.querySelector('[data-a11y-probe-id="' + probeId + '"]');
    if (!el) continue;
    const style = window.getComputedStyle(el);
    const rect = el.getBoundingClientRect();
    const opacity = parseFloat(style.opacity) || 0;
    if (style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && opacity > 0.05) {
      stillVisible++;
    }
  }
  return stillVisible;
}
"""

# Returns true if ANY of the specified probe ID elements are currently visible.
# Used in hoverability checks to confirm the specific tooltip (not just any
# [class*="tooltip"] element) is still visible mid-path and at the endpoint.
PROBE_IDS_VISIBLE_SCRIPT = """
(probeIds) => {
  return probeIds.some(probeId => {
    const el = document.querySelector('[data-a11y-probe-id="' + probeId + '"]');
    if (!el) return false;
    const style = window.getComputedStyle(el);
    const rect = el.getBoundingClientRect();
    const opacity = parseFloat(style.opacity) || 0;
    return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && opacity > 0.05;
  });
}
"""

# Removes all data-a11y-probe-id attributes and resets the counter.
# Called after each candidate probe to prevent ID leakage across candidates.
CLEANUP_PROBE_IDS_SCRIPT = """
() => {
  for (const el of document.querySelectorAll('[data-a11y-probe-id]')) {
    el.removeAttribute('data-a11y-probe-id');
  }
  delete window.__a11yProbeCounter;
}
"""

# ── Python helpers ────────────────────────────────────────────────────────────

def _diff_snapshots(before: List[Dict], after: List[Dict]) -> List[Dict]:
    """Return elements in `after` that became visible relative to `before`.

    Uses `stableKey` (DOM path) as the dict key — stable even when class names
    change between snapshots due to CSS-in-JS or animation class toggling.
    Falls back to tag|id|classes for items without a stableKey (defensive only).
    """
    def key(item: Dict) -> str:
        return item.get("stableKey") or f"{item.get('tag','')}|{item.get('id','')}|{item.get('classes','')}"

    before_map = {key(item): item for item in before}
    appeared = []
    for item in after:
        if not item.get("isVisible"):
            continue
        prior = before_map.get(key(item))
        if prior is None or not prior.get("isVisible"):
            appeared.append(item)
    return appeared


async def _poll_for_condition(
    page: Any,
    script: str,
    args: Any,
    condition_fn: Callable[[Any], bool],
    timeout_ms: int = _HOVER_SETTLE_MS,
    interval_ms: int = _POLL_INTERVAL_MS,
) -> Any:
    """Poll page.evaluate(script) until condition_fn(result) is True or timeout elapses.

    Returns the last result regardless of whether the condition was met.
    Replaces fixed asyncio.sleep() calls with condition-based waiting.
    """
    deadline = time.monotonic() + timeout_ms / 1000.0
    result = None
    while time.monotonic() < deadline:
        try:
            result = await page.evaluate(script, args) if args is not None else await page.evaluate(script)
        except Exception:
            logger.warning("hover_content: poll script eval failed; aborting wait", exc_info=True)
            break
        if condition_fn(result):
            return result
        await asyncio.sleep(interval_ms / 1000.0)
    return result


async def _probe_hoverability(
    page: Any,
    trigger_cx: float,
    trigger_cy: float,
    tooltip_rect: Dict,
    appeared_probe_ids: List[str],
) -> bool:
    """Test WCAG 1.4.13 hoverability: move mouse from trigger to tooltip content.

    Moves the mouse in _HOVERABILITY_STEPS incremental steps from the trigger
    center to the tooltip center, checking whether the specific appeared tooltip
    (identified by probe IDs) is still visible at the midpoint and the endpoint.
    Checking the midpoint detects tooltips that vanish in the gap between trigger
    and content but re-appear when hovering an unrelated element at the destination.
    Returns False immediately if the tooltip disappears at any checkpoint.
    """
    t_cx = tooltip_rect["left"] + tooltip_rect["width"] / 2
    t_cy = tooltip_rect["top"] + tooltip_rect["height"] / 2
    midpoint_step = _HOVERABILITY_STEPS // 2

    for i in range(1, _HOVERABILITY_STEPS + 1):
        frac = i / _HOVERABILITY_STEPS
        x = trigger_cx + (t_cx - trigger_cx) * frac
        y = trigger_cy + (t_cy - trigger_cy) * frac
        try:
            await page.mouse.move(x, y)
        except Exception:
            logger.warning("hover_content: hoverability mouse.move failed at step %d", i, exc_info=True)
            return False
        await asyncio.sleep(0.03)

        if i == midpoint_step or i == _HOVERABILITY_STEPS:
            try:
                still_visible = await page.evaluate(PROBE_IDS_VISIBLE_SCRIPT, appeared_probe_ids)
                if not still_visible:
                    return False
            except Exception:
                logger.debug("hover_content: probe-visible check hiccup at step %d; continuing", i, exc_info=True)

    return True


async def _take_screenshot(page: Any) -> str:
    try:
        raw = await page.screenshot(full_page=False, type="jpeg", quality=55)
        return base64.b64encode(raw).decode()
    except Exception:
        logger.warning("hover_content: viewport screenshot failed", exc_info=True)
        return ""


# ── Main evaluator ────────────────────────────────────────────────────────────

async def run_hover_content_evaluator(page: Any) -> List[Dict[str, Any]]:
    # ── Phase 1: Scan ──────────────────────────────────────────────────────────
    title_elements: List[Dict] = []
    try:
        title_elements = await page.evaluate(TITLE_ELEMENTS_SCRIPT)
    except Exception as e:
        logger.warning("hover_content: TITLE_ELEMENTS_SCRIPT failed: %s: %s", type(e).__name__, e, exc_info=True)

    try:
        candidates: List[Dict] = await page.evaluate(HOVER_TRIGGERS_SCRIPT)
    except Exception as e:
        logger.warning("hover_content: HOVER_TRIGGERS_SCRIPT failed: %s: %s", type(e).__name__, e, exc_info=True)
        return [
            {
                "criterion_id": "1.4.13",
                "source": "custom:hover_content",
                "coverage_status": COVERAGE_SEMI_AUTOMATED,
                "outcome": OUTCOME_ERROR,
                "severity": "moderate",
                "message": f"Hover trigger scan failed due to a script execution error: {type(e).__name__}: {e}",
                "locator": "",
                "element_text": "",
                "metadata": {"error_type": type(e).__name__, "error_message": str(e)},
            }
        ]

    # ── Phase 2: Early exit — nothing found ───────────────────────────────────
    if not candidates and not title_elements:
        return [
            {
                "criterion_id": "1.4.13",
                "source": "custom:hover_content",
                "coverage_status": COVERAGE_SEMI_AUTOMATED,
                "outcome": OUTCOME_NOT_APPLICABLE,
                "severity": "minor",
                "message": (
                    "No hover-triggered content candidates detected via attribute scan "
                    "(aria-describedby, data-tooltip, data-tippy-content, data-bs-toggle) "
                    "or native title attributes. WCAG 1.4.13 does not appear to apply to this page."
                ),
                "locator": "",
                "element_text": "",
                "metadata": {
                    "scanned_selectors": [
                        "[aria-describedby]", "[data-tooltip]", "[data-tippy-content]",
                        '[data-bs-toggle="tooltip"]', '[data-bs-toggle="popover"]',
                    ],
                    "title_elements_found": 0,
                    "trigger_candidates_found": 0,
                },
            }
        ]

    results: List[Dict[str, Any]] = []

    # ── Phase 3: Title path — informational, no probing ───────────────────────
    if title_elements:
        results.append(
            {
                "criterion_id": "1.4.13",
                "source": "custom:hover_content",
                "coverage_status": COVERAGE_SEMI_AUTOMATED,
                "outcome": OUTCOME_NEEDS_REVIEW,
                "severity": "moderate",
                "message": (
                    f"{len(title_elements)} element(s) with native browser title tooltips found. "
                    "These are rendered by the browser chrome, not as DOM elements, and cannot be "
                    "probed for WCAG 1.4.13 dismissibility or hoverability. Manual review required."
                ),
                "locator": title_elements[0].get("locator", ""),
                "element_text": title_elements[0].get("text", ""),
                "metadata": {"title_elements": title_elements, "count": len(title_elements)},
            }
        )

    if not candidates:
        return results

    # ── Phase 4: Probe loop ───────────────────────────────────────────────────
    probe_results: List[Dict[str, Any]] = []

    for candidate in candidates[:_MAX_PROBES]:
        cx = candidate.get("centerX", 0)
        cy = candidate.get("centerY", 0)
        locator = candidate.get("locator", "")
        text = candidate.get("text", "")
        trigger = candidate.get("trigger", "")

        # Cleanup runs after every candidate regardless of how the probe exits
        # (continue, exception, or normal completion) to prevent probe ID leakage.
        try:
            try:
                snapshot_before = await page.evaluate(TOOLTIP_SNAPSHOT_SCRIPT)
            except Exception as e:
                logger.warning("hover_content: pre-hover snapshot failed for %s: %s: %s", locator, type(e).__name__, e, exc_info=True)
                probe_results.append({
                    "locator": locator, "text": text, "trigger": trigger,
                    "error": "pre_hover_snapshot_failed",
                    "error_type": type(e).__name__, "error_message": str(e),
                })
                continue

            try:
                await page.mouse.move(cx, cy)
            except Exception as e:
                logger.warning("hover_content: mouse.move failed for %s: %s: %s", locator, type(e).__name__, e, exc_info=True)
                probe_results.append({
                    "locator": locator, "text": text, "trigger": trigger,
                    "error": "mouse_move_failed",
                    "error_type": type(e).__name__, "error_message": str(e),
                })
                continue

            # Poll for tooltip appearance using snapshot diffing (not mutation counting)
            final_snap = await _poll_for_condition(
                page, TOOLTIP_SNAPSHOT_SCRIPT, None,
                condition_fn=lambda snap: bool(_diff_snapshots(snapshot_before, snap)),
                timeout_ms=_HOVER_SETTLE_MS,
            )
            appeared = _diff_snapshots(snapshot_before, final_snap or [])

            if not appeared:
                # CSS-only path or genuinely no tooltip — preserve snapshots for manual review
                probe_results.append({
                    "locator": locator, "text": text, "trigger": trigger,
                    "content_appeared": False,
                    "snapshot_before": snapshot_before,
                    "snapshot_after": final_snap,
                })
                continue

            # Extract probe IDs of the specific elements that appeared
            appeared_probe_ids = [item.get("probeId", "") for item in appeared if item.get("probeId")]

            screenshot_b64 = await _take_screenshot(page)

            # Escape dismissibility — poll until the specific appeared elements vanish
            try:
                await page.keyboard.press("Escape")
            except Exception as e:
                logger.warning("hover_content: Escape press failed for %s: %s: %s", locator, type(e).__name__, e, exc_info=True)

            if appeared_probe_ids:
                remaining_after_escape = await _poll_for_condition(
                    page, TOOLTIP_GONE_SCRIPT, appeared_probe_ids,
                    condition_fn=lambda n: n == 0,
                    timeout_ms=_ESCAPE_SETTLE_MS,
                )
                escape_dismissed = (remaining_after_escape == 0)
            else:
                escape_dismissed = False  # can't confirm without probe IDs

            # Hoverability — move mouse away, wait for full close, re-hover, then walk to tooltip
            is_hoverable: Optional[bool] = None
            if escape_dismissed and appeared_probe_ids:
                try:
                    await page.mouse.move(0, 0)
                except Exception as e:
                    logger.warning("hover_content: move-away failed for %s: %s: %s", locator, type(e).__name__, e, exc_info=True)

                # Poll for stable "fully closed" state before re-hovering.
                # Replaces the fixed 100ms sleep — handles libraries with close-delay or
                # debounce timers that may still be animating after the Escape poll passed.
                await _poll_for_condition(
                    page, TOOLTIP_GONE_SCRIPT, appeared_probe_ids,
                    condition_fn=lambda n: n == 0,
                    timeout_ms=_ESCAPE_SETTLE_MS,
                )

                try:
                    await page.mouse.move(cx, cy)
                except Exception as e:
                    logger.warning("hover_content: re-hover failed for %s: %s: %s", locator, type(e).__name__, e, exc_info=True)

                rehover_snap = await _poll_for_condition(
                    page, TOOLTIP_SNAPSHOT_SCRIPT, None,
                    condition_fn=lambda snap: bool(_diff_snapshots(snapshot_before, snap)),
                    timeout_ms=_HOVER_SETTLE_MS,
                )
                reopened = _diff_snapshots(snapshot_before, rehover_snap or [])
                if reopened:
                    tooltip_rect = reopened[0].get("rect", {})
                    if tooltip_rect.get("width", 0) > 0:
                        is_hoverable = await _probe_hoverability(
                            page, cx, cy, tooltip_rect, appeared_probe_ids
                        )

            probe_results.append({
                "locator": locator,
                "text": text,
                "trigger": trigger,
                "content_appeared": True,
                "appeared_elements": appeared,
                "escape_dismissed": escape_dismissed,
                "is_hoverable": is_hoverable,
                "screenshot_b64": screenshot_b64,
            })

        finally:
            try:
                await page.evaluate(CLEANUP_PROBE_IDS_SCRIPT)
            except Exception as e:
                logger.warning("hover_content: probe ID cleanup failed for %s: %s: %s", locator, type(e).__name__, e)

    # ── Phase 5: Decision tree ────────────────────────────────────────────────
    appeared_probes = [p for p in probe_results if p.get("content_appeared")]
    css_only_probes = [p for p in probe_results if not p.get("content_appeared") and not p.get("error")]
    error_probes = [p for p in probe_results if p.get("error")]
    not_dismissible = [p for p in appeared_probes if not p.get("escape_dismissed")]
    not_hoverable = [p for p in appeared_probes if p.get("is_hoverable") is False]
    fully_passing = [p for p in appeared_probes if p.get("escape_dismissed") and p.get("is_hoverable") is True]

    first_screenshot = next((p.get("screenshot_b64", "") for p in appeared_probes), "")

    if not_dismissible:
        first = not_dismissible[0]
        results.append({
            "criterion_id": "1.4.13",
            "source": "custom:hover_content",
            "coverage_status": COVERAGE_SEMI_AUTOMATED,
            "outcome": OUTCOME_FAILED,
            "severity": "serious",
            "message": (
                "Hover-triggered content appeared but Escape key did not dismiss it within "
                f"{_ESCAPE_SETTLE_MS}ms, violating WCAG 1.4.13 dismissibility requirement."
            ),
            "locator": first.get("locator", ""),
            "element_text": first.get("text", ""),
            "screenshot_b64": first.get("screenshot_b64", ""),
            "metadata": {
                "candidates": candidates,
                "probe_results": _strip_screenshots(probe_results),
                "not_dismissible": not_dismissible,
                "appeared_elements": first.get("appeared_elements"),
            },
        })
    elif not_hoverable:
        first = not_hoverable[0]
        results.append({
            "criterion_id": "1.4.13",
            "source": "custom:hover_content",
            "coverage_status": COVERAGE_SEMI_AUTOMATED,
            "outcome": OUTCOME_FAILED,
            "severity": "serious",
            "message": (
                "Tooltip disappeared while moving the pointer incrementally from the trigger "
                "element toward the tooltip content, violating WCAG 1.4.13 hoverability requirement."
            ),
            "locator": first.get("locator", ""),
            "element_text": first.get("text", ""),
            "screenshot_b64": first.get("screenshot_b64", ""),
            "metadata": {
                "candidates": candidates,
                "probe_results": _strip_screenshots(probe_results),
                "not_hoverable": not_hoverable,
            },
        })
    elif fully_passing:
        first = fully_passing[0]
        results.append({
            "criterion_id": "1.4.13",
            "source": "custom:hover_content",
            "coverage_status": COVERAGE_SEMI_AUTOMATED,
            "outcome": OUTCOME_PASSED,
            "severity": "minor",
            "message": (
                f"Hover-triggered content appeared, was dismissed by Escape within {_ESCAPE_SETTLE_MS}ms, "
                "and remained visible while moving the pointer from trigger to tooltip. "
                "WCAG 1.4.13 dismissibility and hoverability satisfied for sampled elements."
            ),
            "locator": first.get("locator", ""),
            "element_text": first.get("text", ""),
            "screenshot_b64": first.get("screenshot_b64", ""),
            "metadata": {
                "candidates": candidates,
                "probe_results": _strip_screenshots(probe_results),
                "fully_passing": fully_passing,
            },
        })
    elif appeared_probes:
        # Tooltip appeared and was dismissed but hoverability couldn't be confirmed
        first = appeared_probes[0]
        results.append({
            "criterion_id": "1.4.13",
            "source": "custom:hover_content",
            "coverage_status": COVERAGE_SEMI_AUTOMATED,
            "outcome": OUTCOME_NEEDS_REVIEW,
            "severity": "moderate",
            "message": (
                "Hover-triggered content appeared and Escape dismissed it, but hoverability "
                "(pointer can move from trigger to content without it disappearing) could not "
                "be confirmed automatically because the tooltip did not reopen for re-probing. "
                "Manual verification of hoverability required."
            ),
            "locator": first.get("locator", ""),
            "element_text": first.get("text", ""),
            "screenshot_b64": first_screenshot,
            "metadata": {
                "candidates": candidates,
                "probe_results": _strip_screenshots(probe_results),
                "appeared_probes": appeared_probes,
            },
        })
    elif css_only_probes:
        first_candidate = candidates[0] if candidates else {}
        results.append({
            "criterion_id": "1.4.13",
            "source": "custom:hover_content",
            "coverage_status": COVERAGE_SEMI_AUTOMATED,
            "outcome": OUTCOME_NEEDS_REVIEW,
            "severity": "moderate",
            "message": (
                "Hover trigger candidates were found but tooltip visibility changes were not "
                "detected via DOM snapshot diffing (display, visibility, opacity, bounding rect). "
                "CSS-only transitions that do not alter computed visibility properties, or tooltips "
                "inside shadow DOM, may be in use. Manual review required."
            ),
            "locator": first_candidate.get("locator", ""),
            "element_text": first_candidate.get("text", ""),
            "metadata": {
                "candidates": candidates,
                "probe_results": probe_results,
                "snapshot_before_samples": [p.get("snapshot_before") for p in css_only_probes],
                "snapshot_after_samples": [p.get("snapshot_after") for p in css_only_probes],
            },
        })
    elif error_probes:
        # All probes failed due to execution errors — prevent silent empty return
        results.append({
            "criterion_id": "1.4.13",
            "source": "custom:hover_content",
            "coverage_status": COVERAGE_SEMI_AUTOMATED,
            "outcome": OUTCOME_ERROR,
            "severity": "moderate",
            "message": (
                f"All {len(error_probes)} probe(s) encountered execution errors. "
                "No hover interaction data could be collected."
            ),
            "locator": error_probes[0].get("locator", ""),
            "element_text": error_probes[0].get("text", ""),
            "metadata": {"probe_results": error_probes},
        })

    return results


def _strip_screenshots(probe_results: List[Dict]) -> List[Dict]:
    """Return probe_results with screenshot_b64 removed to avoid duplicating large blobs in metadata."""
    return [{k: v for k, v in p.items() if k != "screenshot_b64"} for p in probe_results]
