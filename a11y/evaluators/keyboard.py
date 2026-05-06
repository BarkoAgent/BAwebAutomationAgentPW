from __future__ import annotations

import asyncio
from typing import Any, Dict, List

from ..models import COVERAGE_SEMI_AUTOMATED, OUTCOME_FAILED, OUTCOME_NEEDS_REVIEW, OUTCOME_PASSED


INTERACTIVE_SELECTOR = (
    "a[href], button, input:not([type='hidden']), select, textarea, "
    "[tabindex]:not([tabindex='-1']), [role='button'], [role='link'], "
    "[role='menuitem'], [contenteditable='true']"
)


ACTIVE_ELEMENT_SCRIPT = """
() => {
  function cssPath(el) {
    if (!el || el.nodeType !== 1) return '';
    if (el.id) return '#' + el.id;
    const tag = el.tagName.toLowerCase();
    const ariaLabel = (el.getAttribute && el.getAttribute('aria-label')) || '';
    if (ariaLabel) return tag + '[aria-label="' + ariaLabel.slice(0, 40).replace(/"/g, '\\"') + '"]';
    const name = (el.getAttribute && el.getAttribute('name')) || '';
    if (name) return tag + '[name="' + name + '"]';
    // Skip class names for SVG / SVG-child elements — Tailwind/util classes are style noise.
    const isSvgish = tag === 'svg' || tag === 'path' || tag === 'g' || tag === 'use' ||
      (el.ownerSVGElement != null);
    if (isSvgish) {
      const parent = el.parentElement;
      if (parent) {
        const idx = Array.prototype.indexOf.call(parent.children, el);
        return tag + ':nth-child(' + (idx + 1) + ')';
      }
      return tag;
    }
    const classes = Array.from(el.classList || []).slice(0, 3).join('.');
    return tag + (classes ? '.' + classes : '');
  }
  const el = document.activeElement;
  if (!el) return null;
  const style = window.getComputedStyle(el);
  return {
    tag: el.tagName.toLowerCase(),
    text: (el.innerText || el.textContent || '').trim().slice(0, 120),
    locator: cssPath(el),
    outlineStyle: style.outlineStyle,
    outlineWidth: style.outlineWidth,
    boxShadow: style.boxShadow,
  };
}
"""

ACTIVATION_MONITOR_SCRIPT = """
() => {
  function cssPath(el) {
    if (!el || el.nodeType !== 1) return '';
    if (el.id) return '#' + el.id;
    const tag = el.tagName.toLowerCase();
    const ariaLabel = (el.getAttribute && el.getAttribute('aria-label')) || '';
    if (ariaLabel) return tag + '[aria-label="' + ariaLabel.slice(0, 40).replace(/"/g, '\\"') + '"]';
    const name = (el.getAttribute && el.getAttribute('name')) || '';
    if (name) return tag + '[name="' + name + '"]';
    // Skip class names for SVG / SVG-child elements — Tailwind/util classes are style noise.
    const isSvgish = tag === 'svg' || tag === 'path' || tag === 'g' || tag === 'use' ||
      (el.ownerSVGElement != null);
    if (isSvgish) {
      const parent = el.parentElement;
      if (parent) {
        const idx = Array.prototype.indexOf.call(parent.children, el);
        return tag + ':nth-child(' + (idx + 1) + ')';
      }
      return tag;
    }
    const classes = Array.from(el.classList || []).slice(0, 3).join('.');
    return tag + (classes ? '.' + classes : '');
  }
  const el = document.activeElement;
  if (!el) return null;
  window.__a11yKeyboardProbe = {
    clickCount: 0,
    changeCount: 0,
    inputCount: 0,
    beforeLocator: cssPath(el),
    beforeHash: window.location.hash,
    beforeExpanded: el.getAttribute('aria-expanded'),
  };
  const tracker = window.__a11yKeyboardProbe;
  el.addEventListener('click', () => { tracker.clickCount += 1; }, { once: false });
  el.addEventListener('change', () => { tracker.changeCount += 1; }, { once: false });
  el.addEventListener('input', () => { tracker.inputCount += 1; }, { once: false });
  return {
    locator: tracker.beforeLocator,
    tag: el.tagName.toLowerCase(),
    role: el.getAttribute('role') || '',
    type: el.getAttribute('type') || '',
    href: el.getAttribute('href') || '',
    text: (el.innerText || el.textContent || '').trim().slice(0, 120),
  };
}
"""

ACTIVATION_RESULT_SCRIPT = """
() => {
  function cssPath(el) {
    if (!el || el.nodeType !== 1) return '';
    if (el.id) return '#' + el.id;
    const tag = el.tagName.toLowerCase();
    const ariaLabel = (el.getAttribute && el.getAttribute('aria-label')) || '';
    if (ariaLabel) return tag + '[aria-label="' + ariaLabel.slice(0, 40).replace(/"/g, '\\"') + '"]';
    const name = (el.getAttribute && el.getAttribute('name')) || '';
    if (name) return tag + '[name="' + name + '"]';
    // Skip class names for SVG / SVG-child elements — Tailwind/util classes are style noise.
    const isSvgish = tag === 'svg' || tag === 'path' || tag === 'g' || tag === 'use' ||
      (el.ownerSVGElement != null);
    if (isSvgish) {
      const parent = el.parentElement;
      if (parent) {
        const idx = Array.prototype.indexOf.call(parent.children, el);
        return tag + ':nth-child(' + (idx + 1) + ')';
      }
      return tag;
    }
    const classes = Array.from(el.classList || []).slice(0, 3).join('.');
    return tag + (classes ? '.' + classes : '');
  }
  const tracker = window.__a11yKeyboardProbe || {};
  const el = document.activeElement;
  return {
    clickCount: tracker.clickCount || 0,
    changeCount: tracker.changeCount || 0,
    inputCount: tracker.inputCount || 0,
    beforeLocator: tracker.beforeLocator || '',
    beforeHash: tracker.beforeHash || '',
    beforeExpanded: tracker.beforeExpanded,
    afterLocator: el ? cssPath(el) : '',
    afterHash: window.location.hash,
    afterExpanded: el ? el.getAttribute('aria-expanded') : null,
  };
}
"""

VISIBLE_INTERACTIVES_SCRIPT = """
selector => Array.from(document.querySelectorAll(selector))
  .filter(el => {
    const style = window.getComputedStyle(el);
    const rect = el.getBoundingClientRect();
    return style.visibility !== 'hidden' &&
      style.display !== 'none' &&
      rect.width > 0 &&
      rect.height > 0;
  })
  .slice(0, 25)
  .map(el => {
    const tag = el.tagName.toLowerCase();
    const ariaLabel = (el.getAttribute('aria-label') || '');
    const name = (el.getAttribute('name') || '');
    const isSvgish = tag === 'svg' || el.ownerSVGElement != null;
    let locator;
    if (el.id) locator = '#' + el.id;
    else if (ariaLabel) locator = tag + '[aria-label="' + ariaLabel.slice(0, 40).replace(/"/g, '\\"') + '"]';
    else if (name) locator = tag + '[name="' + name + '"]';
    else if (isSvgish) locator = tag;
    else {
      const classes = Array.from(el.classList || []).slice(0, 3).join('.');
      locator = tag + (classes ? '.' + classes : '');
    }
    return {
      locator: locator,
      text: (el.innerText || el.textContent || '').trim().slice(0, 120),
      tag: tag,
    };
  })
"""


def _detect_cycle(seq: List[str], min_cycle: int = 2, max_cycle: int = 4) -> bool:
    """Return True if seq ends in a repeated cycle — indicates a focus trap."""
    for length in range(min_cycle, max_cycle + 1):
        if len(seq) >= length * 2:
            if seq[-length:] == seq[-(length * 2):-length]:
                return True
    return False


def _check_focus_visible(forward_trace: List[Dict[str, Any]]) -> List[str]:
    """Return locators of focused elements with no detectable outline or box-shadow."""
    failures = []
    for entry in forward_trace:
        if not entry or not entry.get("locator"):
            continue
        outline = entry.get("outlineStyle", "")
        outline_w = entry.get("outlineWidth", "")
        shadow = entry.get("boxShadow", "")
        no_outline = outline in ("none", "") or outline_w in ("0px", "0", "")
        no_shadow = shadow in ("none", "", "0px 0px 0px 0px")
        if no_outline and no_shadow:
            failures.append(entry["locator"])
    return failures


async def run_keyboard_smoke_evaluator(page: Any) -> List[Dict[str, Any]]:
    interactives: List[Dict[str, Any]] = await page.evaluate(VISIBLE_INTERACTIVES_SCRIPT, INTERACTIVE_SELECTOR)
    if not interactives:
        return []

    forward_trace: List[Dict[str, Any]] = []
    reverse_trace: List[Dict[str, Any]] = []
    unique_locators = set()
    lost_focus = False
    lost_focus_reason = ""   # "null_active" | "body" | "html"
    lost_focus_at_index = -1
    # Larger budget when many interactive elements visible — small Tab caps cause
    # false-trap reports on pages with deep menus / hamburger flyouts.
    tab_attempts = min(max(len(interactives) + 2, 8), 25)

    for i in range(tab_attempts):
        await page.keyboard.press("Tab")
        active = await page.evaluate(ACTIVE_ELEMENT_SCRIPT)
        if not active:
            if not lost_focus:
                lost_focus_reason = "null_active"
                lost_focus_at_index = i
            lost_focus = True
            forward_trace.append({"locator": "", "tag": "", "text": ""})
            continue
        forward_trace.append(active)
        locator = active.get("locator") or ""
        if locator:
            unique_locators.add(locator)
        if active.get("tag") in {"body", "html"}:
            if not lost_focus:
                lost_focus_reason = active.get("tag", "body")
                lost_focus_at_index = i
            lost_focus = True

    reverse_attempts = min(4, len(forward_trace))
    reverse_stuck_count = 0
    previous_locator = forward_trace[-1].get("locator", "") if forward_trace else ""
    for _ in range(reverse_attempts):
        await page.keyboard.press("Shift+Tab")
        active = await page.evaluate(ACTIVE_ELEMENT_SCRIPT)
        if not active:
            reverse_trace.append({"locator": "", "tag": "", "text": ""})
            continue
        reverse_trace.append(active)
        locator = active.get("locator") or ""
        if locator == previous_locator and locator:
            reverse_stuck_count += 1
        previous_locator = locator

    activation_probe = None
    activation_result = None
    await page.keyboard.press("Tab")
    activation_probe = await page.evaluate(ACTIVATION_MONITOR_SCRIPT)
    if activation_probe:
        tag = activation_probe.get("tag")
        role = activation_probe.get("role")
        href = activation_probe.get("href", "")
        input_type = activation_probe.get("type", "")
        is_safe_probe = (
            tag == "button" or
            role == "button" or
            (tag == "a" and (href.startswith("#") or href.startswith("javascript:") or href == "")) or
            (tag == "input" and input_type in {"button", "submit", "checkbox", "radio"})
        )
        if is_safe_probe:
            await page.keyboard.press("Enter")
            await asyncio.sleep(0.1)
            if tag in {"button", "input"} or role == "button":
                await page.keyboard.press("Space")
                await asyncio.sleep(0.1)
            activation_result = await page.evaluate(ACTIVATION_RESULT_SCRIPT)

    base_metadata = {
        "interactive_sample_count": len(interactives),
        "forward_tab_trace": forward_trace,
        "reverse_tab_trace": reverse_trace,
        "unique_focus_count": len(unique_locators),
        "reverse_stuck_count": reverse_stuck_count,
        "lost_focus_reason": lost_focus_reason,
        "lost_focus_at_tab_press": lost_focus_at_index + 1 if lost_focus_at_index >= 0 else None,
        "activation_probe": activation_probe,
        "activation_result": activation_result,
    }

    activation_failed = False
    if activation_result:
        activation_failed = (
            activation_result.get("clickCount", 0) == 0 and
            activation_result.get("changeCount", 0) == 0 and
            activation_result.get("inputCount", 0) == 0 and
            activation_result.get("beforeHash") == activation_result.get("afterHash") and
            activation_result.get("beforeExpanded") == activation_result.get("afterExpanded") and
            activation_result.get("beforeLocator") == activation_result.get("afterLocator")
        )

    # --- Derived signals ---
    locator_sequence = [f.get("locator", "") for f in forward_trace if f.get("locator")]
    focus_trapped = _detect_cycle(locator_sequence)
    reachability_ratio = len(unique_locators) / max(len(interactives), 1)
    focus_visible_failures = _check_focus_visible(forward_trace)

    first_locator = forward_trace[0].get("locator") if forward_trace and forward_trace[0] else ""
    first_text = forward_trace[0].get("text") if forward_trace and forward_trace[0] else ""
    anchor_locator = first_locator or interactives[0].get("locator", "")
    anchor_text = first_text or interactives[0].get("text", "")

    results: List[Dict[str, Any]] = []

    # --- 2.1.1 Keyboard ---
    if lost_focus or len(unique_locators) == 0:
        outcome_211 = OUTCOME_FAILED
        severity_211 = "serious"
        unique_count = len(unique_locators)
        total_count = len(interactives)
        visited_sample = ", ".join(list(unique_locators)[:4]) or "none"
        if unique_count == 0:
            message_211 = (
                "Tab key produced no focus movement — none of {} visible interactive "
                "elements were reachable by keyboard.".format(total_count)
            )
        elif lost_focus_reason == "null_active":
            message_211 = (
                "Focus disappeared at Tab press {} (document.activeElement became null) — "
                "keyboard navigation broke mid-flow. "
                "{} of {} interactive elements reached before failure. "
                "Visited: {}.".format(
                    lost_focus_at_index + 1, unique_count, total_count, visited_sample
                )
            )
        elif lost_focus_reason in ("body", "html"):
            message_211 = (
                "Focus escaped to <{}> at Tab press {} — "
                "the Tab key left the page's interactive content instead of moving to the next element. "
                "{} of {} interactive elements reached before escape. "
                "Visited: {}.".format(
                    lost_focus_reason, lost_focus_at_index + 1,
                    unique_count, total_count, visited_sample
                )
            )
        else:
            message_211 = (
                "Keyboard navigation lost focus unexpectedly. "
                "{} of {} interactive elements reached. "
                "Visited: {}.".format(unique_count, total_count, visited_sample)
            )
    elif activation_failed:
        outcome_211 = OUTCOME_FAILED
        severity_211 = "serious"
        message_211 = "Keyboard smoke focused a safe interactive element but Enter and Space did not produce an observable activation signal."
    elif reachability_ratio >= 0.5 and activation_result:
        outcome_211 = OUTCOME_PASSED
        severity_211 = ""
        message_211 = "Keyboard reached {} of {} sampled interactive targets and activation was confirmed via Enter/Space.".format(
            len(unique_locators), len(interactives)
        )
    elif reachability_ratio >= 0.5:
        # Reached enough targets but no safe probe element — partial automation only.
        outcome_211 = OUTCOME_NEEDS_REVIEW
        severity_211 = "moderate"
        message_211 = (
            "Keyboard reached {} of {} sampled interactive targets but no safe element was available to "
            "probe Enter/Space activation — pass cannot be confirmed automatically; manual review required.".format(
                len(unique_locators), len(interactives)
            )
        )
    else:
        outcome_211 = OUTCOME_NEEDS_REVIEW
        severity_211 = "moderate"
        message_211 = "Keyboard reached only {} of {} sampled interactive targets within the Tab budget — coverage insufficient for automated pass; manual review required.".format(
            len(unique_locators), len(interactives)
        )

    results.append({
        "criterion_id": "2.1.1",
        "source": "custom:keyboard_smoke",
        "coverage_status": COVERAGE_SEMI_AUTOMATED,
        "outcome": outcome_211,
        "severity": severity_211,
        "message": message_211,
        "locator": anchor_locator,
        "element_text": anchor_text,
        "metadata": base_metadata,
    })

    # --- 2.1.2 No Keyboard Trap ---
    if focus_trapped:
        outcome_212 = OUTCOME_FAILED
        severity_212 = "serious"
        message_212 = "Focus trap detected: tab order cycled through the same element sequence repeatedly."
    elif reverse_stuck_count >= max(2, reverse_attempts - 1):
        outcome_212 = OUTCOME_FAILED
        severity_212 = "serious"
        message_212 = "Reverse tab (Shift+Tab) repeatedly landed on the same element — possible keyboard trap."
    elif reverse_attempts > 0 and len(unique_locators) > 1:
        outcome_212 = OUTCOME_PASSED
        severity_212 = ""
        message_212 = "Reverse tab navigated freely with no repeated cycle or stuck focus detected."
    else:
        outcome_212 = OUTCOME_NEEDS_REVIEW
        severity_212 = "moderate"
        message_212 = "Insufficient reverse tab sample to confirm absence of keyboard traps."

    results.append({
        "criterion_id": "2.1.2",
        "source": "custom:keyboard_smoke",
        "coverage_status": COVERAGE_SEMI_AUTOMATED,
        "outcome": outcome_212,
        "severity": severity_212,
        "message": message_212,
        "locator": anchor_locator,
        "element_text": anchor_text,
        "metadata": base_metadata,
    })

    # --- 2.4.3 Focus Order ---
    # Logical sequence requires human judgement; report observed coverage for context
    if len(unique_locators) >= 3:
        message_243 = "Focus visited {} unique targets in sequence; logical order requires manual verification of meaningful tab flow.".format(
            len(unique_locators)
        )
    else:
        message_243 = "Too few focus targets sampled ({} unique) to assess order; manual review required.".format(
            len(unique_locators)
        )

    results.append({
        "criterion_id": "2.4.3",
        "source": "custom:keyboard_smoke",
        "coverage_status": COVERAGE_SEMI_AUTOMATED,
        "outcome": OUTCOME_NEEDS_REVIEW,
        "severity": "moderate",
        "message": message_243,
        "locator": anchor_locator,
        "element_text": anchor_text,
        "metadata": base_metadata,
    })

    # --- 2.4.7 Focus Visible ---
    visible_entries = [e for e in forward_trace if e and e.get("locator")]
    if focus_visible_failures:
        focus_visible_outcome = OUTCOME_FAILED
        focus_visible_severity = "serious"
        focus_visible_message = "{} of {} focused elements showed no detectable outline or box-shadow: {}".format(
            len(focus_visible_failures),
            len(visible_entries),
            ", ".join(focus_visible_failures[:5]),
        )
    else:
        focus_visible_outcome = OUTCOME_NEEDS_REVIEW
        focus_visible_severity = "moderate"
        focus_visible_message = (
            "CSS outline and box-shadow appear present on sampled focus targets; "
            "visual confirmation still required."
        )

    results.append({
        "criterion_id": "2.4.7",
        "source": "custom:keyboard_smoke",
        "coverage_status": COVERAGE_SEMI_AUTOMATED,
        "outcome": focus_visible_outcome,
        "severity": focus_visible_severity,
        "message": focus_visible_message,
        "locator": anchor_locator,
        "element_text": anchor_text,
        "metadata": base_metadata,
    })

    return results
