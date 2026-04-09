from __future__ import annotations

import asyncio
from typing import Any, Dict, List

from ..models import COVERAGE_SEMI_AUTOMATED, OUTCOME_FAILED, OUTCOME_NEEDS_REVIEW


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
    const classes = Array.from(el.classList || []).slice(0, 3).join('.');
    return el.tagName.toLowerCase() + (classes ? '.' + classes : '');
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
    const classes = Array.from(el.classList || []).slice(0, 3).join('.');
    return el.tagName.toLowerCase() + (classes ? '.' + classes : '');
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
    const classes = Array.from(el.classList || []).slice(0, 3).join('.');
    return el.tagName.toLowerCase() + (classes ? '.' + classes : '');
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
  .slice(0, 12)
  .map(el => {
    const classes = Array.from(el.classList || []).slice(0, 3).join('.');
    return {
      locator: el.id ? '#' + el.id : el.tagName.toLowerCase() + (classes ? '.' + classes : ''),
      text: (el.innerText || el.textContent || '').trim().slice(0, 120),
      tag: el.tagName.toLowerCase(),
    };
  })
"""


async def run_keyboard_smoke_evaluator(page: Any) -> List[Dict[str, Any]]:
    interactives: List[Dict[str, Any]] = await page.evaluate(VISIBLE_INTERACTIVES_SCRIPT, INTERACTIVE_SELECTOR)
    if not interactives:
        return []

    forward_trace: List[Dict[str, Any]] = []
    reverse_trace: List[Dict[str, Any]] = []
    unique_locators = set()
    lost_focus = False
    tab_attempts = min(max(len(interactives) + 1, 4), 12)

    for _ in range(tab_attempts):
        await page.keyboard.press("Tab")
        active = await page.evaluate(ACTIVE_ELEMENT_SCRIPT)
        if not active:
            lost_focus = True
            forward_trace.append({"locator": "", "tag": "", "text": ""})
            continue
        forward_trace.append(active)
        locator = active.get("locator") or ""
        if locator:
            unique_locators.add(locator)
        if active.get("tag") in {"body", "html"}:
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

    if lost_focus or len(unique_locators) == 0 or reverse_stuck_count >= max(2, reverse_attempts - 1):
        message = "Keyboard smoke test lost meaningful focus while tabbing through visible interactive elements."
        severity = "serious"
        outcome = OUTCOME_FAILED
    elif activation_failed:
        message = "Keyboard smoke focused a safe interactive element but Enter and Space did not produce an observable activation signal."
        severity = "serious"
        outcome = OUTCOME_FAILED
    else:
        message = "Keyboard smoke reached {} distinct focus targets and sampled reverse tab behavior; full task-flow review is still required.".format(len(unique_locators))
        severity = "moderate"
        outcome = OUTCOME_NEEDS_REVIEW

    first_locator = forward_trace[0].get("locator") if forward_trace and forward_trace[0] else ""
    first_text = forward_trace[0].get("text") if forward_trace and forward_trace[0] else ""

    results: List[Dict[str, Any]] = []
    for criterion_id in ["2.1.1", "2.1.2", "2.4.3"]:
        results.append(
            {
                "criterion_id": criterion_id,
                "source": "custom:keyboard_smoke",
                "coverage_status": COVERAGE_SEMI_AUTOMATED,
                "outcome": outcome,
                "severity": severity,
                "message": message,
                "locator": first_locator or interactives[0].get("locator", ""),
                "element_text": first_text or interactives[0].get("text", ""),
                "metadata": base_metadata,
            }
        )

    focus_visible_message = (
        "Keyboard smoke captured focus transitions; visual focus treatment still requires reviewer confirmation."
    )
    results.append(
        {
            "criterion_id": "2.4.7",
            "source": "custom:keyboard_smoke",
            "coverage_status": COVERAGE_SEMI_AUTOMATED,
            "outcome": OUTCOME_NEEDS_REVIEW,
            "severity": "moderate",
            "message": focus_visible_message,
            "locator": first_locator or interactives[0].get("locator", ""),
            "element_text": first_text or interactives[0].get("text", ""),
            "metadata": base_metadata,
        }
    )
    return results
