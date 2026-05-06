from __future__ import annotations

import asyncio
from typing import Any, Dict, List

from ..models import COVERAGE_SEMI_AUTOMATED, OUTCOME_FAILED, OUTCOME_NEEDS_REVIEW, OUTCOME_PASSED


# WCAG 3.2.1: On Focus — receiving focus must not initiate a change of context
# WCAG 3.2.2: On Input — changing a UI component setting must not automatically
#              cause a change of context unless user is advised beforehand.
#
# Change of context = major change in content (new page, dialog, reorder, etc.)
# We detect: unexpected navigation, popup windows, URL hash changes on focus/input.

SETUP_MONITOR_SCRIPT = """
() => {
  window.__a11yCtxChange = {
    popupCount: 0,
    navigationCount: 0,
    hashBefore: window.location.hash,
    hrefBefore: window.location.href,
    dialogOpened: 0,
    unexpectedFocusChange: 0,
  };

  // Track window.open
  const origOpen = window.open;
  window.open = function(...args) {
    window.__a11yCtxChange.popupCount += 1;
    return origOpen.apply(this, args);
  };

  // Track dialog elements opening
  const dialogObserver = new MutationObserver(mutations => {
    for (const m of mutations) {
      for (const node of m.addedNodes) {
        if (node.nodeType === 1) {
          if (node.tagName === 'DIALOG' || node.getAttribute?.('role') === 'dialog' || node.getAttribute?.('role') === 'alertdialog') {
            window.__a11yCtxChange.dialogOpened += 1;
          }
        }
      }
      // Also check attribute changes (e.g. aria-hidden removed from modal)
      if (m.type === 'attributes' && m.attributeName === 'aria-hidden') {
        const el = m.target;
        if ((el.getAttribute('role') === 'dialog' || el.tagName === 'DIALOG') && el.getAttribute('aria-hidden') === 'false') {
          window.__a11yCtxChange.dialogOpened += 1;
        }
      }
    }
  });
  dialogObserver.observe(document.body, { childList: true, subtree: true, attributes: true, attributeFilter: ['aria-hidden'] });
  window.__a11yCtxDialogObserver = dialogObserver;
}
"""

READ_MONITOR_SCRIPT = """
() => {
  const ctx = window.__a11yCtxChange || {};
  if (window.__a11yCtxDialogObserver) {
    window.__a11yCtxDialogObserver.disconnect();
    delete window.__a11yCtxDialogObserver;
  }
  return {
    popupCount: ctx.popupCount || 0,
    hashBefore: ctx.hashBefore || '',
    hrefBefore: ctx.hrefBefore || '',
    hashAfter: window.location.hash,
    hrefAfter: window.location.href,
    dialogOpened: ctx.dialogOpened || 0,
    navigated: ctx.hrefBefore !== window.location.href,
  };
}
"""

FOCUSABLE_INPUTS_SCRIPT = """
() => {
  function cssPath(el) {
    if (!el || el.nodeType !== 1) return '';
    if (el.id) return '#' + el.id;
    const classes = Array.from(el.classList || []).slice(0, 3).join('.');
    return el.tagName.toLowerCase() + (classes ? '.' + classes : '');
  }

  // Focus-change candidates: links and non-submit buttons (3.2.1)
  const focusCandidates = Array.from(document.querySelectorAll(
    'a[href], button:not([type="submit"]):not([type="button"])'
  ))
  .filter(el => {
    const style = window.getComputedStyle(el);
    const rect = el.getBoundingClientRect();
    return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0;
  })
  .slice(0, 6)
  .map(el => ({
    locator: cssPath(el),
    text: (el.innerText || el.textContent || '').trim().slice(0, 80),
    tag: el.tagName.toLowerCase(),
  }));

  // Input-change candidates: selects and checkboxes (3.2.2)
  const inputCandidates = Array.from(document.querySelectorAll(
    'select, input[type="checkbox"], input[type="radio"]'
  ))
  .filter(el => {
    const style = window.getComputedStyle(el);
    const rect = el.getBoundingClientRect();
    return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0;
  })
  .slice(0, 4)
  .map(el => ({
    locator: cssPath(el),
    text: (el.getAttribute('aria-label') || el.getAttribute('name') || el.id || '').slice(0, 80),
    tag: el.tagName.toLowerCase(),
    type: el.getAttribute('type') || '',
  }));

  return { focusCandidates, inputCandidates };
}
"""


async def run_context_change_evaluator(page: Any) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []

    try:
        candidates = await page.evaluate(FOCUSABLE_INPUTS_SCRIPT)
    except Exception:
        candidates = {"focusCandidates": [], "inputCandidates": []}

    focus_candidates = candidates.get("focusCandidates", [])
    input_candidates = candidates.get("inputCandidates", [])

    focus_violations: List[Dict] = []
    input_violations: List[Dict] = []

    # --- 3.2.1: Test focus on links/buttons ---
    for candidate in focus_candidates[:4]:
        try:
            await page.evaluate(SETUP_MONITOR_SCRIPT)
            initial_href = await page.evaluate("() => window.location.href")

            # Focus via Tab until we reach this element or just click focus
            await page.focus(candidate["locator"])
            await asyncio.sleep(0.3)

            monitor = await page.evaluate(READ_MONITOR_SCRIPT)

            href_changed = monitor.get("hrefAfter", "") != initial_href
            hash_changed = monitor.get("hashAfter", "") != monitor.get("hashBefore", "")
            popup_opened = monitor.get("popupCount", 0) > 0
            dialog_opened = monitor.get("dialogOpened", 0) > 0

            if href_changed or popup_opened:
                focus_violations.append(
                    {
                        "locator": candidate["locator"],
                        "text": candidate["text"],
                        "href_changed": href_changed,
                        "popup_opened": popup_opened,
                        "dialog_opened": dialog_opened,
                        "hash_changed": hash_changed,
                        "monitor": monitor,
                    }
                )

                # Navigate back if we left the page
                if href_changed:
                    try:
                        await page.go_back()
                        await asyncio.sleep(0.5)
                    except Exception:
                        pass
        except Exception:
            pass

    # --- 3.2.2: Test input change on selects/checkboxes ---
    for candidate in input_candidates[:3]:
        try:
            await page.evaluate(SETUP_MONITOR_SCRIPT)
            initial_href = await page.evaluate("() => window.location.href")

            tag = candidate.get("tag", "")
            input_type = candidate.get("type", "")

            if tag == "select":
                # Change selection to the next option
                await page.evaluate(
                    """(locator) => {
                        const el = document.querySelector(locator);
                        if (el && el.options.length > 1) {
                            el.selectedIndex = el.selectedIndex === 0 ? 1 : 0;
                            el.dispatchEvent(new Event('change', { bubbles: true }));
                        }
                    }""",
                    candidate["locator"],
                )
            elif input_type in ("checkbox", "radio"):
                await page.evaluate(
                    """(locator) => {
                        const el = document.querySelector(locator);
                        if (el) {
                            el.checked = !el.checked;
                            el.dispatchEvent(new Event('change', { bubbles: true }));
                        }
                    }""",
                    candidate["locator"],
                )

            await asyncio.sleep(0.4)
            monitor = await page.evaluate(READ_MONITOR_SCRIPT)

            href_changed = monitor.get("hrefAfter", "") != initial_href
            popup_opened = monitor.get("popupCount", 0) > 0

            if href_changed or popup_opened:
                input_violations.append(
                    {
                        "locator": candidate["locator"],
                        "text": candidate["text"],
                        "tag": tag,
                        "type": input_type,
                        "href_changed": href_changed,
                        "popup_opened": popup_opened,
                        "monitor": monitor,
                    }
                )

                if href_changed:
                    try:
                        await page.go_back()
                        await asyncio.sleep(0.5)
                    except Exception:
                        pass
        except Exception:
            pass

    # --- Report 3.2.1 ---
    if focus_violations:
        first = focus_violations[0]
        results.append(
            {
                "criterion_id": "3.2.1",
                "source": "custom:context_change",
                "coverage_status": COVERAGE_SEMI_AUTOMATED,
                "outcome": OUTCOME_FAILED,
                "severity": "serious",
                "message": (
                    "Focusing element '{}' caused an unexpected context change "
                    "(navigation: {}, popup: {}), violating WCAG 3.2.1.".format(
                        first.get("locator", ""),
                        first.get("href_changed", False),
                        first.get("popup_opened", False),
                    )
                ),
                "locator": first.get("locator", ""),
                "element_text": first.get("text", ""),
                "metadata": {
                    "focus_violations": focus_violations,
                    "candidates_tested": focus_candidates,
                },
            }
        )
    elif focus_candidates:
        results.append(
            {
                "criterion_id": "3.2.1",
                "source": "custom:context_change",
                "coverage_status": COVERAGE_SEMI_AUTOMATED,
                "outcome": OUTCOME_NEEDS_REVIEW,
                "severity": "moderate",
                "message": (
                    "No unexpected context changes detected when focusing {} sampled "
                    "interactive elements. Full review of focus behavior across all "
                    "interactive elements is still recommended.".format(len(focus_candidates))
                ),
                "locator": focus_candidates[0].get("locator", "") if focus_candidates else "",
                "element_text": focus_candidates[0].get("text", "") if focus_candidates else "",
                "metadata": {"candidates_tested": focus_candidates},
            }
        )
    else:
        results.append(
            {
                "criterion_id": "3.2.1",
                "source": "custom:context_change",
                "coverage_status": COVERAGE_SEMI_AUTOMATED,
                "outcome": OUTCOME_NEEDS_REVIEW,
                "severity": "moderate",
                "message": "No focusable link or button candidates found to probe for unexpected context changes.",
                "locator": "",
                "element_text": "",
                "metadata": {},
            }
        )

    # --- Report 3.2.2 ---
    if input_violations:
        first = input_violations[0]
        results.append(
            {
                "criterion_id": "3.2.2",
                "source": "custom:context_change",
                "coverage_status": COVERAGE_SEMI_AUTOMATED,
                "outcome": OUTCOME_FAILED,
                "severity": "serious",
                "message": (
                    "Changing '{}' ({}) caused an unexpected context change "
                    "(navigation: {}, popup: {}), violating WCAG 3.2.2.".format(
                        first.get("locator", ""),
                        first.get("tag", ""),
                        first.get("href_changed", False),
                        first.get("popup_opened", False),
                    )
                ),
                "locator": first.get("locator", ""),
                "element_text": first.get("text", ""),
                "metadata": {
                    "input_violations": input_violations,
                    "candidates_tested": input_candidates,
                },
            }
        )
    elif input_candidates:
        results.append(
            {
                "criterion_id": "3.2.2",
                "source": "custom:context_change",
                "coverage_status": COVERAGE_SEMI_AUTOMATED,
                "outcome": OUTCOME_NEEDS_REVIEW,
                "severity": "moderate",
                "message": (
                    "No unexpected context changes detected when modifying {} sampled "
                    "input controls (select, checkbox, radio). Full review still recommended.".format(
                        len(input_candidates)
                    )
                ),
                "locator": input_candidates[0].get("locator", "") if input_candidates else "",
                "element_text": input_candidates[0].get("text", "") if input_candidates else "",
                "metadata": {"candidates_tested": input_candidates},
            }
        )
    else:
        results.append(
            {
                "criterion_id": "3.2.2",
                "source": "custom:context_change",
                "coverage_status": COVERAGE_SEMI_AUTOMATED,
                "outcome": OUTCOME_NEEDS_REVIEW,
                "severity": "moderate",
                "message": "No select, checkbox, or radio inputs found to probe for on-input context changes.",
                "locator": "",
                "element_text": "",
                "metadata": {},
            }
        )

    return results
