from __future__ import annotations

import asyncio
from typing import Any, Dict, List

from ..models import COVERAGE_SEMI_AUTOMATED, OUTCOME_FAILED, OUTCOME_NEEDS_REVIEW, OUTCOME_PASSED


# WCAG 1.4.13: Content on Hover or Focus
# Tooltip/popover content that appears on hover must be:
#   1. Dismissible — user can dismiss without moving pointer (e.g. Escape key)
#   2. Hoverable — pointer can move to the new content without it disappearing
#   3. Persistent — content stays visible until dismissed, pointer leaves, or focus moves

HOVER_TRIGGERS_SCRIPT = """
() => {
  function cssPath(el) {
    if (!el || el.nodeType !== 1) return '';
    if (el.id) return '#' + el.id;
    const classes = Array.from(el.classList || []).slice(0, 3).join('.');
    return el.tagName.toLowerCase() + (classes ? '.' + classes : '');
  }

  // Candidates: elements with title attr, aria-describedby, or data-tooltip
  const candidates = [];
  const selectors = [
    '[title]:not([title=""])',
    '[aria-describedby]',
    '[data-tooltip]',
    '[data-tippy-content]',
    '[data-bs-toggle="tooltip"]',
    '[data-bs-toggle="popover"]',
    '[role="tooltip"]',
  ];

  const seen = new Set();
  for (const sel of selectors) {
    for (const el of Array.from(document.querySelectorAll(sel)).slice(0, 5)) {
      const path = cssPath(el);
      if (seen.has(path)) continue;
      seen.add(path);
      const rect = el.getBoundingClientRect();
      const style = window.getComputedStyle(el);
      if (
        style.display === 'none' ||
        style.visibility === 'hidden' ||
        rect.width === 0 ||
        rect.height === 0
      ) continue;
      candidates.push({
        locator: path,
        text: (el.innerText || el.textContent || '').trim().slice(0, 80),
        tag: el.tagName.toLowerCase(),
        trigger: sel,
        centerX: rect.left + rect.width / 2,
        centerY: rect.top + rect.height / 2,
      });
      if (candidates.length >= 5) break;
    }
    if (candidates.length >= 5) break;
  }
  return candidates;
}
"""

DOM_MUTATION_SCRIPT = """
() => {
  window.__a11yHoverMutations = 0;
  window.__a11yHoverObserver = new MutationObserver(() => {
    window.__a11yHoverMutations += 1;
  });
  window.__a11yHoverObserver.observe(document.body, {
    childList: true, subtree: true, attributes: true,
    attributeFilter: ['style', 'class', 'hidden', 'aria-hidden'],
  });
}
"""

GET_MUTATIONS_SCRIPT = """
() => {
  const count = window.__a11yHoverMutations || 0;
  if (window.__a11yHoverObserver) {
    window.__a11yHoverObserver.disconnect();
    delete window.__a11yHoverObserver;
  }
  window.__a11yHoverMutations = 0;
  return count;
}
"""

TOOLTIP_VISIBLE_AFTER_ESCAPE_SCRIPT = """
() => {
  // Check if any tooltip/popover role element is visible after Escape
  const tooltips = Array.from(document.querySelectorAll(
    '[role="tooltip"], [data-tooltip], .tooltip, .popover, [class*="tooltip"], [class*="popover"]'
  ));
  return tooltips.filter(el => {
    const style = window.getComputedStyle(el);
    const rect = el.getBoundingClientRect();
    return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0;
  }).length;
}
"""


async def run_hover_content_evaluator(page: Any) -> List[Dict[str, Any]]:
    try:
        candidates = await page.evaluate(HOVER_TRIGGERS_SCRIPT)
    except Exception:
        candidates = []

    if not candidates:
        return [
            {
                "criterion_id": "1.4.13",
                "source": "custom:hover_content",
                "coverage_status": COVERAGE_SEMI_AUTOMATED,
                "outcome": OUTCOME_NEEDS_REVIEW,
                "severity": "moderate",
                "message": (
                    "No tooltip/hover-triggered content candidates detected via common patterns "
                    "(title attribute, aria-describedby, data-tooltip, ARIA tooltip role). "
                    "Manual review required for custom hover implementations."
                ),
                "locator": "",
                "element_text": "",
                "metadata": {"candidates": []},
            }
        ]

    results: List[Dict[str, Any]] = []
    probe_results: List[Dict[str, Any]] = []

    for candidate in candidates[:3]:
        cx = candidate.get("centerX", 0)
        cy = candidate.get("centerY", 0)

        try:
            await page.evaluate(DOM_MUTATION_SCRIPT)
            await page.mouse.move(cx, cy)
            await asyncio.sleep(0.4)

            mutations_on_hover = await page.evaluate(GET_MUTATIONS_SCRIPT)

            # Check dismissibility: press Escape and see if tooltip disappears
            await page.evaluate(DOM_MUTATION_SCRIPT)
            await page.keyboard.press("Escape")
            await asyncio.sleep(0.2)
            mutations_on_escape = await page.evaluate(GET_MUTATIONS_SCRIPT)
            tooltips_remaining = await page.evaluate(TOOLTIP_VISIBLE_AFTER_ESCAPE_SCRIPT)

            # Re-hover to check if tooltip is still there (hoverable / persistent)
            await page.mouse.move(cx, cy)
            await asyncio.sleep(0.3)
            content_appeared = mutations_on_hover > 0

            probe_results.append(
                {
                    "locator": candidate.get("locator"),
                    "text": candidate.get("text"),
                    "trigger": candidate.get("trigger"),
                    "content_appeared_on_hover": content_appeared,
                    "mutations_on_hover": mutations_on_hover,
                    "mutations_on_escape": mutations_on_escape,
                    "tooltips_remaining_after_escape": tooltips_remaining,
                    "escape_dismissed": mutations_on_escape > 0 and tooltips_remaining == 0,
                }
            )
        except Exception:
            probe_results.append(
                {
                    "locator": candidate.get("locator"),
                    "text": candidate.get("text"),
                    "trigger": candidate.get("trigger"),
                    "error": "probe_failed",
                }
            )

    # Evaluate results
    appeared = [p for p in probe_results if p.get("content_appeared_on_hover")]
    not_dismissible = [p for p in appeared if not p.get("escape_dismissed") and not p.get("error")]

    if not appeared:
        results.append(
            {
                "criterion_id": "1.4.13",
                "source": "custom:hover_content",
                "coverage_status": COVERAGE_SEMI_AUTOMATED,
                "outcome": OUTCOME_NEEDS_REVIEW,
                "severity": "moderate",
                "message": (
                    "Hover candidates were found but hover interactions did not produce "
                    "detectable DOM mutations. Tooltip implementations may rely on CSS-only "
                    "transitions — manual review of dismissibility and hoverability required."
                ),
                "locator": candidates[0].get("locator", ""),
                "element_text": candidates[0].get("text", ""),
                "metadata": {"candidates": candidates, "probe_results": probe_results},
            }
        )
    elif not_dismissible:
        first = not_dismissible[0]
        results.append(
            {
                "criterion_id": "1.4.13",
                "source": "custom:hover_content",
                "coverage_status": COVERAGE_SEMI_AUTOMATED,
                "outcome": OUTCOME_FAILED,
                "severity": "serious",
                "message": (
                    "Hover-triggered content appeared but pressing Escape did not dismiss it, "
                    "violating WCAG 1.4.13 dismissibility requirement."
                ),
                "locator": first.get("locator", ""),
                "element_text": first.get("text", ""),
                "metadata": {"candidates": candidates, "probe_results": probe_results, "not_dismissible": not_dismissible},
            }
        )
    else:
        first = appeared[0]
        results.append(
            {
                "criterion_id": "1.4.13",
                "source": "custom:hover_content",
                "coverage_status": COVERAGE_SEMI_AUTOMATED,
                "outcome": OUTCOME_NEEDS_REVIEW,
                "severity": "moderate",
                "message": (
                    "Hover-triggered content was detected and Escape key dismissed it. "
                    "Hoverability (pointer can move to content) and persistence still require visual review."
                ),
                "locator": first.get("locator", ""),
                "element_text": first.get("text", ""),
                "metadata": {"candidates": candidates, "probe_results": probe_results},
            }
        )

    return results
