from __future__ import annotations

import asyncio
from typing import Any, Dict, List

from ..models import COVERAGE_AUTOMATED, COVERAGE_SEMI_AUTOMATED, OUTCOME_NEEDS_REVIEW, OUTCOME_PASSED


LIVE_REGION_SCRIPT = """
() => {
  function cssPath(el) {
    if (!el || el.nodeType !== 1) return '';
    if (el.id) return '#' + CSS.escape(el.id);
    const classes = Array.from(el.classList || []).slice(0, 3).map(c => CSS.escape(c)).join('.');
    return el.tagName.toLowerCase() + (classes ? '.' + classes : '');
  }

  const liveNodes = Array.from(document.querySelectorAll('[aria-live], [role="status"], [role="alert"]'))
    .slice(0, 20)
    .map(el => ({
      locator: cssPath(el),
      text: (el.innerText || el.textContent || '').trim().slice(0, 160),
      ariaLive: el.getAttribute('aria-live') || '',
      role: el.getAttribute('role') || '',
      atomic: el.getAttribute('aria-atomic') || '',
    }));

  return {
    liveRegions: liveNodes,
    count: liveNodes.length,
  };
}
"""


async def run_live_region_evaluator(page: Any) -> List[Dict[str, Any]]:
    audit: Dict[str, Any] = await page.evaluate(LIVE_REGION_SCRIPT)
    live_regions = audit.get("liveRegions", [])
    count = audit.get("count", 0)

    await page.evaluate(
        """
        () => {
          window.__a11yLiveRegionProbe = { events: [] };
          const probe = window.__a11yLiveRegionProbe;
          const record = el => {
            if (!el || !el.matches || !el.matches('[aria-live], [role="status"], [role="alert"]')) return;
            const classes = Array.from(el.classList || []).slice(0, 3).map(c => CSS.escape(c)).join('.');
            const locator = el.id ? '#' + CSS.escape(el.id) : el.tagName.toLowerCase() + (classes ? '.' + classes : '');
            probe.events.push({
              locator,
              text: (el.innerText || el.textContent || '').trim().slice(0, 160),
              role: el.getAttribute('role') || '',
              ariaLive: el.getAttribute('aria-live') || '',
            });
          };
          const observer = new MutationObserver(mutations => {
            for (const mutation of mutations) {
              if (mutation.target && mutation.target.nodeType === 1) {
                record(mutation.target);
              }
              for (const node of Array.from(mutation.addedNodes || [])) {
                if (node && node.nodeType === 1) record(node);
              }
            }
          });
          observer.observe(document.body, { subtree: true, childList: true, characterData: true, attributes: true });
          window.__a11yLiveRegionObserver = observer;
        }
        """
    )

    await page.evaluate(
        """
        () => {
          const safeButtons = Array.from(document.querySelectorAll('button, input[type="button"], input[type="submit"], [role="button"], a[href="#"]'))
            .filter(el => {
              const style = window.getComputedStyle(el);
              const rect = el.getBoundingClientRect();
              return !el.disabled && style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
            })
            .slice(0, 2);
          safeButtons.forEach(el => {
            try { el.click(); } catch (e) {}
          });

          const forms = Array.from(document.querySelectorAll('form')).slice(0, 2);
          forms.forEach(form => {
            form.addEventListener('submit', event => {
              event.preventDefault();
              event.stopPropagation();
            }, { capture: true });
            try {
              if (typeof form.requestSubmit === 'function') {
                form.requestSubmit();
              } else if (typeof form.reportValidity === 'function') {
                form.reportValidity();
              }
            } catch (e) {}
          });
        }
        """
    )
    await asyncio.sleep(0.2)
    dynamic_events = await page.evaluate(
        """
        () => {
          const probe = window.__a11yLiveRegionProbe || { events: [] };
          if (window.__a11yLiveRegionObserver) {
            window.__a11yLiveRegionObserver.disconnect();
          }
          return probe.events || [];
        }
        """
    )

    if dynamic_events:
        first = dynamic_events[0]
        return [
            {
                "criterion_id": "4.1.3",
                "source": "custom:live_regions",
                "coverage_status": COVERAGE_AUTOMATED,
                "outcome": OUTCOME_PASSED,
                "severity": "moderate",
                "message": "A live region or status node changed during a safe interaction probe.",
                "locator": first.get("locator", ""),
                "element_text": first.get("text", ""),
                "metadata": {
                    "live_regions": live_regions,
                    "dynamic_events": dynamic_events,
                },
            }
        ]

    if count == 0:
        return [
            {
                "criterion_id": "4.1.3",
                "source": "custom:live_regions",
                "coverage_status": COVERAGE_SEMI_AUTOMATED,
                "outcome": OUTCOME_NEEDS_REVIEW,
                "severity": "moderate",
                "message": "No live region semantics were detected and safe interaction probes did not surface a status region; verify dynamic status messages during richer flows.",
                "locator": "body",
                "element_text": "",
                "metadata": {"live_regions": live_regions, "dynamic_events": dynamic_events},
            }
        ]

    first = live_regions[0]
    return [
        {
            "criterion_id": "4.1.3",
            "source": "custom:live_regions",
            "coverage_status": COVERAGE_SEMI_AUTOMATED,
            "outcome": OUTCOME_NEEDS_REVIEW,
            "severity": "moderate",
            "message": "Live region semantics were detected in the DOM, but safe interaction probes did not trigger a status update in this run.",
            "locator": first.get("locator", ""),
            "element_text": first.get("text", ""),
            "metadata": {"live_regions": live_regions, "dynamic_events": dynamic_events},
        }
    ]
