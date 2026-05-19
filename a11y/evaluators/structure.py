from __future__ import annotations

from typing import Any, Dict, List

from ..models import COVERAGE_AUTOMATED, OUTCOME_FAILED, OUTCOME_PASSED


STRUCTURE_AUDIT_SCRIPT = """
() => {
  function cssPath(el) {
    if (!el || el.nodeType !== 1) return '';
    if (el.id) return '#' + CSS.escape(el.id);
    const classes = Array.from(el.classList || []).slice(0, 3).map(c => CSS.escape(c)).join('.');
    return el.tagName.toLowerCase() + (classes ? '.' + classes : '');
  }

  const mainCount = document.querySelectorAll('main, [role="main"]').length;
  const landmarks = {
    banner: document.querySelectorAll('header, [role="banner"]').length,
    main: mainCount,
    contentinfo: document.querySelectorAll('footer, [role="contentinfo"]').length,
    navigation: document.querySelectorAll('nav, [role="navigation"]').length,
  };

  const headings = Array.from(document.querySelectorAll('h1,h2,h3,h4,h5,h6'))
    .map(el => ({
      level: Number(el.tagName.slice(1)),
      text: (el.innerText || el.textContent || '').trim().slice(0, 120),
      locator: cssPath(el),
    }));

  let headingJump = null;
  for (let i = 1; i < headings.length; i++) {
    if (headings[i].level > headings[i - 1].level + 1) {
      headingJump = { previous: headings[i - 1], current: headings[i] };
      break;
    }
  }

  const ids = new Map();
  const duplicates = [];
  for (const el of Array.from(document.querySelectorAll('[id]')).slice(0, 500)) {
    const id = el.id;
    if (!id) continue;
    if (ids.has(id)) {
      duplicates.push({ id, locator: cssPath(el) });
    } else {
      ids.set(id, cssPath(el));
    }
  }

  // Detect skip-link: first focusable anchor whose href targets an in-page
  // landmark (#main / #content / etc) and whose visible text reads as a skip
  // affordance. WCAG 2.4.1 wants both an in-page jump *and* a target landmark.
  const focusableAnchors = Array.from(document.querySelectorAll(
    'a[href^="#"]:not([href="#"]), [role="link"][href^="#"]'
  ));
  let skipLink = null;
  const skipPatterns = /(skip|jump|go)\s*(to)?\s*(main|content|nav|primary)/i;
  for (const a of focusableAnchors.slice(0, 8)) {
    const href = a.getAttribute('href') || '';
    const text = (a.innerText || a.textContent || a.getAttribute('aria-label') || '').trim();
    const id = href.slice(1);
    const target = id ? document.getElementById(id) : null;
    const targetIsLandmark = !!target && (
      target.tagName.toLowerCase() === 'main' ||
      target.getAttribute('role') === 'main' ||
      /^(content|main|primary)/i.test(id)
    );
    if (targetIsLandmark || skipPatterns.test(text)) {
      skipLink = {
        locator: cssPath(a),
        text: text.slice(0, 80),
        href,
        targetExists: !!target,
        targetIsLandmark,
      };
      break;
    }
  }

  return {
    landmarks,
    headings,
    headingJump,
    duplicateIds: duplicates,
    skipLink,
  };
}
"""


async def run_structure_evaluator(page: Any) -> List[Dict[str, Any]]:
    audit: Dict[str, Any] = await page.evaluate(STRUCTURE_AUDIT_SCRIPT)
    results: List[Dict[str, Any]] = []

    landmarks = audit.get("landmarks", {})
    headings = audit.get("headings", [])
    heading_jump = audit.get("headingJump")
    duplicate_ids = audit.get("duplicateIds", [])
    skip_link = audit.get("skipLink")

    if landmarks.get("main", 0) < 1:
        results.append(
            {
                "criterion_id": "1.3.1",
                "source": "custom:structure",
                "coverage_status": COVERAGE_AUTOMATED,
                "outcome": OUTCOME_FAILED,
                "severity": "serious",
                "message": "No main landmark was detected on the page.",
                "locator": "main",
                "element_text": "",
                "metadata": {"landmarks": landmarks},
            }
        )
        results.append(
            {
                "criterion_id": "2.4.1",
                "source": "custom:structure",
                "coverage_status": COVERAGE_AUTOMATED,
                "outcome": OUTCOME_FAILED,
                "severity": "serious",
                "message": "No main landmark was detected, which weakens bypass-block support.",
                "locator": "main",
                "element_text": "",
                "metadata": {"landmarks": landmarks},
            }
        )
    else:
        results.append(
            {
                "criterion_id": "1.3.1",
                "source": "custom:structure",
                "coverage_status": COVERAGE_AUTOMATED,
                "outcome": OUTCOME_PASSED,
                "severity": "moderate",
                "message": "Basic landmark structure includes a main region.",
                "locator": "main",
                "element_text": "",
                "metadata": {"landmarks": landmarks},
            }
        )
        # WCAG 2.4.1 wants both a landmark target AND an in-page bypass affordance
        # (skip-link, heading nav, or list of links). A <main> alone is insufficient.
        if skip_link and skip_link.get("targetIsLandmark"):
            results.append(
                {
                    "criterion_id": "2.4.1",
                    "source": "custom:structure",
                    "coverage_status": COVERAGE_AUTOMATED,
                    "outcome": OUTCOME_PASSED,
                    "severity": "moderate",
                    "message": "Main landmark plus in-page skip link target a landmark — basic bypass-block mechanism present.",
                    "locator": skip_link.get("locator", "main"),
                    "element_text": skip_link.get("text", ""),
                    "metadata": {"landmarks": landmarks, "skip_link": skip_link},
                }
            )
        else:
            results.append(
                {
                    "criterion_id": "2.4.1",
                    "source": "custom:structure",
                    "coverage_status": COVERAGE_AUTOMATED,
                    "outcome": OUTCOME_FAILED,
                    "severity": "serious",
                    "message": (
                        "Main landmark detected but no in-page skip link targeting it was found. "
                        "WCAG 2.4.1 requires a real bypass mechanism, not landmark presence alone."
                    ),
                    "locator": "main",
                    "element_text": "",
                    "metadata": {"landmarks": landmarks, "skip_link": skip_link},
                }
            )

    if not headings:
        results.append(
            {
                "criterion_id": "2.4.6",
                "source": "custom:structure",
                "coverage_status": COVERAGE_AUTOMATED,
                "outcome": OUTCOME_FAILED,
                "severity": "serious",
                "message": "No headings were detected on the page.",
                "locator": "body",
                "element_text": "",
                "metadata": {"headings": headings},
            }
        )
    elif heading_jump:
        results.append(
            {
                "criterion_id": "2.4.6",
                "source": "custom:structure",
                "coverage_status": COVERAGE_AUTOMATED,
                "outcome": OUTCOME_FAILED,
                "severity": "moderate",
                "message": "Heading levels jump by more than one level in the sampled outline.",
                "locator": heading_jump["current"].get("locator", ""),
                "element_text": heading_jump["current"].get("text", ""),
                "metadata": {"headings": headings, "heading_jump": heading_jump},
            }
        )
    else:
        results.append(
            {
                "criterion_id": "2.4.6",
                "source": "custom:structure",
                "coverage_status": COVERAGE_AUTOMATED,
                "outcome": OUTCOME_PASSED,
                "severity": "moderate",
                "message": "Sampled heading outline did not show a skipped heading level.",
                "locator": headings[0].get("locator", ""),
                "element_text": headings[0].get("text", ""),
                "metadata": {"headings": headings},
            }
        )

    if duplicate_ids:
        first = duplicate_ids[0]
        results.append(
            {
                "criterion_id": "4.1.2",
                "source": "custom:structure",
                "coverage_status": COVERAGE_AUTOMATED,
                "outcome": OUTCOME_FAILED,
                "severity": "serious",
                "message": "Duplicate element IDs were detected, which can break accessible relationships.",
                "locator": first.get("locator", ""),
                "element_text": first.get("id", ""),
                "metadata": {"duplicate_ids": duplicate_ids},
            }
        )

    return results
