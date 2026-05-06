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

  return {
    landmarks,
    headings,
    headingJump,
    duplicateIds: duplicates,
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
        results.append(
            {
                "criterion_id": "2.4.1",
                "source": "custom:structure",
                "coverage_status": COVERAGE_AUTOMATED,
                "outcome": OUTCOME_PASSED,
                "severity": "moderate",
                "message": "A main landmark was detected for basic bypass-block support.",
                "locator": "main",
                "element_text": "",
                "metadata": {"landmarks": landmarks},
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
