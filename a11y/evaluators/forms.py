from __future__ import annotations

from typing import Any, Dict, List

from ..models import COVERAGE_AUTOMATED, COVERAGE_SEMI_AUTOMATED, OUTCOME_FAILED, OUTCOME_NEEDS_REVIEW, OUTCOME_PASSED


FORM_AUDIT_SCRIPT = """
() => {
  function cssPath(el) {
    if (!el || el.nodeType !== 1) return '';
    if (el.id) return '#' + el.id;
    const classes = Array.from(el.classList || []).slice(0, 3).join('.');
    return el.tagName.toLowerCase() + (classes ? '.' + classes : '');
  }

  function textContent(el) {
    return ((el && (el.innerText || el.textContent)) || '').trim().replace(/\\s+/g, ' ').slice(0, 160);
  }

  function labelText(el) {
    const labels = Array.from(el.labels || []).map(textContent).filter(Boolean);
    const ariaLabel = el.getAttribute('aria-label') || '';
    const ariaLabelledBy = (el.getAttribute('aria-labelledby') || '')
      .split(/\\s+/)
      .map(id => document.getElementById(id))
      .map(textContent)
      .filter(Boolean);
    return labels.concat(ariaLabel ? [ariaLabel] : []).concat(ariaLabelledBy).join(' ').trim();
  }

  function nearbyErrorText(el) {
    const describedBy = (el.getAttribute('aria-describedby') || '')
      .split(/\\s+/)
      .map(id => document.getElementById(id))
      .map(textContent)
      .filter(Boolean);
    if (describedBy.length) return describedBy.join(' ');

    const container = el.closest('label, fieldset, form, div, li, td') || el.parentElement;
    if (!container) return '';
    const errorNode = container.querySelector('[role="alert"], [aria-live], .error, .field-error, .invalid-feedback, .help-error');
    return textContent(errorNode);
  }

  const forms = Array.from(document.querySelectorAll('form'));
  forms.forEach(form => {
    form.addEventListener('submit', event => {
      event.preventDefault();
      event.stopPropagation();
    }, { capture: true });
  });
  forms.forEach(form => {
    try {
      if (typeof form.requestSubmit === 'function') {
        form.requestSubmit();
      } else if (typeof form.reportValidity === 'function') {
        form.reportValidity();
      }
    } catch (e) {}
  });

  const controls = Array.from(document.querySelectorAll('input, select, textarea'))
    .filter(el => {
      const style = window.getComputedStyle(el);
      const rect = el.getBoundingClientRect();
      return el.type !== 'hidden' &&
        style.visibility !== 'hidden' &&
        style.display !== 'none' &&
        rect.width > 0 &&
        rect.height > 0;
    })
    .slice(0, 20);

  const unlabeled = [];
  const invalid = [];
  for (const el of controls) {
    const label = labelText(el);
    if (!label) {
      unlabeled.push({
        locator: cssPath(el),
        text: textContent(el),
        tag: el.tagName.toLowerCase(),
        type: el.getAttribute('type') || '',
      });
    }

    const hadInvalid = !el.checkValidity();
    const validationMessage = el.validationMessage || '';
    const errorText = nearbyErrorText(el);
    if (hadInvalid) {
      invalid.push({
        locator: cssPath(el),
        text: label || textContent(el),
        validationMessage,
        errorText,
        hasDescribedBy: !!(el.getAttribute('aria-describedby') || '').trim(),
      });
    }
  }

  return {
    formsCount: forms.length,
    controlsCount: controls.length,
    unlabeled,
    invalid,
  };
}
"""


async def run_form_labeling_evaluator(page: Any) -> List[Dict[str, Any]]:
    audit: Dict[str, Any] = await page.evaluate(FORM_AUDIT_SCRIPT)
    controls_count = audit.get("controlsCount", 0)
    forms_count = audit.get("formsCount", 0)
    if controls_count == 0:
        return []

    unlabeled = audit.get("unlabeled", [])
    invalid = audit.get("invalid", [])
    results: List[Dict[str, Any]] = []

    if unlabeled:
        first = unlabeled[0]
        results.append(
            {
                "criterion_id": "3.3.2",
                "source": "custom:form_labeling",
                "coverage_status": COVERAGE_AUTOMATED,
                "outcome": OUTCOME_FAILED,
                "severity": "serious",
                "message": "At least one visible form control did not have an associated accessible label.",
                "locator": first.get("locator", ""),
                "element_text": first.get("text", ""),
                "metadata": {
                    "forms_count": forms_count,
                    "controls_count": controls_count,
                    "unlabeled_controls": unlabeled,
                },
            }
        )
    else:
        results.append(
            {
                "criterion_id": "3.3.2",
                "source": "custom:form_labeling",
                "coverage_status": COVERAGE_AUTOMATED,
                "outcome": OUTCOME_PASSED,
                "severity": "moderate",
                "message": "Sampled visible form controls exposed associated labels.",
                "locator": "form",
                "element_text": "",
                "metadata": {
                    "forms_count": forms_count,
                    "controls_count": controls_count,
                },
            }
        )

    if invalid:
        first_invalid = invalid[0]
        results.append(
            {
                "criterion_id": "3.3.1",
                "source": "custom:form_validation",
                "coverage_status": COVERAGE_SEMI_AUTOMATED,
                "outcome": OUTCOME_NEEDS_REVIEW,
                "severity": "moderate",
                "message": "Invalid fields were detected; verify that visible error identification is presented consistently to users.",
                "locator": first_invalid.get("locator", ""),
                "element_text": first_invalid.get("text", ""),
                "metadata": {
                    "forms_count": forms_count,
                    "controls_count": controls_count,
                    "invalid_controls": invalid,
                },
            }
        )
        results.append(
            {
                "criterion_id": "3.3.3",
                "source": "custom:form_validation",
                "coverage_status": COVERAGE_SEMI_AUTOMATED,
                "outcome": OUTCOME_NEEDS_REVIEW,
                "severity": "moderate",
                "message": "Invalid fields were detected; verify whether users receive corrective suggestions, not only error states.",
                "locator": first_invalid.get("locator", ""),
                "element_text": first_invalid.get("text", ""),
                "metadata": {
                    "forms_count": forms_count,
                    "controls_count": controls_count,
                    "invalid_controls": invalid,
                },
            }
        )
    return results
