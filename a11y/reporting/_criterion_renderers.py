from __future__ import annotations

import json
from typing import Any, Dict, List

from ._helpers import _escape, _status_badge, _render_sources, _source_label


_OUTCOME_PRIORITY = {"FAILED": 0, "ERROR": 0, "NEEDS_REVIEW": 1, "PASSED": 2, "NOT_APPLICABLE": 3, "NOT_TESTED": 4}
_EVIDENCE_LIMIT = 3


def _criterion_csv_row(criterion: Dict[str, Any]) -> str:
    """HTML-escaped JSON array for the data-csv attribute (used by JS CSV export)."""
    row = [
        criterion.get("id", ""),
        criterion.get("name", ""),
        criterion.get("principle", ""),
        criterion.get("level", ""),
        criterion.get("outcome_status", ""),
        criterion.get("coverage_status", ""),
        ", ".join(_source_label(s) for s in (criterion.get("sources") or [])[:3]),
        ", ".join((criterion.get("affected_screens") or [])[:3]),
        ", ".join((criterion.get("affected_urls") or [])[:3]),
    ]
    return _escape(json.dumps(row))


def _first_issue_message(criterion: Dict[str, Any]) -> str:
    for item in (criterion.get("evidence") or []):
        if item.get("outcome") in ("FAILED", "ERROR", "NEEDS_REVIEW"):
            msg = item.get("message", "")
            if msg:
                return msg
    return ""


def _outcome_summary(criterion: Dict[str, Any]) -> str:
    outcome = criterion.get("outcome_status", "")
    notes = [n for n in (criterion.get("coverage_notes") or []) if n]

    if outcome in ("FAILED", "ERROR"):
        msg = _first_issue_message(criterion) or (notes[0] if notes else "See evidence below.")
        return '<p class="action-item action-failed"><strong>Fix:</strong> {}</p>'.format(_escape(msg))
    if outcome == "NEEDS_REVIEW":
        msg = notes[0] if notes else "Manual verification required \u2014 no automated check covers this criterion in the current flow."
        return '<p class="action-item action-review"><strong>Check:</strong> {}</p>'.format(_escape(msg))
    if outcome == "PASSED":
        return '<p class="outcome-summary-text">Tested \u2014 no violations found.</p>'
    if outcome == "NOT_APPLICABLE":
        return '<p class="outcome-summary-text">Does not apply to the tested flow.</p>'
    if outcome == "NOT_TESTED":
        return '<p class="outcome-summary-text">No evidence collected in this run.</p>'
    return ""


def _render_evidence_items(evidence: List[Dict[str, Any]]) -> str:
    if not evidence:
        return ""

    # Show failures/reviews first, fall back to all items
    priority = [i for i in evidence if i.get("outcome") in ("FAILED", "ERROR", "NEEDS_REVIEW")]
    ordered = priority or evidence

    cards = []
    for item in ordered:
        location = item.get("location", {})
        metadata = item.get("metadata", {})

        locator_val = location.get("locator", "")
        url_val = location.get("page_url", "")
        element_val = location.get("element_text", "")
        screenshot_ref = location.get("screenshot_ref", "")
        help_url = metadata.get("helpUrl") or ""

        # Primary fields — always shown
        primary_html = ""
        if locator_val:
            primary_html += '<div class="ev-row"><span class="label">Locator</span><code>{}</code></div>'.format(_escape(locator_val))
        if url_val:
            primary_html += '<div class="ev-row"><span class="label">URL</span><span>{}</span></div>'.format(_escape(url_val))
        if element_val:
            display = element_val if len(element_val) <= 160 else element_val[:160] + "\u2026"
            primary_html += '<div class="ev-row"><span class="label">Element</span><code>{}</code></div>'.format(_escape(display))
        if screenshot_ref and screenshot_ref.startswith("data:"):
            primary_html += '<div class="ev-row"><img class="evidence-screenshot" src="{}" alt="Screenshot" loading="lazy"></div>'.format(screenshot_ref)
        elif screenshot_ref:
            primary_html += '<div class="ev-row"><span class="label">Screenshot</span><span>{}</span></div>'.format(_escape(screenshot_ref))

        # Secondary fields — collapsed
        detail_rows = ""
        source_val = item.get("source", "")
        if source_val:
            detail_rows += "<div><span class='label'>Source</span><span>{}</span></div>".format(_escape(_source_label(source_val)))
        severity_val = item.get("severity", "")
        if severity_val:
            detail_rows += "<div><span class='label'>Severity</span><span>{}</span></div>".format(_escape(severity_val))
        screen_val = location.get("screen_label", "")
        if screen_val:
            detail_rows += "<div><span class='label'>Screen</span><span>{}</span></div>".format(_escape(screen_val))
        step_val = location.get("journey_step_label", "")
        if step_val:
            detail_rows += "<div><span class='label'>Step</span><span>{}</span></div>".format(_escape(step_val))
        container_val = location.get("container_label", "")
        if container_val:
            detail_rows += "<div><span class='label'>Container</span><span>{}</span></div>".format(_escape(container_val))
        if location.get("frame_name"):
            detail_rows += "<div><span class='label'>Frame</span><span>{}</span></div>".format(_escape(location["frame_name"]))
        if metadata.get("validationMessage"):
            detail_rows += "<div><span class='label'>Validation</span><span>{}</span></div>".format(_escape(metadata["validationMessage"]))
        if help_url:
            detail_rows += "<div><span class='label'>Rule</span><span><a href='{}' target='_blank' rel='noreferrer'>Axe docs \u2197</a></span></div>".format(_escape(help_url))

        details_block = ""
        if detail_rows:
            details_block = (
                '<details class="evidence-detail-toggle">'
                "<summary>More details</summary>"
                '<div class="evidence-detail-grid">{}</div>'
                "</details>"
            ).format(detail_rows)

        cards.append(
            """
            <article class="evidence-card" id="{anchor}">
              <div class="evidence-head">
                <strong>{message}</strong>
                {status}
              </div>
              {primary_html}
              {details_block}
            </article>
            """.format(
                anchor=_escape(location.get("report_anchor", "")),
                message=_escape(item.get("message", "")),
                status=_status_badge(item.get("outcome", "")),
                primary_html=primary_html,
                details_block=details_block,
            )
        )

    if len(cards) <= _EVIDENCE_LIMIT:
        return "".join(cards)

    # First N cards shown immediately; the rest are inside a collapsible block
    visible = "".join(cards[:_EVIDENCE_LIMIT])
    hidden_count = len(cards) - _EVIDENCE_LIMIT
    collapsed = (
        '<details class="evidence-overflow-toggle">'
        '<summary>Show {} more issue{}</summary>'
        '<div class="evidence-overflow-inner">{}</div>'
        "</details>"
    ).format(hidden_count, "s" if hidden_count != 1 else "", "".join(cards[_EVIDENCE_LIMIT:]))

    return visible + collapsed


def _render_criterion_panels(criteria: List[Dict[str, Any]]) -> str:
    # Sort by priority: failures first, then needs-review, passed, n/a, not-tested
    sorted_criteria = sorted(
        criteria,
        key=lambda c: _OUTCOME_PRIORITY.get(c.get("outcome_status", ""), 5),
    )

    panels: List[str] = []
    for criterion in sorted_criteria:
        is_not_tested = criterion.get("outcome_status") == "NOT_TESTED"
        panel_class    = "criterion-panel criterion-panel-not-tested" if is_not_tested else "criterion-panel"
        panel_style    = ' style="display:none"' if is_not_tested else ""
        hidden_default = ' data-default-hidden="true"' if is_not_tested else ""

        cid       = criterion.get("id", "")
        principle = (cid or " ")[0]

        notes = [n for n in (criterion.get("coverage_notes") or []) if n]
        not_tested_explanation = criterion.get("not_tested_explanation")
        if not_tested_explanation:
            notes.append(not_tested_explanation)

        # Coverage details — collapsed
        sources_html = _render_sources(criterion.get("sources", []))
        screens = ", ".join(criterion.get("affected_screens", [])) or "None"
        steps   = ", ".join(criterion.get("affected_steps", []))   or "None"
        urls    = ", ".join(criterion.get("affected_urls", []))     or "None"
        notes_html = "<ul>{}</ul>".format("".join("<li>{}</li>".format(_escape(n)) for n in notes)) if notes else ""
        coverage_details = (
            '<details class="coverage-details-toggle">'
            "<summary>Coverage details</summary>"
            '<div class="coverage-details-inner">'
            "{notes}"
            '<p><span class="label-inline">Sources:</span> {sources}</p>'
            '<p><span class="label-inline">Screens:</span> {screens}</p>'
            '<p><span class="label-inline">Steps:</span> {steps}</p>'
            '<p><span class="label-inline">URLs:</span> {urls}</p>'
            "</div>"
            "</details>"
        ).format(
            notes=notes_html,
            sources=sources_html,
            screens=_escape(screens),
            steps=_escape(steps),
            urls=_escape(urls),
        )

        evidence_html = _render_evidence_items(criterion.get("evidence", []))

        panels.append(
            """
            <section class="{panel_class}" id="criterion-{criterion_id}"{panel_style}
              data-outcome="{outcome_status}"
              data-level="{level}"
              data-principle="{principle}"{hidden_default}
              data-csv="{csv_data}">
              <div class="criterion-head">
                <div>
                  <h3>{criterion_id} {name}</h3>
                  <p class="criterion-meta">{guideline} &middot; Level {level} &middot; <a href="{doc_url}" target="_blank" rel="noreferrer">WCAG Reference</a></p>
                </div>
                <div class="criterion-statuses">
                  {outcome}
                </div>
              </div>
              {outcome_summary}
              {evidence_html}
              {coverage_details}
            </section>
            """.format(
                panel_class=panel_class,
                panel_style=panel_style,
                hidden_default=hidden_default,
                criterion_id=_escape(cid),
                name=_escape(criterion.get("name", "")),
                guideline=_escape(criterion.get("guideline", "")),
                level=_escape(criterion.get("level", "")),
                principle=_escape(principle),
                outcome_status=_escape(criterion.get("outcome_status", "")),
                doc_url=_escape(criterion.get("doc_url", "")),
                outcome=_status_badge(criterion.get("outcome_status", "")),
                outcome_summary=_outcome_summary(criterion),
                evidence_html=evidence_html,
                coverage_details=coverage_details,
                csv_data=_criterion_csv_row(criterion),
            )
        )
    return "".join(panels)


def _render_criteria_table(criteria: List[Dict[str, Any]]) -> str:
    rows = []
    for criterion in criteria:
        rows.append(
            """
            <tr>
              <td><a href="#criterion-{criterion_id}">{criterion_id}</a></td>
              <td>{name}</td>
              <td>{level}</td>
              <td>{outcome}</td>
              <td>{coverage}</td>
              <td>{sources}</td>
            </tr>
            """.format(
                criterion_id=_escape(criterion.get("id", "")),
                name=_escape(criterion.get("name", "")),
                level=_escape(criterion.get("level", "")),
                outcome=_status_badge(criterion.get("outcome_status", "")),
                coverage=_status_badge(criterion.get("coverage_status", "")),
                sources=_escape(", ".join(_source_label(s) for s in criterion.get("sources", [])[:3]) or "None"),
            )
        )
    return "".join(rows)


def _render_criteria_overview(criteria: List[Dict[str, Any]], sections: List[Dict[str, Any]]) -> str:
    criteria_by_id = {criterion.get("id"): criterion for criterion in criteria}
    blocks = []
    for section in sections:
        section_criteria = [criteria_by_id[row_id] for row_id in section.get("rows", []) if row_id in criteria_by_id]
        if not section_criteria:
            continue
        blocks.append(
            """
            <section class="section-block">
              <h3>{title}</h3>
              <table>
                <thead>
                  <tr>
                    <th>ID</th>
                    <th>Name</th>
                    <th>Level</th>
                    <th>Outcome</th>
                    <th>Coverage</th>
                    <th>Sources</th>
                  </tr>
                </thead>
                <tbody>{rows}</tbody>
              </table>
            </section>
            """.format(
                title=_escape(section.get("title", "")),
                rows=_render_criteria_table(section_criteria),
            )
        )
    return "".join(blocks)
