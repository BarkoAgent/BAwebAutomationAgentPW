from __future__ import annotations

from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse

from ._helpers import _escape


_OUTCOME_RANK = {
    "FAILED": 0,
    "ERROR": 1,
    "NEEDS_REVIEW": 2,
    "PASSED": 3,
    "NOT_APPLICABLE": 4,
    "NOT_TESTED": 5,
}

_OUTCOME_CELL_CLASS = {
    "FAILED": "matrix-failed",
    "ERROR": "matrix-error",
    "NEEDS_REVIEW": "matrix-review",
    "PASSED": "matrix-passed",
    "NOT_APPLICABLE": "matrix-na",
}

_OUTCOME_CELL_SYMBOL = {
    "FAILED": "✕",
    "ERROR": "!",
    "NEEDS_REVIEW": "~",
    "PASSED": "✓",
    "NOT_APPLICABLE": "–",
}


def _step_screen_label_html(step: Dict[str, Any]) -> str:
    step_label = (step.get("journey_step_label") or "").strip()
    title = (step.get("page_title") or "").strip()
    url = (step.get("page_url") or "").strip()
    index = step.get("journey_step_index", "")

    try:
        path = urlparse(url).path or "/"
    except Exception:
        path = "/"
    base = title or (path.strip("/") if path != "/" else "") or url

    if step_label and step_label.lower() != base.lower():
        return "{} — {}".format(step_label, base) if base else step_label
    if step_label:
        return step_label
    return "Step {} — {}".format(index, base) if base else "Step {}".format(index)


def _build_screen_data(
    criteria: List[Dict[str, Any]],
    journey_steps: List[Dict[str, Any]],
) -> Tuple[List[Tuple[str, str, Dict]], Dict[str, Dict[str, str]], List[Dict[str, Any]]]:
    """
    Returns:
        screens_ordered: list of (screen_label, page_url, step) in checkpoint order.
        matrix: { screen_label: { criterion_id: worst_outcome } }
        active_criteria: list of criterion dicts that have at least one piece of evidence
    """
    screens_ordered: List[Tuple[str, str, Dict]] = []
    seen_labels: Dict[str, int] = {}
    for step in journey_steps:
        raw_label = _step_screen_label_html(step)
        if raw_label in seen_labels:
            seen_labels[raw_label] += 1
            label = "{} ({})".format(raw_label, seen_labels[raw_label])
        else:
            seen_labels[raw_label] = 1
            label = raw_label
        screens_ordered.append((label, step.get("page_url", ""), step))

    matrix: Dict[str, Dict[str, str]] = {label: {} for label, _, _ in screens_ordered}

    active_criterion_ids = set()
    for criterion in criteria:
        cid = criterion.get("id", "")
        evidence = criterion.get("evidence", [])
        if not evidence:
            continue
        for item in evidence:
            location = item.get("location", {})
            screen_label = (location.get("screen_label") or "").strip()
            outcome = item.get("outcome", "NOT_TESTED")
            if not screen_label:
                continue
            active_criterion_ids.add(cid)
            if screen_label not in matrix:
                matrix[screen_label] = {}
                screens_ordered.append((screen_label, "", {}))
            current = matrix[screen_label].get(cid)
            if current is None or _OUTCOME_RANK.get(outcome, 99) < _OUTCOME_RANK.get(current, 99):
                matrix[screen_label][cid] = outcome

    active_criteria = [c for c in criteria if c.get("id") in active_criterion_ids]
    active_criteria.sort(
        key=lambda c: min(
            (_OUTCOME_RANK.get(i.get("outcome", "NOT_TESTED"), 99) for i in c.get("evidence", [])),
            default=99,
        )
    )

    return screens_ordered, matrix, active_criteria


def _render_screen_journey(journey_steps: List[Dict[str, Any]]) -> str:
    if not journey_steps:
        return '<p class="muted">No screens recorded in this audit.</p>'

    if len(journey_steps) == 1:
        step = journey_steps[0]
        label = ((step.get("page_title") or step.get("page_url") or "Step 1")).strip()
        violations = step.get("axe_violations_count")
        viol_note = " &mdash; {} violation{} found".format(violations, "s" if violations != 1 else "") if violations else ""
        return (
            '<p class="muted">Single checkpoint captured: <strong>{}</strong>{}. '
            "Multi-screen navigation tracing requires more than one audit checkpoint.</p>"
        ).format(_escape(label), viol_note)

    prev_url = None
    nodes = []

    for i, step in enumerate(journey_steps):
        title = (step.get("page_title") or "").strip()
        url = (step.get("page_url") or "").strip()
        step_label = (step.get("journey_step_label") or "").strip()
        index = step.get("journey_step_index", i + 1)
        violations = step.get("axe_violations_count")

        display_label = step_label or title or url or "Step {}".format(index)

        url_changed = url != prev_url
        try:
            path = urlparse(url).path or "/"
        except Exception:
            path = url
        url_sub = '<div class="flow-url-sub {cls}">{path}</div>'.format(
            cls="flow-url-new" if url_changed else "flow-url-same",
            path=_escape(path),
        ) if url else ""

        violation_badge = ""
        if violations is not None:
            badge_class = "flow-violations-badge flow-violations-fail" if violations > 0 else "flow-violations-badge flow-violations-pass"
            violation_badge = '<span class="{}">{} violation{}</span>'.format(
                badge_class, violations, "s" if violations != 1 else ""
            )

        connector = ""
        if i < len(journey_steps) - 1:
            next_url = (journey_steps[i + 1].get("page_url") or "").strip()
            nav_class = "flow-arrow flow-arrow-nav" if next_url != url else "flow-arrow"
            connector = '<div class="{}">→</div>'.format(nav_class)

        nodes.append(
            """
            <div class="flow-node-wrap">
              <div class="flow-node{new_cls}" title="{tooltip}">
                <div class="flow-step-index">#{index}</div>
                <div class="flow-screen-label">{label}</div>
                {url_sub}
                {violation_badge}
              </div>
              {connector}
            </div>
            """.format(
                new_cls=" flow-node-nav" if url_changed and prev_url is not None else "",
                tooltip=_escape(url),
                index=_escape(index),
                label=_escape(display_label),
                url_sub=url_sub,
                violation_badge=violation_badge,
                connector=connector,
            )
        )
        prev_url = url

    return '<div class="screen-flow">{}</div>'.format("".join(nodes))


def _render_screen_matrix(
    screens_ordered: List[Any],
    matrix: Dict[str, Dict[str, str]],
    active_criteria: List[Dict[str, Any]],
) -> str:
    if not screens_ordered or not active_criteria:
        return '<p class="muted">No cross-screen criterion evidence available to build a matrix.</p>'

    header_cells = ["<th class='matrix-corner'>Screen</th>"]
    for criterion in active_criteria:
        cid = criterion.get("id", "")
        name = criterion.get("name", "")
        short_name = name if len(name) <= 28 else name[:26] + "…"
        level = criterion.get("level", "")
        header_cells.append(
            '<th class="matrix-th" title="{name}">'
            '<span class="matrix-cid">{cid}</span>'
            '<span class="matrix-cname">{short_name}</span>'
            '<span class="matrix-clevel">{level}</span>'
            "</th>".format(
                name=_escape(name),
                cid=_escape(cid),
                short_name=_escape(short_name),
                level=_escape(level),
            )
        )

    rows = []
    for screen_label, page_url, _step in screens_ordered:
        row_cells = ['<td class="matrix-screen-cell" title="{}">{}</td>'.format(
            _escape(page_url), _escape(screen_label)
        )]
        screen_outcomes = matrix.get(screen_label, {})
        for criterion in active_criteria:
            cid = criterion.get("id", "")
            outcome = screen_outcomes.get(cid)
            if outcome is None:
                row_cells.append('<td class="matrix-cell matrix-empty" title="No evidence on this screen">·</td>')
            else:
                cell_class = _OUTCOME_CELL_CLASS.get(outcome, "matrix-na")
                symbol = _OUTCOME_CELL_SYMBOL.get(outcome, "?")
                row_cells.append(
                    '<td class="matrix-cell {cls}" title="{outcome}">{sym}</td>'.format(
                        cls=cell_class,
                        outcome=_escape(outcome),
                        sym=symbol,
                    )
                )
        rows.append("<tr>{}</tr>".format("".join(row_cells)))

    legend_items = [
        ("matrix-failed", "✕ Failed"),
        ("matrix-review", "~ Needs Review"),
        ("matrix-passed", "✓ Passed"),
        ("matrix-na", "– Not Applicable"),
        ("matrix-empty", "· No evidence"),
    ]
    legend_html = "".join(
        '<span class="matrix-legend-item"><span class="matrix-cell {cls}">{label[0]}</span>{label}</span>'.format(
            cls=cls, label=label
        )
        for cls, label in legend_items
    )

    return """
    <div class="matrix-legend">{legend}</div>
    <div class="matrix-scroll-wrap">
      <table class="screen-matrix">
        <thead><tr>{headers}</tr></thead>
        <tbody>{rows}</tbody>
      </table>
    </div>
    """.format(
        legend=legend_html,
        headers="".join(header_cells),
        rows="".join(rows),
    )
