from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict

from ._helpers import _escape, _render_counts, _render_list
from ._step_renderers import (
    _execution_limitations,
    _render_journey_steps,
    _render_scenario_steps,
    _scenario_summary,
)
from ._criterion_renderers import _render_criteria_overview, _render_criterion_panels
from ._screen_matrix import _build_screen_data, _render_screen_journey, _render_screen_matrix
from ._score_renderers import (
    build_report_json,
    compute_health_score,
    render_category_breakdown,
    render_compliance_badges,
    render_health_panel,
    render_outcome_donut,
    render_pillar_grid,
    render_top_components,
)
from ._template import HTML_TEMPLATE


def _fmt_timestamp(iso_str: str) -> str:
    try:
        dt = datetime.fromisoformat(iso_str)
        return dt.strftime("%-d %b %Y, %H:%M UTC")
    except Exception:
        return iso_str


def _fmt_report_id(report_id: str) -> str:
    # Strip leading "a11y_" prefix, then replace the trailing ISO timestamp
    # e.g. "a11y_bulk-accessibility-audit_2026-04-09T09-43-07Z"
    #   -> "Bulk Accessibility Audit · 9 Apr 2026, 09:43"
    rid = re.sub(r"^a11y_", "", report_id)
    # Extract trailing timestamp pattern like 2026-04-09T09-43-07Z
    m = re.search(r"_?(\d{4}-\d{2}-\d{2})T(\d{2})-(\d{2})-(\d{2})Z?$", rid)
    if m:
        slug = rid[: m.start()].replace("-", " ").title()
        date_part = m.group(1)
        time_part = "{}:{}".format(m.group(2), m.group(3))
        try:
            dt = datetime.strptime("{} {}".format(date_part, time_part), "%Y-%m-%d %H:%M")
            formatted_date = dt.strftime("%-d %b %Y, %H:%M")
        except Exception:
            formatted_date = "{} {}".format(date_part, time_part)
        return "{} · {}".format(slug, formatted_date) if slug else formatted_date
    return rid.replace("-", " ").title()


def render_html_report(report: Dict[str, Any]) -> str:
    meta       = report.get("report_meta", {})
    execution  = report.get("execution", {})
    summary    = report.get("summary", {})
    criteria   = report.get("criteria", [])
    sections   = report.get("sections", [])
    journey_steps = execution.get("journey_steps", [])

    outcome_counts = _render_counts(
        summary.get("outcome_counts", {}),
        ["FAILED", "NEEDS_REVIEW", "PASSED", "NOT_TESTED", "NOT_APPLICABLE", "ERROR"],
    )
    coverage_counts = _render_counts(
        summary.get("coverage_counts", {}),
        ["AUTOMATED", "SEMI_AUTOMATED", "MANUAL_REQUIRED", "NOT_TESTED", "NOT_APPLICABLE"],
    )

    screens_ordered, screen_matrix, active_criteria = _build_screen_data(criteria, journey_steps)

    # ── New enhanced sections ──────────────────────────────────────────────────
    score = compute_health_score(criteria)

    return HTML_TEMPLATE.format(
        title=_escape(meta.get("audit_name") or meta.get("page_title") or "Accessibility Report"),
        url=_escape(meta.get("url", "")),
        report_id=_escape(_fmt_report_id(meta.get("report_id", ""))),
        generated_at=_escape(_fmt_timestamp(meta.get("generated_at", ""))),
        page_title=_escape(meta.get("page_title", "")),
        standard_profile=_escape(meta.get("standard_profile", "")),
        limitations=_render_list(_execution_limitations(execution, summary), "No explicit limitations recorded."),
        scenario_summary=_render_list(_scenario_summary(execution, summary), "No scenario summary available."),
        screen_journey=_render_screen_journey(journey_steps),
        screen_matrix=_render_screen_matrix(screens_ordered, screen_matrix, active_criteria),
        outcome_counts=outcome_counts,
        coverage_counts=coverage_counts,
        scenario_steps=_render_scenario_steps(execution.get("scenario_steps_executed", [])),
        journey_steps=_render_journey_steps(journey_steps),
        criteria_overview=_render_criteria_overview(criteria, sections),
        criterion_panels=_render_criterion_panels(criteria),
        # New placeholders
        health_panel=render_health_panel(score, criteria),
        pillar_grid=render_pillar_grid(criteria),
        category_breakdown=render_category_breakdown(criteria),
        top_components=render_top_components(criteria),
        outcome_donut=render_outcome_donut(criteria),
        compliance_badges=render_compliance_badges(criteria),
        report_json=build_report_json(report),
    )
