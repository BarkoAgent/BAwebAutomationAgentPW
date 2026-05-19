from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any, Dict, List

from ._helpers import _escape
from ._template import HTML_TEMPLATE, STAKEHOLDER_TEMPLATE


def _fmt_timestamp(iso_str: str) -> str:
    try:
        dt = datetime.fromisoformat(iso_str)
        return "{} {}, {} UTC".format(dt.day, dt.strftime("%b %Y"), dt.strftime("%H:%M"))
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
            formatted_date = "{} {}, {}".format(dt.day, dt.strftime("%b %Y"), time_part)
        except Exception:
            formatted_date = "{} {}".format(date_part, time_part)
        return "{} · {}".format(slug, formatted_date) if slug else formatted_date
    return rid.replace("-", " ").title()


_STAKEHOLDER_RATIONALE_LABELS: Dict[str, str] = {
    "all_checked_clean": "Verified clean",
    "axe_rule_clean": "Verified clean",
    "heuristic_proxy": "Heuristically clean",
    "no_applicable_elements": "No applicable elements",
    "limitation_pass": "Scanned but inconclusive",
}


def render_stakeholder_summary(report: Dict[str, Any], detail_artifact: str = "") -> str:
    """Single-page exec-readable summary. Pulls from the same report dict."""
    meta = report.get("report_meta", {}) or {}
    summary = report.get("summary", {}) or {}
    criteria = report.get("criteria", []) or []
    execution = report.get("execution", {}) or {}
    manifests = report.get("evaluator_manifests", []) or []

    outcome_counts = summary.get("outcome_counts", {}) or {}
    rationale_counts = summary.get("pass_rationale_counts", {}) or {}

    # Headline: four buckets the stakeholder actually cares about.
    verified_clean = rationale_counts.get("all_checked_clean", 0) + rationale_counts.get("axe_rule_clean", 0)
    heuristic = rationale_counts.get("heuristic_proxy", 0)
    inconclusive = rationale_counts.get("limitation_pass", 0)
    no_elements = rationale_counts.get("no_applicable_elements", 0)
    failed = outcome_counts.get("FAILED", 0) + outcome_counts.get("ERROR", 0)
    review = outcome_counts.get("NEEDS_REVIEW", 0)

    headline_cards = [
        ("fail", failed + review, "Issues to fix or verify",
         "Failed criteria plus open questions awaiting human confirmation."),
        ("pass", verified_clean, "Verified clean",
         "Tested with full coverage and no violations."),
        ("soft", inconclusive + heuristic, "Soft passes",
         "Scanned but either inconclusive or only sampled — manual confirmation recommended."),
        ("review", no_elements, "Not exercised",
         "Passed because no applicable elements were present in the tested flow."),
    ]
    headline_html = "".join(
        '<div class="count-card {cls}"><span class="num">{n}</span>'
        '<span class="lbl">{label}</span><span class="desc">{desc}</span></div>'.format(
            cls=cls, n=n, label=_escape(label), desc=_escape(desc)
        )
        for cls, n, label, desc in headline_cards
    )

    # Top gaps: failed + needs-review criteria, sorted by level (A > AA > AAA), capped.
    level_rank = {"A": 0, "AA": 1, "AAA": 2}
    gaps = sorted(
        [c for c in criteria if c.get("outcome_status") in ("FAILED", "ERROR", "NEEDS_REVIEW")],
        key=lambda c: (level_rank.get(c.get("level", "AA"), 1), c.get("id", "")),
    )
    if gaps:
        gap_items = []
        for c in gaps[:10]:
            gap_items.append(
                "<li><strong>{cid}</strong> {name} <span class='muted'>(Level {lvl} · {out})</span></li>".format(
                    cid=_escape(c.get("id", "")),
                    name=_escape(c.get("name", "")),
                    lvl=_escape(c.get("level", "")),
                    out=_escape(c.get("outcome_status", "")),
                )
            )
        if len(gaps) > 10:
            gap_items.append("<li class='muted'>+ {} more — see full report.</li>".format(len(gaps) - 10))
        top_gaps_html = "<ul>{}</ul>".format("".join(gap_items))
    else:
        top_gaps_html = "<p class='muted'>No failures or open questions in this run.</p>"

    # Not testable by automation: criteria with default coverage MANUAL_REQUIRED.
    not_testable = [
        c for c in criteria
        if c.get("coverage_status") in ("MANUAL_REQUIRED", "NOT_TESTED") and not c.get("manifest_refs")
    ]
    if not_testable:
        nt_items = "".join(
            "<li><strong>{cid}</strong> {name} <span class='muted'>(Level {lvl})</span></li>".format(
                cid=_escape(c.get("id", "")),
                name=_escape(c.get("name", "")),
                lvl=_escape(c.get("level", "")),
            )
            for c in not_testable[:15]
        )
        more = ""
        if len(not_testable) > 15:
            more = "<p class='muted'>+ {} more — see full report.</p>".format(len(not_testable) - 15)
        not_testable_html = "<ul>{}</ul>{}".format(nt_items, more)
    else:
        not_testable_html = "<p class='muted'>All scoped criteria have at least partial automation coverage.</p>"

    # Methodology TL;DR — one-liner per custom evaluator.
    custom_manifests = [m for m in manifests if not (m.get("id") or "").startswith("axe:")]
    method_lines = "".join(
        "<li><strong>{name}</strong> <span class='muted'>({crit})</span></li>".format(
            name=_escape(m.get("name", "")),
            crit=_escape(", ".join(m.get("criteria") or []) or "—"),
        )
        for m in custom_manifests
    )
    method_html = "<ul>{}</ul>".format(method_lines) if method_lines else "<p class='muted'>No evaluator manifests recorded.</p>"

    # Scope: what was visited.
    journey_steps = execution.get("journey_steps", []) or []
    scope_lines = ["<p><strong>Standard:</strong> {}</p>".format(_escape(meta.get("standard_profile", "")))]
    if journey_steps:
        urls = ", ".join({s.get("page_url", "") for s in journey_steps if s.get("page_url")})
        scope_lines.append("<p><strong>URLs visited:</strong> {}</p>".format(_escape(urls or "—")))
        scope_lines.append("<p><strong>Checkpoints captured:</strong> {}</p>".format(len(journey_steps)))

    return STAKEHOLDER_TEMPLATE.format(
        title=_escape(meta.get("audit_name") or meta.get("page_title") or "Accessibility Report"),
        generated_at=_escape(_fmt_timestamp(meta.get("generated_at", ""))),
        detail_link=_escape(detail_artifact or "#"),
        scope_block="".join(scope_lines),
        headline_counts=headline_html,
        top_gaps=top_gaps_html,
        not_testable=not_testable_html,
        custom_count=len(custom_manifests),
        checkpoint_count=len(journey_steps) or 1,
        methodology_tldr=method_html,
    )


def _render_methodology_section(manifests: List[Dict[str, Any]]) -> str:
    """Top-of-report block listing every evaluator manifest.

    Stakeholder-facing answer to 'what did this tool actually do?'.
    """
    if not manifests:
        return '<p class="muted">No evaluator manifests recorded for this run.</p>'

    custom = [m for m in manifests if not (m.get("id") or "").startswith("axe:")]
    axe_block = [m for m in manifests if (m.get("id") or "").startswith("axe:")]

    def _ul(items: List[str]) -> str:
        items = [i for i in items or [] if i]
        if not items:
            return '<p class="muted">None.</p>'
        return "<ul>{}</ul>".format("".join("<li>{}</li>".format(_escape(i)) for i in items))

    def _card(m: Dict[str, Any]) -> str:
        criteria = ", ".join(m.get("criteria") or []) or "—"
        sampling = m.get("sampling") or ""
        sampling_html = '<p><span class="label-inline">Sampling:</span> {}</p>'.format(_escape(sampling)) if sampling else ""
        return (
            '<details class="methodology-card">'
            "<summary><strong>{name}</strong> "
            '<span class="muted">— {criteria} · {mode}</span></summary>'
            '<div class="methodology-card-body">'
            "<h5>What was checked</h5>{tested}"
            "<h5>What was not checked</h5>{untested}"
            "<h5>Automation limits</h5>{limits}"
            "<h5>Manual follow-up</h5>{manual}"
            "{sampling_html}"
            '<p class="muted"><span class="label-inline">Source ID:</span> <code>{src}</code></p>'
            "</div>"
            "</details>"
        ).format(
            name=_escape(m.get("name") or m.get("id") or ""),
            criteria=_escape(criteria),
            mode=_escape(m.get("coverage_mode") or ""),
            tested=_ul(m.get("what_tested")),
            untested=_ul(m.get("what_not_tested")),
            limits=_ul(m.get("automation_limits")),
            manual=_ul(m.get("manual_followup")),
            sampling_html=sampling_html,
            src=_escape(m.get("id") or ""),
        )

    custom_html = "".join(_card(m) for m in custom)
    axe_html = "".join(_card(m) for m in axe_block[:6])  # cap to first 6 axe-criterion cards
    axe_more = ""
    if len(axe_block) > 6:
        axe_more = (
            '<details class="methodology-axe-more">'
            "<summary>Show {n} more axe-core criterion blocks</summary>"
            "<div>{html}</div></details>"
        ).format(
            n=len(axe_block) - 6,
            html="".join(_card(m) for m in axe_block[6:]),
        )

    return (
        '<div class="methodology-section">'
        '<p class="muted">Each block below describes one evaluator: what slice of the criterion it probes, '
        'what it does <em>not</em> probe, where automation hits a wall, and what to verify by hand.</p>'
        '<h4>Custom evaluators</h4>{custom}'
        '<h4>axe-core (per criterion)</h4>{axe}{axe_more}'
        '</div>'
    ).format(custom=custom_html, axe=axe_html, axe_more=axe_more)


def _serialize_report_payload(report: Dict[str, Any]) -> str:
    """Serialise the full report dict for inline embedding in the HTML.

    The new design renders entirely client-side from the JSON payload, so
    we ship the whole `report` (criteria with evidence, sections, execution,
    evaluator_manifests, summary, meta) verbatim. We escape `</` to prevent
    accidental closure of the surrounding `<script>` tag.
    """
    raw = json.dumps(report, default=str, ensure_ascii=False)
    return raw.replace("</", "<\\/")


def render_html_report(report: Dict[str, Any]) -> str:
    meta = report.get("report_meta", {}) or {}
    title = meta.get("audit_name") or meta.get("page_title") or "Accessibility Report"
    payload = _serialize_report_payload(report)
    return (
        HTML_TEMPLATE
        .replace("__REPORT_TITLE__", _escape(title))
        .replace("__REPORT_JSON__", payload)
    )
