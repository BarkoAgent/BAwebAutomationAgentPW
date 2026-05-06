from __future__ import annotations

from typing import Any, Dict, List, Optional

from ._helpers import _escape, _status_badge, _render_list


_ACTION_LABEL_MAP = {
    "navigate_to_url": "Navigation step",
    "get_page_html": "HTML snapshot",
    "click": "Click interaction",
    "double_click": "Double click interaction",
    "right_click": "Right click interaction",
    "send_keys": "Field input",
    "exists": "Visibility check",
    "does_not_exist": "Absence check",
    "exists_with_text": "Text verification",
    "scroll_to_element": "Scroll step",
    "select_native_dropdown": "Dropdown selection",
    "refresh_page": "Page refresh",
    "change_windows_tabs": "Window or tab change",
    "change_frame_by_id": "Frame change",
    "change_frame_by_locator": "Frame change",
    "change_frame_to_original": "Return to main frame",
    "upload_file_to_form": "File upload",
    "wait_for_download": "Download wait",
    "maximize_window": "Viewport change",
}

_GENERIC_PREFIXES = tuple(_ACTION_LABEL_MAP.keys())


def _display_step_label(action: str, label: str, audit_label: str) -> str:
    raw = (audit_label or label or "").strip()
    normalized = raw.lower()
    if raw and not normalized.startswith(_GENERIC_PREFIXES):
        return raw
    return _ACTION_LABEL_MAP.get(action, raw or action.replace("_", " "))


def _execution_limitations(execution: Dict[str, Any], summary: Dict[str, Any]) -> List[str]:
    notes = list(execution.get("notes", []) or [])
    limitations: List[str] = []
    for note in notes:
        cleaned = str(note).strip()
        if cleaned:
            limitations.append(cleaned)

    outcome_counts = summary.get("outcome_counts", {})
    if outcome_counts.get("NOT_TESTED", 0):
        limitations.append(
            "{} criteria remain not tested in this run and still require broader automation or manual review.".format(
                outcome_counts["NOT_TESTED"]
            )
        )

    journey_steps = execution.get("journey_steps", []) or []
    if len(journey_steps) <= 1:
        limitations.append("This report captured only one audit checkpoint, so step-level issue location is still shallow.")

    if not any((step.get("axe_status") or "").lower() == "success" for step in journey_steps):
        limitations.append("Axe did not complete successfully in this run, so automated rule coverage is partial.")

    return limitations


def _scenario_summary(execution: Dict[str, Any], summary: Dict[str, Any]) -> List[str]:
    steps = execution.get("scenario_steps_executed", []) or []
    checkpoints = execution.get("journey_steps", []) or []
    outcome_counts = summary.get("outcome_counts", {})
    statements = [
        "Scenario steps recorded: {}.".format(len(steps)),
        "Accessibility checkpoints recorded: {}.".format(len(checkpoints)),
        "Criteria needing review: {}.".format(outcome_counts.get("NEEDS_REVIEW", 0)),
    ]
    if outcome_counts.get("FAILED", 0):
        statements.append("Criteria failed automatically: {}.".format(outcome_counts.get("FAILED", 0)))
    return statements


def _render_axe_snapshot_block(snapshot: Optional[str], report: Optional[str], violations_count: Optional[int]) -> str:
    parts = []

    count_label = ""
    if violations_count is not None:
        count_label = '<span class="violations-count">{} violation{}</span>'.format(
            violations_count, "s" if violations_count != 1 else ""
        )

    content = snapshot or report
    if not content:
        if count_label:
            return '<div class="axe-summary">{}</div>'.format(count_label)
        return ""

    _MAX_SNAPSHOT_CHARS = 8000
    truncated = False
    if len(content) > _MAX_SNAPSHOT_CHARS:
        content = content[:_MAX_SNAPSHOT_CHARS]
        truncated = True

    source_label = "Axe snapshot" if snapshot else "Axe report"
    truncation_note = " (truncated — see JSON report for full output)" if truncated else ""

    parts.append(
        """
        <details class="axe-snapshot-details">
          <summary>{count_label}<span class="snapshot-toggle-label">{source_label}{truncation_note}</span></summary>
          <pre class="axe-snapshot-pre">{content}</pre>
        </details>
        """.format(
            count_label=count_label + "&nbsp;" if count_label else "",
            source_label=_escape(source_label),
            truncation_note=_escape(truncation_note),
            content=_escape(content),
        )
    )
    return "".join(parts)


def _render_scenario_steps(steps: List[Dict[str, Any]]) -> str:
    if not steps:
        return '<p class="muted">No scenario execution steps were recorded in this report.</p>'

    cards = []
    for step in steps:
        status = step.get("status", "unknown").upper()
        action = step.get("action", "")
        result = step.get("result") or step.get("error") or ""
        result_meta = step.get("result_meta") or {}
        display_label = _display_step_label(action, step.get("label", ""), step.get("audit_label", ""))
        if action == "get_page_html":
            raw_length = result_meta.get("raw_length") or len(str(result))
            result = "HTML snapshot captured ({} chars omitted from report).".format(raw_length)
            result_meta = dict(result_meta)
            result_meta["omitted_large_output"] = True
        extra_note = ""
        if result_meta.get("omitted_large_output"):
            extra_note = '<p class="muted">Large step output was intentionally omitted from the shareable report.</p>'
        cards.append(
            """
            <article class="timeline-card">
              <div class="timeline-head">
                <strong>#{index} {label}</strong>
                {status_badge}
              </div>
              <div class="timeline-grid">
                <div><span class="label">Action</span><span><code>{action}</code></span></div>
                <div><span class="label">Audit Label</span><span>{audit_label}</span></div>
                <div><span class="label">Result</span><span>{result}</span></div>
              </div>
              {extra_note}
            </article>
            """.format(
                index=_escape(step.get("step_index", "")),
                label=_escape(display_label),
                status_badge=_status_badge(status),
                action=_escape(action),
                audit_label=_escape(display_label),
                result=_escape(result),
                extra_note=extra_note,
            )
        )
    return "".join(cards)


def _render_journey_steps(steps: List[Dict[str, Any]]) -> str:
    if not steps:
        return '<p class="muted">No audit checkpoints were recorded in this report.</p>'

    cards = []
    for step in steps:
        label = step.get("journey_step_label", "")
        snapshot_block = _render_axe_snapshot_block(
            snapshot=step.get("axe_snapshot"),
            report=step.get("axe_report"),
            violations_count=step.get("axe_violations_count"),
        )
        screenshot = step.get("screenshot") or ""
        screenshot_block = (
            '<div class="checkpoint-screenshot-wrap">'
            '<img class="checkpoint-screenshot" src="{}" alt="Page state at checkpoint" loading="lazy">'
            '</div>'.format(screenshot)
        ) if screenshot else ""
        cards.append(
            """
            <article class="timeline-card">
              <div class="timeline-head">
                <strong>#{index} {label}</strong>
                {axe_status}
              </div>
              <div class="timeline-grid">
                <div><span class="label">Page</span><span>{page_title}</span></div>
                <div><span class="label">URL</span><span>{page_url}</span></div>
                <div><span class="label">Browser</span><span>{browser}</span></div>
              </div>
              {screenshot_block}
              {snapshot_block}
            </article>
            """.format(
                index=_escape(step.get("journey_step_index", "")),
                label=_escape(_display_step_label("exists", label, label)),
                axe_status=_status_badge((step.get("axe_status") or "unknown").upper()),
                page_title=_escape(step.get("page_title", "")),
                page_url=_escape(step.get("page_url", "")),
                browser=_escape(step.get("browser", "")),
                screenshot_block=screenshot_block,
                snapshot_block=snapshot_block,
            )
        )
    return "".join(cards)
