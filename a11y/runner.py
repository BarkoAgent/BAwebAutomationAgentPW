from __future__ import annotations

import asyncio
import base64
import json
import os
import re
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import ba_ws_sdk.file_system as file_system

from .evaluators import (
    run_context_change_evaluator,
    run_focus_visibility_evaluator,
    run_focus_appearance_evaluator,
    run_form_labeling_evaluator,
    run_hover_content_evaluator,
    run_keyboard_smoke_evaluator,
    run_live_region_evaluator,
    run_media_alternatives_evaluator,
    run_motion_preference_evaluator,
    run_orientation_evaluator,
    run_pointer_target_evaluator,
    run_structure_evaluator,
    run_text_resize_evaluator,
    run_timing_evaluator,
    run_viewport_reflow_evaluator,
)
from .axe_adapter import run_axe_scan
from .mappings import criterion_ids_from_axe
from .models import (
    COVERAGE_AUTOMATED,
    COVERAGE_MANUAL_REQUIRED,
    COVERAGE_NOT_TESTED,
    COVERAGE_SEMI_AUTOMATED,
    EvidenceItem,
    EvidenceLocation,
    CriterionResult,
    AccessibilityReport,
    ReportSection,
    OUTCOME_ERROR,
    OUTCOME_FAILED,
    OUTCOME_NEEDS_REVIEW,
    OUTCOME_NOT_APPLICABLE,
    OUTCOME_NOT_TESTED,
    OUTCOME_PASSED,
)
from .reporting import build_digest_json, render_html_report
from .registry import SECTION_IDS, build_registry


LAST_REPORT_BY_RUN: Dict[str, str] = {}

# Per-evaluator timeout (each evaluator in asyncio.gather gets this cap).
# Axe scan gets its own (larger) cap since it scans the full DOM.
# Both configurable via env vars for heavy-page deployments.
_EVALUATOR_TIMEOUT_S = float(os.getenv("A11Y_EVALUATOR_TIMEOUT_S", "60"))
_AXE_TIMEOUT_S = float(os.getenv("A11Y_AXE_TIMEOUT_S", "90"))


def _parse_bool(value: str, default: bool) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _normalize_network_idle_mode(value: str, default: str = "always") -> str:
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return "always"
    if text in {"0", "false", "no", "off"}:
        return "never"
    if text in {"always", "navigation_only", "never"}:
        return text
    return default


def _normalize_screen_label(page_title: str, page_path: str, url: str) -> str:
    if page_title:
        return page_title.strip()
    if page_path and page_path != "/":
        return page_path.strip("/")
    return url


def _normalize_screen_key(page_path: str, page_title: str) -> str:
    source = page_path.strip("/") or page_title.strip() or "page"
    source = source.lower()
    return re.sub(r"[^a-z0-9]+", "-", source).strip("-") or "page"


def _step_screen_label(journey_step_label: str, journey_step_index: int, page_title: str, page_path: str, url: str) -> str:
    """
    Produce a screen label that is unique per checkpoint step, not per URL.
    This ensures that DOM state changes within the same URL (e.g. login form empty
    vs. filled vs. validation error) each appear as distinct screens in the matrix.
    """
    base = _normalize_screen_label(page_title, page_path, url)
    step = (journey_step_label or "").strip()
    if step and step.lower() != base.lower():
        return "{} — {}".format(step, base) if base else step
    if step:
        return step
    return "Step {} — {}".format(journey_step_index, base) if base else "Step {}".format(journey_step_index)


def _base_location(
    report_id: str,
    page_url: str,
    page_title: str,
    journey_step_label: str,
    journey_step_index: int,
    journey_name: str,
    frame_name: Optional[str],
    locator: str,
    element_text: str,
    report_anchor: str,
    view_type: str = "page",
) -> EvidenceLocation:
    parsed = urlparse(page_url)
    page_path = parsed.path or "/"
    screen_label = _step_screen_label(journey_step_label, journey_step_index, page_title, page_path, page_url)
    screen_key = _normalize_screen_key(page_path, page_title)
    return EvidenceLocation(
        page_url=page_url,
        page_path=page_path,
        page_title=page_title,
        screen_key=screen_key,
        screen_label=screen_label,
        journey_name=journey_name,
        journey_step_index=journey_step_index,
        journey_step_label=journey_step_label,
        view_type=view_type,
        container_label=None,
        frame_name=frame_name,
        modal_label=None,
        locator=locator,
        element_text=element_text,
        screenshot_ref=None,
        report_anchor=report_anchor,
    )


def _criterion_result_from_definition(definition) -> CriterionResult:
    coverage_status = definition.default_coverage
    if definition.default_coverage != COVERAGE_MANUAL_REQUIRED:
        coverage_status = COVERAGE_NOT_TESTED

    return CriterionResult(
        id=definition.id,
        kind=definition.kind,
        name=definition.name,
        principle=definition.principle,
        guideline=definition.guideline,
        level=definition.level,
        coverage_status=coverage_status,
        outcome_status=OUTCOME_NOT_TESTED,
        doc_url=definition.doc_url,
        coverage_notes=list(definition.notes),
        not_tested_explanation="No evidence collected for this criterion in the current run.",
    )


def _apply_evidence(
    criterion: CriterionResult,
    evidence: EvidenceItem,
    bucket_name: str,
    bucket_payload: Dict[str, Any],
    coverage_status: Optional[str] = None,
) -> None:
    criterion.add_source(evidence.source)
    criterion.add_affected_location(evidence.location)
    criterion.evidence.append(evidence)
    criterion.not_tested_explanation = ""
    if coverage_status is not None:
        criterion.coverage_status = coverage_status

    if bucket_name == "failed_nodes":
        criterion.failed_nodes.append(bucket_payload)
        criterion.outcome_status = OUTCOME_FAILED
    elif bucket_name == "incomplete_checks":
        criterion.incomplete_checks.append(bucket_payload)
        if criterion.outcome_status != OUTCOME_FAILED:
            criterion.outcome_status = OUTCOME_NEEDS_REVIEW
    elif bucket_name == "passed_checks":
        criterion.passed_checks.append(bucket_payload)
        if criterion.outcome_status == OUTCOME_NOT_TESTED:
            criterion.outcome_status = OUTCOME_PASSED


def _apply_custom_check_results(
    criteria_by_id: Dict[str, CriterionResult],
    check_results: List[Dict[str, Any]],
    report_id: str,
    page_url: str,
    page_title: str,
    journey_name: str,
    journey_step_label: str,
    journey_step_index: int,
    frame_name: Optional[str],
) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []

    for check in check_results:
        criterion = criteria_by_id.get(check["criterion_id"])
        if criterion is None:
            continue

        anchor = "criterion-{}-issue-{}".format(check["criterion_id"], len(criterion.evidence) + 1)
        location = _base_location(
            report_id=report_id,
            page_url=page_url,
            page_title=page_title,
            journey_step_label=journey_step_label,
            journey_step_index=journey_step_index,
            journey_name=journey_name,
            frame_name=frame_name,
            locator=check.get("locator", ""),
            element_text=check.get("element_text", ""),
            report_anchor=anchor,
        )
        if check.get("screenshot_b64"):
            location.screenshot_ref = "data:image/jpeg;base64,{}".format(check["screenshot_b64"])
        evidence = EvidenceItem(
            source=check["source"],
            severity=check["severity"],
            target=[check.get("locator", "")] if check.get("locator") else [],
            message=check["message"],
            outcome=check["outcome"],
            location=location,
            metadata=check.get("metadata", {}),
        )

        if check["outcome"] == OUTCOME_FAILED:
            bucket_name = "failed_nodes"
        elif check["outcome"] == OUTCOME_NEEDS_REVIEW:
            bucket_name = "incomplete_checks"
        else:
            bucket_name = "passed_checks"

        _apply_evidence(
            criterion,
            evidence,
            bucket_name,
            {
                "source": check["source"],
                "message": check["message"],
                "metadata": check.get("metadata", {}),
            },
            coverage_status=check.get("coverage_status"),
        )
        normalized.append(check)

    return normalized


async def _run_metadata_checks(
    criteria_by_id: Dict[str, CriterionResult],
    page: Any,
    report_id: str,
    page_url: str,
    page_title: str,
    journey_name: str,
    journey_step_label: str,
    journey_step_index: int,
    frame_name: Optional[str],
) -> List[Dict[str, Any]]:
    custom_results: List[Dict[str, Any]] = []
    html_lang = await page.evaluate("() => document.documentElement.getAttribute('lang') || ''")
    checks = [
        {
            "criterion_id": "2.4.2",
            "source": "metadata:page-title",
            "passed": bool(page_title and page_title.strip()),
            "message": "Page has a descriptive document title." if page_title and page_title.strip() else "Document title is missing or empty.",
            "locator": "document.title",
            "element_text": page_title or "",
            "metadata": {"title": page_title or ""},
        },
        {
            "criterion_id": "3.1.1",
            "source": "metadata:html-lang",
            "passed": bool(html_lang.strip()),
            "message": "HTML root has a language value." if html_lang.strip() else "<html> is missing a lang attribute.",
            "locator": "html",
            "element_text": html_lang or "",
            "metadata": {"lang": html_lang or ""},
        },
    ]

    for check in checks:
        criterion = criteria_by_id.get(check["criterion_id"])
        if criterion is None:
            continue
        anchor = "criterion-{}-issue-{}".format(check["criterion_id"], len(criterion.evidence) + 1)
        location = _base_location(
            report_id=report_id,
            page_url=page_url,
            page_title=page_title,
            journey_step_label=journey_step_label,
            journey_step_index=journey_step_index,
            journey_name=journey_name,
            frame_name=frame_name,
            locator=check["locator"],
            element_text=check["element_text"],
            report_anchor=anchor,
        )
        evidence = EvidenceItem(
            source=check["source"],
            severity="moderate" if check["passed"] else "serious",
            target=[check["locator"]],
            message=check["message"],
            outcome=OUTCOME_PASSED if check["passed"] else OUTCOME_FAILED,
            location=location,
            metadata=check["metadata"],
        )
        bucket = "passed_checks" if check["passed"] else "failed_nodes"
        _apply_evidence(
            criterion,
            evidence,
            bucket,
            {
                "source": check["source"],
                "message": check["message"],
                "metadata": check["metadata"],
            },
            coverage_status=COVERAGE_AUTOMATED,
        )
        custom_results.append(
            {
                "source": check["source"],
                "criterion_id": check["criterion_id"],
                "passed": check["passed"],
                "metadata": check["metadata"],
            }
        )

    return custom_results


def _compact_node_text(node: Dict[str, Any]) -> str:
    html = (node.get("html") or "").strip()
    if len(html) > 160:
        return "{}...".format(html[:157])
    return html


# Maximum screenshots per axe rule so large violation lists don't bloat reports.
_AXE_SCREENSHOT_LIMIT_PER_RULE = 3


async def _attach_violation_screenshots(
    page: Any,
    axe_payload: Dict[str, Any],
    rule_ids: Optional[set] = None,
) -> None:
    """Take element-level screenshots for axe violation nodes and store the
    base64 JPEG on each node as ``screenshot_b64``.  Only violations are
    screenshotted (incomplete / passes are left untouched).

    Args:
        page: Playwright page to screenshot against.
        axe_payload: The dict returned by ``run_axe_scan``.
        rule_ids: If given, only screenshot nodes belonging to these rule IDs.
                  Pass ``None`` to screenshot all violation rules.
    """
    if axe_payload.get("status") != "success" or not axe_payload.get("results"):
        return

    for rule in axe_payload["results"].get("violations", []) or []:
        rule_id = rule.get("id", "")
        if rule_ids is not None and rule_id not in rule_ids:
            continue

        shot_count = 0
        for node in rule.get("nodes", []) or []:
            if shot_count >= _AXE_SCREENSHOT_LIMIT_PER_RULE:
                break
            target = node.get("target", [])
            screenshot_b64 = None
            # Try element-level screenshot first (cleaner view for the engineer).
            # ``target`` entries can be strings (plain CSS) or lists (shadow-DOM
            # path).  We only attempt element screenshots for plain CSS strings.
            css = target[0] if target and isinstance(target[0], str) else None
            if css:
                try:
                    loc = page.locator(css).first
                    await loc.scroll_into_view_if_needed(timeout=2000)
                    raw = await loc.screenshot(type="jpeg", quality=70)
                    screenshot_b64 = base64.b64encode(raw).decode()
                except Exception:
                    pass
            # Fall back to a full-viewport screenshot.
            if not screenshot_b64:
                try:
                    raw = await page.screenshot(full_page=False, type="jpeg", quality=55)
                    screenshot_b64 = base64.b64encode(raw).decode()
                except Exception:
                    pass
            if screenshot_b64:
                node["screenshot_b64"] = screenshot_b64
                shot_count += 1


def _register_axe_results(
    criteria_by_id: Dict[str, CriterionResult],
    axe_payload: Dict[str, Any],
    report_id: str,
    page_url: str,
    page_title: str,
    journey_name: str,
    journey_step_label: str,
    journey_step_index: int,
    frame_name: Optional[str],
) -> None:
    if axe_payload.get("status") != "success" or not axe_payload.get("results"):
        return

    results = axe_payload["results"]
    result_buckets = [
        ("violations", OUTCOME_FAILED, "failed_nodes"),
        ("incomplete", OUTCOME_NEEDS_REVIEW, "incomplete_checks"),
        ("passes", OUTCOME_PASSED, "passed_checks"),
    ]

    for result_type, outcome, bucket_name in result_buckets:
        for rule in results.get(result_type, []) or []:
            criterion_ids = criterion_ids_from_axe(rule.get("id", ""), rule.get("tags", []))
            if not criterion_ids:
                continue
            nodes = rule.get("nodes", []) or [{}]
            for criterion_id in criterion_ids:
                criterion = criteria_by_id.get(criterion_id)
                if criterion is None:
                    continue

                for node in nodes:
                    locator = ""
                    if node.get("target"):
                        locator = node["target"][0]
                    anchor = "criterion-{}-issue-{}".format(criterion_id, len(criterion.evidence) + 1)
                    location = _base_location(
                        report_id=report_id,
                        page_url=page_url,
                        page_title=page_title,
                        journey_step_label=journey_step_label,
                        journey_step_index=journey_step_index,
                        journey_name=journey_name,
                        frame_name=frame_name,
                        locator=locator or rule.get("id", ""),
                        element_text=_compact_node_text(node),
                        report_anchor=anchor,
                    )
                    if node.get("screenshot_b64"):
                        location.screenshot_ref = "data:image/jpeg;base64,{}".format(node["screenshot_b64"])
                    # Extract per-node check data (e.g. contrast ratio, colours, reason)
                    node_check_data: Dict[str, Any] = {}
                    for _chk in (node.get("any") or []) + (node.get("all") or []):
                        if _chk.get("id") == rule.get("id"):
                            node_check_data = _chk.get("data") or {}
                            break
                    evidence = EvidenceItem(
                        source="axe:{}".format(rule.get("id", "unknown-rule")),
                        severity=rule.get("impact") or "unknown",
                        target=node.get("target") or [],
                        message=node.get("failureSummary") or rule.get("help") or rule.get("description") or rule.get("id", ""),
                        outcome=outcome,
                        location=location,
                        metadata={
                            "help": rule.get("help"),
                            "helpUrl": rule.get("helpUrl"),
                            "tags": rule.get("tags", []),
                            "nodeCheckData": node_check_data,
                        },
                    )
                    _apply_evidence(
                        criterion,
                        evidence,
                        bucket_name,
                        {
                            "rule_id": rule.get("id"),
                            "impact": rule.get("impact"),
                            "tags": rule.get("tags", []),
                            "target": node.get("target", []),
                            "html": node.get("html", ""),
                        },
                        coverage_status=COVERAGE_AUTOMATED,
                    )

    # inapplicable rules: the axe rule found no matching elements on this page.
    # Mark those criteria as NOT_APPLICABLE so they are not counted as NOT_TESTED.
    for rule in results.get("inapplicable", []) or []:
        criterion_ids = criterion_ids_from_axe(rule.get("id", ""), rule.get("tags", []))
        for criterion_id in criterion_ids:
            criterion = criteria_by_id.get(criterion_id)
            if criterion is None:
                continue
            # Only promote NOT_TESTED → NOT_APPLICABLE; don't downgrade real results.
            if criterion.outcome_status == OUTCOME_NOT_TESTED:
                criterion.outcome_status = OUTCOME_NOT_APPLICABLE
                criterion.coverage_status = COVERAGE_AUTOMATED
                criterion.not_tested_explanation = ""
                criterion.coverage_notes.append(
                    "Axe rule '{}' found no applicable elements on this page.".format(rule.get("id", ""))
                )
                criterion.add_source("axe:{}".format(rule.get("id", "")))


def _build_sections(criteria: List[CriterionResult]) -> List[ReportSection]:
    grouped: Dict[str, List[str]] = {}
    for criterion in criteria:
        grouped.setdefault(criterion.principle, []).append(criterion.id)

    sections: List[ReportSection] = []
    ordering = ["Perceivable", "Operable", "Understandable", "Robust", "Conformance Requirements"]
    for title in ordering:
        rows = grouped.get(title, [])
        if not rows:
            continue
        sections.append(
            ReportSection(
                id=SECTION_IDS[title],
                title=title,
                rows=rows,
            )
        )
    return sections


def _summary_from_criteria(criteria: List[CriterionResult]) -> Dict[str, Any]:
    outcome_counts: Dict[str, int] = {
        OUTCOME_PASSED: 0,
        OUTCOME_FAILED: 0,
        OUTCOME_NEEDS_REVIEW: 0,
        OUTCOME_NOT_TESTED: 0,
        OUTCOME_NOT_APPLICABLE: 0,
        OUTCOME_ERROR: 0,
    }
    coverage_counts: Dict[str, int] = {
        COVERAGE_AUTOMATED: 0,
        COVERAGE_SEMI_AUTOMATED: 0,
        COVERAGE_MANUAL_REQUIRED: 0,
        COVERAGE_NOT_TESTED: 0,
    }
    for criterion in criteria:
        outcome_counts[criterion.outcome_status] = outcome_counts.get(criterion.outcome_status, 0) + 1
        coverage_counts[criterion.coverage_status] = coverage_counts.get(criterion.coverage_status, 0) + 1
    return {
        "total_rows": len(criteria),
        "outcome_counts": outcome_counts,
        "coverage_counts": coverage_counts,
    }


def _report_storage_dir() -> Path:
    reports_dir = Path(os.getenv("A11Y_REPORTS_DIR", "./a11y_reports")).resolve()
    reports_dir.mkdir(parents=True, exist_ok=True)

    # One-time migration away from the agent attachments directory so accessibility
    # reports do not share storage with uploaded/downloaded user files.
    legacy_attachments_dir = file_system.get_attachments_dir()
    if legacy_attachments_dir.is_dir() and legacy_attachments_dir.resolve() != reports_dir:
        for legacy_path in legacy_attachments_dir.glob("a11y_*.*"):
            target_path = reports_dir / legacy_path.name
            if target_path.exists():
                continue
            try:
                shutil.move(str(legacy_path), str(target_path))
            except Exception:
                continue

    return reports_dir


def _artifact_path(report_id: str, suffix: str) -> Path:
    return _report_storage_dir() / "{}.{}".format(report_id, suffix)


def _persist_json_report(report: AccessibilityReport) -> str:
    path = _artifact_path(report.report_meta["report_id"], "json")
    path.write_text(json.dumps(report.to_dict(), indent=2), encoding="utf-8")
    return path.name


def _persist_html_report(report: AccessibilityReport) -> str:
    path = _artifact_path(report.report_meta["report_id"], "html")
    path.write_text(render_html_report(report.to_dict()), encoding="utf-8")
    return path.name


def _persist_digest_report(report: AccessibilityReport) -> str:
    path = _artifact_path(report.report_meta["report_id"] + "_digest", "json")
    path.write_text(build_digest_json(report.to_dict()), encoding="utf-8")
    return path.name


def _git_commit() -> str:
    try:
        output = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True,
        )
        return output.strip()
    except Exception:
        return "unknown"


def _browser_label(user_agent: str) -> str:
    if "Chrome/" in user_agent and "Edg/" not in user_agent:
        return "Chromium"
    if "Edg/" in user_agent:
        return "Microsoft Edge"
    if "Firefox/" in user_agent:
        return "Firefox"
    if "Safari/" in user_agent and "Chrome/" not in user_agent:
        return "Safari"
    return "Unknown"


def _new_report_id(audit_name: str = "") -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    if audit_name:
        slug = re.sub(r"[^a-z0-9]+", "-", audit_name.strip().lower()).strip("-")[:40]
        if slug:
            return "a11y_{}_{}".format(slug, timestamp)
    return "a11y_{}".format(timestamp)


async def _stabilize_page(page: Any, network_idle_mode: str, execution_notes: List[str]) -> None:
    try:
        await page.wait_for_load_state("domcontentloaded", timeout=20000)
    except Exception as exc:
        execution_notes.append("domcontentloaded wait failed: {}".format(exc))
    if network_idle_mode == "always":
        try:
            await page.wait_for_load_state("networkidle", timeout=15000)
        except Exception as exc:
            execution_notes.append("networkidle wait failed: {}".format(exc))
    await asyncio.sleep(0.4)


async def _create_shadow_page(page: Any, url: str, execution_notes: List[str]) -> Optional[Any]:
    """Open a new tab in the same browser context for isolated evaluation.

    Evaluators run on the shadow page so persistent JS mutations (event
    listeners, viewport changes, CSS injections) never reach the live test page.
    The caller is responsible for closing the returned page when done.
    Returns None if creation fails; the caller should then fall back to
    DOM-read-only checks on the original page.
    """
    try:
        shadow = await page.context.new_page()
        if url and url.startswith("http"):
            await shadow.goto(url, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(0.5)
        return shadow
    except Exception as exc:
        execution_notes.append(
            "Shadow page creation failed ({}); evaluating with read-only suite on original page.".format(exc)
        )
        return None


async def _collect_page_context(driver_state: Dict[str, Any]) -> Dict[str, Any]:
    page = driver_state.get("page")
    frame = driver_state.get("frame")
    active_frame = None
    try:
        if frame is not None and frame != page.main_frame:
            active_frame = frame
    except Exception:
        active_frame = None

    page_url = page.url
    try:
        page_title = await page.title()
    except Exception:
        page_title = ""

    try:
        user_agent = await page.evaluate("() => navigator.userAgent")
    except Exception:
        user_agent = "unknown"

    viewport = getattr(page, "viewport_size", None) or {}
    frame_name = None
    if active_frame is not None:
        try:
            frame_name = active_frame.name or None
        except Exception:
            frame_name = None

    return {
        "page": page,
        "page_url": page_url,
        "page_title": page_title,
        "user_agent": user_agent,
        "viewport": viewport,
        "frame_name": frame_name,
    }


def _create_session(
    driver_state: Dict[str, Any],
    audit_name: str,
    standard_profile: str,
    scope_selector: str,
    include_best_practices: str,
    include_experimental: str,
    include_manual_placeholders: str,
    viewport_profile: str,
    wait_for_network_idle: str,
    axe_full_scan: str,
    axe_custom_tags: str,
    axe_exclude_tags: str,
    axe_enabled_rules: str,
    axe_disabled_rules: str,
    axe_include_iframes: str,
    axe_include_selectors: str,
    axe_include_ancestry: str,
    axe_result_types: str,
    axe_reporter: str,
    _run_test_id: str,
) -> Dict[str, Any]:
    criteria = [_criterion_result_from_definition(row) for row in build_registry()]
    return {
        "driver_state": driver_state,
        "report_id": _new_report_id(audit_name),
        "audit_name": audit_name,
        "standard_profile": standard_profile,
        "scope_selector": scope_selector,
        "include_best_practices": _parse_bool(include_best_practices, True),
        "include_experimental": _parse_bool(include_experimental, False),
        "include_manual_placeholders": _parse_bool(include_manual_placeholders, True),
        "viewport_profile": viewport_profile,
        "wait_for_network_idle": _normalize_network_idle_mode(wait_for_network_idle, "always"),
        "axe_full_scan": _parse_bool(axe_full_scan, False),
        "axe_custom_tags": axe_custom_tags,
        "axe_exclude_tags": axe_exclude_tags,
        "axe_enabled_rules": axe_enabled_rules,
        "axe_disabled_rules": axe_disabled_rules,
        "axe_include_iframes": _parse_bool(axe_include_iframes, True),
        "axe_include_selectors": _parse_bool(axe_include_selectors, True),
        "axe_include_ancestry": _parse_bool(axe_include_ancestry, True),
        "axe_result_types": axe_result_types,
        "axe_reporter": axe_reporter,
        "_run_test_id": _run_test_id,
        "criteria": criteria,
        "criteria_by_id": {criterion.id: criterion for criterion in criteria},
        "execution_notes": [],
        "journey_steps": [],
        "scenario_steps_executed": [],
        "raw_sources": {
            "axe": [],
            "custom_checks": [],
        },
    }


async def append_accessibility_audit_checkpoint(
    session: Dict[str, Any],
    journey_step_label: str,
    journey_step_index: int,
    checkpoint_kind: str = "step",
) -> Dict[str, Any]:
    # Sentinel values used if page context collection fails early.
    page_url = ""
    page_title = ""
    user_agent = "unknown"
    viewport: Dict[str, Any] = {}
    shadow: Optional[Any] = None
    axe_payload: Dict[str, Any] = {"status": "skipped"}
    checkpoint_screenshot_b64: str = ""

    try:
        driver_state = session["driver_state"]
        context = await _collect_page_context(driver_state)
        page = context["page"]
        await _stabilize_page(page, session["wait_for_network_idle"], session["execution_notes"])
        context = await _collect_page_context(driver_state)

        page = context["page"]
        page_url = context["page_url"]
        page_title = context["page_title"]
        user_agent = context["user_agent"]
        viewport = context["viewport"]
        frame_name = context["frame_name"]

        try:
            _ss_bytes = await page.screenshot(full_page=False, type="jpeg", quality=55)
            checkpoint_screenshot_b64 = base64.b64encode(_ss_bytes).decode()
        except Exception:
            pass

        journey_name = session["audit_name"].strip() or "Accessibility audit"
        if frame_name:
            session["execution_notes"].append(
                "A frame was active during checkpoint '{}'; the scan targeted the current page object.".format(journey_step_label)
            )

        custom_results = await _run_metadata_checks(
            criteria_by_id=session["criteria_by_id"],
            page=page,
            report_id=session["report_id"],
            page_url=page_url,
            page_title=page_title,
            journey_name=journey_name,
            journey_step_label=journey_step_label,
            journey_step_index=journey_step_index,
            frame_name=frame_name,
        )
    except Exception as exc:
        session["execution_notes"].append(
            "Checkpoint setup error at step '{}': {}".format(journey_step_label, exc)
        )
        checkpoint = {
            "journey_step_index": journey_step_index,
            "journey_step_label": journey_step_label,
            "page_url": page_url,
            "page_title": page_title,
            "viewport": viewport,
            "browser": _browser_label(user_agent),
            "axe_status": "error",
            "axe_violations_count": None,
            "axe_snapshot": None,
            "axe_report": None,
            "screenshot": "data:image/jpeg;base64,{}".format(checkpoint_screenshot_b64) if checkpoint_screenshot_b64 else None,
        }
        session["journey_steps"].append(checkpoint)
        return checkpoint

    # All evaluators run on a shadow page (a new tab in the same browser
    # context) so the live test page is never mutated. Evaluators that click
    # buttons, send keypresses, resize the viewport or inject CSS are safe on
    # the shadow page; any persistent JS mutations are discarded when the
    # shadow page is closed. If shadow page creation fails we fall back to the
    # original page with only DOM-read-only evaluators.
    shadow = await _create_shadow_page(page, page_url, session["execution_notes"])
    eval_page = shadow if shadow is not None else page
    _evaluator_names = (
        "keyboard_smoke",
        "focus_visibility",
        "focus_appearance",
        "form_labeling",
        "hover_content",
        "structure",
        "live_region",
        "viewport_reflow",
        "text_resize",
        "timing",
        "pointer_target",
        "orientation",
        "motion_preference",
        "media_alternatives",
        "context_change",
    )
    try:
        _evaluator_tasks = await asyncio.gather(
            asyncio.wait_for(run_keyboard_smoke_evaluator(eval_page), timeout=_EVALUATOR_TIMEOUT_S),
            asyncio.wait_for(run_focus_visibility_evaluator(eval_page), timeout=_EVALUATOR_TIMEOUT_S),
            asyncio.wait_for(run_focus_appearance_evaluator(eval_page), timeout=_EVALUATOR_TIMEOUT_S),
            asyncio.wait_for(run_form_labeling_evaluator(eval_page), timeout=_EVALUATOR_TIMEOUT_S),
            asyncio.wait_for(run_hover_content_evaluator(eval_page), timeout=_EVALUATOR_TIMEOUT_S),
            asyncio.wait_for(run_structure_evaluator(eval_page), timeout=_EVALUATOR_TIMEOUT_S),
            asyncio.wait_for(run_live_region_evaluator(eval_page), timeout=_EVALUATOR_TIMEOUT_S),
            asyncio.wait_for(run_viewport_reflow_evaluator(eval_page, session["viewport_profile"]), timeout=_EVALUATOR_TIMEOUT_S),
            asyncio.wait_for(run_text_resize_evaluator(eval_page), timeout=_EVALUATOR_TIMEOUT_S),
            asyncio.wait_for(run_timing_evaluator(eval_page), timeout=_EVALUATOR_TIMEOUT_S),
            asyncio.wait_for(run_pointer_target_evaluator(eval_page), timeout=_EVALUATOR_TIMEOUT_S),
            asyncio.wait_for(run_orientation_evaluator(eval_page), timeout=_EVALUATOR_TIMEOUT_S),
            asyncio.wait_for(run_motion_preference_evaluator(eval_page), timeout=_EVALUATOR_TIMEOUT_S),
            asyncio.wait_for(run_media_alternatives_evaluator(eval_page), timeout=_EVALUATOR_TIMEOUT_S),
            asyncio.wait_for(run_context_change_evaluator(eval_page), timeout=_EVALUATOR_TIMEOUT_S),
            return_exceptions=True,
        )
        for _eval_name, _eval_result in zip(_evaluator_names, _evaluator_tasks):
            if isinstance(_eval_result, BaseException):
                session["execution_notes"].append(
                    "Evaluator '{}' raised an error: {}".format(_eval_name, _eval_result)
                )
                continue
            custom_results.extend(
                _apply_custom_check_results(
                    criteria_by_id=session["criteria_by_id"],
                    check_results=_eval_result,
                    report_id=session["report_id"],
                    page_url=page_url,
                    page_title=page_title,
                    journey_name=journey_name,
                    journey_step_label=journey_step_label,
                    journey_step_index=journey_step_index,
                    frame_name=frame_name,
                )
            )
        axe_payload = await asyncio.wait_for(
            run_axe_scan(
                page=eval_page,
                standard_profile=session["standard_profile"],
                scope_selector=session["scope_selector"],
                include_best_practices=session["include_best_practices"],
                include_experimental=session["include_experimental"],
                full_scan=session["axe_full_scan"],
                custom_tags=session["axe_custom_tags"],
                exclude_tags=session["axe_exclude_tags"],
                enabled_rules=session["axe_enabled_rules"],
                disabled_rules=session["axe_disabled_rules"],
                include_iframes=session["axe_include_iframes"],
                include_selectors=session["axe_include_selectors"],
                include_ancestry=session["axe_include_ancestry"],
                result_types=session["axe_result_types"],
                reporter=session["axe_reporter"],
            ),
            timeout=_AXE_TIMEOUT_S,
        )
        await _attach_violation_screenshots(
            eval_page,
            axe_payload,
            rule_ids=None,
        )
    except Exception as exc:
        session["execution_notes"].append(
            "Checkpoint evaluation error at step '{}': {}".format(journey_step_label, exc)
        )
    finally:
        if shadow is not None:
            try:
                await shadow.close()
            except Exception:
                pass
    if axe_payload.get("status") == "unavailable":
        session["execution_notes"].append(axe_payload["error"])
    elif axe_payload.get("status") == "error":
        session["execution_notes"].append("Axe scan failed: {}".format(axe_payload["error"]))
    _register_axe_results(
        criteria_by_id=session["criteria_by_id"],
        axe_payload=axe_payload,
        report_id=session["report_id"],
        page_url=page_url,
        page_title=page_title,
        journey_name=journey_name,
        journey_step_label=journey_step_label,
        journey_step_index=journey_step_index,
        frame_name=frame_name,
    )

    checkpoint = {
        "journey_step_index": journey_step_index,
        "journey_step_label": journey_step_label,
        "page_url": page_url,
        "page_title": page_title,
        "viewport": viewport,
        "browser": _browser_label(user_agent),
        "axe_status": axe_payload.get("status"),
        "axe_violations_count": axe_payload.get("violations_count"),
        "axe_snapshot": axe_payload.get("snapshot"),
        "axe_report": axe_payload.get("report"),
        "screenshot": "data:image/jpeg;base64,{}".format(checkpoint_screenshot_b64) if checkpoint_screenshot_b64 else None,
    }
    session["journey_steps"].append(checkpoint)
    session["raw_sources"]["axe"].append(
        {
            "journey_step_index": journey_step_index,
            "journey_step_label": journey_step_label,
            "page_url": page_url,
            "violations_count": axe_payload.get("violations_count"),
            "snapshot": axe_payload.get("snapshot"),
            "report": axe_payload.get("report"),
            "payload": axe_payload,
        }
    )
    session["raw_sources"]["custom_checks"].append(
        {
            "journey_step_index": journey_step_index,
            "journey_step_label": journey_step_label,
            "page_url": page_url,
            "checks": custom_results,
        }
    )
    return checkpoint


async def finalize_accessibility_audit_session(session: Dict[str, Any]) -> str:
    context = await _collect_page_context(session["driver_state"])
    criteria = session["criteria"]
    for criterion in criteria:
        if criterion.outcome_status == OUTCOME_PASSED and not criterion.passed_checks:
            criterion.outcome_status = OUTCOME_NOT_TESTED

    summary = _summary_from_criteria(criteria)
    sections = _build_sections(criteria)
    browser = _browser_label(context["user_agent"])
    report = AccessibilityReport(
        report_meta={
            "report_id": session["report_id"],
            "audit_name": session["audit_name"] or _normalize_screen_label(context["page_title"], urlparse(context["page_url"]).path or "/", context["page_url"]),
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "url": context["page_url"],
            "page_title": context["page_title"],
            "browser": browser,
            "viewport": context["viewport"],
            "standard_profile": session["standard_profile"],
            "tool_versions": {
                "runner": "phase-3-scenario",
                "axe_wrapper": "optional",
                "axe_core": "unknown_at_runtime",
                "agent_version": _git_commit(),
            },
        },
        execution={
            "run_test_id": session["_run_test_id"],
            "scope_selector": session["scope_selector"],
            "viewport_profile": session["viewport_profile"],
            "include_best_practices": session["include_best_practices"],
            "include_experimental": session["include_experimental"],
            "include_manual_placeholders": session["include_manual_placeholders"],
            "wait_for_network_idle": session["wait_for_network_idle"],
            "axe_full_scan": session["axe_full_scan"],
            "axe_custom_tags": session["axe_custom_tags"],
            "axe_exclude_tags": session["axe_exclude_tags"],
            "axe_enabled_rules": session["axe_enabled_rules"],
            "axe_disabled_rules": session["axe_disabled_rules"],
            "axe_include_iframes": session["axe_include_iframes"],
            "axe_include_selectors": session["axe_include_selectors"],
            "axe_include_ancestry": session["axe_include_ancestry"],
            "axe_result_types": session["axe_result_types"],
            "axe_reporter": session["axe_reporter"],
            "page_context": {
                "viewport": context["viewport"],
                "user_agent": context["user_agent"],
                "frame_name": context["frame_name"],
            },
            "journey_steps": session["journey_steps"],
            "scenario_steps_executed": session.get("scenario_steps_executed", []),
            "notes": session["execution_notes"],
        },
        summary=summary,
        sections=sections,
        criteria=criteria,
        raw_sources=session["raw_sources"],
        artifacts={},
    )

    json_artifact_name = _persist_json_report(report)
    report.artifacts["json"] = json_artifact_name
    html_artifact_name = _persist_html_report(report)
    report.artifacts["html"] = html_artifact_name
    digest_artifact_name = _persist_digest_report(report)
    report.artifacts["digest"] = digest_artifact_name
    _persist_json_report(report)
    LAST_REPORT_BY_RUN[session["_run_test_id"]] = json_artifact_name

    compact = {
        "status": "success",
        "audit_name": report.report_meta["audit_name"],
        "report_id": session["report_id"],
        "summary": {
            "failed": summary["outcome_counts"].get(OUTCOME_FAILED, 0),
            "passed": summary["outcome_counts"].get(OUTCOME_PASSED, 0),
            "needs_review": summary["outcome_counts"].get(OUTCOME_NEEDS_REVIEW, 0),
            "not_tested": summary["outcome_counts"].get(OUTCOME_NOT_TESTED, 0),
            "errors": summary["outcome_counts"].get(OUTCOME_ERROR, 0),
        },
        "artifacts": report.artifacts,
    }
    return json.dumps(compact)


async def run_accessibility_audit_for_driver(
    driver_state: Dict[str, Any],
    audit_name: str = "",
    standard_profile: str = "wcag22aa",
    scope_selector: str = "",
    include_best_practices: str = "true",
    include_experimental: str = "false",
    include_manual_placeholders: str = "true",
    viewport_profile: str = "desktop,mobile",
    wait_for_network_idle: str = "always",
    axe_full_scan: str = "false",
    axe_custom_tags: str = "",
    axe_exclude_tags: str = "",
    axe_enabled_rules: str = "",
    axe_disabled_rules: str = "",
    axe_include_iframes: str = "true",
    axe_include_selectors: str = "true",
    axe_include_ancestry: str = "true",
    axe_result_types: str = "",
    axe_reporter: str = "v2",
    _run_test_id: str = "1",
) -> str:
    page = driver_state.get("page")
    if page is None:
        return json.dumps({"status": "error", "error": "No active Playwright page for this run."})
    session = _create_session(
        driver_state=driver_state,
        audit_name=audit_name,
        standard_profile=standard_profile,
        scope_selector=scope_selector,
        include_best_practices=include_best_practices,
        include_experimental=include_experimental,
        include_manual_placeholders=include_manual_placeholders,
        viewport_profile=viewport_profile,
        wait_for_network_idle=wait_for_network_idle,
        axe_full_scan=axe_full_scan,
        axe_custom_tags=axe_custom_tags,
        axe_exclude_tags=axe_exclude_tags,
        axe_enabled_rules=axe_enabled_rules,
        axe_disabled_rules=axe_disabled_rules,
        axe_include_iframes=axe_include_iframes,
        axe_include_selectors=axe_include_selectors,
        axe_include_ancestry=axe_include_ancestry,
        axe_result_types=axe_result_types,
        axe_reporter=axe_reporter,
        _run_test_id=_run_test_id,
    )
    await append_accessibility_audit_checkpoint(session, "Run accessibility audit", 1)
    return await finalize_accessibility_audit_session(session)


def _list_report_files() -> List[Path]:
    attachments_dir = _report_storage_dir()
    return sorted(attachments_dir.glob("a11y_*.json"), reverse=True)


def _report_json_path(report_id: str) -> Path:
    return _artifact_path(report_id, "json")


def _load_report_payload(report_id: str) -> Dict[str, Any]:
    path = _report_json_path(report_id)
    if not path.is_file():
        raise FileNotFoundError("Accessibility report not found: {}".format(report_id))
    return json.loads(path.read_text(encoding="utf-8"))


def list_accessibility_reports_json(_run_test_id: str = "1") -> str:
    reports: List[Dict[str, Any]] = []
    for path in _list_report_files():
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            reports.append(
                {
                    "file_name": path.name,
                    "status": "error",
                    "error": str(exc),
                }
            )
            continue
        reports.append(
            {
                "file_name": path.name,
                "report_id": payload.get("report_meta", {}).get("report_id"),
                "audit_name": payload.get("report_meta", {}).get("audit_name"),
                "generated_at": payload.get("report_meta", {}).get("generated_at"),
                "url": payload.get("report_meta", {}).get("url"),
                "summary": payload.get("summary", {}).get("outcome_counts", {}),
            }
        )
    return json.dumps(reports)


def get_accessibility_report_json(report_id: str, _run_test_id: str = "1") -> str:
    try:
        payload = _load_report_payload(report_id)
    except FileNotFoundError as exc:
        return json.dumps({"status": "error", "error": str(exc), "report_id": report_id})
    except Exception as exc:
        return json.dumps({"status": "error", "error": str(exc), "report_id": report_id})
    return json.dumps(payload)


def export_accessibility_report_json(report_id: str, format: str = "json", _run_test_id: str = "1") -> str:
    requested_format = (format or "json").strip().lower()
    if requested_format not in {"json", "html", "excel", "pdf"}:
        return json.dumps({
            "status": "error",
            "report_id": report_id,
            "error": "Unsupported export format: {}. Supported values are json, html, excel, pdf.".format(requested_format),
        })

    try:
        payload = _load_report_payload(report_id)
    except FileNotFoundError as exc:
        return json.dumps({"status": "error", "error": str(exc), "report_id": report_id})
    except Exception as exc:
        return json.dumps({"status": "error", "error": str(exc), "report_id": report_id})

    artifacts = payload.get("artifacts", {})
    file_name = artifacts.get(requested_format)

    if requested_format in {"excel", "pdf"}:
        return json.dumps({
            "status": "error",
            "report_id": report_id,
            "format": requested_format,
            "error": "{} export is not implemented yet for this report pipeline.".format(requested_format.upper()),
            "available_formats": [fmt for fmt in ["json", "html"] if artifacts.get(fmt)],
        })

    if requested_format == "html":
        artifact_path = _artifact_path(report_id, "html")
        content = render_html_report(payload)
        artifact_path.write_text(content, encoding="utf-8")
        file_name = file_name or artifact_path.name
    else:
        artifact_path = _artifact_path(report_id, "json")
        content = json.dumps(payload)
        file_name = file_name or artifact_path.name
        if not artifact_path.is_file():
            artifact_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    return json.dumps({
        "status": "success",
        "report_id": report_id,
        "format": requested_format,
        "file_name": file_name,
        "content": content,
        "available_formats": [fmt for fmt in ["json", "html"] if artifacts.get(fmt)],
    })


def get_last_accessibility_report_json(_run_test_id: str = "1") -> str:
    last_name = LAST_REPORT_BY_RUN.get(_run_test_id)
    candidate_paths: List[Path] = []
    if last_name:
        candidate_paths.append(_report_storage_dir() / last_name)
    candidate_paths.extend(_list_report_files())

    seen = set()
    for path in candidate_paths:
        if path in seen:
            continue
        seen.add(path)
        if path.is_file():
            return path.read_text(encoding="utf-8")
    return json.dumps({"status": "error", "error": "No accessibility reports found."})
