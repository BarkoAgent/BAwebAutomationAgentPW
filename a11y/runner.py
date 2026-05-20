from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import re
import shutil
import subprocess

from playwright._impl._errors import TargetClosedError

logger = logging.getLogger(__name__)
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import ba_ws_sdk.file_system as file_system

from .evaluators import (
    all_manifests,
    manifests_for_criterion,
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
    PASS_RATIONALE_ALL_CHECKED_CLEAN,
    PASS_RATIONALE_AXE_RULE_CLEAN,
    PASS_RATIONALE_HEURISTIC_PROXY,
    PASS_RATIONALE_LIMITATION_PASS,
    PASS_RATIONALE_NO_APPLICABLE,
)
from .reporting import build_digest_json, render_html_report, render_stakeholder_summary
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

    if definition.default_coverage == COVERAGE_MANUAL_REQUIRED:
        explanation = (
            "Not tested by automation in this run — this criterion requires manual review. "
            "Recorded as Not Tested so the gap is visible; it does not affect the health score."
        )
    elif definition.default_coverage == COVERAGE_AUTOMATED:
        explanation = (
            "Automation is registered for this criterion but produced no evidence in this run "
            "(no applicable elements detected, or evaluator did not fire)."
        )
    else:
        explanation = (
            "Semi-automated coverage available but no evidence was produced in this run — "
            "manual confirmation needed."
        )

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
        not_tested_explanation=explanation,
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
_AXE_SCREENSHOT_LIMIT_PER_RULE = 25
# Custom-evaluator screenshot pass cap.
_CUSTOM_SCREENSHOT_LIMIT = 60


async def _capture_overlay_screenshot(page: Any, css_chain: List[str]) -> Optional[str]:
    """Resolve a CSS-chain locator (with optional iframe descent), highlight the
    element with an inline outline, and return a viewport-clipped JPEG as
    base64. Returns None if the element cannot be resolved or screenshotted.
    Falls back to plain viewport capture without highlight if highlighting
    fails."""
    if not css_chain:
        return None
    css = css_chain[-1]
    screenshot_b64: Optional[str] = None
    try:
        scope: Any = page
        for frame_sel in css_chain[:-1]:
            scope = scope.frame_locator(frame_sel)
        base_loc = scope.locator(css)
        loc = None
        try:
            count = await base_loc.count()
        except Exception:
            logger.warning("locator count() failed for %s; defaulting to 1", css, exc_info=True)
            count = 1
        for _i in range(min(count or 1, 8)):
            cand = base_loc.nth(_i)
            try:
                visible = await cand.evaluate(
                    """el => {
                      const r = el.getBoundingClientRect();
                      const cs = getComputedStyle(el);
                      return r.width > 0 && r.height > 0
                        && cs.visibility !== 'hidden'
                        && cs.display !== 'none';
                    }""",
                    timeout=2000,
                )
            except Exception:
                logger.debug("visibility probe failed for %s nth=%d", css, _i, exc_info=True)
                visible = False
            if visible:
                loc = cand
                break
        if loc is None:
            raise RuntimeError("no visible candidate for %s" % css)
        await loc.scroll_into_view_if_needed(timeout=2000)
        rect = None
        try:
            rect = await loc.evaluate(
                """el => {
                  const r = el.getBoundingClientRect();
                  return {
                    vTop: r.top, vLeft: r.left,
                    width: Math.max(r.width, 1), height: Math.max(r.height, 1),
                    vw: window.innerWidth, vh: window.innerHeight
                  };
                }"""
            )
        except Exception:
            logger.warning("rect probe failed for %s", css, exc_info=True)
            rect = None
        if not rect or rect["width"] < 2 or rect["height"] < 2:
            raise RuntimeError("no usable rect")
        style_snap = None
        try:
            style_snap = await loc.evaluate(
                """el => {
                  const prev = {
                    outline: el.style.outline,
                    outlineOffset: el.style.outlineOffset,
                    boxShadow: el.style.boxShadow,
                    priority: {
                      outline: el.style.getPropertyPriority('outline'),
                      outlineOffset: el.style.getPropertyPriority('outline-offset'),
                      boxShadow: el.style.getPropertyPriority('box-shadow')
                    }
                  };
                  el.style.setProperty('outline', '4px solid #B00020', 'important');
                  el.style.setProperty('outline-offset', '2px', 'important');
                  el.style.setProperty('box-shadow', '0 0 0 4px rgba(255,255,255,0.9), 0 0 0 8px rgba(176,0,32,0.35)', 'important');
                  return prev;
                }"""
            )
        except Exception:
            logger.warning("highlight style apply failed for %s", css, exc_info=True)
        frame_off_x = 0
        frame_off_y = 0
        main_vw = rect["vw"]
        main_vh = rect["vh"]
        if css_chain[:-1]:
            try:
                outer_loc = page.locator(css_chain[0]).first
                outer_rect = await outer_loc.evaluate(
                    """el => {
                      const r = el.getBoundingClientRect();
                      return {x: r.left, y: r.top, vw: window.innerWidth, vh: window.innerHeight};
                    }"""
                )
                frame_off_x = outer_rect["x"]
                frame_off_y = outer_rect["y"]
                main_vw = outer_rect["vw"]
                main_vh = outer_rect["vh"]
            except Exception:
                logger.warning("iframe outer rect probe failed for %s", css_chain[0], exc_info=True)
        clip = None
        try:
            pad_x = max(180, int(main_vw * 0.35))
            pad_y = max(140, int(main_vh * 0.28))
            elem_x = rect["vLeft"] + frame_off_x
            elem_y = rect["vTop"] + frame_off_y
            x = max(0, elem_x - pad_x)
            y = max(0, elem_y - pad_y)
            w = min(main_vw - x, rect["width"] + pad_x * 2)
            h = min(main_vh - y, rect["height"] + pad_y * 2)
            if w > 4 and h > 4:
                clip = {"x": x, "y": y, "width": w, "height": h}
        except Exception:
            logger.warning("clip computation failed for %s", css, exc_info=True)
            clip = None
        try:
            if clip:
                raw = await page.screenshot(full_page=False, type="jpeg", quality=70, clip=clip)
            else:
                raw = await page.screenshot(full_page=False, type="jpeg", quality=55)
            screenshot_b64 = base64.b64encode(raw).decode()
        except Exception:
            logger.warning("screenshot capture failed for %s", css, exc_info=True)
            screenshot_b64 = None
        if style_snap is not None:
            try:
                await loc.evaluate(
                    """(el, prev) => {
                      const set = (prop, val, prio) => {
                        if (val) el.style.setProperty(prop, val, prio || '');
                        else el.style.removeProperty(prop);
                      };
                      set('outline', prev.outline, prev.priority.outline);
                      set('outline-offset', prev.outlineOffset, prev.priority.outlineOffset);
                      set('box-shadow', prev.boxShadow, prev.priority.boxShadow);
                    }""",
                    style_snap,
                )
            except Exception:
                logger.debug("highlight style restore failed for %s", css, exc_info=True)
    except Exception:
        logger.warning("element screenshot pipeline failed for %s", css, exc_info=True)
    if not screenshot_b64:
        try:
            raw = await page.screenshot(full_page=False, type="jpeg", quality=55)
            screenshot_b64 = base64.b64encode(raw).decode()
        except Exception:
            logger.warning("fallback viewport screenshot failed", exc_info=True)
            screenshot_b64 = None
    return screenshot_b64


async def _attach_custom_screenshots(page: Any, custom_results: List[Dict[str, Any]]) -> None:
    """Capture element-targeted screenshots for custom-evaluator check results
    that have a ``locator`` and no existing ``screenshot_b64``. Best-effort —
    any per-check failure leaves the result unchanged."""
    if not custom_results:
        return
    await _freeze_motion_for_screenshot(page)
    shots = 0
    for check in custom_results:
        if shots >= _CUSTOM_SCREENSHOT_LIMIT:
            break
        if check.get("screenshot_b64"):
            continue
        loc = check.get("locator") or ""
        if not loc or not isinstance(loc, str):
            continue
        # Skip non-DOM pseudo-locators emitted by metadata checks.
        if loc in ("document.title",) or loc.startswith("document."):
            continue
        b64 = await _capture_overlay_screenshot(page, [loc])
        if b64:
            check["screenshot_b64"] = b64
            shots += 1


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

    await _freeze_motion_for_screenshot(page)

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
            # axe target is a chain: [iframeSel, ..., elementSel]. Descend through
            # frame_locator for each leading iframe selector so we resolve the
            # actual offending element, not its containing iframe.
            css_chain = [t for t in target if isinstance(t, str)] if target else []
            css = css_chain[-1] if css_chain else None
            if css:
                overlay_added = False
                try:
                    scope: Any = page
                    for frame_sel in css_chain[:-1]:
                        scope = scope.frame_locator(frame_sel)
                    # Pick first VISIBLE match. Class-based axe selectors can
                    # match hidden mobile/desktop duplicates; .first alone may
                    # land on a display:none element with rect 0,0,0,0.
                    base_loc = scope.locator(css)
                    loc = None
                    try:
                        count = await base_loc.count()
                    except Exception:
                        logger.warning("axe shot: locator count() failed for %s", css, exc_info=True)
                        count = 1
                    for _i in range(min(count or 1, 8)):
                        cand = base_loc.nth(_i)
                        try:
                            visible = await cand.evaluate(
                                """el => {
                                  const r = el.getBoundingClientRect();
                                  const cs = getComputedStyle(el);
                                  return r.width > 0 && r.height > 0
                                    && cs.visibility !== 'hidden'
                                    && cs.display !== 'none';
                                }""",
                                timeout=2000,
                            )
                        except Exception:
                            logger.debug("axe shot: visibility probe failed for %s nth=%d", css, _i, exc_info=True)
                            visible = False
                        if visible:
                            loc = cand
                            break
                    if loc is None:
                        raise RuntimeError("no visible candidate for %s" % css)
                    await loc.scroll_into_view_if_needed(timeout=2000)
                    # Measure once BEFORE overlay insertion so overlay placement
                    # and clip use the same coordinates. Re-measuring after
                    # insertion drifts when the overlay forces reflow (scrollbar
                    # appears, document height grows). Also detect fixed/sticky
                    # elements so we can avoid full_page capture, which expands
                    # the layout viewport and shifts those elements relative to
                    # an absolute-positioned overlay.
                    rect = None
                    try:
                        rect = await loc.evaluate(
                            """el => {
                              const r = el.getBoundingClientRect();
                              return {
                                top: r.top + window.scrollY,
                                left: r.left + window.scrollX,
                                vTop: r.top,
                                vLeft: r.left,
                                width: Math.max(r.width, 1),
                                height: Math.max(r.height, 1),
                                docW: Math.max(document.documentElement.scrollWidth, document.body.scrollWidth),
                                docH: Math.max(document.documentElement.scrollHeight, document.body.scrollHeight),
                                vw: window.innerWidth,
                                vh: window.innerHeight
                              };
                            }"""
                        )
                    except Exception:
                        logger.warning("axe shot: rect probe failed for %s", css, exc_info=True)
                        rect = None
                    # If rect missing or element has zero size, skip overlay
                    # and let the fallback viewport screenshot below run.
                    if not rect or rect["width"] < 2 or rect["height"] < 2:
                        raise RuntimeError("no usable rect")
                    # Highlight via inline style on the element itself. Outline
                    # paints inside the element's stacking context and follows
                    # the element regardless of scroll, layout shift, fixed
                    # ancestors, drawers, or z-index ordering. Save+restore
                    # original style so the page returns to its original state.
                    style_snap = None
                    try:
                        style_snap = await loc.evaluate(
                            """el => {
                              const prev = {
                                outline: el.style.outline,
                                outlineOffset: el.style.outlineOffset,
                                boxShadow: el.style.boxShadow,
                                priority: {
                                  outline: el.style.getPropertyPriority('outline'),
                                  outlineOffset: el.style.getPropertyPriority('outline-offset'),
                                  boxShadow: el.style.getPropertyPriority('box-shadow')
                                }
                              };
                              el.style.setProperty('outline', '4px solid #B00020', 'important');
                              el.style.setProperty('outline-offset', '2px', 'important');
                              el.style.setProperty('box-shadow', '0 0 0 4px rgba(255,255,255,0.9), 0 0 0 8px rgba(176,0,32,0.35)', 'important');
                              return prev;
                            }"""
                        )
                        overlay_added = True
                    except Exception:
                        logger.warning("axe shot: highlight style apply failed for %s", css, exc_info=True)
                    # If element lives inside an iframe, rect is iframe-local.
                    # Translate by iframe element offset on main page so clip
                    # (which is in main-page viewport coords) aligns with the
                    # screenshot. page.screenshot also captures iframe content,
                    # so the overlay (position:fixed inside iframe) renders at
                    # the correct visible location automatically.
                    frame_off_x = 0
                    frame_off_y = 0
                    main_vw = rect["vw"]
                    main_vh = rect["vh"]
                    if css_chain[:-1]:
                        try:
                            outer_loc = page.locator(css_chain[0]).first
                            outer_rect = await outer_loc.evaluate(
                                """el => {
                                  const r = el.getBoundingClientRect();
                                  return {x: r.left, y: r.top, vw: window.innerWidth, vh: window.innerHeight};
                                }"""
                            )
                            frame_off_x = outer_rect["x"]
                            frame_off_y = outer_rect["y"]
                            main_vw = outer_rect["vw"]
                            main_vh = outer_rect["vh"]
                        except Exception:
                            logger.warning("axe shot: iframe outer rect probe failed for %s", css_chain[0], exc_info=True)
                    clip = None
                    try:
                        pad_x = max(180, int(main_vw * 0.35))
                        pad_y = max(140, int(main_vh * 0.28))
                        elem_x = rect["vLeft"] + frame_off_x
                        elem_y = rect["vTop"] + frame_off_y
                        x = max(0, elem_x - pad_x)
                        y = max(0, elem_y - pad_y)
                        w = min(main_vw - x, rect["width"] + pad_x * 2)
                        h = min(main_vh - y, rect["height"] + pad_y * 2)
                        if w > 4 and h > 4:
                            clip = {"x": x, "y": y, "width": w, "height": h}
                    except Exception:
                        logger.warning("axe shot: clip computation failed for %s", css, exc_info=True)
                        clip = None
                    try:
                        if clip:
                            raw = await page.screenshot(
                                full_page=False, type="jpeg", quality=70, clip=clip
                            )
                        else:
                            raw = await page.screenshot(
                                full_page=False, type="jpeg", quality=55
                            )
                        screenshot_b64 = base64.b64encode(raw).decode()
                    except Exception:
                        logger.warning("axe shot: screenshot capture failed for %s", css, exc_info=True)
                        screenshot_b64 = None
                    if overlay_added and style_snap is not None:
                        try:
                            await loc.evaluate(
                                """(el, prev) => {
                                  const set = (prop, val, prio) => {
                                    if (val) el.style.setProperty(prop, val, prio || '');
                                    else el.style.removeProperty(prop);
                                  };
                                  set('outline', prev.outline, prev.priority.outline);
                                  set('outline-offset', prev.outlineOffset, prev.priority.outlineOffset);
                                  set('box-shadow', prev.boxShadow, prev.priority.boxShadow);
                                }""",
                                style_snap,
                            )
                        except Exception:
                            logger.debug("axe shot: highlight style restore failed for %s", css, exc_info=True)
                except Exception:
                    logger.warning("axe shot: element screenshot pipeline failed for %s", css, exc_info=True)
            # Fall back to a viewport screenshot if the outlined capture failed.
            if not screenshot_b64:
                try:
                    raw = await page.screenshot(full_page=False, type="jpeg", quality=55)
                    screenshot_b64 = base64.b64encode(raw).decode()
                except Exception:
                    logger.warning("axe shot: fallback viewport screenshot failed", exc_info=True)
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


def _finalize_criterion_transparency(criteria: List[CriterionResult]) -> None:
    """Populate manifest_refs / tested_aspects / untested_aspects /
    automation_limits / pass_rationale on every CriterionResult.

    Called once during finalize after outcomes have stabilised. Decisions are
    derived purely from the evidence already on the criterion — no extra DOM
    work.
    """
    for criterion in criteria:
        manifests = manifests_for_criterion(criterion.id)
        # Restrict per-criterion manifest list to the ones that actually fired
        # at least one evidence item; if none fired, keep the full list so the
        # reader still sees what *would* have been checked.
        contributing_sources = set(criterion.sources or [])
        fired = [m for m in manifests if m.id in contributing_sources or any(
            src.startswith(m.id + ":") or src == m.id for src in contributing_sources
        )]
        # axe manifests use id 'axe:wcag-<crit>' but evidence sources are 'axe:<rule>',
        # so include any axe manifest for this criterion if any axe source contributed.
        axe_fired = any(src.startswith("axe:") for src in contributing_sources)
        for m in manifests:
            if m.id.startswith("axe:wcag-") and axe_fired and m not in fired:
                fired.append(m)
        active = fired or manifests

        criterion.manifest_refs = [m.id for m in active]

        tested: List[str] = []
        untested: List[str] = []
        limits: List[str] = []
        for m in active:
            tested.extend(m.what_tested)
            if m.sampling:
                tested.append("Sampling: {}".format(m.sampling))
            untested.extend(m.what_not_tested)
            limits.extend(m.automation_limits)
        # De-duplicate while preserving order.
        criterion.tested_aspects = list(dict.fromkeys(tested))
        criterion.untested_aspects = list(dict.fromkeys(untested))

        # Add axe runtime limits (per-criterion) from incomplete_checks data.
        for inc in criterion.incomplete_checks or []:
            meta = (inc.get("metadata") or {})
            check = (meta.get("nodeCheckData") or {})
            key = check.get("messageKey")
            if key:
                limits.append("axe incomplete: {}".format(key))
        criterion.automation_limits = list(dict.fromkeys(limits))

        # Pass rationale only for PASSED outcomes.
        if criterion.outcome_status != OUTCOME_PASSED:
            continue

        passed = criterion.passed_checks or []
        incomplete = criterion.incomplete_checks or []
        axe_passed = [p for p in passed if str(p.get("source", "") or p.get("rule_id", "")).startswith("axe") or "rule_id" in p]
        custom_passed = [p for p in passed if not (str(p.get("source", "") or "").startswith("axe") or "rule_id" in p)]

        if not passed:
            criterion.pass_rationale = PASS_RATIONALE_NO_APPLICABLE
        elif incomplete and len(incomplete) >= len(axe_passed) and axe_passed and not custom_passed:
            criterion.pass_rationale = PASS_RATIONALE_LIMITATION_PASS
        elif axe_passed and not custom_passed:
            criterion.pass_rationale = PASS_RATIONALE_AXE_RULE_CLEAN
        elif custom_passed and not axe_passed:
            criterion.pass_rationale = PASS_RATIONALE_HEURISTIC_PROXY
        else:
            criterion.pass_rationale = PASS_RATIONALE_ALL_CHECKED_CLEAN


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
    pass_rationale_counts: Dict[str, int] = {}
    for criterion in criteria:
        outcome_counts[criterion.outcome_status] = outcome_counts.get(criterion.outcome_status, 0) + 1
        coverage_counts[criterion.coverage_status] = coverage_counts.get(criterion.coverage_status, 0) + 1
        if criterion.pass_rationale:
            pass_rationale_counts[criterion.pass_rationale] = pass_rationale_counts.get(criterion.pass_rationale, 0) + 1
    return {
        "total_rows": len(criteria),
        "outcome_counts": outcome_counts,
        "coverage_counts": coverage_counts,
        "pass_rationale_counts": pass_rationale_counts,
    }


def _report_storage_dir(project_id: str = "") -> Path:
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
                logger.warning("legacy attachments migration failed for %s", legacy_path, exc_info=True)
                continue

    if project_id:
        project_dir = reports_dir / project_id
        project_dir.mkdir(parents=True, exist_ok=True)
        return project_dir

    return reports_dir


def _artifact_path(report_id: str, suffix: str, project_id: str = "") -> Path:
    return _report_storage_dir(project_id) / "{}.{}".format(report_id, suffix)


def _persist_json_report(report: AccessibilityReport) -> str:
    project_id = report.report_meta.get("project_id", "")
    path = _artifact_path(report.report_meta["report_id"], "json", project_id)
    path.write_text(json.dumps(report.to_dict(), indent=2), encoding="utf-8")
    return path.name


def _persist_html_report(report: AccessibilityReport) -> str:
    project_id = report.report_meta.get("project_id", "")
    path = _artifact_path(report.report_meta["report_id"], "html", project_id)
    path.write_text(render_html_report(report.to_dict()), encoding="utf-8")
    return path.name


def _persist_stakeholder_summary(report: AccessibilityReport, detail_html_name: str) -> str:
    project_id = report.report_meta.get("project_id", "")
    path = _report_storage_dir(project_id) / "{}_summary.html".format(report.report_meta["report_id"])
    path.write_text(
        render_stakeholder_summary(report.to_dict(), detail_artifact=detail_html_name),
        encoding="utf-8",
    )
    return path.name


def _persist_digest_report(report: AccessibilityReport) -> str:
    project_id = report.report_meta.get("project_id", "")
    path = _artifact_path(report.report_meta["report_id"] + "_digest", "json", project_id)
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
        logger.debug("git rev-parse failed", exc_info=True)
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
    if page is None or page.is_closed():
        raise TargetClosedError("Page is closed; cannot stabilize")
    try:
        await page.wait_for_load_state("domcontentloaded", timeout=20000)
    except TargetClosedError:
        raise
    except Exception as exc:
        execution_notes.append("domcontentloaded wait failed: {}".format(exc))
    if network_idle_mode == "always":
        try:
            await page.wait_for_load_state("networkidle", timeout=15000)
        except TargetClosedError:
            raise
        except Exception as exc:
            execution_notes.append("networkidle wait failed: {}".format(exc))
    await asyncio.sleep(0.4)


# CSS injected before screenshots to halt animations/transitions/scroll-behavior
# so captured frames are not mid-animation. Combined with reduced-motion emulation
# this gives a deterministic visual snapshot.
_FREEZE_MOTION_CSS = (
    "*, *::before, *::after {"
    " animation-duration: 0s !important;"
    " animation-delay: 0s !important;"
    " animation-iteration-count: 1 !important;"
    " animation-play-state: paused !important;"
    " transition-duration: 0s !important;"
    " transition-delay: 0s !important;"
    " scroll-behavior: auto !important;"
    " caret-color: transparent !important;"
    "}"
)


async def _freeze_motion_for_screenshot(page: Any) -> None:
    """Disable animations/transitions and wait for fonts + two RAFs so the next
    screenshot lands on a settled frame. Best-effort; failures are swallowed."""
    try:
        await page.emulate_media(reduced_motion="reduce")
    except Exception:
        logger.warning("emulate_media(reduced_motion) failed", exc_info=True)
    try:
        await page.add_style_tag(content=_FREEZE_MOTION_CSS)
    except Exception:
        logger.warning("freeze-motion style tag injection failed", exc_info=True)
    try:
        await page.evaluate(
            "() => Promise.all(["
            " document.fonts && document.fonts.ready,"
            " new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r)))"
            "])"
        )
    except Exception:
        logger.warning("fonts.ready / RAF settle wait failed", exc_info=True)


async def _create_shadow_page(page: Any, url: str, execution_notes: List[str]) -> Optional[Any]:
    """Open a new tab in the same browser context for isolated evaluation.

    Evaluators run on the shadow page so persistent JS mutations (event
    listeners, viewport changes, CSS injections) never reach the live test page.
    The caller is responsible for closing the returned page when done.
    Returns None if creation fails; the caller should then fall back to
    DOM-read-only checks on the original page.
    """
    if page is None or page.is_closed():
        execution_notes.append("Shadow page creation skipped: page already closed.")
        return None
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

    if page is None or page.is_closed():
        raise TargetClosedError("Page is closed; cannot collect context")

    active_frame = None
    try:
        if frame is not None and frame != page.main_frame:
            active_frame = frame
    except Exception:
        logger.debug("active frame detection failed", exc_info=True)
        active_frame = None

    page_url = page.url
    try:
        page_title = await page.title()
    except TargetClosedError:
        raise
    except Exception:
        logger.warning("page.title() failed", exc_info=True)
        page_title = ""

    try:
        user_agent = await page.evaluate("() => navigator.userAgent")
    except TargetClosedError:
        raise
    except Exception:
        logger.warning("user agent probe failed", exc_info=True)
        user_agent = "unknown"

    viewport = getattr(page, "viewport_size", None) or {}
    frame_name = None
    if active_frame is not None:
        try:
            frame_name = active_frame.name or None
        except Exception:
            logger.debug("frame name lookup failed", exc_info=True)
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
    project_id: str = "",
) -> Dict[str, Any]:
    criteria = [_criterion_result_from_definition(row) for row in build_registry()]
    return {
        "driver_state": driver_state,
        "report_id": _new_report_id(audit_name),
        "audit_name": audit_name,
        "project_id": project_id,
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
            await _freeze_motion_for_screenshot(page)
            _ss_bytes = await page.screenshot(full_page=False, type="jpeg", quality=55)
            checkpoint_screenshot_b64 = base64.b64encode(_ss_bytes).decode()
        except Exception:
            logger.warning("checkpoint screenshot capture failed", exc_info=True)

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

    if page.is_closed():
        session["execution_notes"].append(
            "Page closed before evaluators could run at step '{}'; checkpoint recorded without evaluation.".format(journey_step_label)
        )
        if shadow is not None:
            try:
                await shadow.close()
            except Exception:
                pass
        checkpoint = {
            "journey_step_index": journey_step_index,
            "journey_step_label": journey_step_label,
            "page_url": page_url,
            "page_title": page_title,
            "viewport": viewport,
            "browser": _browser_label(user_agent),
            "axe_status": "skipped",
            "axe_violations_count": None,
            "axe_snapshot": None,
            "axe_report": None,
            "screenshot": "data:image/jpeg;base64,{}".format(checkpoint_screenshot_b64) if checkpoint_screenshot_b64 else None,
        }
        session["journey_steps"].append(checkpoint)
        return checkpoint

    eval_page = shadow if shadow is not None else page
    # wcag22aa profile excludes AAA-only criteria; skip evaluators bound to them.
    _aaa_only_evaluators = {"focus_appearance", "motion_preference"}
    _evaluator_specs = [
        ("keyboard_smoke", run_keyboard_smoke_evaluator, ()),
        ("focus_visibility", run_focus_visibility_evaluator, ()),
        ("focus_appearance", run_focus_appearance_evaluator, ()),
        ("form_labeling", run_form_labeling_evaluator, ()),
        ("hover_content", run_hover_content_evaluator, ()),
        ("structure", run_structure_evaluator, ()),
        ("live_region", run_live_region_evaluator, ()),
        ("viewport_reflow", run_viewport_reflow_evaluator, (session["viewport_profile"],)),
        ("text_resize", run_text_resize_evaluator, ()),
        ("timing", run_timing_evaluator, ()),
        ("pointer_target", run_pointer_target_evaluator, ()),
        ("orientation", run_orientation_evaluator, ()),
        ("motion_preference", run_motion_preference_evaluator, ()),
        ("media_alternatives", run_media_alternatives_evaluator, ()),
        ("context_change", run_context_change_evaluator, ()),
    ]
    _evaluator_specs = [s for s in _evaluator_specs if s[0] not in _aaa_only_evaluators]
    _evaluator_names = tuple(s[0] for s in _evaluator_specs)
    try:
        # Run axe + capture violation screenshots BEFORE evaluators mutate the
        # shadow page (clicks, focus, viewport resize, CSS injection). Otherwise
        # axe sees a randomly-mutated DOM and screenshots reflect that state
        # (open menus, drawers, scrolled-away content) instead of the clean page.
        axe_payload = await asyncio.wait_for(
            run_axe_scan(
                page=eval_page,
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
        _evaluator_tasks = await asyncio.gather(
            *[asyncio.wait_for(fn(eval_page, *args), timeout=_EVALUATOR_TIMEOUT_S) for _, fn, args in _evaluator_specs],
            return_exceptions=True,
        )
        # Gather raw check results first so screenshots can be attached BEFORE
        # _apply_custom_check_results bakes screenshot_b64 into evidence.
        _pending_checks: List[Dict[str, Any]] = []
        for _eval_name, _eval_result in zip(_evaluator_names, _evaluator_tasks):
            if isinstance(_eval_result, BaseException):
                session["execution_notes"].append(
                    "Evaluator '{}' raised an error: {}".format(_eval_name, _eval_result)
                )
                continue
            _pending_checks.extend(_eval_result)
        # Screenshot custom checks against the live (un-mutated) page so the
        # capture reflects the checkpoint state, not the evaluator-mutated
        # shadow DOM (clicks, focus, viewport resize, injected CSS).
        try:
            await _attach_custom_screenshots(page, _pending_checks)
        except Exception as _ss_exc:
            session["execution_notes"].append(
                "Custom screenshot pass error at step '{}': {}".format(journey_step_label, _ss_exc)
            )
        custom_results.extend(
            _apply_custom_check_results(
                criteria_by_id=session["criteria_by_id"],
                check_results=_pending_checks,
                report_id=session["report_id"],
                page_url=page_url,
                page_title=page_title,
                journey_name=journey_name,
                journey_step_label=journey_step_label,
                journey_step_index=journey_step_index,
                frame_name=frame_name,
            )
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
                logger.warning("shadow page close failed", exc_info=True)
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
    try:
        context = await _collect_page_context(session["driver_state"])
    except Exception:
        last_step = session["journey_steps"][-1] if session.get("journey_steps") else {}
        context = {
            "page_url": last_step.get("page_url", ""),
            "page_title": last_step.get("page_title", ""),
            "user_agent": "unknown",
            "viewport": last_step.get("viewport", {}),
            "frame_name": None,
        }
    criteria = session["criteria"]
    for criterion in criteria:
        if criterion.outcome_status == OUTCOME_PASSED and not criterion.passed_checks:
            criterion.outcome_status = OUTCOME_NOT_TESTED
            if not criterion.not_tested_explanation:
                criterion.not_tested_explanation = (
                    "No evidence collected in this run — automation did not produce a verifiable result."
                )

    _finalize_criterion_transparency(criteria)

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
            "project_id": session.get("project_id", ""),
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
        evaluator_manifests=all_manifests(),
    )

    json_artifact_name = _persist_json_report(report)
    report.artifacts["json"] = json_artifact_name
    html_artifact_name = _persist_html_report(report)
    report.artifacts["html"] = html_artifact_name
    summary_artifact_name = _persist_stakeholder_summary(report, html_artifact_name)
    report.artifacts["stakeholder_summary"] = summary_artifact_name
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
    project_id: str = "",
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
        project_id=project_id,
    )
    await append_accessibility_audit_checkpoint(session, "Run accessibility audit", 1)
    return await finalize_accessibility_audit_session(session)


def _list_report_files(project_id: str = "") -> List[Path]:
    base = _report_storage_dir()
    if project_id:
        proj_dir = base / project_id
        if proj_dir.is_dir():
            return sorted(
                (p for p in proj_dir.glob("a11y_*.json") if not p.name.endswith("_digest.json")),
                reverse=True,
            )
        return []
    return sorted(
        (p for p in base.glob("a11y_*.json") if not p.name.endswith("_digest.json")),
        reverse=True,
    )


def _report_json_path(report_id: str, project_id: str = "") -> Path:
    return _artifact_path(report_id, "json", project_id)


def _load_report_payload(report_id: str, project_id: str = "") -> Dict[str, Any]:
    path = _report_json_path(report_id, project_id)
    if not path.is_file() and project_id:
        path = _report_json_path(report_id, "")
    if not path.is_file():
        raise FileNotFoundError("Accessibility report not found: {}".format(report_id))
    return json.loads(path.read_text(encoding="utf-8"))


def list_accessibility_reports_json(project_id: str = "", _run_test_id: str = "1") -> str:
    reports: List[Dict[str, Any]] = []
    for path in _list_report_files(project_id):
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


def get_accessibility_report_json(report_id: str, project_id: str = "", _run_test_id: str = "1") -> str:
    try:
        payload = _load_report_payload(report_id, project_id)
    except FileNotFoundError as exc:
        return json.dumps({"status": "error", "error": str(exc), "report_id": report_id})
    except Exception as exc:
        return json.dumps({"status": "error", "error": str(exc), "report_id": report_id})
    return json.dumps(payload)


def export_accessibility_report_json(report_id: str, format: str = "json", project_id: str = "", _run_test_id: str = "1") -> str:
    requested_format = (format or "json").strip().lower()
    if requested_format not in {"json", "html", "excel", "pdf"}:
        return json.dumps({
            "status": "error",
            "report_id": report_id,
            "error": "Unsupported export format: {}. Supported values are json, html, excel, pdf.".format(requested_format),
        })

    try:
        payload = _load_report_payload(report_id, project_id)
    except FileNotFoundError as exc:
        return json.dumps({"status": "error", "error": str(exc), "report_id": report_id})
    except Exception as exc:
        return json.dumps({"status": "error", "error": str(exc), "report_id": report_id})

    _proj_id = payload.get("report_meta", {}).get("project_id", "") or project_id
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
        artifact_path = _artifact_path(report_id, "html", _proj_id)
        content = render_html_report(payload)
        artifact_path.write_text(content, encoding="utf-8")
        file_name = file_name or artifact_path.name
    else:
        artifact_path = _artifact_path(report_id, "json", _proj_id)
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
