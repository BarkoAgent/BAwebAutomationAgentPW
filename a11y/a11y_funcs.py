"""
Accessibility agent functions — drop-in extension module.

Loaded automatically by ba_ws_sdk.ws_core.build_function_map when the a11y
package is available. Exposes six public functions to the FUNCTION_MAP via
get_agent_functions(). Nothing here is imported into agent_func, so none of
these symbols leak into the core function map accidentally.

Requires init(driver_store) to be called before any public function is invoked.
build_function_map handles this automatically.
"""
import re
import os
import json
import logging
from urllib.parse import urlparse

try:
    from ba_ws_sdk import streaming as _streaming
except ImportError:
    _streaming = None

# ---------------------------------------------------------------------------
# Action arg filtering (kept here — only used by a11y test case execution)
# ---------------------------------------------------------------------------

_ACTION_ALLOWED_ARGS = {
    "navigate_to_url": {"url", "use_vars"},
    "click": {"locator"},
    "double_click": {"locator"},
    "right_click": {"locator"},
    "send_keys": {"locator", "value", "use_vars"},
    "exists": {"locator"},
    "exists_with_text": {"text", "use_vars"},
    "does_not_exist": {"locator"},
    "scroll_to_element": {"locator"},
    "select_native_dropdown": {"locator", "option", "by"},
    "refresh_page": set(),
    "change_windows_tabs": set(),
    "change_frame_by_id": {"frame_name"},
    "change_frame_by_locator": {"locator"},
    "change_frame_to_original": set(),
    "upload_file_to_form": {"locator", "file_name", "wait_for", "timeout"},
    "wait_for_download": {"timeout"},
    "maximize_window": set(),
    "get_page_html": set(),
    "return_current_url": set(),
}


def _sanitize_action_args(action: str, args: dict) -> dict:
    allowed = _ACTION_ALLOWED_ARGS.get(action)
    if allowed is None:
        return dict(args)
    sanitized = {}
    for key, value in args.items():
        if key in allowed or key == "_run_test_id":
            sanitized[key] = value
    return sanitized


# ---------------------------------------------------------------------------
# Availability check
# ---------------------------------------------------------------------------

_A11Y_AVAILABLE = os.getenv("A11Y_ENABLED", "true").lower() not in {"0", "false", "no", "off"}
if _A11Y_AVAILABLE:
    try:
        import a11y  # noqa: F401 — probe only
    except ImportError as _a11y_import_err:
        _A11Y_AVAILABLE = False
        logging.warning(
            "a11y module not available: %s. Accessibility functions are disabled.",
            _a11y_import_err,
        )

# ---------------------------------------------------------------------------
# Driver store — injected by build_function_map via init()
# ---------------------------------------------------------------------------

_driver_store: dict = {}


def init(driver_store: dict) -> None:
    """
    Bind this module to the agent's driver dict.
    Called once by build_function_map; driver_store is the same mutable dict
    used by agent_func so all run-id keys are visible here automatically.
    """
    global _driver_store
    _driver_store = driver_store


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _a11y_unavailable() -> str:
    return json.dumps({
        "status": "error",
        "error": (
            "Accessibility module is not available in this environment. "
            "Set A11Y_ENABLED=true and ensure the a11y package is installed."
        ),
    })


def _short_text(value, limit: int = 180) -> str:
    text = "" if value is None else str(value)
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def _step_target(action: str, args: dict) -> str:
    if action == "navigate_to_url":
        url = args.get("url", "")
        parsed = urlparse(url)
        return parsed.path or parsed.netloc or url
    if action in {
        "click", "double_click", "right_click", "send_keys", "exists",
        "does_not_exist", "scroll_to_element", "change_frame_by_locator",
        "upload_file_to_form",
    }:
        return args.get("locator", "")
    if action == "exists_with_text":
        return args.get("text", "")
    if action == "change_frame_by_id":
        return args.get("frame_name", "")
    if action == "select_native_dropdown":
        locator = args.get("locator", "")
        option = args.get("option", "")
        return "{} -> {}".format(locator, option).strip(" ->")
    return ""


def _checkpoint_label(action: str, label: str, args: dict) -> str:
    target = _short_text(_step_target(action, args), 80)
    fallback = _short_text(label or action.replace("_", " "), 80)

    if action == "navigate_to_url":
        return "Page loaded: {}".format(target or fallback)
    if action == "exists":
        return "Verified visible: {}".format(target or fallback)
    if action == "does_not_exist":
        return "Verified absent: {}".format(target or fallback)
    if action == "exists_with_text":
        return "Verified text: {}".format(target or fallback)
    if action == "send_keys":
        return "Updated field: {}".format(target or fallback)
    if action in {"click", "double_click", "right_click"}:
        verb = {
            "click": "After click",
            "double_click": "After double click",
            "right_click": "After right click",
        }[action]
        return "{}: {}".format(verb, target or fallback)
    if action == "select_native_dropdown":
        return "Updated selection: {}".format(target or fallback)
    if action == "scroll_to_element":
        return "Scrolled to: {}".format(target or fallback)
    if action in {"change_frame_by_id", "change_frame_by_locator", "change_frame_to_original"}:
        return "Frame context changed: {}".format(target or fallback)
    if action == "upload_file_to_form":
        return "Uploaded file: {}".format(target or fallback)
    if action == "wait_for_download":
        return "Download wait completed"
    if action == "refresh_page":
        return "Page refreshed"
    if action == "maximize_window":
        return "Viewport updated"
    return fallback


def _summarize_step_result(
    action: str,
    args: dict,
    result: object = None,
    error: str | None = None,
) -> tuple[str, dict]:
    target = _short_text(_step_target(action, args), 100)
    metadata: dict = {}

    if error:
        return _short_text(error, 220), metadata

    if action == "get_page_html":
        raw = "" if result is None else str(result)
        metadata = {"omitted_large_output": True, "raw_length": len(raw)}
        return "HTML snapshot captured ({} chars omitted from report).".format(len(raw)), metadata

    if action == "navigate_to_url":
        return _short_text(result, 220), metadata
    if action == "send_keys":
        return "Input completed for {}.".format(target or "field"), metadata
    if action in {"click", "double_click", "right_click"}:
        return "{} {}.".format(action.replace("_", " "), target or "target").strip(), metadata
    if action in {"exists", "does_not_exist", "exists_with_text", "scroll_to_element"}:
        return "{} {}.".format(action.replace("_", " "), target or "").strip(), metadata
    if action == "select_native_dropdown":
        return "Selection updated for {}.".format(target or "dropdown"), metadata

    text = _short_text(result, 220)
    return text or "Completed.", metadata


def _checkpoint_kind_for_action(action: str) -> str:
    if action in {
        "navigate_to_url",
        "refresh_page",
        "change_windows_tabs",
        "change_frame_by_id",
        "change_frame_by_locator",
        "change_frame_to_original",
    }:
        return "navigation"
    return "step"


# Actions that are pure reads — they do not change page state, so running a
# full accessibility audit after them would only duplicate findings from the
# immediately preceding checkpoint.
_NO_AUDIT_ACTIONS = {"get_page_html", "return_current_url"}


# ---------------------------------------------------------------------------
# Public agent functions
# ---------------------------------------------------------------------------

async def run_accessibility_audit(
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
    """Runs an accessibility audit for the current page and persists a JSON report artifact."""
    if not _A11Y_AVAILABLE:
        return _a11y_unavailable()
    driver_state = _driver_store.get(_run_test_id)
    if not driver_state:
        return json.dumps({"status": "error", "error": "No driver found for this run id."})

    from a11y.runner import run_accessibility_audit_for_driver as _run_audit
    return await _run_audit(
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


async def get_last_accessibility_report(_run_test_id: str = "1") -> str:
    """Returns the full JSON for the latest accessibility report available for this run."""
    if not _A11Y_AVAILABLE:
        return _a11y_unavailable()
    from a11y.runner import get_last_accessibility_report_json as _get_last
    return _get_last(_run_test_id=_run_test_id)


async def list_accessibility_reports(_run_test_id: str = "1") -> str:
    """Returns the list of persisted accessibility report artifacts."""
    if not _A11Y_AVAILABLE:
        return _a11y_unavailable()
    from a11y.runner import list_accessibility_reports_json as _list_reports
    return _list_reports(_run_test_id=_run_test_id)


async def get_accessibility_report(report_id: str, _run_test_id: str = "1") -> str:
    """Returns the full persisted JSON for a specific accessibility report id."""
    if not _A11Y_AVAILABLE:
        return _a11y_unavailable()
    from a11y.runner import get_accessibility_report_json as _get_report
    return _get_report(report_id=report_id, _run_test_id=_run_test_id)


async def export_accessibility_report(report_id: str, format: str = "json", _run_test_id: str = "1") -> str:
    """Returns a specific persisted accessibility report artifact by report id and format."""
    if not _A11Y_AVAILABLE:
        return _a11y_unavailable()
    from a11y.runner import export_accessibility_report_json as _export_report
    return _export_report(report_id=report_id, format=format, _run_test_id=_run_test_id)


async def run_accessibility_test_case(
    test_case_json: str,
    audit_name: str = "",
    audit_after_each_step: str = "true",
    standard_profile: str = "wcag22aa",
    scope_selector: str = "",
    include_best_practices: str = "true",
    include_experimental: str = "false",
    include_manual_placeholders: str = "true",
    viewport_profile: str = "desktop,mobile",
    wait_for_network_idle: str = "navigation_only",
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
    """
    Executes a JSON test case and accumulates one accessibility report across the scenario.

    Step shape:
    {
      "label": "Open site",
      "action": "navigate_to_url",
      "args": {"url": "https://example.test"},
      "audit_after": true
    }
    """
    if not _A11Y_AVAILABLE:
        return _a11y_unavailable()

    driver_state = _driver_store.get(_run_test_id)
    if not driver_state:
        return json.dumps({"status": "error", "error": "No driver found for this run id."})

    try:
        payload = json.loads(test_case_json)
    except json.JSONDecodeError as exc:
        return json.dumps({"status": "error", "error": f"Invalid JSON for test_case_json: {exc}"})

    from a11y.testcase import normalize_test_case_payload as _normalize_payload
    try:
        scenario_name, steps = _normalize_payload(payload, audit_name=audit_name)
    except ValueError as exc:
        return json.dumps({"status": "error", "error": str(exc)})

    from ba_ws_sdk.a11y_adapter import get_bindings as _get_a11y_bindings
    _a11y_bindings, _a11y_err = _get_a11y_bindings(_driver_store)
    if _a11y_bindings is None:
        return json.dumps({"status": "error", "error": _a11y_err})

    session = _a11y_bindings["create_session"](
        driver_state=driver_state,
        audit_name=scenario_name,
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

    import agent_func as _agent_module
    action_map = {
        "navigate_to_url": _agent_module.navigate_to_url,
        "click": _agent_module.click,
        "double_click": _agent_module.double_click,
        "right_click": _agent_module.right_click,
        "send_keys": _agent_module.send_keys,
        "exists": _agent_module.exists,
        "exists_with_text": _agent_module.exists_with_text,
        "does_not_exist": _agent_module.does_not_exist,
        "scroll_to_element": _agent_module.scroll_to_element,
        "select_native_dropdown": _agent_module.select_native_dropdown,
        "refresh_page": _agent_module.refresh_page,
        "change_windows_tabs": _agent_module.change_windows_tabs,
        "change_frame_by_id": _agent_module.change_frame_by_id,
        "change_frame_by_locator": _agent_module.change_frame_by_locator,
        "change_frame_to_original": _agent_module.change_frame_to_original,
        "upload_file_to_form": _agent_module.upload_file_to_form,
        "wait_for_download": _agent_module.wait_for_download,
        "maximize_window": _agent_module.maximize_window,
        "get_page_html": _agent_module.get_page_html,
        "return_current_url": _agent_module.return_current_url,
    }

    step_results = []
    print(f"Executing test case '{scenario_name}' with {len(steps)} steps, run_id={_run_test_id}")

    for step_index, step in enumerate(steps, start=1):
        if not isinstance(step, dict):
            session["execution_notes"].append(f"Skipped non-object step at index {step_index}.")
            continue

        action = step.get("action", "").strip()
        label = step.get("label") or step.get("name") or action.replace("_", " ")
        args = step.get("args", {})
        if not isinstance(args, dict):
            args = {}

        checkpoint_label = _checkpoint_label(action, label, args)
        checkpoint_kind = _checkpoint_kind_for_action(action)

        if action == "audit_checkpoint":
            await _a11y_bindings["append_checkpoint"](session, label, step_index, checkpoint_kind="step")
            step_results.append({
                "step_index": step_index,
                "label": label,
                "action": action,
                "status": "success",
                "result": "audit checkpoint created",
                "audit_label": label,
            })
            continue

        if action not in action_map:
            session["execution_notes"].append(f"Unsupported test case action at step {step_index}: {action}")
            step_results.append({
                "step_index": step_index,
                "label": label or f"Step {step_index}",
                "action": action,
                "status": "error",
                "error": f"Unsupported action: {action}",
            })
            break

        args = _sanitize_action_args(action, args)
        args["_run_test_id"] = _run_test_id

        try:
            result = await action_map[action](**args)
            result_summary, result_meta = _summarize_step_result(action, args, result=result)
            step_results.append({
                "step_index": step_index,
                "label": label,
                "action": action,
                "status": "success",
                "result": result_summary,
                "result_meta": result_meta,
                "audit_label": checkpoint_label,
            })
            if _streaming is not None:
                try:
                    element_hint = {"locator": args.get("locator")} if args.get("locator") else None
                    await _streaming.capture_step_frame_async(
                        run_id=_run_test_id,
                        step_index=step_index,
                        func_name=action,
                        element_hint=element_hint,
                        step_result=result,
                    )
                    print(f"Captured frame for step {step_index} ({action})")
                except Exception:
                    print(f"Warning: Failed to capture frame for step {step_index} ({action})")
        except Exception as exc:
            session["execution_notes"].append(f"Step {step_index} failed ({label}): {exc}")
            error_summary, result_meta = _summarize_step_result(action, args, error=str(exc))
            step_results.append({
                "step_index": step_index,
                "label": label,
                "action": action,
                "status": "error",
                "error": error_summary,
                "result_meta": result_meta,
                "audit_label": checkpoint_label,
            })
            if action not in _NO_AUDIT_ACTIONS:
                await _a11y_bindings["append_checkpoint"](session, checkpoint_label, step_index, checkpoint_kind=checkpoint_kind)
            break

        if action not in _NO_AUDIT_ACTIONS:
            await _a11y_bindings["append_checkpoint"](session, checkpoint_label, step_index, checkpoint_kind=checkpoint_kind)

    if step_results:
        last_audited_index = session["journey_steps"][-1]["journey_step_index"] if session["journey_steps"] else None
        final_step = step_results[-1]
        if last_audited_index != final_step["step_index"] and final_step["action"] not in _NO_AUDIT_ACTIONS:
            await _a11y_bindings["append_checkpoint"](
                session,
                final_step.get("audit_label") or final_step["label"],
                final_step["step_index"],
                checkpoint_kind="step",
            )

    session["scenario_steps_executed"] = step_results
    session["execution_notes"].append("Scenario steps executed: {}".format(len(step_results)))
    result = await _a11y_bindings["finalize_session"](session)
    payload = json.loads(result)
    payload["scenario"] = {
        "name": scenario_name,
        "steps_executed": step_results,
    }
    return json.dumps(payload)


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------

def get_agent_functions() -> dict:
    """Returns the public a11y functions to be merged into the FUNCTION_MAP."""
    return {
        "run_accessibility_audit": run_accessibility_audit,
        "get_last_accessibility_report": get_last_accessibility_report,
        "list_accessibility_reports": list_accessibility_reports,
        "get_accessibility_report": get_accessibility_report,
        "export_accessibility_report": export_accessibility_report,
        "run_accessibility_test_case": run_accessibility_test_case,
    }
