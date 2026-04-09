from __future__ import annotations

from typing import Any, Dict, List, Tuple


ACTION_ALIASES = {
    "navigate": "navigate_to_url",
    "navigate_to_url": "navigate_to_url",
    "goto": "navigate_to_url",
    "go_to": "navigate_to_url",
    "visit": "navigate_to_url",
    "open": "navigate_to_url",
    "open_site": "navigate_to_url",
    "open_url": "navigate_to_url",
    "click": "click",
    "tap": "click",
    "press": "click",
    "double_click": "double_click",
    "dblclick": "double_click",
    "right_click": "right_click",
    "context_click": "right_click",
    "type": "send_keys",
    "fill": "send_keys",
    "send_keys": "send_keys",
    "enter_text": "send_keys",
    "input_text": "send_keys",
    "exists": "exists",
    "wait_for": "exists",
    "assert_exists": "exists",
    "expect_exists": "exists",
    "exists_with_text": "exists_with_text",
    "assert_text": "exists_with_text",
    "assert_visible_text": "exists_with_text",
    "expect_text": "exists_with_text",
    "does_not_exist": "does_not_exist",
    "assert_not_exists": "does_not_exist",
    "expect_hidden": "does_not_exist",
    "scroll_to_element": "scroll_to_element",
    "scroll_into_view": "scroll_to_element",
    "select": "select_native_dropdown",
    "select_native_dropdown": "select_native_dropdown",
    "refresh": "refresh_page",
    "refresh_page": "refresh_page",
    "switch_tab": "change_windows_tabs",
    "change_windows_tabs": "change_windows_tabs",
    "switch_frame_by_id": "change_frame_by_id",
    "change_frame_by_id": "change_frame_by_id",
    "switch_frame_by_locator": "change_frame_by_locator",
    "change_frame_by_locator": "change_frame_by_locator",
    "switch_to_main_frame": "change_frame_to_original",
    "change_frame_to_original": "change_frame_to_original",
    "upload": "upload_file_to_form",
    "upload_file_to_form": "upload_file_to_form",
    "wait_for_download": "wait_for_download",
    "maximize": "maximize_window",
    "maximize_window": "maximize_window",
    "audit": "audit_checkpoint",
    "a11y_audit": "audit_checkpoint",
    "audit_checkpoint": "audit_checkpoint",
}

STEP_COLLECTION_KEYS = ["steps", "test_steps", "testSteps", "actions", "case_steps"]
SCENARIO_NAME_KEYS = ["name", "title", "scenario_name", "scenarioName", "test_case_name", "testCaseName"]


def _first_present(mapping: Dict[str, Any], keys: List[str], default: Any = None) -> Any:
    for key in keys:
        if key in mapping and mapping[key] not in (None, ""):
            return mapping[key]
    return default


def _normalize_action(raw_action: str) -> str:
    key = (raw_action or "").strip().lower()
    return ACTION_ALIASES.get(key, key)


def _normalize_step(step: Dict[str, Any], step_index: int) -> Dict[str, Any]:
    raw_action = _first_present(step, ["action", "type", "command", "operation", "keyword"], "")
    action = _normalize_action(str(raw_action))
    label = _first_present(step, ["label", "name", "title", "description"], "") or action.replace("_", " ") or "step"

    args = step.get("args", {})
    if not isinstance(args, dict):
        args = {}
    normalized_args = dict(args)

    if action == "navigate_to_url":
        normalized_args.setdefault("url", _first_present(step, ["url", "value", "target"]))
    elif action in {"click", "double_click", "right_click", "exists", "does_not_exist", "scroll_to_element", "change_frame_by_locator"}:
        normalized_args.setdefault("locator", _first_present(step, ["locator", "selector", "target"]))
    elif action == "send_keys":
        normalized_args.setdefault("locator", _first_present(step, ["locator", "selector", "target"]))
        normalized_args.setdefault("value", _first_present(step, ["value", "text", "input"]))
    elif action == "exists_with_text":
        normalized_args.setdefault("locator", _first_present(step, ["locator", "selector", "target"]))
        normalized_args.setdefault("text", _first_present(step, ["text", "value", "expected_text", "expectedText"]))
    elif action == "select_native_dropdown":
        normalized_args.setdefault("locator", _first_present(step, ["locator", "selector", "target"]))
        normalized_args.setdefault("option", _first_present(step, ["option", "value", "text"]))
        normalized_args.setdefault("by", _first_present(step, ["by", "match_by", "matchBy"], "label"))
    elif action == "change_frame_by_id":
        normalized_args.setdefault("frame_name", _first_present(step, ["frame_name", "frameName", "value", "target"]))
    elif action == "upload_file_to_form":
        normalized_args.setdefault("locator", _first_present(step, ["locator", "selector", "target"]))
        normalized_args.setdefault("file_name", _first_present(step, ["file_name", "fileName", "value"]))
        if "wait_for" not in normalized_args:
            normalized_args["wait_for"] = _first_present(step, ["wait_for", "waitFor"], "")
    elif action == "wait_for_download":
        normalized_args.setdefault("timeout", _first_present(step, ["timeout", "value"], "30"))

    for key, value in step.items():
        if key in {
            "label", "name", "title", "description", "action", "type", "command", "operation", "keyword",
            "args", "audit_after", "auditAfter", "audit_before", "auditBefore"
        }:
            continue
        normalized_args.setdefault(key, value)

    return {
        "label": label,
        "action": action,
        "args": normalized_args,
        "audit_after": _first_present(step, ["audit_after", "auditAfter"]),
        "audit_before": _first_present(step, ["audit_before", "auditBefore"]),
        "original_step": step,
        "step_index": step_index,
    }


def normalize_test_case_payload(payload: Any, audit_name: str = "") -> Tuple[str, List[Dict[str, Any]]]:
    if isinstance(payload, list):
        steps = payload
        scenario_name = audit_name or "Accessibility test case"
    elif isinstance(payload, dict):
        steps = None
        for key in STEP_COLLECTION_KEYS:
            if key in payload:
                steps = payload[key]
                break
        if steps is None:
            steps = []
        scenario_name = audit_name or _first_present(payload, SCENARIO_NAME_KEYS, "Accessibility test case")
    else:
        raise ValueError("test_case_json must decode to an array of steps or an object with a steps array.")

    if not isinstance(steps, list) or not steps:
        raise ValueError("No test steps found in test_case_json.")

    normalized_steps = []
    for step_index, step in enumerate(steps, start=1):
        if not isinstance(step, dict):
            normalized_steps.append(
                {
                    "label": "Step {}".format(step_index),
                    "action": "",
                    "args": {},
                    "audit_after": None,
                    "audit_before": None,
                    "original_step": step,
                    "step_index": step_index,
                }
            )
            continue
        normalized_steps.append(_normalize_step(step, step_index))

    return scenario_name, normalized_steps
