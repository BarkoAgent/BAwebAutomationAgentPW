from __future__ import annotations

import importlib
from typing import Any, Dict, List, Optional


DEFAULT_RESULT_TYPES = ["violations", "passes", "incomplete", "inapplicable"]

WCAG22AA_TAGS = ["wcag2a", "wcag2aa", "wcag21a", "wcag21aa", "wcag22aa"]


def _parse_csv(value: Optional[str]) -> List[str]:
    if value is None:
        return []
    return [part.strip() for part in str(value).split(",") if part.strip()]


def _parse_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _load_axe_class():
    module_candidates = [
        ("axe_playwright_python.async_playwright", "Axe"),
        ("axe_playwright_python", "Axe"),
        ("axe_playwright_python.sync_playwright", "Axe"),
        ("axe_playwright_python.axe", "Axe"),
    ]
    for module_name, attr_name in module_candidates:
        try:
            module = importlib.import_module(module_name)
        except ImportError:
            continue
        axe_cls = getattr(module, attr_name, None)
        if axe_cls is not None:
            return axe_cls
    return None


async def run_axe_scan(
    page: Any,
    scope_selector: str = "",
    include_best_practices: bool = True,
    include_experimental: bool = False,
    full_scan: bool = False,
    custom_tags: str = "",
    exclude_tags: str = "",
    enabled_rules: str = "",
    disabled_rules: str = "",
    include_iframes: bool = True,
    include_selectors: bool = True,
    include_ancestry: bool = True,
    result_types: str = "",
    reporter: str = "v2",
) -> Dict[str, Any]:
    axe_cls = _load_axe_class()
    if axe_cls is None:
        return {
            "status": "unavailable",
            "error": "axe-playwright-python is not installed in this environment.",
            "results": None,
        }

    tags = _parse_csv(custom_tags)
    if not tags and not full_scan:
        tags = list(WCAG22AA_TAGS)
        if include_best_practices:
            tags.append("best-practice")
        if include_experimental:
            tags.append("experimental")

    excluded = set(_parse_csv(exclude_tags))
    if excluded:
        tags = [tag for tag in tags if tag not in excluded]

    options: Dict[str, Any] = {
        "resultTypes": _parse_csv(result_types) or list(DEFAULT_RESULT_TYPES),
        "iframes": _parse_bool(include_iframes, True),
        "selectors": _parse_bool(include_selectors, True),
        "ancestry": _parse_bool(include_ancestry, True),
        "reporter": reporter or "v2",
    }
    if tags and not full_scan:
        options["runOnly"] = {
            "type": "tag",
            "values": tags,
        }

    rules: Dict[str, Dict[str, Any]] = {}
    for rule_id in _parse_csv(disabled_rules):
        rules[rule_id] = {"enabled": False}
    for rule_id in _parse_csv(enabled_rules):
        rules.setdefault(rule_id, {})
        rules[rule_id]["enabled"] = True
    if rules:
        options["rules"] = rules

    context: Optional[str] = scope_selector or None

    try:
        axe = axe_cls()
        results = await axe.run(page, context=context, options=options)
        result_payload = getattr(results, "response", results)

        snapshot: Optional[str] = None
        report: Optional[str] = None
        violations_count: Optional[int] = None

        try:
            violations_count = getattr(results, "violations_count", None)
        except Exception:
            pass

        try:
            snapshot = results.generate_snapshot()
        except Exception:
            snapshot = None

        try:
            report = results.generate_report()
        except Exception:
            report = None

        return {
            "status": "success",
            "error": None,
            "results": result_payload,
            "options": options,
            "violations_count": violations_count,
            "snapshot": snapshot,
            "report": report,
        }
    except Exception as exc:
        return {
            "status": "error",
            "error": str(exc),
            "results": None,
            "options": options,
            "violations_count": None,
            "snapshot": None,
            "report": None,
        }
