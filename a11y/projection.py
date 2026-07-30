"""
Report projections computed agent-side.

The full report JSON is dominated by evidence payloads (inline base64
screenshots in particular), while the UI only ever renders a small slice of
it. Projecting here instead of in the API keeps the WS reply small: the digest
for a 27 MB report is ~86 KB, and the reply no longer risks tripping uvicorn's
16 MiB ws_max_size cap.

These functions are pure — they take an already-loaded report dict and return
plain data. The API mirrors them as a fallback for agents that predate the
digest/criterion RPCs, so keep the two implementations in sync.
"""
from typing import Any, Dict, List, Optional

_PILLAR_KEY = {"P": "perceivable", "O": "operable", "U": "understandable", "R": "robust"}
_SOFT_PASS_RATIONALES = {"heuristic_proxy", "limitation_pass", "no_applicable_elements"}
_FAIL_OUTCOMES = {"FAILED", "ERROR", "NEEDS_REVIEW"}
_MATRIX_MAX_SCREENS = 50
_MATRIX_MAX_CRITERIA = 30

# Criteria carry no own `impact` field; severity lives only on evidence/failed nodes.
# Derive a criterion-level impact as the worst severity across its failing evidence
# so the UI's impact filter and impact sort have data to act on.
_IMPACT_RANK = {"critical": 0, "serious": 1, "moderate": 2, "minor": 3}

# Evidence is previewed only — the full rows come from slice_criterion().
_EVIDENCE_FIELDS = ("source", "severity", "target", "message", "outcome")
_EVIDENCE_LIMIT = 5
_SOURCES_LIMIT = 5


def _trim_evaluator_manifests(manifests):
    if not isinstance(manifests, list):
        return manifests
    out = []
    for m in manifests:
        if not isinstance(m, dict):
            continue
        out.append({k: m.get(k) for k in ("id", "name", "version") if k in m})
    return out


def _derive_impact(c: dict):
    """Worst (lowest-rank) severity among a criterion's failed nodes + evidence, or None."""
    best = None
    best_rank = 99
    for n in c.get("failed_nodes") or []:
        if not isinstance(n, dict):
            continue
        imp = str(n.get("impact") or "").lower()
        rank = _IMPACT_RANK.get(imp, 99)
        if rank < best_rank:
            best, best_rank = imp, rank
    for e in c.get("evidence") or []:
        if not isinstance(e, dict):
            continue
        sev = str(e.get("severity") or "").lower()
        rank = _IMPACT_RANK.get(sev, 99)
        if rank < best_rank:
            best, best_rank = sev, rank
    return best


def _principle_letter(principle: str) -> str:
    if not principle:
        return ""
    p = str(principle).strip().upper()
    return p[0] if p and p[0] in _PILLAR_KEY else ""


def _bucket(outcome: str) -> str:
    o = str(outcome or "").upper()
    if o in ("FAILED", "ERROR"):
        return "fail"
    if o == "NEEDS_REVIEW":
        return "review"
    if o == "PASSED":
        return "pass"
    if o in ("NOT_TESTED", "N_A", "NA", ""):
        return "na"
    return "unknown"


def build_overview(criteria: list) -> dict:
    pillars = {k: {"fail": 0, "review": 0, "pass": 0, "na": 0, "unknown": 0} for k in ("P", "O", "U", "R")}
    levels = {k: {"fail": 0, "review": 0, "pass": 0, "na": 0, "unknown": 0} for k in ("A", "AA", "AAA")}
    screen_counts: dict = {}
    matrix_screens: list = []
    matrix_screens_set: set = set()
    matrix_criteria: list = []
    cells: dict = {}

    for c in criteria:
        if not isinstance(c, dict):
            continue
        outcome = c.get("outcome_status") or c.get("outcome")
        bucket = _bucket(outcome)
        letter = _principle_letter(c.get("principle"))
        if letter:
            pillars[letter][bucket] = pillars[letter].get(bucket, 0) + 1
        lvl = str(c.get("level") or "").upper()
        if lvl in levels:
            levels[lvl][bucket] = levels[lvl].get(bucket, 0) + 1

        affected = [s for s in (c.get("affected_screens") or []) if s and isinstance(s, str)]
        if outcome in _FAIL_OUTCOMES:
            for s in affected:
                screen_counts[s] = screen_counts.get(s, 0) + 1

        if outcome in _FAIL_OUTCOMES and len(matrix_criteria) < _MATRIX_MAX_CRITERIA:
            cid = c.get("id")
            if cid:
                matrix_criteria.append({"id": cid, "name": c.get("name"), "outcome": outcome})
                row = {}
                for s in affected:
                    if s not in matrix_screens_set and len(matrix_screens) < _MATRIX_MAX_SCREENS:
                        matrix_screens.append(s)
                        matrix_screens_set.add(s)
                    if s in matrix_screens_set:
                        row[s] = outcome
                cells[cid] = row

    top_offenders = [
        {"screen": s, "count": n}
        for s, n in sorted(screen_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:10]
    ]

    matrix_truncated = (
        len(matrix_criteria) >= _MATRIX_MAX_CRITERIA
        or len(matrix_screens) >= _MATRIX_MAX_SCREENS
    )

    return {
        "pillars": pillars,
        "levels": levels,
        "top_offender_screens": top_offenders,
        "screen_criterion_matrix": {
            "screens": matrix_screens,
            "criteria": matrix_criteria,
            "cells": cells,
            "truncated": matrix_truncated,
            "included_criteria_count": len(matrix_criteria),
            "included_screens_count": len(matrix_screens),
        },
    }


def _slim_criterion(c: dict) -> dict:
    evidence_full = c.get("evidence") or []
    evidence_slim = [
        {k: e[k] for k in _EVIDENCE_FIELDS if k in e}
        for e in evidence_full[:_EVIDENCE_LIMIT]
        if isinstance(e, dict)
    ]
    sources = c.get("sources") or []
    affected_screens = c.get("affected_screens") or []
    affected_urls = c.get("affected_urls") or []
    affected_steps = c.get("affected_steps") or []
    return {
        "id": c.get("id"),
        "name": c.get("name"),
        "outcome_status": c.get("outcome_status") or c.get("outcome"),
        "impact": c.get("impact") or _derive_impact(c),
        "level": c.get("level"),
        "principle": c.get("principle"),
        "guideline": c.get("guideline"),
        "coverage_status": c.get("coverage_status"),
        "pass_rationale": c.get("pass_rationale"),
        "evidence_count": len(evidence_full),
        "evidence_truncated": len(evidence_full) > _EVIDENCE_LIMIT,
        "evidence": evidence_slim,
        "failed_node_count": len(c.get("failed_nodes") or []),
        "doc_url": c.get("doc_url"),
        "tags": c.get("tags"),
        "sources": sources[:_SOURCES_LIMIT],
        "sources_count": len(sources),
        "affected_screens": affected_screens,
        "affected_screens_count": len(affected_screens),
        "affected_urls_count": len(affected_urls),
        "affected_steps_count": len(affected_steps),
        "tested_aspects_count": len(c.get("tested_aspects") or []),
        "untested_aspects_count": len(c.get("untested_aspects") or []),
        "automation_limits_count": len(c.get("automation_limits") or []),
        "not_tested_explanation": c.get("not_tested_explanation") or "",
    }


def build_digest(full: Dict[str, Any]) -> Dict[str, Any]:
    """Project a full report into the payload the A11y tab's overview needs."""
    criteria = full.get("criteria") or []
    failed: List[dict] = []
    needs_review: List[dict] = []
    soft_pass: List[dict] = []
    not_tested: List[dict] = []

    for c in criteria:
        if not isinstance(c, dict):
            continue
        slim = _slim_criterion(c)
        rationale = c.get("pass_rationale")
        outcome_u = str(slim["outcome_status"] or "").upper()
        if outcome_u in ("FAILED", "ERROR"):
            failed.append(slim)
        elif outcome_u == "NEEDS_REVIEW":
            needs_review.append(slim)
        elif outcome_u == "PASSED" and rationale in _SOFT_PASS_RATIONALES:
            soft_pass.append(slim)
        elif outcome_u == "NOT_TESTED" or str(c.get("coverage_status") or "").upper() == "NOT_TESTED":
            not_tested.append(slim)

    # Single source of truth for the headline metrics the UI surfaces.
    # failed_criteria stays the primary number; failing_nodes is the secondary
    # instance-level metric (distinct DOM nodes that failed, across failed criteria).
    issue_totals = {
        "failed_criteria": len(failed),
        "needs_review_criteria": len(needs_review),
        "failing_nodes": sum(int(item.get("failed_node_count") or 0) for item in failed),
    }

    return {
        "report_meta": full.get("report_meta"),
        "summary": full.get("summary"),
        "artifacts": full.get("artifacts"),
        "evaluator_manifests": _trim_evaluator_manifests(full.get("evaluator_manifests")),
        "overview": build_overview(criteria),
        "issue_totals": issue_totals,
        "failed": failed,
        "needs_review": needs_review,
        "soft_pass": soft_pass,
        "not_tested": not_tested,
    }


def screenshots_for_criterion(full: Dict[str, Any], criterion: Dict[str, Any]) -> Dict[str, str]:
    """The subset of the report's screenshot map this criterion's evidence references.

    Sending the used keys rather than inline data URIs is what keeps a criterion with
    many rows sharing one page screenshot small — the worst case measured went from
    11.84 MiB to well under 1 MiB. Reports predating the dedup carry inline data URIs
    and yield an empty map, which consumers handle.
    """
    store = full.get("screenshots") or {}
    if not store:
        return {}
    used: Dict[str, str] = {}
    for item in criterion.get("evidence") or []:
        location = item.get("location") if isinstance(item, dict) else None
        if not isinstance(location, dict):
            continue
        ref = location.get("screenshot_ref")
        if isinstance(ref, str) and ref and not ref.startswith("data:") and ref in store:
            used[ref] = store[ref]
    return used


def slice_criterion(
    full: Dict[str, Any], criterion_id: str, include_passed: bool = False
) -> Optional[Dict[str, Any]]:
    """Return one criterion in full detail, or None when the id is unknown."""
    criteria = full.get("criteria") or []
    match = None
    for c in criteria:
        if isinstance(c, dict) and c.get("id") == criterion_id:
            match = c
            break
    if match is None:
        return None

    out = dict(match)
    if not include_passed:
        out.pop("passed_checks", None)
    return {
        "report_id": (full.get("report_meta") or {}).get("report_id"),
        "criterion": out,
        "screenshots": screenshots_for_criterion(full, out),
    }
