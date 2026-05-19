"""Digest JSON builder for LLM analysis."""
from __future__ import annotations

import json
from typing import Any, Dict, List


_DIGEST_EVIDENCE_KEYS = {"source", "severity", "outcome", "message", "target"}

# Bumped when digest schema changes. Strict consumers can branch on this.
_DIGEST_SCHEMA_VERSION = 2


def build_digest_json(report: Dict[str, Any]) -> str:
    """Minimal JSON for LLM analysis.

    v2 schema additions:
      - per-criterion: pass_rationale, tested_aspects, untested_aspects,
        automation_limits, manifest_refs.
      - top-level: evaluator_manifests (every manifest, not just used).
      - top-level: schema_version.

    Includes FAILED + NEEDS_REVIEW criteria. PASSED with limitation_pass /
    heuristic_proxy rationales are surfaced in a soft_pass list so reviewers
    can audit them without parsing the full report.
    """
    failed: List[Dict[str, Any]] = []
    soft_pass: List[Dict[str, Any]] = []
    for c in (report.get("criteria") or []):
        outcome = c.get("outcome_status")
        if not outcome:
            continue
        rationale = c.get("pass_rationale")
        if outcome not in ("FAILED", "ERROR", "NEEDS_REVIEW") and rationale not in (
            "limitation_pass",
            "heuristic_proxy",
        ):
            continue
        slim_evidence = [
            {k: v for k, v in e.items() if k in _DIGEST_EVIDENCE_KEYS}
            for e in (c.get("evidence") or [])
        ]
        entry = {
            "id": c.get("id"),
            "name": c.get("name"),
            "level": c.get("level"),
            "principle": c.get("principle"),
            "coverage_status": c.get("coverage_status"),
            "outcome_status": outcome,
            "pass_rationale": rationale,
            "tested_aspects": c.get("tested_aspects") or [],
            "untested_aspects": c.get("untested_aspects") or [],
            "automation_limits": c.get("automation_limits") or [],
            "manifest_refs": c.get("manifest_refs") or [],
            "affected_screens": c.get("affected_screens"),
            "affected_urls": c.get("affected_urls"),
            "evidence_count": len(slim_evidence),
            "evidence": slim_evidence,
        }
        if outcome in ("FAILED", "ERROR", "NEEDS_REVIEW"):
            failed.append(entry)
        else:
            soft_pass.append(entry)

    payload = {
        "schema_version": _DIGEST_SCHEMA_VERSION,
        "report_meta": report.get("report_meta", {}),
        "summary": report.get("summary", {}),
        "evaluator_manifests": report.get("evaluator_manifests", []) or [],
        "failed": failed,
        "soft_pass": soft_pass,
    }
    return json.dumps(payload, indent=2, default=str)
