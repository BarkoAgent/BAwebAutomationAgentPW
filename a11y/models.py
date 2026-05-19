from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional


COVERAGE_AUTOMATED = "AUTOMATED"
COVERAGE_SEMI_AUTOMATED = "SEMI_AUTOMATED"
COVERAGE_MANUAL_REQUIRED = "MANUAL_REQUIRED"
COVERAGE_NOT_TESTED = "NOT_TESTED"

OUTCOME_PASSED = "PASSED"
OUTCOME_FAILED = "FAILED"
OUTCOME_NEEDS_REVIEW = "NEEDS_REVIEW"
OUTCOME_NOT_TESTED = "NOT_TESTED"
OUTCOME_NOT_APPLICABLE = "NOT_APPLICABLE"
OUTCOME_ERROR = "ERROR"

# Pass-rationale tags. Populated only when outcome_status == PASSED.
PASS_RATIONALE_ALL_CHECKED_CLEAN = "all_checked_clean"      # every applicable element verified
PASS_RATIONALE_NO_APPLICABLE = "no_applicable_elements"     # nothing on the page matched the rule scope
PASS_RATIONALE_AXE_RULE_CLEAN = "axe_rule_clean"            # axe rule fired and reported only passes
PASS_RATIONALE_HEURISTIC_PROXY = "heuristic_proxy"          # custom evaluator probed a sample, no failure
PASS_RATIONALE_LIMITATION_PASS = "limitation_pass"          # axe ran but incomplete checks dominate

KIND_SUCCESS_CRITERION = "success_criterion"
KIND_CONFORMANCE_REQUIREMENT = "conformance_requirement"


@dataclass
class CriterionDefinition:
    id: str
    kind: str
    name: str
    principle: str
    guideline: str
    level: str
    wcag_version: str
    doc_url: str
    default_coverage: str
    automation_sources: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)


@dataclass
class EvidenceLocation:
    page_url: str
    page_path: str
    page_title: str
    screen_key: str
    screen_label: str
    journey_name: str
    journey_step_index: int
    journey_step_label: str
    view_type: str
    container_label: Optional[str]
    frame_name: Optional[str]
    modal_label: Optional[str]
    locator: str
    element_text: str
    screenshot_ref: Optional[str]
    report_anchor: str


@dataclass
class EvidenceItem:
    source: str
    severity: str
    target: List[str]
    message: str
    outcome: str
    location: EvidenceLocation
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class EvaluatorManifest:
    """Self-description of one evaluator (axe block or custom evaluator).

    Lives next to the probing logic so methodology cannot drift from code.
    Aggregated into reports so stakeholders see what was tested and where
    automation stops, per criterion.
    """
    id: str                                  # e.g. "custom:hover_content" or "axe:wcag-1.4.13"
    name: str                                # human-readable label
    criteria: List[str]                      # WCAG criterion IDs covered
    coverage_mode: str                       # AUTOMATED / SEMI_AUTOMATED
    what_tested: List[str] = field(default_factory=list)
    what_not_tested: List[str] = field(default_factory=list)
    sampling: Optional[str] = None           # e.g. "first 3 candidate triggers per checkpoint"
    automation_limits: List[str] = field(default_factory=list)
    manual_followup: List[str] = field(default_factory=list)


@dataclass
class CriterionResult:
    id: str
    kind: str
    name: str
    principle: str
    guideline: str
    level: str
    coverage_status: str
    outcome_status: str
    doc_url: str
    coverage_notes: List[str] = field(default_factory=list)
    affected_screens: List[str] = field(default_factory=list)
    affected_urls: List[str] = field(default_factory=list)
    affected_steps: List[str] = field(default_factory=list)
    sources: List[str] = field(default_factory=list)
    evidence: List[EvidenceItem] = field(default_factory=list)
    failed_nodes: List[Dict[str, Any]] = field(default_factory=list)
    passed_checks: List[Dict[str, Any]] = field(default_factory=list)
    incomplete_checks: List[Dict[str, Any]] = field(default_factory=list)
    not_tested_explanation: str = ""
    run_history_summary: Dict[str, Any] = field(default_factory=dict)
    # Transparency layer — populated during aggregation in runner.py.
    pass_rationale: Optional[str] = None
    tested_aspects: List[str] = field(default_factory=list)
    untested_aspects: List[str] = field(default_factory=list)
    automation_limits: List[str] = field(default_factory=list)
    manifest_refs: List[str] = field(default_factory=list)

    def add_source(self, source: str) -> None:
        if source not in self.sources:
            self.sources.append(source)

    def add_affected_location(self, location: EvidenceLocation) -> None:
        if location.screen_label and location.screen_label not in self.affected_screens:
            self.affected_screens.append(location.screen_label)
        if location.page_url and location.page_url not in self.affected_urls:
            self.affected_urls.append(location.page_url)
        if location.journey_step_label and location.journey_step_label not in self.affected_steps:
            self.affected_steps.append(location.journey_step_label)


@dataclass
class ReportSection:
    id: str
    title: str
    rows: List[str]


@dataclass
class AccessibilityReport:
    report_meta: Dict[str, Any]
    execution: Dict[str, Any]
    summary: Dict[str, Any]
    sections: List[ReportSection]
    criteria: List[CriterionResult]
    raw_sources: Dict[str, Any]
    artifacts: Dict[str, Any]
    evaluator_manifests: List[EvaluatorManifest] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
