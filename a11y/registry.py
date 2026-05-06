import re
from typing import List

from .models import (
    COVERAGE_AUTOMATED,
    COVERAGE_MANUAL_REQUIRED,
    COVERAGE_SEMI_AUTOMATED,
    KIND_CONFORMANCE_REQUIREMENT,
    KIND_SUCCESS_CRITERION,
    CriterionDefinition,
)


PRINCIPLE_NAMES = {
    "1": "Perceivable",
    "2": "Operable",
    "3": "Understandable",
    "4": "Robust",
}

SECTION_IDS = {
    "Perceivable": "perceivable",
    "Operable": "operable",
    "Understandable": "understandable",
    "Robust": "robust",
    "Conformance Requirements": "conformance",
}

DEFAULT_AUTOMATED_IDS = {
    "1.1.1",
    "1.2.2",  # Captions detected via <track> element presence
    "1.3.1",
    "1.3.5",
    "1.4.2",  # Audio control: autoplay without mute/controls
    "1.4.3",
    "1.4.10",
    "1.4.11",
    "1.4.12",  # Text spacing overflow detected programmatically
    "2.2.2",  # Carousel/marquee pause control detection
    "2.4.1",
    "2.4.2",
    "2.4.4",
    "2.4.6",
    "2.5.3",
    "2.5.8",  # Target size measured via getBoundingClientRect
    "3.1.1",
    "3.2.3",
    "3.3.2",
    "4.1.2",
    "4.1.3",
}

DEFAULT_SEMI_AUTOMATED_IDS = {
    "1.2.1",  # Audio/video alternatives: transcript link detection
    "1.2.5",  # Audio description track detection
    "1.3.2",
    "1.3.4",  # Orientation: CSS lock detection + viewport reflow
    "1.4.4",  # Resize text: zoom clipping detection
    "1.4.13",  # Hover content: dismissibility probe
    "2.1.1",
    "2.1.2",
    "2.2.1",  # Timing adjustable: countdown timer detection
    "2.4.3",
    "2.4.7",
    "2.4.11",
    "3.2.1",  # On focus: context change monitoring
    "3.2.2",  # On input: context change monitoring
    "3.3.1",
    "3.3.3",
    "3.3.4",
    "3.3.7",
    "3.3.8",
}

SUCCESS_CRITERIA = [
    ("1.1.1", "Non-text Content", "1.1 Text Alternatives", "A"),
    ("1.2.1", "Audio-only and Video-only (Prerecorded)", "1.2 Time-based Media", "A"),
    ("1.2.2", "Captions (Prerecorded)", "1.2 Time-based Media", "A"),
    ("1.2.3", "Audio Description or Media Alternative (Prerecorded)", "1.2 Time-based Media", "A"),
    ("1.2.4", "Captions (Live)", "1.2 Time-based Media", "AA"),
    ("1.2.5", "Audio Description (Prerecorded)", "1.2 Time-based Media", "AA"),
    ("1.3.1", "Info and Relationships", "1.3 Adaptable", "A"),
    ("1.3.2", "Meaningful Sequence", "1.3 Adaptable", "A"),
    ("1.3.3", "Sensory Characteristics", "1.3 Adaptable", "A"),
    ("1.3.4", "Orientation", "1.3 Adaptable", "AA"),
    ("1.3.5", "Identify Input Purpose", "1.3 Adaptable", "AA"),
    ("1.4.1", "Use of Color", "1.4 Distinguishable", "A"),
    ("1.4.2", "Audio Control", "1.4 Distinguishable", "A"),
    ("1.4.3", "Contrast (Minimum)", "1.4 Distinguishable", "AA"),
    ("1.4.4", "Resize Text", "1.4 Distinguishable", "AA"),
    ("1.4.5", "Images of Text", "1.4 Distinguishable", "AA"),
    ("1.4.10", "Reflow", "1.4 Distinguishable", "AA"),
    ("1.4.11", "Non-text Contrast", "1.4 Distinguishable", "AA"),
    ("1.4.12", "Text Spacing", "1.4 Distinguishable", "AA"),
    ("1.4.13", "Content on Hover or Focus", "1.4 Distinguishable", "AA"),
    ("2.1.1", "Keyboard", "2.1 Keyboard Accessible", "A"),
    ("2.1.2", "No Keyboard Trap", "2.1 Keyboard Accessible", "A"),
    ("2.1.4", "Character Key Shortcuts", "2.1 Keyboard Accessible", "A"),
    ("2.2.1", "Timing Adjustable", "2.2 Enough Time", "A"),
    ("2.2.2", "Pause, Stop, Hide", "2.2 Enough Time", "A"),
    ("2.3.1", "Three Flashes or Below Threshold", "2.3 Seizures and Physical Reactions", "A"),
    ("2.4.1", "Bypass Blocks", "2.4 Navigable", "A"),
    ("2.4.2", "Page Titled", "2.4 Navigable", "A"),
    ("2.4.3", "Focus Order", "2.4 Navigable", "A"),
    ("2.4.4", "Link Purpose (In Context)", "2.4 Navigable", "A"),
    ("2.4.5", "Multiple Ways", "2.4 Navigable", "AA"),
    ("2.4.6", "Headings and Labels", "2.4 Navigable", "AA"),
    ("2.4.7", "Focus Visible", "2.4 Navigable", "AA"),
    ("2.4.11", "Focus Not Obscured (Minimum)", "2.4 Navigable", "AA"),
    ("2.5.1", "Pointer Gestures", "2.5 Input Modalities", "A"),
    ("2.5.2", "Pointer Cancellation", "2.5 Input Modalities", "A"),
    ("2.5.3", "Label in Name", "2.5 Input Modalities", "A"),
    ("2.5.4", "Motion Actuation", "2.5 Input Modalities", "A"),
    ("2.5.7", "Dragging Movements", "2.5 Input Modalities", "AA"),
    ("2.5.8", "Target Size (Minimum)", "2.5 Input Modalities", "AA"),
    ("3.1.1", "Language of Page", "3.1 Readable", "A"),
    ("3.1.2", "Language of Parts", "3.1 Readable", "AA"),
    ("3.2.1", "On Focus", "3.2 Predictable", "A"),
    ("3.2.2", "On Input", "3.2 Predictable", "A"),
    ("3.2.3", "Consistent Navigation", "3.2 Predictable", "AA"),
    ("3.2.4", "Consistent Identification", "3.2 Predictable", "AA"),
    ("3.2.6", "Consistent Help", "3.2 Predictable", "A"),
    ("3.3.1", "Error Identification", "3.3 Input Assistance", "A"),
    ("3.3.2", "Labels or Instructions", "3.3 Input Assistance", "A"),
    ("3.3.3", "Error Suggestion", "3.3 Input Assistance", "AA"),
    ("3.3.4", "Error Prevention (Legal, Financial, Data)", "3.3 Input Assistance", "AA"),
    ("3.3.7", "Redundant Entry", "3.3 Input Assistance", "A"),
    ("3.3.8", "Accessible Authentication (Minimum)", "3.3 Input Assistance", "AA"),
    ("4.1.2", "Name, Role, Value", "4.1 Compatible", "A"),
    ("4.1.3", "Status Messages", "4.1 Compatible", "AA"),
]

CONFORMANCE_REQUIREMENTS = [
    ("CC1", "Conformance Level"),
    ("CC2", "Full pages"),
    ("CC3", "Complete processes"),
    ("CC4", "Only Accessibility-Supported Ways of Using Technologies"),
    ("CC5", "Non-Interference"),
]


def _doc_url_for(criterion_id: str) -> str:
    row_map = {row_id: name for row_id, name, _, _ in SUCCESS_CRITERIA}
    name = row_map.get(criterion_id)
    if not name:
        return "https://www.w3.org/TR/WCAG22/"
    anchor = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    return "https://www.w3.org/TR/WCAG22/#{}".format(anchor)


def _default_coverage_for(criterion_id: str) -> str:
    if criterion_id in DEFAULT_AUTOMATED_IDS:
        return COVERAGE_AUTOMATED
    if criterion_id in DEFAULT_SEMI_AUTOMATED_IDS:
        return COVERAGE_SEMI_AUTOMATED
    return COVERAGE_MANUAL_REQUIRED


ALLOWED_LEVELS = frozenset({"A", "AA"})


def build_registry() -> List[CriterionDefinition]:
    rows: List[CriterionDefinition] = []

    for criterion_id, name, guideline, level in SUCCESS_CRITERIA:
        if level not in ALLOWED_LEVELS:
            continue
        principle = PRINCIPLE_NAMES[criterion_id.split(".")[0]]
        rows.append(
            CriterionDefinition(
                id=criterion_id,
                kind=KIND_SUCCESS_CRITERION,
                name=name,
                principle=principle,
                guideline=guideline,
                level=level,
                wcag_version="2.2",
                doc_url=_doc_url_for(criterion_id),
                default_coverage=_default_coverage_for(criterion_id),
                notes=[
                    "Default coverage classification is based on current automation support, not a claim of completed evaluation.",
                ],
            )
        )

    for criterion_id, name in CONFORMANCE_REQUIREMENTS:
        rows.append(
            CriterionDefinition(
                id=criterion_id,
                kind=KIND_CONFORMANCE_REQUIREMENT,
                name=name,
                principle="Conformance Requirements",
                guideline="5.2 Conformance Requirements",
                level="N/A",
                wcag_version="2.2",
                doc_url="https://www.w3.org/TR/WCAG22/#conformance-requirements",
                default_coverage=COVERAGE_MANUAL_REQUIRED,
                notes=["Requires contextual review beyond automated page scanning."],
            )
        )

    return rows
