"""Self-describing manifests for every evaluator that contributes evidence.

Centralised so the methodology stays in one auditable location, and so the
report can render "what was checked / what was not / what cannot be checked"
without each evaluator module growing prose alongside its JS strings.

Each manifest declares the narrow slice of a WCAG criterion the evaluator
exercises. Anything outside that slice belongs in `what_not_tested` or
`automation_limits`.
"""
from __future__ import annotations

from typing import Dict, List

from ..mappings import AXE_RULE_TO_WCAG
from ..models import (
    COVERAGE_AUTOMATED,
    COVERAGE_SEMI_AUTOMATED,
    EvaluatorManifest,
)


CUSTOM_MANIFESTS: List[EvaluatorManifest] = [
    EvaluatorManifest(
        id="custom:keyboard_smoke",
        name="Keyboard reachability & no-trap smoke test",
        criteria=["2.1.1", "2.1.2", "2.4.3", "2.4.7"],
        coverage_mode=COVERAGE_SEMI_AUTOMATED,
        what_tested=[
            "Tab walks through interactive elements (anchors, buttons, inputs, [tabindex>=0]).",
            "Detects keyboard trap by watching for the active element to repeat.",
            "Captures basic focus order and reachability snapshot.",
        ],
        what_not_tested=[
            "Activation by keyboard (Enter/Space) of every control.",
            "Composite-widget keyboard semantics (arrow-key navigation in menus, listboxes, grids).",
            "Mouse-only handlers that have no keyboard equivalent (event-listener inspection only catches obvious cases).",
            "Shift+Tab reverse traversal across the full DOM.",
        ],
        sampling="Up to 30 forward Tab presses per checkpoint; stops on first detected loop or trap.",
        automation_limits=[
            "A 'no trap' result only proves no trap was hit within the Tab budget — long pages may have traps further in.",
            "Frames/iframes are walked only as far as the browser's default focus traversal allows.",
        ],
        manual_followup=[
            "Drive each composite widget (menus, dialogs, custom dropdowns) with a keyboard and verify ARIA-spec'd key handling.",
        ],
    ),
    EvaluatorManifest(
        id="custom:focus_visibility",
        name="Focus visibility (outline / shadow / border)",
        criteria=["2.4.7", "2.4.11", "2.4.12"],
        coverage_mode=COVERAGE_SEMI_AUTOMATED,
        what_tested=[
            "After each Tab, inspects computed style of document.activeElement for non-zero outline, box-shadow, or border change.",
            "Flags missing visible focus indicators per element walked.",
            "Detects whether the focused element is fully obscured (covered by other elements at its centre).",
        ],
        what_not_tested=[
            "Whether the indicator meets WCAG 2.4.13 area + contrast thresholds (handled by focus_appearance evaluator).",
            "Custom focus rings drawn outside the element's bounding box (e.g. via ::before).",
            "Focus visibility under high-contrast / forced-colors mode.",
        ],
        sampling="One probe per Tab stop walked by the keyboard evaluator.",
        automation_limits=[
            "Reading computed style approximates 'visible' — a 1px outline of near-background colour reads as present but is invisible to humans.",
        ],
        manual_followup=[
            "Tab through with a real keyboard on the deployed colour theme and verify each indicator is unmistakable.",
        ],
    ),
    EvaluatorManifest(
        id="custom:focus_appearance",
        name="Focus appearance — area & contrast (2.4.13 AAA)",
        criteria=["2.4.13"],
        coverage_mode=COVERAGE_SEMI_AUTOMATED,
        what_tested=[
            "For each focused element: indicator perimeter area against the 2px×perimeter minimum.",
            "Indicator-vs-adjacent contrast computed from outline / box-shadow colour.",
        ],
        what_not_tested=[
            "Whether the indicator remains visible against patterned or image backgrounds.",
            "Indicator continuity (broken outlines on rounded corners, partial occlusion).",
        ],
        sampling="One probe per Tab stop walked by the keyboard evaluator.",
        automation_limits=[
            "Adjacent-colour sampling reads the computed background of the parent element, not the rendered pixel — gradients/images defeat it.",
            "Box-shadow alpha and offset are heuristically converted to an effective indicator area; complex shadow stacks are approximated.",
        ],
        manual_followup=[
            "Verify focus appearance against the live theme on each background variant and dark mode.",
        ],
    ),
    EvaluatorManifest(
        id="custom:form_labeling",
        name="Form field labelling & error feedback",
        criteria=["3.3.1", "3.3.2", "3.3.3"],
        coverage_mode=COVERAGE_SEMI_AUTOMATED,
        what_tested=[
            "Each input/select/textarea is checked for an associated <label>, aria-label, aria-labelledby, or wrapping label.",
            "Submitted-state validationMessage is captured for native validation evidence.",
            "Detects label-as-placeholder antipattern.",
        ],
        what_not_tested=[
            "Whether the visible label semantically matches the field's purpose.",
            "Server-rendered error messages that appear only after a real submit.",
            "Multi-step form flows beyond the captured checkpoint.",
        ],
        sampling="All form fields visible at the captured checkpoint.",
        automation_limits=[
            "ARIA-labelled fields whose referenced node is hidden or removed read as labelled.",
        ],
        manual_followup=[
            "Submit forms with invalid data and verify the error suggestion text is helpful and announced.",
        ],
    ),
    EvaluatorManifest(
        id="custom:hover_content",
        name="Content on hover or focus (1.4.13)",
        criteria=["1.4.13"],
        coverage_mode=COVERAGE_SEMI_AUTOMATED,
        what_tested=[
            "Dismissibility: hover triggers, then sends Escape and watches for tooltip removal.",
            "Hoverability: incrementally moves the pointer trigger→tooltip and watches that the tooltip stays.",
            "Reports presence of native title-attribute tooltips (informational only).",
        ],
        what_not_tested=[
            "Persistence — whether tooltip stays until dismissed, focus moves, or pointer leaves (only Escape + hover-bridge are probed).",
            "Browser-chrome `title` tooltips (no DOM node — cannot probe; reported informational only).",
            "CSS-only :hover tooltips that appear without DOM mutation (no MutationObserver event to detect them).",
            "Focus-triggered tooltips that require keyboard interaction beyond the keyboard evaluator's walk.",
        ],
        sampling="First 3 candidate triggers per checkpoint (`_MAX_PROBES = 3`); 8 incremental hover steps trigger→tooltip.",
        automation_limits=[
            "MutationObserver only sees DOM-attached tooltips. Pure-CSS or canvas-rendered tooltips are invisible to this probe.",
            "Some triggers hide their tooltip via opacity/visibility transitions slower than the 600ms settle window.",
        ],
        manual_followup=[
            "For every tooltip-emitting control, verify Escape dismisses, pointer-bridge works, and content stays until intentional dismissal.",
        ],
    ),
    EvaluatorManifest(
        id="custom:structure",
        name="Document structure: landmarks, headings, lists, ARIA roles",
        criteria=["1.3.1", "2.4.1", "2.4.6", "4.1.2"],
        coverage_mode=COVERAGE_SEMI_AUTOMATED,
        what_tested=[
            "Landmark counts (banner / main / contentinfo / navigation) and uniqueness.",
            "Heading hierarchy: presence of h1, level-skip detection.",
            "List semantics: <ul>/<ol> contain only <li> children, definition lists well-formed.",
            "Empty headings / empty interactive labels.",
        ],
        what_not_tested=[
            "Whether heading text accurately summarises its section.",
            "Reading order vs visual order on canvas-positioned content (1.3.2 — only partially covered).",
            "Custom-role widgets implementing their own internal structure.",
        ],
        sampling="Whole-page DOM scan at the captured checkpoint.",
        automation_limits=[
            "JS-driven late-injected content arriving after the checkpoint is missed.",
        ],
        manual_followup=[
            "Sanity-check headings against the visual hierarchy on the live page.",
        ],
    ),
    EvaluatorManifest(
        id="custom:live_region",
        name="ARIA live regions (4.1.3 Status Messages)",
        criteria=["4.1.3"],
        coverage_mode=COVERAGE_SEMI_AUTOMATED,
        what_tested=[
            "Presence and configuration (politeness, atomic, role) of [aria-live], [role=status], [role=alert] regions.",
            "Captures up to 20 live-region nodes for review.",
        ],
        what_not_tested=[
            "Whether the live region actually announces in real screen readers.",
            "Timing/throttling of announcements relative to user actions.",
            "Status messages delivered via DOM mutations without an aria-live container (will be missed).",
        ],
        sampling="First 20 live-region nodes at the checkpoint.",
        automation_limits=[
            "The DOM presence of an aria-live region does not prove a screen reader will announce it correctly.",
        ],
        manual_followup=[
            "Trigger each status flow with NVDA / VoiceOver and confirm the announcement.",
        ],
    ),
    EvaluatorManifest(
        id="custom:viewport_reflow",
        name="Viewport reflow at 320 CSS px (1.4.10)",
        criteria=["1.4.10"],
        coverage_mode=COVERAGE_AUTOMATED,
        what_tested=[
            "Resizes to a narrow viewport and detects horizontal-scroll requirement.",
            "Captures the first 5 elements overflowing the viewport width.",
        ],
        what_not_tested=[
            "Reflow at intermediate widths (only the configured narrow profile is probed).",
            "Functional loss at 400% zoom (related but distinct check).",
        ],
        sampling="Single resize per profile; first 5 overflow offenders captured.",
        automation_limits=[
            "Some sites only set their narrow layout after a media-query-bound JS hook — Playwright resize emits the event, but late-binding hooks may still miss.",
        ],
        manual_followup=[
            "Visually inspect the page at 320 CSS px and verify all functionality is reachable.",
        ],
    ),
    EvaluatorManifest(
        id="custom:text_resize",
        name="Text resize & text-spacing overflow (1.4.4 / 1.4.12)",
        criteria=["1.4.4", "1.4.12"],
        coverage_mode=COVERAGE_SEMI_AUTOMATED,
        what_tested=[
            "Injects WCAG 1.4.12 text-spacing overrides; detects clipped, hidden, or overflowing text after the override.",
            "Simulates 200% zoom via CSS transform and detects content/functionality loss.",
        ],
        what_not_tested=[
            "Browser-level text-only zoom (Ctrl+) which behaves slightly differently from CSS scale.",
            "Whether the page remains usable, only that no element clips after the override.",
        ],
        sampling="Whole-page DOM scan after override injection.",
        automation_limits=[
            "CSS transform zoom is a proxy — real text-only zoom does not scale layout boxes the same way.",
        ],
        manual_followup=[
            "In the browser, set text size to 200% and verify all controls remain reachable and readable.",
        ],
    ),
    EvaluatorManifest(
        id="custom:timing",
        name="Timing & auto-updating content (2.2.1 / 2.2.2)",
        criteria=["2.2.1", "2.2.2"],
        coverage_mode=COVERAGE_SEMI_AUTOMATED,
        what_tested=[
            "Detects carousels, sliders, marquee/blink, auto-playing media without controls.",
            "Heuristic check for visible pause/stop/hide controls accompanying detected auto-updating components.",
            "<meta http-equiv='refresh'> presence (2.2.1).",
        ],
        what_not_tested=[
            "Session timeouts and re-authentication windows (2.2.5/2.2.6 — out of scope).",
            "Whether 'pause' actually halts the underlying timer (only its presence is checked).",
            "Background JS-driven polling that affects user-visible state.",
        ],
        sampling="DOM scan for known carousel/slider class patterns at the checkpoint.",
        automation_limits=[
            "Carousel detection is class-pattern-based — bespoke widgets without recognisable class names are missed.",
        ],
        manual_followup=[
            "For every auto-updating region, verify the user can pause and that timing is adjustable per WCAG 2.2.1.",
        ],
    ),
    EvaluatorManifest(
        id="custom:pointer_target",
        name="Pointer target size (2.5.8 — 24×24 minimum)",
        criteria=["2.5.8"],
        coverage_mode=COVERAGE_AUTOMATED,
        what_tested=[
            "getBoundingClientRect() of every interactive element checked against the 24×24 CSS-px minimum.",
            "Spacing-offset exception: targets smaller than 24×24 are tolerated when neighbour spacing meets the WCAG offset rule.",
        ],
        what_not_tested=[
            "Targets that only become visible after a user gesture (hover, expand) on a state not captured at the checkpoint.",
            "The 'inline-text-link' exception is approximated, not strictly evaluated.",
        ],
        sampling="All visible interactive elements at the captured checkpoint.",
        automation_limits=[
            "Bounding rects of CSS-transformed elements may report unscaled dimensions on some renderers.",
        ],
        manual_followup=[
            "Use a touch device and verify smaller controls are still tappable without misfires.",
        ],
    ),
    EvaluatorManifest(
        id="custom:orientation",
        name="Orientation lock & reflow (1.3.4)",
        criteria=["1.3.4"],
        coverage_mode=COVERAGE_SEMI_AUTOMATED,
        what_tested=[
            "Inspects stylesheets for @media (orientation: …) blocks that hide content or restrict layout to a single orientation.",
            "Cross-checks with viewport reflow result.",
        ],
        what_not_tested=[
            "JS code paths that lock orientation via screen.orientation.lock() at runtime.",
            "Native app shells that enforce orientation outside CSS.",
        ],
        sampling="All accessible CSSStyleSheet rules at the checkpoint.",
        automation_limits=[
            "Cross-origin stylesheets are inaccessible from JS — orientation rules in those are invisible.",
        ],
        manual_followup=[
            "Rotate the device on a real handset and verify the layout adapts and content remains accessible.",
        ],
    ),
    EvaluatorManifest(
        id="custom:motion_preference",
        name="Reduced-motion compliance (2.3.3)",
        criteria=["2.3.3"],
        coverage_mode=COVERAGE_SEMI_AUTOMATED,
        what_tested=[
            "Scans first 500 elements for active animation/transition durations.",
            "Checks whether stylesheet contains a prefers-reduced-motion media query.",
        ],
        what_not_tested=[
            "JS-driven animations (requestAnimationFrame loops, GSAP, Framer Motion) — only CSS animation/transition is detected.",
            "Whether reduced-motion is *honoured* at runtime (only the presence of the media query is checked).",
        ],
        sampling="First 500 candidate elements per checkpoint.",
        automation_limits=[
            "JS animation libraries control animation outside CSS — a passing CSS-only check does not prove reduced motion is respected.",
        ],
        manual_followup=[
            "Enable OS reduced-motion and verify all interaction-triggered animation actually attenuates.",
        ],
    ),
    EvaluatorManifest(
        id="custom:media_alternatives",
        name="Time-based media alternatives (1.2.x / 1.4.2)",
        criteria=["1.2.1", "1.2.2", "1.2.5", "1.4.2"],
        coverage_mode=COVERAGE_SEMI_AUTOMATED,
        what_tested=[
            "<video>/<audio> elements scanned for <track kind='captions'/'descriptions'/'subtitles'> and transcript-link affordances near the embed.",
            "autoplay attribute checked against the presence of pause/mute controls.",
        ],
        what_not_tested=[
            "Whether the caption / transcript content is accurate or in sync.",
            "Live-streamed media (1.2.4) — detection of liveness is not attempted.",
            "Embedded third-party players (YouTube/Vimeo iframe) where tracks are not exposed.",
        ],
        sampling="All <video>/<audio> elements at the checkpoint.",
        automation_limits=[
            "Iframed players hide their tracks behind the cross-origin boundary; only their presence can be reported.",
        ],
        manual_followup=[
            "Watch each media item with captions on and verify quality, sync, and transcript availability.",
        ],
    ),
    EvaluatorManifest(
        id="custom:context_change",
        name="Context change on focus/input (3.2.1 / 3.2.2)",
        criteria=["3.2.1", "3.2.2"],
        coverage_mode=COVERAGE_SEMI_AUTOMATED,
        what_tested=[
            "Installs runtime monitors for window.open, navigation, hash change, dialog opens.",
            "Drives focus and synthesised input on form fields and watches for monitor counter increments.",
        ],
        what_not_tested=[
            "Soft visual changes that constitute a context change for a user but not by these signals (large layout shifts, surprise insertions far from the field).",
            "Focus/input flows that require multi-step orchestration the checkpoint did not capture.",
        ],
        sampling="Probe set comes from the keyboard evaluator's walked elements.",
        automation_limits=[
            "Monitor counters do not capture the *user's* perception of 'context change' — they are necessary, not sufficient.",
        ],
        manual_followup=[
            "Tab through and type into each field and observe whether the user would be surprised by the response.",
        ],
    ),
]


# ── Axe-core pseudo-manifests ────────────────────────────────────────────────
#
# Axe runs as a single block but targets dozens of WCAG criteria. We expose one
# manifest per criterion that axe-core covers, listing the rules involved so the
# report can point an engineer at the exact axe rule that fired (or didn't).

def _axe_rules_by_criterion() -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    for rule_id, criteria in AXE_RULE_TO_WCAG.items():
        for cid in criteria:
            out.setdefault(cid, []).append(rule_id)
    for cid in out:
        out[cid].sort()
    return out


_AXE_RULES_BY_CRIT = _axe_rules_by_criterion()


def _axe_manifest_for(criterion_id: str) -> EvaluatorManifest:
    rules = _AXE_RULES_BY_CRIT.get(criterion_id, [])
    rule_list = ", ".join(rules) if rules else "(WCAG-tagged rules)"
    return EvaluatorManifest(
        id="axe:wcag-{}".format(criterion_id),
        name="axe-core (criterion {})".format(criterion_id),
        criteria=[criterion_id],
        coverage_mode=COVERAGE_AUTOMATED,
        what_tested=[
            "axe-core rules: {}.".format(rule_list),
            "DOM-level checks against the current frame and selected sub-frames.",
        ],
        what_not_tested=[
            "Anything requiring layout-rendered pixels (axe runs as JS only).",
            "User-flow assertions across multiple pages or async states that the checkpoint did not capture.",
        ],
        sampling="Whole DOM at the checkpoint, scoped to the run's selector / iframe configuration.",
        automation_limits=[
            "axe-core marks a node 'incomplete' when it cannot determine pass/fail with certainty (e.g. background images for contrast). These show up as NEEDS_REVIEW.",
        ],
        manual_followup=[
            "Review axe 'incomplete' nodes manually with browser DevTools or the WebAIM Contrast Checker.",
        ],
    )


# Used by reporting layer; runner.py threads this list onto AccessibilityReport.
def all_manifests() -> List[EvaluatorManifest]:
    """Custom evaluators + per-criterion axe manifests for every criterion axe covers."""
    axe_manifests = [_axe_manifest_for(cid) for cid in sorted(_AXE_RULES_BY_CRIT.keys())]
    return list(CUSTOM_MANIFESTS) + axe_manifests


def manifests_for_criterion(criterion_id: str) -> List[EvaluatorManifest]:
    return [m for m in all_manifests() if criterion_id in m.criteria]
