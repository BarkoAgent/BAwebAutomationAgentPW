from __future__ import annotations

import html
from typing import Any, Dict, Iterable, List


_AXE_RULE_DESCRIPTIONS: Dict[str, str] = {
    "area-alt": "Image map areas must have alternate text",
    "button-name": "Buttons must have discernible text",
    "image-alt": "Images must have alternate text",
    "image-redundant-alt": "Image alt text must not duplicate surrounding text",
    "input-image-alt": "Image submit buttons must have alternate text",
    "object-alt": "Object elements must have alternate text",
    "role-img-alt": "Elements with img role must have accessible text",
    "server-side-image-map": "Server-side image maps must not be used",
    "svg-img-alt": "SVG images must have accessible text",
    "audio-caption": "Audio elements must have captions",
    "video-caption": "Video elements must have captions",
    "video-description": "Videos must have audio descriptions",
    "aria-braille-equivalent": "Braille attributes must have non-braille equivalents",
    "aria-required-children": "Required child ARIA roles must be present",
    "aria-required-parent": "Required parent ARIA role must be present",
    "aria-text": "role=text elements must not contain focusable descendants",
    "definition-list": "Definition lists must be structured correctly",
    "dlitem": "Definition list items must be within a definition list",
    "empty-heading": "Headings must not be empty",
    "empty-table-header": "Table headers must not be empty",
    "heading-order": "Heading levels must not be skipped",
    "label": "Form inputs must have associated labels",
    "landmark-banner-is-top-level": "Banner landmark must not be nested within another landmark",
    "landmark-complementary-is-top-level": "Complementary landmark must not be nested",
    "landmark-contentinfo-is-top-level": "Contentinfo landmark must not be nested",
    "landmark-main-is-top-level": "Main landmark must not be nested",
    "landmark-no-duplicate-banner": "Page must not have more than one banner landmark",
    "landmark-no-duplicate-contentinfo": "Page must not have more than one contentinfo landmark",
    "landmark-no-duplicate-main": "Page must not have more than one main landmark",
    "landmark-one-main": "Page must contain one main landmark",
    "landmark-unique": "Landmark regions must have unique roles or labels",
    "list": "Lists must use valid list markup",
    "listitem": "List items must be within a list element",
    "page-has-heading-one": "Page must contain an h1 heading",
    "region": "All content must be contained in a landmark region",
    "scope-attr-valid": "Table scope attributes must be used correctly",
    "summary-name": "Summary elements must have accessible names",
    "table-duplicate-name": "Table summary must not duplicate the caption",
    "td-headers-attr": "Table cells must reference valid header IDs",
    "th-has-data-cells": "Table header cells must have corresponding data cells",
    "autocomplete-valid": "Autocomplete attributes must be correct for the input",
    "link-in-text-block": "Links must be visually distinguishable from surrounding text",
    "no-autoplay-audio": "Audio must not autoplay for more than 3 seconds without controls",
    "color-contrast": "Text must meet minimum color contrast ratio (4.5:1 for normal text)",
    "color-contrast-enhanced": "Text must meet enhanced color contrast ratio (7:1)",
    "meta-viewport": "Viewport must allow user scaling and zooming",
    "meta-viewport-large": "Viewport must not prevent text scaling to 500%",
    "avoid-inline-spacing": "Inline CSS text spacing must not cause content clipping",
    "accesskeys": "Accesskey attribute values must be unique",
    "frame-focusable-content": "Frames with focusable content must be accessible via keyboard",
    "scrollable-region-focusable": "Scrollable regions must be keyboard accessible",
    "tabindex": "Elements must not use tabindex greater than zero",
    "blink": "Blink elements are deprecated and must not be used",
    "marquee": "Marquee elements are deprecated and must not be used",
    "meta-refresh": "Timed page refresh must not be used",
    "meta-refresh-no-exceptions": "Meta refresh with any delay must not be used",
    "bypass": "Page must have a mechanism to bypass repeated blocks of content",
    "document-title": "Page must have a descriptive title",
    "frame-title": "Frame and iframe elements must have accessible titles",
    "frame-title-unique": "Frame titles must be unique",
    "skip-link": "Skip navigation links must be focusable",
    "label-content-name-mismatch": "Visible label must be included in the accessible name",
    "target-size": "Interactive elements must meet minimum touch target size (24×24 px)",
    "html-has-lang": "HTML element must have a lang attribute",
    "html-lang-valid": "HTML lang attribute must have a valid BCP 47 language value",
    "html-xml-lang-mismatch": "HTML lang and xml:lang attributes must agree",
    "valid-lang": "lang attributes must have valid BCP 47 language values",
    "form-field-multiple-labels": "Form fields must not have multiple label elements",
    "input-button-name": "Input buttons must have discernible text",
    "label-title-only": "Form inputs must not rely solely on placeholder or title for their label",
    "aria-allowed-attr": "ARIA attributes must be allowed for the element's role",
    "aria-allowed-role": "ARIA roles must be appropriate for the element type",
    "aria-command-name": "ARIA command elements (button, link, menuitem) must have accessible names",
    "aria-conditional-attr": "ARIA attributes must be valid for the element's current state",
    "aria-deprecated-role": "Deprecated ARIA roles must not be used",
    "aria-dialog-name": "ARIA dialog and alertdialog elements must have accessible names",
    "aria-hidden-body": "aria-hidden must not be present on the document body element",
    "aria-hidden-focus": "aria-hidden elements must not contain focusable elements",
    "aria-input-field-name": "ARIA input fields must have accessible names",
    "aria-meter-name": "ARIA meter elements must have accessible names",
    "aria-progressbar-name": "ARIA progressbar elements must have accessible names",
    "aria-prohibited-attr": "Elements must not use ARIA attributes that are prohibited for their role",
    "aria-required-attr": "Required ARIA attributes must be provided for the element's role",
    "aria-roles": "ARIA role values must be valid",
    "aria-toggle-field-name": "ARIA toggle elements (checkbox, switch) must have accessible names",
    "aria-tooltip-name": "ARIA tooltip elements must have accessible names",
    "aria-treeitem-name": "ARIA tree items must have accessible names",
    "aria-valid-attr": "ARIA attributes must be valid WAI-ARIA attributes",
    "aria-valid-attr-value": "ARIA attribute values must be valid for the attribute",
    "duplicate-id-aria": "IDs referenced by ARIA attributes must be unique across the document",
    "nested-interactive": "Interactive controls must not be nested inside other interactive controls",
    "presentation-role-conflict": "Elements with presentation/none role must not have semantic children",
    "select-name": "Select elements must have accessible names",
}

_CUSTOM_SOURCE_DESCRIPTIONS: Dict[str, str] = {
    "custom:structure": "DOM structure audit — landmark regions, heading hierarchy, duplicate IDs",
    "custom:keyboard_smoke": "Keyboard smoke test — focusable element survey and tab order",
    "custom:media_alternatives": "Media alternatives audit — transcripts, captions, audio descriptions",
    "custom:live_regions": "Live regions audit — ARIA live region configuration and correctness",
    "custom:orientation": "Orientation audit — CSS orientation lock detection",
    "custom:timing": "Timing audit — session timeout and countdown timer detection",
    "custom:focus_appearance": "Focus appearance audit — visible focus indicator measurement",
    "custom:context_change": "Context change audit — on-focus and on-input context change monitoring",
    "custom:hover_content": "Hover content audit — tooltip and hover content dismissibility",
    "custom:focus_visibility": "Focus visibility audit — keyboard focus indicator detection",
    "custom:viewport_reflow": "Viewport reflow audit — content reflow at 320 px width",
    "custom:text_resize": "Text resize audit — text reflow and zoom clipping detection",
    "custom:pointer_target": "Pointer target audit — touch target size measurement",
    "custom:motion_preference": "Motion preference audit — prefers-reduced-motion compliance",
    "custom:form_labeling": "Form labeling audit — programmatic label association for form fields",
    "custom:form_validation": "Form validation audit — error message accessibility",
}


def _source_label(source: str) -> str:
    """Return a human-readable label for a source identifier.

    axe:<rule-id>  → description from _AXE_RULE_DESCRIPTIONS, or formatted rule ID
    custom:<name>  → description from _CUSTOM_SOURCE_DESCRIPTIONS
    anything else  → returned as-is
    """
    if not source:
        return ""
    if source in _CUSTOM_SOURCE_DESCRIPTIONS:
        return _CUSTOM_SOURCE_DESCRIPTIONS[source]
    if source.startswith("axe:"):
        rule_id = source[4:]
        desc = _AXE_RULE_DESCRIPTIONS.get(rule_id)
        if desc:
            return "Axe: {}".format(desc)
        return "Axe: {}".format(rule_id.replace("-", " ").title())
    return source


STATUS_CLASS = {
    "PASSED": "passed",
    "FAILED": "failed",
    "NEEDS_REVIEW": "needs-review",
    "NOT_TESTED": "not-tested",
    "NOT_APPLICABLE": "not-applicable",
    "ERROR": "error",
    "AUTOMATED": "automated",
    "SEMI_AUTOMATED": "semi-automated",
    "MANUAL_REQUIRED": "manual-required",
}

_HUMAN_LABEL = {
    "PASSED": "Passed",
    "FAILED": "Failed",
    "NEEDS_REVIEW": "Needs Review",
    "NOT_TESTED": "Not Tested",
    "NOT_APPLICABLE": "N/A",
    "ERROR": "Error",
    "AUTOMATED": "Automated",
    "SEMI_AUTOMATED": "Semi-Automated",
    "MANUAL_REQUIRED": "Manual Check Needed",
}


def _escape(value: Any) -> str:
    return html.escape("" if value is None else str(value))


def _status_badge(label: str) -> str:
    css_class = STATUS_CLASS.get(label, "neutral")
    human = _HUMAN_LABEL.get(label, label.replace("_", " ").title())
    return '<span class="badge {}">{}</span>'.format(css_class, _escape(human))


def _render_counts(counts: Dict[str, int], ordered_keys: List[str]) -> str:
    items = []
    for key in ordered_keys:
        items.append(
            '<div class="metric"><span class="metric-label">{}</span><strong>{}</strong></div>'.format(
                _escape(key.replace("_", " ").title()),
                counts.get(key, 0),
            )
        )
    return "".join(items)


def _render_sources(sources: Iterable[str]) -> str:
    rendered = []
    for source in sources:
        label = _source_label(source)
        rendered.append(
            '<code class="source-chip" title="{raw}">{label}</code>'.format(
                raw=_escape(source),
                label=_escape(label),
            )
        )
    return "".join(rendered) or '<span class="muted">None recorded</span>'


def _render_list(items: List[str], empty_message: str = "None recorded") -> str:
    if not items:
        return '<p class="muted">{}</p>'.format(_escape(empty_message))
    return "<ul>{}</ul>".format("".join("<li>{}</li>".format(_escape(item)) for item in items))
