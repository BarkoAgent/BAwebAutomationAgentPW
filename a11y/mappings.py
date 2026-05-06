import re
from typing import Iterable, List, Set


AXE_RULE_TO_WCAG = {
    # 1.1 Text Alternatives
    "area-alt": ["1.1.1"],
    "button-name": ["1.1.1", "4.1.2"],
    "image-alt": ["1.1.1"],
    "image-redundant-alt": ["1.1.1"],
    "input-image-alt": ["1.1.1"],
    "object-alt": ["1.1.1"],
    "role-img-alt": ["1.1.1"],
    "server-side-image-map": ["1.1.1", "2.1.1"],
    "svg-img-alt": ["1.1.1"],

    # 1.2 Time-based Media
    "audio-caption": ["1.2.1", "1.2.2"],
    "video-caption": ["1.2.2"],
    "video-description": ["1.2.5"],

    # 1.3 Adaptable
    "aria-braille-equivalent": ["1.3.1", "4.1.2"],
    "aria-required-children": ["1.3.1", "4.1.2"],
    "aria-required-parent": ["1.3.1", "4.1.2"],
    "aria-text": ["1.3.1", "4.1.2"],
    "definition-list": ["1.3.1"],
    "dlitem": ["1.3.1"],
    "empty-heading": ["1.3.1", "2.4.6"],
    "empty-table-header": ["1.3.1"],
    "heading-order": ["1.3.1", "2.4.6"],
    "label": ["1.3.1", "3.3.2"],
    "landmark-banner-is-top-level": ["1.3.1"],
    "landmark-complementary-is-top-level": ["1.3.1"],
    "landmark-contentinfo-is-top-level": ["1.3.1"],
    "landmark-main-is-top-level": ["1.3.1"],
    "landmark-no-duplicate-banner": ["1.3.1"],
    "landmark-no-duplicate-contentinfo": ["1.3.1"],
    "landmark-no-duplicate-main": ["1.3.1"],
    "landmark-one-main": ["1.3.1"],
    "landmark-unique": ["1.3.1"],
    "list": ["1.3.1"],
    "listitem": ["1.3.1"],
    "page-has-heading-one": ["1.3.1", "2.4.6"],
    "region": ["1.3.1", "2.4.1"],
    "scope-attr-valid": ["1.3.1"],
    "summary-name": ["1.3.1"],
    "table-duplicate-name": ["1.3.1"],
    "td-headers-attr": ["1.3.1"],
    "th-has-data-cells": ["1.3.1"],
    "autocomplete-valid": ["1.3.5"],

    # 1.4 Distinguishable
    "link-in-text-block": ["1.4.1"],
    "no-autoplay-audio": ["1.4.2"],
    "color-contrast": ["1.4.3"],
    "color-contrast-enhanced": ["1.4.6"],
    "meta-viewport": ["1.3.4", "1.4.4", "1.4.10"],
    "meta-viewport-large": ["1.4.4"],
    "avoid-inline-spacing": ["1.4.12"],

    # 2.1 Keyboard Accessible
    "accesskeys": ["2.1.4"],
    "frame-focusable-content": ["2.1.1", "4.1.2"],
    "scrollable-region-focusable": ["2.1.1", "2.1.2"],
    "tabindex": ["2.1.1", "2.4.3"],

    # 2.2 Enough Time
    "blink": ["2.2.2"],
    "marquee": ["2.2.2"],
    "meta-refresh": ["2.2.1", "3.2.5"],
    "meta-refresh-no-exceptions": ["2.2.1"],

    # 2.4 Navigable
    "bypass": ["2.4.1"],
    "document-title": ["2.4.2"],
    "frame-title": ["2.4.1", "4.1.2"],
    "frame-title-unique": ["4.1.2"],
    "skip-link": ["2.4.1"],

    # 2.5 Input Modalities
    "label-content-name-mismatch": ["2.5.3", "4.1.2"],
    "target-size": ["2.5.8"],

    # 3.1 Readable
    "html-has-lang": ["3.1.1"],
    "html-lang-valid": ["3.1.1"],
    "html-xml-lang-mismatch": ["3.1.1"],
    "valid-lang": ["3.1.2"],

    # 3.3 Input Assistance
    "form-field-multiple-labels": ["3.3.2"],
    "input-button-name": ["4.1.2"],
    "label-title-only": ["3.3.2"],

    # 4.1 Compatible
    "aria-allowed-attr": ["4.1.2"],
    "aria-allowed-role": ["4.1.2"],
    "aria-command-name": ["4.1.2"],
    "aria-conditional-attr": ["4.1.2"],
    "aria-deprecated-role": ["4.1.2"],
    "aria-dialog-name": ["4.1.2"],
    "aria-hidden-body": ["4.1.2"],
    "aria-hidden-focus": ["4.1.2"],
    "aria-input-field-name": ["4.1.2"],
    "aria-meter-name": ["4.1.2"],
    "aria-progressbar-name": ["4.1.2"],
    "aria-prohibited-attr": ["4.1.2"],
    "aria-required-attr": ["4.1.2"],
    "aria-roles": ["4.1.2"],
    "aria-toggle-field-name": ["4.1.2"],
    "aria-tooltip-name": ["4.1.2"],
    "aria-treeitem-name": ["4.1.2"],
    "aria-valid-attr": ["4.1.2"],
    "aria-valid-attr-value": ["4.1.2"],
    "duplicate-id-aria": ["4.1.2"],
    "nested-interactive": ["4.1.2"],
    "presentation-role-conflict": ["4.1.2"],
    "select-name": ["4.1.2"],
}

WCAG_TAG_PATTERN = re.compile(r"^wcag(\d)(\d)(\d+)$")


def criterion_ids_from_axe(rule_id: str, tags: Iterable[str]) -> List[str]:
    criterion_ids: Set[str] = set(AXE_RULE_TO_WCAG.get(rule_id, []))
    for tag in tags:
        match = WCAG_TAG_PATTERN.match(tag)
        if not match:
            continue
        criterion_ids.add("{}.{}.{}".format(match.group(1), match.group(2), match.group(3)))
    return sorted(criterion_ids)
