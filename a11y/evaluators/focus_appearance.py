from __future__ import annotations

import asyncio
import math
from typing import Any, Dict, List

from ..models import COVERAGE_SEMI_AUTOMATED, OUTCOME_FAILED, OUTCOME_NEEDS_REVIEW


# WCAG 2.4.13 (AA, WCAG 2.2):
#   - Focus indicator must have area >= perimeter of unfocused component × 2px
#   - Contrast ratio of focus indicator vs adjacent color must be >= 3:1

FOCUS_APPEARANCE_SCRIPT = """
() => {
  function cssPath(el) {
    if (!el || el.nodeType !== 1) return '';
    if (el.id) return '#' + CSS.escape(el.id);
    const classes = Array.from(el.classList || []).slice(0, 3).map(c => CSS.escape(c)).join('.');
    return el.tagName.toLowerCase() + (classes ? '.' + classes : '');
  }

  function parseColor(str) {
    if (!str || str === 'transparent' || str === 'none') return null;
    const m = str.match(/rgba?\\((\\d+),\\s*(\\d+),\\s*(\\d+)/);
    if (!m) return null;
    return { r: parseInt(m[1]), g: parseInt(m[2]), b: parseInt(m[3]) };
  }

  function relativeLuminance(r, g, b) {
    const sRGB = [r, g, b].map(c => {
      c = c / 255;
      return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
    });
    return 0.2126 * sRGB[0] + 0.7152 * sRGB[1] + 0.0722 * sRGB[2];
  }

  function contrastRatio(c1, c2) {
    if (!c1 || !c2) return null;
    const l1 = relativeLuminance(c1.r, c1.g, c1.b);
    const l2 = relativeLuminance(c2.r, c2.g, c2.b);
    const lighter = Math.max(l1, l2);
    const darker = Math.min(l1, l2);
    return (lighter + 0.05) / (darker + 0.05);
  }

  const el = document.activeElement;
  if (!el || el === document.body || el === document.documentElement) return null;

  const style = window.getComputedStyle(el);
  const rect = el.getBoundingClientRect();

  const outlineWidth = parseFloat(style.outlineWidth) || 0;
  const outlineColor = parseColor(style.outlineColor);
  const bgColor = parseColor(style.backgroundColor);

  // Approximate perimeter of the element
  const perimeter = 2 * (rect.width + rect.height);
  // Minimum required area: perimeter × 2px (simplified WCAG 2.4.13 formula)
  const minArea = perimeter * 2;
  // Actual outline area approximation: (outerPerimeter * outlineWidth)
  // outer perimeter ≈ perimeter + 8*outlineWidth (corners)
  const outerPerimeter = perimeter + 8 * outlineWidth;
  const focusArea = outlineWidth > 0 ? outerPerimeter * outlineWidth : 0;

  // Also check box-shadow as a focus indicator.
  // Strip the optional "inset" keyword, then collect all px values — use spread (4th) if
  // present, otherwise blur (3rd), so patterns like "0 0 0 3px", "inset 0 0 5px", and
  // "0 5px 10px" are all handled.
  let boxShadowWidth = 0;
  if (style.boxShadow && style.boxShadow !== 'none') {
    const bsTokens = style.boxShadow.replace(/\\binset\\b/gi, '').match(/(\\d+(?:\\.\\d+)?)px/g);
    if (bsTokens && bsTokens.length >= 1) {
      const vals = bsTokens.map(t => parseFloat(t));
      boxShadowWidth = vals.length >= 4 ? (vals[3] || vals[2] || 0) : (vals[vals.length - 1] || 0);
    }
  }

  // Effective indicator width: use whichever is bigger
  const effectiveWidth = Math.max(outlineWidth, boxShadowWidth);
  const effectiveArea = effectiveWidth > 0 ? outerPerimeter * effectiveWidth : 0;

  // Contrast: outline color vs element background color.
  // NOTE: WCAG 2.4.13 requires contrast against "adjacent non-focus-indicator colors".
  // Using the element's own backgroundColor is incorrect when the element is transparent
  // or the indicator extends beyond the element bounds. This is a known limitation —
  // elements with transparent backgrounds should be flagged for manual review.
  const focusContrast = contrastRatio(outlineColor, bgColor);

  return {
    locator: cssPath(el),
    text: (el.innerText || el.textContent || '').trim().slice(0, 120),
    tag: el.tagName.toLowerCase(),
    outlineWidth,
    boxShadowWidth,
    effectiveWidth,
    focusArea: effectiveArea,
    minArea,
    perimeter,
    meetsAreaThreshold: effectiveArea >= minArea,
    focusContrast,
    meetsContrastThreshold: focusContrast !== null && focusContrast >= 3.0,
    outlineColor: style.outlineColor,
    backgroundColor: style.backgroundColor,
    rect: { width: rect.width, height: rect.height, top: rect.top, left: rect.left },
  };
}
"""


async def run_focus_appearance_evaluator(page: Any) -> List[Dict[str, Any]]:
    samples: List[Dict[str, Any]] = []
    seen: set = set()

    for _ in range(10):
        await page.keyboard.press("Tab")
        await asyncio.sleep(0.05)
        sample = await page.evaluate(FOCUS_APPEARANCE_SCRIPT)
        if not sample:
            continue
        locator = sample.get("locator", "")
        rect = sample.get("rect", {})
        dedup_key = (locator, rect.get("top"), rect.get("left"))
        if dedup_key in seen:
            continue
        seen.add(dedup_key)
        samples.append(sample)

    if not samples:
        return []

    area_failures = [s for s in samples if not s.get("meetsAreaThreshold") and s.get("effectiveWidth", 0) > 0]
    contrast_failures = [s for s in samples if not s.get("meetsContrastThreshold") and s.get("focusContrast") is not None]
    no_indicator = [s for s in samples if s.get("effectiveWidth", 0) == 0]

    results: List[Dict[str, Any]] = []

    if no_indicator:
        first = no_indicator[0]
        results.append(
            {
                "criterion_id": "2.4.13",
                "source": "custom:focus_appearance",
                "coverage_status": COVERAGE_SEMI_AUTOMATED,
                "outcome": OUTCOME_FAILED,
                "severity": "serious",
                "message": (
                    "Sampled focused element has no measurable outline or box-shadow focus indicator, "
                    "failing WCAG 2.4.13 minimum focus appearance."
                ),
                "locator": first.get("locator", ""),
                "element_text": first.get("text", ""),
                "metadata": {"samples": samples, "no_indicator": no_indicator},
            }
        )
    elif area_failures:
        first = area_failures[0]
        results.append(
            {
                "criterion_id": "2.4.13",
                "source": "custom:focus_appearance",
                "coverage_status": COVERAGE_SEMI_AUTOMATED,
                "outcome": OUTCOME_FAILED,
                "severity": "serious",
                "message": (
                    "Focus indicator area ({:.0f}px²) is below the WCAG 2.4.13 minimum "
                    "required area ({:.0f}px²) for the sampled element.".format(
                        first.get("focusArea", 0), first.get("minArea", 0)
                    )
                ),
                "locator": first.get("locator", ""),
                "element_text": first.get("text", ""),
                "metadata": {"samples": samples, "area_failures": area_failures},
            }
        )
    elif contrast_failures:
        first = contrast_failures[0]
        contrast_val = first.get("focusContrast")
        results.append(
            {
                "criterion_id": "2.4.13",
                "source": "custom:focus_appearance",
                "coverage_status": COVERAGE_SEMI_AUTOMATED,
                "outcome": OUTCOME_FAILED,
                "severity": "serious",
                "message": (
                    "Focus indicator contrast ratio ({:.2f}:1) is below the WCAG 2.4.13 "
                    "minimum of 3:1 against adjacent background color.".format(contrast_val or 0)
                ),
                "locator": first.get("locator", ""),
                "element_text": first.get("text", ""),
                "metadata": {"samples": samples, "contrast_failures": contrast_failures},
            }
        )
    else:
        first = samples[0]
        results.append(
            {
                "criterion_id": "2.4.13",
                "source": "custom:focus_appearance",
                "coverage_status": COVERAGE_SEMI_AUTOMATED,
                "outcome": OUTCOME_NEEDS_REVIEW,
                "severity": "moderate",
                "message": (
                    "Sampled focus indicators appear to meet WCAG 2.4.13 area and contrast thresholds; "
                    "visual review against all interactive element types is still recommended."
                ),
                "locator": first.get("locator", ""),
                "element_text": first.get("text", ""),
                "metadata": {"samples": samples},
            }
        )

    return results
