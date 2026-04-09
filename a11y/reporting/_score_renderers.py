"""
Score computation and visual renderers for the enhanced accessibility report.

Provides: health score gauge, outcome donut, WCAG pillar grid, issue-category
breakdown, top-affected-components list, compliance badges, and JSON export.
"""
from __future__ import annotations

import json
import math
from typing import Any, Dict, List, Tuple

from ._helpers import _escape


# ── Severity weights (BrowserStack-style ratio scoring) ───────────────────────
# WCAG A ≈ Critical, AA ≈ Serious, AAA ≈ Moderate.
# Fail weight is 2× pass weight so failures penalise proportionally more.

_PASS_WEIGHT: Dict[str, int] = {"A": 10, "AA": 7, "AAA": 3}
_FAIL_WEIGHT: Dict[str, int] = {"A": 20, "AA": 14, "AAA": 6}

# ── WCAG Pillars ───────────────────────────────────────────────────────────────

_PILLARS: List[Tuple[str, str, str]] = [
    ("Perceivable",    "1.", "Information must be presentable in ways users can perceive."),
    ("Operable",       "2.", "UI components and navigation must be operable."),
    ("Understandable", "3.", "Information and UI operation must be understandable."),
    ("Robust",         "4.", "Content must be robust enough for a wide variety of user agents."),
]

# ── Issue categories (criterion ID → bucket) ───────────────────────────────────

_CATEGORIES: List[Tuple[str, List[str]]] = [
    ("Color & Contrast",       ["1.4.1", "1.4.3", "1.4.6", "1.4.11", "1.4.12"]),
    ("Keyboard & Focus",       ["2.1.1", "2.1.2", "2.1.4", "2.4.3", "2.4.7", "2.4.11", "2.4.12"]),
    ("ARIA & Semantics",       ["1.3.1", "1.3.2", "1.3.3", "4.1.1", "4.1.2", "4.1.3"]),
    ("Text Alternatives",      ["1.1.1", "1.2.1", "1.2.2", "1.2.3", "1.2.4", "1.2.5"]),
    ("Forms & Labels",         ["1.3.5", "3.3.1", "3.3.2", "3.3.3", "3.3.4"]),
    ("Structure & Navigation", ["2.4.1", "2.4.2", "2.4.6", "2.4.10"]),
]


# ── Internal helpers ───────────────────────────────────────────────────────────

def _score_color(score: int) -> str:
    if score >= 80: return "#28A745"
    if score >= 60: return "#FFC107"
    if score >= 40: return "#FD7E14"
    return "#B00020"


def _score_label(score: int) -> str:
    if score >= 90: return "Excellent"
    if score >= 80: return "Good"
    if score >= 60: return "Fair"
    if score >= 40: return "Poor"
    return "Critical"


def _arc(cx: float, cy: float, r: float, start_deg: float, sweep_deg: float) -> str:
    """SVG arc path data string (clockwise, sweep-flag=1)."""
    sr = math.radians(start_deg)
    er = math.radians(start_deg + sweep_deg)
    x1 = cx + r * math.cos(sr)
    y1 = cy + r * math.sin(sr)
    x2 = cx + r * math.cos(er)
    y2 = cy + r * math.sin(er)
    large = 1 if sweep_deg > 180 else 0
    return "M{:.2f} {:.2f} A{:.2f} {:.2f} 0 {} 1 {:.2f} {:.2f}".format(
        x1, y1, r, r, large, x2, y2
    )


def _outcome_counts(criteria: List[Dict[str, Any]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for c in criteria:
        o = c.get("outcome_status", "NOT_TESTED")
        counts[o] = counts.get(o, 0) + 1
    return counts


# ── Public computation ─────────────────────────────────────────────────────────

def _weighted_sums(criteria: List[Dict[str, Any]]) -> Tuple[int, int]:
    """Return (weighted_passed, weighted_failed) for all scoreable criteria."""
    wp = wf = 0
    for c in criteria:
        outcome = c.get("outcome_status", "")
        level = c.get("level", "AA")
        if outcome == "PASSED":
            wp += _PASS_WEIGHT.get(level, 7)
        elif outcome in ("FAILED", "ERROR", "NEEDS_REVIEW"):
            wf += _FAIL_WEIGHT.get(level, 14)
    return wp, wf


def compute_health_score(criteria: List[Dict[str, Any]]) -> int:
    """0–100 ratio score: weighted_passed / (weighted_passed + weighted_failed) × 100.

    Formula mirrors BrowserStack: WCAG A → Critical weights (10/20),
    AA → Serious (7/14), AAA → Moderate (3/6). Fail weight is 2× pass weight.
    NOT_TESTED and NOT_APPLICABLE criteria are excluded from both sides.
    """
    wp, wf = _weighted_sums(criteria)
    total = wp + wf
    return round(wp / total * 100) if total else 100


def _potential_gain(criteria: List[Dict[str, Any]], *outcomes: str) -> int:
    """Score-point gain if all criteria matching any of *outcomes* were resolved to PASSED."""
    wp, wf = _weighted_sums(criteria)
    total = wp + wf
    if total == 0:
        return 0
    current_score = round(wp / total * 100)
    extra_wp = sum(
        _PASS_WEIGHT.get(c.get("level", "AA"), 7)
        for c in criteria if c.get("outcome_status") in outcomes
    )
    freed_wf = sum(
        _FAIL_WEIGHT.get(c.get("level", "AA"), 14)
        for c in criteria if c.get("outcome_status") in outcomes
    )
    new_wp = wp + extra_wp
    new_wf = wf - freed_wf
    new_total = new_wp + new_wf
    if new_total == 0:
        return 100 - current_score
    return max(0, round(new_wp / new_total * 100) - current_score)


# ── SVG charts ─────────────────────────────────────────────────────────────────

def render_gauge(score: int) -> str:
    """Circular speedometer-style gauge (gap at the bottom)."""
    cx, cy, r, sw = 80.0, 80.0, 54.0, 16
    color = _score_color(score)
    label = _score_label(score)
    track = _arc(cx, cy, r, 135, 270)          # full track arc
    filled_path = ""
    if score > 0:
        d = _arc(cx, cy, r, 135, 270 * score / 100)
        filled_path = (
            '<path d="{d}" fill="none" stroke="{c}" stroke-width="{sw}" stroke-linecap="round"/>'.format(
                d=d, c=color, sw=sw
            )
        )
    return (
        '<svg width="160" height="160" viewBox="0 0 160 160"'
        ' role="img" aria-label="Health score {s} out of 100">'
        '<path d="{track}" fill="none" stroke="#e9ecef" stroke-width="{sw}" stroke-linecap="round"/>'
        "{fp}"
        '<text x="{cx}" y="{y1}" text-anchor="middle"'
        ' font-size="38" font-weight="800" fill="{c}">{s}</text>'
        '<text x="{cx}" y="{y2}" text-anchor="middle"'
        ' font-size="11" fill="#9ca3af">/ 100 &middot; {label}</text>'
        "</svg>"
    ).format(
        s=score, track=track, sw=sw, fp=filled_path,
        cx=int(cx), y1=int(cy) + 10, y2=int(cy) + 26,
        c=color, label=label,
    )


def render_outcome_donut(criteria: List[Dict[str, Any]]) -> str:
    """Donut chart showing criteria by outcome status."""
    counts = _outcome_counts(criteria)
    cx, cy, r, sw = 80.0, 80.0, 50.0, 22
    segments = [
        (counts.get("FAILED",       0), "#B00020"),
        (counts.get("ERROR",        0), "#FD7E14"),
        (counts.get("NEEDS_REVIEW", 0), "#FFC107"),
        (counts.get("PASSED",       0), "#28A745"),
        (counts.get("NOT_TESTED",   0) + counts.get("NOT_APPLICABLE", 0), "#adb5bd"),
    ]
    total = sum(v for v, _ in segments)
    if total == 0:
        return (
            '<svg width="160" height="160" viewBox="0 0 160 160">'
            '<circle cx="{cx}" cy="{cy}" r="{r}" fill="none" stroke="#e9ecef" stroke-width="{sw}"/>'
            '<text x="{cx}" y="{cy}" text-anchor="middle"'
            ' dominant-baseline="middle" font-size="13" fill="#9ca3af">No data</text>'
            "</svg>"
        ).format(cx=int(cx), cy=int(cy), r=int(r), sw=sw)

    paths: List[str] = []
    angle = -90.0
    for value, color in segments:
        if value == 0:
            continue
        sweep = 360.0 * value / total
        if sweep >= 359.9:
            paths.append(
                '<circle cx="{}" cy="{}" r="{}" fill="none" stroke="{}" stroke-width="{}"/>'.format(
                    int(cx), int(cy), int(r), color, sw
                )
            )
        else:
            d = _arc(cx, cy, r, angle, sweep)
            paths.append(
                '<path d="{}" fill="none" stroke="{}" stroke-width="{}" stroke-linecap="butt"/>'.format(
                    d, color, sw
                )
            )
        angle += sweep

    failed = counts.get("FAILED", 0) + counts.get("ERROR", 0)
    cc = "#B00020" if failed else "#28A745"
    cv = str(failed) if failed else str(counts.get("PASSED", 0))
    cs = "failed"   if failed else "passed"

    return (
        '<svg width="160" height="160" viewBox="0 0 160 160"'
        ' role="img" aria-label="Criteria severity distribution">'
        '<circle cx="{cx}" cy="{cy}" r="{r}" fill="none" stroke="#f3f4f6" stroke-width="{sw}"/>'
        "{paths}"
        '<text x="{cx}" y="{y1}" text-anchor="middle"'
        ' font-size="30" font-weight="800" fill="{cc}">{cv}</text>'
        '<text x="{cx}" y="{y2}" text-anchor="middle" font-size="11" fill="#9ca3af">{cs}</text>'
        "</svg>"
    ).format(
        cx=int(cx), cy=int(cy), r=int(r), sw=sw,
        paths="".join(paths),
        y1=int(cy) + 6, y2=int(cy) + 22,
        cc=cc, cv=_escape(cv), cs=cs,
    )


# ── Section renderers ──────────────────────────────────────────────────────────

def render_health_panel(score: int, criteria: List[Dict[str, Any]]) -> str:
    counts  = _outcome_counts(criteria)
    gauge   = render_gauge(score)
    donut   = render_outcome_donut(criteria)
    failed  = counts.get("FAILED", 0) + counts.get("ERROR", 0)
    review  = counts.get("NEEDS_REVIEW", 0)
    passed  = counts.get("PASSED", 0)
    active  = failed + review + passed
    pass_rt = int(passed / active * 100) if active else 0

    failed_gain = _potential_gain(criteria, "FAILED", "ERROR")
    review_gain = _potential_gain(criteria, "NEEDS_REVIEW")
    gains: List[str] = []
    if failed_gain:
        gains.append(
            '<div class="gain-row"><span class="gain-badge gain-failed">{f}&nbsp;failed</span>'
            '<span>Fix these to recover <strong>~{g}&thinsp;pts</strong></span></div>'.format(
                f=failed, g=failed_gain
            )
        )
    if review_gain:
        gains.append(
            '<div class="gain-row"><span class="gain-badge gain-review">{r}&nbsp;in&nbsp;review</span>'
            '<span>Resolve to recover <strong>~{g}&thinsp;pts</strong></span></div>'.format(
                r=review, g=review_gain
            )
        )
    if not gains:
        gains.append('<p class="muted">No score improvement available from current criteria.</p>')

    return (
        '<div class="health-layout">'
        '<div class="health-col">{gauge}</div>'
        '<div class="health-col health-col-stats">'
        '<div class="health-stat-grid">'
        '<div class="health-stat"><span class="health-stat-label">Pass Rate</span>'
        '<strong>{pr}%</strong></div>'
        '<div class="health-stat"><span class="health-stat-label">Failed</span>'
        '<strong class="stat-failed">{f}</strong></div>'
        '<div class="health-stat"><span class="health-stat-label">Needs Review</span>'
        '<strong class="stat-review">{r}</strong></div>'
        '<div class="health-stat"><span class="health-stat-label">Passed</span>'
        '<strong class="stat-passed">{p}</strong></div>'
        '</div>'
        '<div class="gain-section">'
        '<h4 class="gain-title">Score Improvement Potential</h4>'
        '{gains}'
        '</div>'
        '<p class="muted standards-note">'
        'Weighted by WCAG level: A&thinsp;=&thinsp;3&times;&thinsp;penalty, '
        'AA&thinsp;=&thinsp;2&times;, AAA&thinsp;=&thinsp;1&times;. '
        'WCAG&nbsp;2.1&nbsp;AA aligns with Section&nbsp;508, ADA, and EAA baselines.'
        '</p>'
        '</div>'
        '<div class="health-col">{donut}</div>'
        '</div>'
    ).format(
        gauge=gauge, donut=donut,
        pr=pass_rt, f=failed, r=review, p=passed,
        gains="".join(gains),
    )


def render_pillar_grid(criteria: List[Dict[str, Any]]) -> str:
    cards: List[str] = []
    for pillar, prefix, desc in _PILLARS:
        pc     = [c for c in criteria if c.get("id", "").startswith(prefix)]
        total  = len(pc)
        failed = sum(1 for c in pc if c.get("outcome_status") in ("FAILED", "ERROR"))
        review = sum(1 for c in pc if c.get("outcome_status") == "NEEDS_REVIEW")
        passed = sum(1 for c in pc if c.get("outcome_status") == "PASSED")

        if not total:
            sc, st = "pillar-na",   "N/A"
        elif failed:
            sc, st = "pillar-fail", "{} failed".format(failed)
        elif review:
            sc, st = "pillar-review", "{} in review".format(review)
        else:
            sc, st = "pillar-pass", "Passing"

        pw = int(passed / total * 100) if total else 0
        rw = int(review / total * 100) if total else 0
        fw = int(failed / total * 100) if total else 0

        cards.append((
            '<div class="pillar-card {sc}">'
            '<div class="pillar-header"><strong>{pl}</strong>'
            '<span class="pillar-badge">{st}</span></div>'
            '<p class="pillar-desc">{desc}</p>'
            '<div class="pillar-bar">'
            '<div class="pb-pass"   style="width:{pw}%"></div>'
            '<div class="pb-review" style="width:{rw}%"></div>'
            '<div class="pb-fail"   style="width:{fw}%"></div>'
            '</div>'
            '<div class="pillar-counts">'
            '<span class="pc-p">{pa}&thinsp;passed</span>'
            '<span class="pc-r">{re}&thinsp;review</span>'
            '<span class="pc-f">{fa}&thinsp;failed</span>'
            '<span class="muted">/&thinsp;{to}&thinsp;total</span>'
            '</div></div>'
        ).format(
            sc=sc, pl=_escape(pillar), st=_escape(st), desc=_escape(desc),
            pw=pw, rw=rw, fw=fw, pa=passed, re=review, fa=failed, to=total,
        ))
    return '<div class="pillar-grid">{}</div>'.format("".join(cards))


def render_category_breakdown(criteria: List[Dict[str, Any]]) -> str:
    rows: List[str] = []
    for cat, ids in _CATEGORIES:
        matched = [c for c in criteria if c.get("id") in ids]
        if not matched:
            continue
        failed = sum(1 for c in matched if c.get("outcome_status") in ("FAILED", "ERROR"))
        review = sum(1 for c in matched if c.get("outcome_status") == "NEEDS_REVIEW")
        passed = sum(1 for c in matched if c.get("outcome_status") == "PASSED")
        total  = len(matched)
        issues = failed + review
        fw = int(failed / total * 100) if total else 0
        rw = int(review / total * 100) if total else 0
        pw = int(passed / total * 100) if total else 0
        ic = "cat-fail" if failed else ("cat-review" if review else "cat-pass")
        rows.append((
            '<div class="cat-row">'
            '<div class="cat-name">{cat}</div>'
            '<div class="cat-bar-wrap"><div class="cat-bar">'
            '<div class="cat-seg cat-seg-fail"   style="width:{fw}%"></div>'
            '<div class="cat-seg cat-seg-review" style="width:{rw}%"></div>'
            '<div class="cat-seg cat-seg-pass"   style="width:{pw}%"></div>'
            '</div></div>'
            '<div class="cat-count {ic}">{issues}&thinsp;issue{pl}</div>'
            '</div>'
        ).format(
            cat=_escape(cat), fw=fw, rw=rw, pw=pw,
            issues=issues, pl="s" if issues != 1 else "", ic=ic,
        ))
    if not rows:
        return '<p class="muted">No category data available.</p>'
    return '<div class="category-list">{}</div>'.format("".join(rows))


def render_top_components(criteria: List[Dict[str, Any]], limit: int = 10) -> str:
    freq: Dict[str, int] = {}
    for c in criteria:
        for item in c.get("evidence", []):
            if item.get("outcome") not in ("FAILED", "ERROR", "NEEDS_REVIEW"):
                continue
            for target in item.get("target", []):
                sel = str(target).strip()
                if sel:
                    freq[sel] = freq.get(sel, 0) + 1
    top = sorted(freq.items(), key=lambda x: -x[1])[:limit]
    if not top:
        return '<p class="muted">No failing component selectors found in evidence.</p>'
    max_cnt = top[0][1]
    rows: List[str] = []
    for selector, count in top:
        bw      = int(count / max_cnt * 100)
        display = selector if len(selector) <= 56 else selector[:54] + "\u2026"
        rows.append((
            '<div class="component-row" data-selector="{sel}">'
            '<code class="comp-selector" title="{sel}">{disp}</code>'
            '<div class="comp-bar"><div class="comp-bar-fill" style="width:{bw}%"></div></div>'
            '<span class="comp-count">{cnt}</span>'
            '</div>'
        ).format(sel=_escape(selector), disp=_escape(display), bw=bw, cnt=count))
    return '<div class="components-list">{}</div>'.format("".join(rows))


def render_compliance_badges(criteria: List[Dict[str, Any]]) -> str:
    levels = {c.get("level", "") for c in criteria if c.get("outcome_status") != "NOT_APPLICABLE"}
    badges: List[str] = []
    if "A" in levels and "AA" in levels:
        badges = ["WCAG 2.1 AA", "Section 508", "ADA", "EAA"]
    elif "A" in levels:
        badges = ["WCAG 2.1 A"]
    if "AAA" in levels:
        badges.append("WCAG 2.1 AAA")
    if not badges:
        return ""
    return '<div class="compliance-badges">{}</div>'.format(
        "".join('<span class="compliance-badge">{}</span>'.format(_escape(b)) for b in badges)
    )


def build_report_json(report: Dict[str, Any]) -> str:
    """Trimmed JSON for the download button (omits large raw evidence blobs)."""
    slim_criteria = []
    for c in (report.get("criteria") or []):
        slim = {k: v for k, v in c.items()
                if k not in ("evidence", "failed_nodes", "passed_checks", "incomplete_checks")}
        slim["evidence_count"] = len(c.get("evidence") or [])
        slim_criteria.append(slim)
    payload = {
        "report_meta": report.get("report_meta", {}),
        "summary":     report.get("summary", {}),
        "criteria":    slim_criteria,
    }
    raw = json.dumps(payload, indent=2, default=str)
    # Prevent </script> tag injection inside the embedded <script> element
    return raw.replace("</", "<\\/")
