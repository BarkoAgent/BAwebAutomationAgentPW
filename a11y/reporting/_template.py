"""
Single-file HTML template for the accessibility report (v2).

All CSS and JS are inlined so the generated .html is fully self-contained and
shareable without any external assets.

New in v2
---------
- Health score (0-100) with SVG gauge and penalty breakdown
- Outcome donut chart in the summary section
- WCAG pillar grid (Perceivable / Operable / Understandable / Robust)
- Issue-category breakdown with stacked bars
- Top affected components list with search
- Compliance standard badges (WCAG 2.1 AA, Section 508, ADA, EAA)
- Filter toolbar (by outcome, level, pillar)
- Export buttons: JSON download, CSV download, Print
- :target highlight animation on criterion panels
"""

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>A11y Report &mdash; {title}</title>
  <style>
    /* ── Design tokens ────────────────────────────────────────────────────── */
    :root {{
      --bg:      #f3efe7;
      --panel:   #fffdf9;
      --ink:     #1d1a16;
      --muted:   #6a6259;
      --border:  #d8d0c3;
      --accent:  #0f766e;
      --danger:  #b42318;
      --warn:    #b54708;
      --ok:      #027a48;
      --manual:  #6941c6;
      --shadow:  0 18px 48px rgba(42, 34, 25, 0.09);
      /* Spec severity palette */
      --c-pass:     #28A745;
      --c-critical: #B00020;
      --c-serious:  #FD7E14;
      --c-moderate: #FFC107;
      --c-neutral:  #6C757D;
    }}

    /* ── Reset & base ─────────────────────────────────────────────────────── */
    *, *::before, *::after {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(15,118,110,.12), transparent 26rem),
        linear-gradient(180deg, #f8f4ec 0%, var(--bg) 100%);
    }}
    a {{ color: var(--accent); }}
    code, pre {{ font-family: ui-monospace, "SFMono-Regular", Menlo, monospace; }}

    /* ── Layout ───────────────────────────────────────────────────────────── */
    .page {{
      max-width: 1280px;
      margin: 0 auto;
      padding: 32px 20px 56px;
    }}
    .hero, .panel, .criterion-panel {{
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 20px;
      box-shadow: var(--shadow);
    }}
    .hero {{ padding: 28px; margin-bottom: 22px; }}
    .hero-top {{
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      gap: 16px;
      flex-wrap: wrap;
    }}
    .hero h1 {{
      margin: 0 0 8px;
      font-size: clamp(1.6rem, 4vw, 2.6rem);
      line-height: 1.05;
    }}
    .hero p {{ margin: 6px 0; color: var(--muted); }}
    .meta-grid, .metrics-grid {{
      display: grid;
      gap: 12px;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    }}
    .panel {{ padding: 20px; margin-bottom: 20px; }}
    .metrics-grid {{ margin-top: 12px; }}
    .metric {{
      padding: 14px 16px;
      background: #faf7f1;
      border: 1px solid var(--border);
      border-radius: 16px;
    }}
    .metric-label, .label {{
      display: block;
      font-size: 12px;
      letter-spacing: .04em;
      text-transform: uppercase;
      color: var(--muted);
      margin-bottom: 6px;
    }}
    .label-inline {{ color: var(--muted); font-size: .92rem; }}
    .muted {{ color: var(--muted); }}

    /* ── Two-column layout ────────────────────────────────────────────────── */
    .two-col {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 20px;
      margin-bottom: 20px;
    }}
    .two-col > .panel {{ margin-bottom: 0; }}
    @media (max-width: 720px) {{
      .two-col {{ grid-template-columns: 1fr; }}
    }}

    /* ── Badges ───────────────────────────────────────────────────────────── */
    .badge {{
      display: inline-flex;
      align-items: center;
      border-radius: 999px;
      padding: .28rem .7rem;
      font-size: .79rem;
      font-weight: 700;
      letter-spacing: .02em;
      border: 1px solid transparent;
      white-space: nowrap;
    }}
    .passed, .automated {{
      background: rgba(2,122,72,.10); color: var(--ok); border-color: rgba(2,122,72,.18);
    }}
    .failed, .error {{
      background: rgba(180,35,24,.10); color: var(--danger); border-color: rgba(180,35,24,.18);
    }}
    .needs-review {{
      background: rgba(105,65,198,.08); color: var(--manual); border-color: rgba(105,65,198,.18);
    }}
    .semi-automated {{
      background: rgba(181,71,8,.10); color: var(--warn); border-color: rgba(181,71,8,.18);
    }}
    .manual-required {{
      background: rgba(105,65,198,.10); color: var(--manual); border-color: rgba(105,65,198,.18);
    }}
    .not-tested, .not-applicable, .neutral {{
      background: rgba(82,82,91,.10); color: #57534e; border-color: rgba(82,82,91,.18);
    }}

    /* ── Compliance badges ────────────────────────────────────────────────── */
    .compliance-badges {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 12px;
    }}
    .compliance-badge {{
      font-size: .75rem;
      font-weight: 700;
      border-radius: 6px;
      padding: 3px 9px;
      border: 1px solid rgba(15,118,110,.3);
      background: rgba(15,118,110,.08);
      color: var(--accent);
      text-decoration: none;
    }}
    .compliance-badge:hover {{
      background: rgba(15,118,110,.16);
    }}

    /* ── Export bar ───────────────────────────────────────────────────────── */
    .export-bar {{
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      align-items: center;
    }}
    .export-btn {{
      display: inline-flex;
      align-items: center;
      gap: 5px;
      padding: 6px 14px;
      border-radius: 999px;
      font-size: .82rem;
      font-weight: 600;
      cursor: pointer;
      border: 1px solid var(--border);
      background: var(--panel);
      color: var(--ink);
      text-decoration: none;
      white-space: nowrap;
    }}
    .export-btn:hover {{ background: #f2ede4; }}
    .export-btn-json {{ border-color: rgba(15,118,110,.4); color: var(--accent); }}
    .export-btn-csv  {{ border-color: rgba(15,118,110,.4); color: var(--accent); }}
    .export-btn-print{{ border-color: rgba(82,82,91,.3);   color: var(--muted); }}

    /* ── Alert / limitations panel ────────────────────────────────────────── */
    .alert-panel {{
      border-color: rgba(181,71,8,.35);
      background: #fff8ef;
    }}
    .alert-panel h2 {{ margin-top: 0; }}

    /* ── Health dashboard ─────────────────────────────────────────────────── */
    .health-layout {{
      display: grid;
      grid-template-columns: 160px 1fr 160px;
      gap: 24px;
      align-items: start;
      margin-top: 12px;
    }}
    @media (max-width: 720px) {{
      .health-layout {{ grid-template-columns: 1fr; justify-items: center; }}
    }}
    .health-col {{ display: flex; align-items: center; justify-content: center; }}
    .health-col-stats {{
      display: block;
      justify-content: unset;
      align-items: unset;
    }}
    .health-stat-grid {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
      margin-bottom: 16px;
    }}
    .health-stat {{
      background: #faf7f1;
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 10px 14px;
    }}
    .health-stat-label {{
      display: block;
      font-size: 11px;
      letter-spacing: .04em;
      text-transform: uppercase;
      color: var(--muted);
      margin-bottom: 4px;
    }}
    .stat-failed {{ color: var(--c-critical); }}
    .stat-review {{ color: #d97706; }}
    .stat-passed {{ color: var(--c-pass); }}
    .gain-section {{ margin-bottom: 12px; }}
    .gain-title {{
      margin: 0 0 8px;
      font-size: .82rem;
      text-transform: uppercase;
      letter-spacing: .05em;
      color: var(--muted);
    }}
    .gain-row {{
      display: flex;
      align-items: center;
      gap: 10px;
      margin-bottom: 6px;
      font-size: .88rem;
      flex-wrap: wrap;
    }}
    .gain-badge {{
      border-radius: 999px;
      padding: 2px 10px;
      font-size: .78rem;
      font-weight: 700;
      white-space: nowrap;
    }}
    .gain-failed {{ background: rgba(176,0,32,.1);  color: var(--c-critical); }}
    .gain-review {{ background: rgba(105,65,198,.08); color: var(--manual); }}
    .standards-note {{ font-size: .78rem; margin: 10px 0 0; }}

    /* ── WCAG Pillar grid ─────────────────────────────────────────────────── */
    .pillar-grid {{
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 16px;
      margin-top: 12px;
    }}
    @media (max-width: 900px) {{ .pillar-grid {{ grid-template-columns: repeat(2, 1fr); }} }}
    @media (max-width: 500px) {{ .pillar-grid {{ grid-template-columns: 1fr; }} }}
    .pillar-card {{
      border: 2px solid var(--border);
      border-radius: 16px;
      padding: 16px;
      background: #faf7f1;
    }}
    .pillar-fail   {{ border-color: rgba(176,0,32,.4); }}
    .pillar-review {{ border-color: rgba(253,126,20,.4); }}
    .pillar-pass   {{ border-color: rgba(40,167,69,.4); }}
    .pillar-na     {{ border-color: var(--border); }}
    .pillar-header {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 8px;
    }}
    .pillar-badge {{
      font-size: .72rem;
      font-weight: 700;
      border-radius: 999px;
      padding: 2px 8px;
    }}
    .pillar-fail   .pillar-badge {{ background: rgba(176,0,32,.1);  color: var(--c-critical); }}
    .pillar-review .pillar-badge {{ background: rgba(253,126,20,.1); color: #d97706; }}
    .pillar-pass   .pillar-badge {{ background: rgba(40,167,69,.1);  color: var(--c-pass); }}
    .pillar-na     .pillar-badge {{ background: #e9ecef; color: var(--c-neutral); }}
    .pillar-desc {{ font-size: .78rem; color: var(--muted); margin: 0 0 10px; }}
    .pillar-bar {{
      height: 6px;
      background: #e9ecef;
      border-radius: 3px;
      display: flex;
      overflow: hidden;
      margin-bottom: 8px;
    }}
    .pb-pass   {{ height: 100%; background: var(--c-pass); }}
    .pb-review {{ height: 100%; background: var(--c-moderate); }}
    .pb-fail   {{ height: 100%; background: var(--c-critical); }}
    .pillar-counts {{
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      font-size: .72rem;
    }}
    .pc-p {{ color: var(--c-pass);     font-weight: 600; }}
    .pc-r {{ color: #d97706;           font-weight: 600; }}
    .pc-f {{ color: var(--c-critical); font-weight: 600; }}

    /* ── Issue categories ─────────────────────────────────────────────────── */
    .category-list {{ display: flex; flex-direction: column; gap: 10px; margin-top: 12px; }}
    .cat-row {{
      display: grid;
      grid-template-columns: 150px 1fr 72px;
      align-items: center;
      gap: 12px;
    }}
    @media (max-width: 500px) {{
      .cat-row {{ grid-template-columns: 1fr 1fr; }}
      .cat-bar-wrap {{ display: none; }}
    }}
    .cat-name {{ font-size: .84rem; font-weight: 600; }}
    .cat-bar {{
      height: 8px;
      background: #e9ecef;
      border-radius: 4px;
      display: flex;
      overflow: hidden;
    }}
    .cat-seg {{ height: 100%; }}
    .cat-seg-fail   {{ background: var(--c-critical); }}
    .cat-seg-review {{ background: var(--c-moderate); }}
    .cat-seg-pass   {{ background: var(--c-pass); }}
    .cat-count {{ font-size: .8rem; font-weight: 700; text-align: right; }}
    .cat-fail   {{ color: var(--c-critical); }}
    .cat-review {{ color: #d97706; }}
    .cat-pass   {{ color: var(--c-pass); }}

    /* ── Top components ───────────────────────────────────────────────────── */
    .components-list {{ display: flex; flex-direction: column; gap: 8px; margin-top: 8px; }}
    .component-row {{
      display: grid;
      grid-template-columns: 1fr 80px 36px 36px;
      align-items: center;
      gap: 10px;
    }}
    .comp-selector {{
      font-size: .76rem;
      background: rgba(0,0,0,.04);
      border-radius: 4px;
      padding: 2px 6px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }}
    .comp-bar {{ display: flex; height: 6px; background: #e9ecef; border-radius: 3px; overflow: hidden; }}
    .comp-bar-fail   {{ height: 100%; background: var(--c-critical); }}
    .comp-bar-review {{ height: 100%; background: #d97706; }}
    .comp-count-fail   {{ font-size: .76rem; font-weight: 700; color: var(--c-critical); text-align: right; }}
    .comp-count-review {{ font-size: .76rem; font-weight: 700; color: #d97706;           text-align: right; }}
    .comp-count-zero   {{ opacity: .35; }}

    /* ── Outcome with donut ───────────────────────────────────────────────── */
    .outcome-with-chart {{
      display: flex;
      gap: 20px;
      align-items: flex-start;
      flex-wrap: wrap;
      margin-top: 12px;
    }}
    .outcome-with-chart .metrics-grid {{ flex: 1; min-width: 180px; margin-top: 0; }}

    /* ── Filter toolbar ───────────────────────────────────────────────────── */
    .filter-toolbar {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px 16px;
      margin-bottom: 16px;
      padding: 12px 0 14px;
      border-bottom: 1px solid var(--border);
    }}
    .filter-group {{ display: flex; align-items: center; gap: 6px; flex-wrap: wrap; }}
    .filter-group-label {{
      font-size: .72rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: .06em;
      color: var(--muted);
    }}
    .filter-btn {{
      padding: 4px 12px;
      border-radius: 999px;
      font-size: .78rem;
      font-weight: 600;
      cursor: pointer;
      border: 1px solid var(--border);
      background: var(--panel);
      color: var(--muted);
    }}
    .filter-btn:hover {{ background: #f2ede4; }}
    .filter-btn.filter-active {{ background: var(--accent); color: #fff; border-color: var(--accent); }}
    .filter-result-count {{
      font-size: .78rem;
      color: var(--muted);
      align-self: center;
      margin-left: auto;
    }}

    /* ── Misc panels ──────────────────────────────────────────────────────── */
    .source-chip {{
      background: #f2ede4;
      border-radius: 999px;
      padding: .15rem .45rem;
      margin-right: .3rem;
      display: inline-block;
      margin-bottom: .3rem;
    }}
    .criterion-panel {{
      padding: 22px;
      margin-bottom: 18px;
    }}
    .section-block + .section-block {{ margin-top: 24px; }}
    .timeline-card {{
      border: 1px solid var(--border);
      border-radius: 16px;
      padding: 14px;
      margin-bottom: 12px;
      background: #fcfaf6;
    }}
    .timeline-head {{
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: flex-start;
      margin-bottom: 10px;
    }}
    .timeline-grid {{
      display: grid;
      gap: 10px;
      grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
      font-size: .92rem;
    }}
    .axe-snapshot-details {{
      margin-top: 12px;
      border: 1px solid var(--border);
      border-radius: 10px;
      overflow: hidden;
    }}
    .axe-snapshot-details summary {{
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 8px 12px;
      cursor: pointer;
      font-size: .88rem;
      font-weight: 600;
      background: #f4f1ea;
      user-select: none;
    }}
    .axe-snapshot-details summary::-webkit-details-marker {{ display: none; }}
    .axe-snapshot-details summary::before {{
      content: "\u25B6";
      font-size: .75rem;
      transition: transform .15s;
    }}
    .axe-snapshot-details[open] summary::before {{ transform: rotate(90deg); }}
    .snapshot-toggle-label {{ color: #888; font-weight: 400; font-size: .82rem; }}
    .axe-snapshot-pre {{
      margin: 0; padding: 12px 14px;
      font-size: .78rem; line-height: 1.55;
      white-space: pre-wrap; word-break: break-word;
      background: #1e1e1e; color: #d4d4d4;
      max-height: 480px; overflow-y: auto;
    }}
    .violations-count {{
      background: #fde8e8; color: #b91c1c;
      border-radius: 12px; padding: 2px 9px;
      font-size: .8rem; font-weight: 700; white-space: nowrap;
    }}
    .axe-summary {{ margin-top: 10px; }}
    .criterion-head {{
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: flex-start;
      margin-bottom: 16px;
    }}
    .criterion-head h3 {{ margin: 0 0 6px; font-size: 1.25rem; }}
    .criterion-meta {{ margin: 0; color: var(--muted); }}
    .criterion-statuses {{ display: flex; gap: 8px; flex-wrap: wrap; }}
    .criterion-body {{
      display: grid;
      grid-template-columns: minmax(220px, 340px) 1fr;
      gap: 18px;
    }}
    .criterion-column h4 {{ margin-top: 0; margin-bottom: 10px; }}
    .evidence-card {{
      border: 1px solid var(--border);
      border-radius: 16px;
      padding: 14px;
      margin-bottom: 12px;
      background: #fcfaf6;
      overflow: hidden;
    }}
    .evidence-head {{
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: flex-start;
      margin-bottom: 10px;
    }}
    .evidence-head strong {{ overflow-wrap: break-word; word-break: break-word; min-width: 0; }}
    .evidence-grid {{
      display: grid;
      gap: 8px 14px;
      grid-template-columns: repeat(auto-fill, minmax(min(100%, 170px), 1fr));
      font-size: .9rem;
    }}
    .evidence-grid > div {{ min-width: 0; overflow-wrap: break-word; word-break: break-word; }}
    .evidence-grid > div > span:last-child {{ display: block; overflow-wrap: break-word; word-break: break-word; }}
    .evidence-cell-full {{ grid-column: 1 / -1; }}
    .evidence-grid code {{
      display: block; white-space: pre-wrap; word-break: break-all; overflow-wrap: anywhere;
      font-size: .82rem; background: rgba(0,0,0,.04); border-radius: 6px; padding: 4px 8px; margin-top: 2px;
    }}
    .evidence-overflow-toggle {{ margin-top: 12px; }}
    .evidence-overflow-toggle > summary {{
      font-size: .85rem; color: var(--muted);
      cursor: pointer; user-select: none;
    }}
    .evidence-overflow-inner {{ margin-top: 8px; }}
    .evidence-screenshot {{
      display: block; max-width: 100%; margin-top: 6px; border-radius: 8px;
      border: 1px solid var(--border); box-shadow: 0 2px 8px rgba(0,0,0,.08);
    }}
    .checkpoint-screenshot-wrap {{ margin: 10px 0 4px; }}
    .checkpoint-screenshot {{
      display: block; max-width: 100%; border-radius: 8px;
      border: 1px solid var(--border); box-shadow: 0 2px 8px rgba(0,0,0,.08);
    }}
    table {{ width: 100%; border-collapse: collapse; margin-top: 12px; font-size: .95rem; }}
    th, td {{ text-align: left; border-bottom: 1px solid var(--border); padding: 12px 10px; vertical-align: top; }}
    ul {{ padding-left: 20px; }}

    /* ── Screen flow ──────────────────────────────────────────────────────── */
    .screen-flow {{
      display: flex; flex-wrap: wrap;
      align-items: center; gap: 0;
      padding: 8px 0; overflow-x: auto;
    }}
    .flow-node-wrap {{ display: flex; align-items: center; gap: 0; }}
    .flow-node {{
      display: flex; flex-direction: column;
      align-items: center; justify-content: center;
      gap: 5px; background: var(--panel);
      border: 2px solid var(--border); border-radius: 14px;
      padding: 12px 16px; min-width: 120px; max-width: 180px;
      text-align: center; cursor: default; transition: border-color .15s;
    }}
    .flow-node:hover {{ border-color: var(--accent); }}
    .flow-step-index {{
      font-size: .72rem; color: var(--muted);
      font-weight: 600; text-transform: uppercase; letter-spacing: .04em;
    }}
    .flow-screen-label {{ font-size: .88rem; font-weight: 700; word-break: break-word; }}
    .flow-violations-badge {{ border-radius: 999px; padding: 2px 10px; font-size: .78rem; font-weight: 700; }}
    .flow-violations-fail {{ background: #fde8e8; color: #b91c1c; }}
    .flow-violations-pass {{ background: #d1fae5; color: #065f46; }}
    .flow-node-nav {{ border-color: var(--accent); }}
    .flow-url-sub {{
      font-size: .68rem; font-family: ui-monospace, monospace;
      border-radius: 4px; padding: 1px 6px;
      max-width: 160px; overflow: hidden; text-overflow: ellipsis;
    }}
    .flow-url-new  {{ background: #d1fae5; color: #065f46; font-weight: 600; }}
    .flow-url-same {{ background: #f3f4f6; color: #9ca3af; }}
    .flow-arrow {{ font-size: 1.4rem; color: var(--muted); padding: 0 8px; flex-shrink: 0; }}
    .flow-arrow-nav {{ color: var(--accent); font-weight: 700; }}

    /* ── Screen matrix ────────────────────────────────────────────────────── */
    .matrix-scroll-wrap {{
      overflow-x: auto; margin-top: 14px;
      border: 1px solid var(--border); border-radius: 12px;
    }}
    .screen-matrix {{ border-collapse: collapse; width: 100%; font-size: .82rem; }}
    .screen-matrix th, .screen-matrix td {{
      border: 1px solid var(--border); padding: 0; white-space: nowrap;
    }}
    .matrix-corner {{
      background: #f4f1ea; padding: 8px 12px; text-align: left; font-weight: 700;
      position: sticky; left: 0; z-index: 2; min-width: 140px;
    }}
    .matrix-th {{
      background: #f4f1ea; text-align: center; vertical-align: bottom;
      padding: 8px 6px 6px; min-width: 68px; max-width: 90px;
    }}
    .matrix-cid   {{ display: block; font-weight: 800; font-size: .78rem; }}
    .matrix-cname {{ display: block; font-size: .68rem; color: var(--muted); white-space: normal; line-height: 1.3; max-width: 80px; }}
    .matrix-clevel {{
      display: inline-block; margin-top: 3px; font-size: .65rem;
      background: #e6e0d6; border-radius: 4px; padding: 1px 5px; font-weight: 600;
    }}
    .matrix-screen-cell {{
      background: #f9f6f1; padding: 6px 10px; font-weight: 600;
      position: sticky; left: 0; z-index: 1;
      max-width: 200px; overflow: hidden; text-overflow: ellipsis;
    }}
    .matrix-cell {{
      text-align: center; vertical-align: middle;
      font-weight: 700; font-size: .9rem;
      width: 50px; height: 38px; cursor: default;
    }}
    .matrix-failed  {{ background: #fde8e8; color: #b91c1c; }}
    .matrix-error   {{ background: #fff0e0; color: #92400e; }}
    .matrix-review  {{ background: #fffbeb; color: #92400e; }}
    .matrix-passed  {{ background: #d1fae5; color: #065f46; }}
    .matrix-na      {{ background: #f3f4f6; color: #9ca3af; }}
    .matrix-empty   {{ background: transparent; color: #d1d5db; font-weight: 400; }}
    .matrix-legend  {{ display: flex; flex-wrap: wrap; gap: 12px; font-size: .8rem; margin-bottom: 10px; }}
    .matrix-legend-item {{ display: flex; align-items: center; gap: 6px; }}
    .matrix-legend-item .matrix-cell {{
      width: 22px; height: 22px; border-radius: 5px;
      border: 1px solid var(--border);
      display: inline-flex; align-items: center; justify-content: center;
      font-size: .75rem;
    }}

    /* ── Criterion panel highlight on :target ─────────────────────────────── */
    @keyframes highlight-panel {{
      0%  {{ box-shadow: 0 0 0 4px var(--accent); }}
      100%{{ box-shadow: var(--shadow); }}
    }}
    .criterion-panel:target {{ animation: highlight-panel 1.8s ease; }}

    /* ── Responsive tweaks ────────────────────────────────────────────────── */
    @media (max-width: 900px) {{
      .criterion-body {{ grid-template-columns: 1fr; }}
      .criterion-head {{ flex-direction: column; }}
    }}
    @media (max-width: 640px) {{
      .page {{ padding: 16px 12px 40px; }}
      .hero {{ padding: 18px 16px; }}
      .panel, .criterion-panel {{ padding: 16px; }}
      .timeline-head {{ flex-wrap: wrap; }}
      .filter-group-label {{ display: none; }}
      .legend-desc {{ display: none; }}
      table {{ display: block; overflow-x: auto; -webkit-overflow-scrolling: touch; }}
      .flow-arrow {{ padding: 0 4px; font-size: 1.1rem; }}
    }}
    @media (max-width: 500px) {{
      .component-row {{ grid-template-columns: 1fr 36px 36px; }}
      .comp-bar {{ display: none; }}
      .health-layout {{ justify-items: start; }}
      .health-stat-grid {{ grid-template-columns: 1fr 1fr; }}
    }}

    /* ── Action items ─────────────────────────────────────────────────────── */
    .action-item {{
      padding: 10px 14px;
      border-radius: 10px;
      margin: 0 0 14px;
      font-size: .95rem;
      line-height: 1.5;
    }}
    .action-failed {{
      background: rgba(180,35,24,.06);
      border-left: 3px solid var(--danger);
    }}
    .action-review {{
      background: rgba(105,65,198,.05);
      border-left: 3px solid var(--manual);
    }}
    .outcome-summary-text {{
      color: var(--muted);
      font-size: .92rem;
      margin: 0 0 14px;
    }}

    /* ── Evidence (simplified) ────────────────────────────────────────────── */
    .ev-row {{ margin-bottom: 6px; font-size: .9rem; }}
    .ev-row .label {{ display: inline-block; min-width: 70px; margin-bottom: 0; }}
    .ev-row code {{
      display: inline; font-size: .85rem;
      background: rgba(0,0,0,.04); border-radius: 4px; padding: 1px 5px;
    }}
    .evidence-detail-toggle {{ margin-top: 8px; }}
    .evidence-detail-toggle > summary {{
      font-size: .78rem; color: var(--muted);
      cursor: pointer; user-select: none;
    }}
    .evidence-detail-grid {{
      display: grid; gap: 6px 12px;
      grid-template-columns: repeat(auto-fill, minmax(min(100%, 180px), 1fr));
      font-size: .85rem; margin-top: 8px; padding: 10px;
      background: #faf7f1; border-radius: 8px;
    }}

    /* ── Coverage details toggle ──────────────────────────────────────────── */
    .coverage-details-toggle {{ margin-top: 12px; }}
    .coverage-details-toggle > summary {{
      font-size: .82rem; color: var(--muted);
      cursor: pointer; user-select: none;
      font-weight: 600;
    }}
    .coverage-details-inner {{
      margin-top: 8px; padding: 12px 14px;
      background: #faf7f1; border-radius: 8px; font-size: .88rem;
    }}
    .coverage-details-inner p {{ margin: 4px 0; }}
    .coverage-details-inner ul {{ margin: 6px 0; padding-left: 18px; }}

    /* ── Automation limit callout ────────────────────────────────────────── */
    .automation-limit-callout {{ margin: 0 0 14px; }}
    .automation-limit-callout > summary {{
      font-size: .82rem; color: var(--muted);
      cursor: pointer; user-select: none;
      font-weight: 600;
    }}
    .automation-limit-body {{
      margin-top: 8px; padding: 12px 14px;
      background: #eef3fb; border-radius: 8px;
      border-left: 3px solid #4a7cc9;
      font-size: .86rem;
    }}
    .automation-limit-body p {{ margin: 6px 0; }}
    .automation-limit-body ul {{ margin: 6px 0; padding-left: 18px; }}
    .automation-limit-body li {{ margin-bottom: 4px; line-height: 1.5; }}

    /* ── Status legend ────────────────────────────────────────────────────── */
    .status-legend {{
      display: flex; flex-wrap: wrap; gap: 14px 20px;
      margin: 8px 0 16px; font-size: .84rem;
      padding: 12px 14px; background: #faf7f1;
      border-radius: 10px; border: 1px solid var(--border);
    }}
    .legend-item {{ display: flex; align-items: center; gap: 8px; }}
    .legend-desc {{ color: var(--muted); }}

    /* ── Print ────────────────────────────────────────────────────────────── */
    @media print {{
      .filter-toolbar, .export-bar,
      .not-tested-toggle-bar {{ display: none !important; }}
      .criterion-panel-not-tested {{ display: block !important; }}
      .page {{ max-width: 100%; padding: 12px; box-shadow: none; }}
      .hero, .panel, .criterion-panel {{ box-shadow: none; break-inside: avoid; padding: 20px; }}
      .two-col {{ grid-template-columns: 1fr; }}
      .health-layout {{ grid-template-columns: 1fr; justify-items: start; }}
      .pillar-grid {{ grid-template-columns: repeat(2, 1fr); }}
      table {{ display: table; overflow-x: visible; }}
      .legend-desc {{ display: inline; }}
    }}
  </style>
</head>
<body>
  <main class="page">

    <!-- ═══════ Hero ═══════════════════════════════════════════════════════ -->
    <section class="hero">
      <div class="hero-top">
        <h1>{title}</h1>
      </div>
      <p>{url}</p>
      <div class="meta-grid" style="margin-top:12px">
        <div><span class="label">Report ID</span><strong>{report_id}</strong></div>
        <div><span class="label">Generated</span><strong>{generated_at}</strong></div>
        <div><span class="label">Page Title</span><strong>{page_title}</strong></div>
        <div><span class="label">Standard</span><strong>{standard_profile}</strong></div>
      </div>
      {compliance_badges}
    </section>

    <!-- ═══════ Health Score ════════════════════════════════════════════════ -->
    <section class="panel">
      <h2 style="margin-top:0">Accessibility Health Score</h2>
      <p class="muted" style="margin-top:0">Weighted metric: higher-severity outcomes and higher-priority WCAG levels apply a steeper score penalty.</p>
      {health_panel}
    </section>

    <!-- ═══════ WCAG Pillars ════════════════════════════════════════════════ -->
    <section class="panel">
      <h2 style="margin-top:0">WCAG Compliance Pillars</h2>
      <p class="muted" style="margin-top:0">Status across the four WCAG principles. Each bar shows pass / review / fail ratio for criteria in that pillar.</p>
      {pillar_grid}
    </section>

    <!-- ═══════ Categories + Components (two-col) ══════════════════════════ -->
    <div class="two-col">
      <section class="panel">
        <h2 style="margin-top:0">Issue Categories</h2>
        <p class="muted" style="margin-top:0">Failures grouped by functional area.</p>
        {category_breakdown}
      </section>
      <section class="panel">
        <h2 style="margin-top:0">Top Affected Components</h2>
        <p class="muted" style="margin-top:0">CSS selectors ranked by total issue count.
          <span style="color:var(--c-critical);font-weight:600">&#9632; Failed</span> &nbsp;
          <span style="color:#d97706;font-weight:600">&#9632; Needs review</span>
        </p>
        {top_components}
      </section>
    </div>

    <!-- ═══════ Audit Limitations ═══════════════════════════════════════════ -->
    <section class="panel alert-panel">
      <h2>Audit Limitations</h2>
      {limitations}
    </section>

    <!-- ═══════ Scenario Coverage ═══════════════════════════════════════════ -->
    <section class="panel">
      <h2>Scenario Coverage</h2>
      {scenario_summary}
    </section>

    <!-- ═══════ Screen Journey ═══════════════════════════════════════════════ -->
    <section class="panel">
      <h2>Screen Journey</h2>
      <p class="muted">Unique screens visited during the test case, in order. Arrows show navigation transitions. Violation counts are from axe at each checkpoint.</p>
      {screen_journey}
    </section>

    <!-- ═══════ Screen × Criterion Matrix ══════════════════════════════════ -->
    <section class="panel">
      <h2>Screen &times; Criterion Matrix</h2>
      <p class="muted">Each cell shows the worst outcome for a WCAG criterion on a given screen. Only criteria with at least one evidence item appear as columns.</p>
      {screen_matrix}
    </section>

    <!-- ═══════ Outcome + Coverage Summary (two-col) ════════════════════════ -->
    <div class="two-col">
      <section class="panel">
        <h2 style="margin-top:0">Outcome Summary</h2>
        <div class="outcome-with-chart">
          <div>{outcome_donut}</div>
          <div class="metrics-grid">{outcome_counts}</div>
        </div>
      </section>
      <section class="panel">
        <h2 style="margin-top:0">Coverage Summary</h2>
        <div class="metrics-grid">{coverage_counts}</div>
      </section>
    </div>

    <!-- ═══════ Scenario Timeline ═══════════════════════════════════════════ -->
    <section class="panel">
      <h2>Scenario Timeline</h2>
      <p class="muted">Scenario steps are summarised for readability. Oversized outputs are omitted from the shareable report.</p>
      {scenario_steps}
    </section>

    <!-- ═══════ Audit Checkpoints ════════════════════════════════════════════ -->
    <section class="panel">
      <h2>Audit Checkpoints</h2>
      <p class="muted">Accessibility checkpoints recorded across the scenario or audit flow.</p>
      {journey_steps}
    </section>

    <!-- ═══════ Criteria Overview ════════════════════════════════════════════ -->
    <section class="panel">
      <h2>Criteria Overview</h2>
      <p class="muted">Criteria are grouped by WCAG section. Click a criterion ID to jump to its detail panel.</p>
      {criteria_overview}
    </section>

    <!-- ═══════ Criterion Details (with filter toolbar) ══════════════════════ -->
    <section class="panel">
      <h2>Criterion Details</h2>
      <p class="muted">Sorted by priority &mdash; failures first. Use filters to narrow results. Click &ldquo;Coverage details&rdquo; on any criterion for sources and affected screens.</p>
      <div class="status-legend" role="note" aria-label="Status legend">
        <div class="legend-item"><span class="badge failed">Failed</span><span class="legend-desc">Automated violation confirmed &mdash; fix required</span></div>
        <div class="legend-item"><span class="badge needs-review">Needs Review</span><span class="legend-desc">Open question &mdash; automation flagged but could not confirm; human review required</span></div>
        <div class="legend-item"><span class="badge passed">Passed</span><span class="legend-desc">Tested &mdash; no violations found</span></div>
        <div class="legend-item"><span class="badge not-tested">Not Tested</span><span class="legend-desc">No evidence collected this run</span></div>
        <div class="legend-item"><span class="badge not-applicable">N/A</span><span class="legend-desc">Does not apply to this flow</span></div>
      </div>
      <div class="filter-toolbar" role="group" aria-label="Filter criterion panels">
        <div class="filter-group">
          <span class="filter-group-label">Outcome</span>
          <button class="filter-btn filter-btn-outcome" onclick="setFilter('outcome','FAILED',this)">Failed</button>
          <button class="filter-btn filter-btn-outcome" onclick="setFilter('outcome','NEEDS_REVIEW',this)">Needs Review</button>
          <button class="filter-btn filter-btn-outcome" onclick="setFilter('outcome','PASSED',this)">Passed</button>
          <button class="filter-btn filter-btn-outcome" onclick="setFilter('outcome','NOT_TESTED',this)">Not Tested</button>
        </div>
        <div class="filter-group">
          <span class="filter-group-label">Level</span>
          <button class="filter-btn filter-btn-level" onclick="setFilter('level','A',this)">A</button>
          <button class="filter-btn filter-btn-level" onclick="setFilter('level','AA',this)">AA</button>
          <button class="filter-btn filter-btn-level" onclick="setFilter('level','AAA',this)">AAA</button>
        </div>
        <div class="filter-group">
          <span class="filter-group-label">Pillar</span>
          <button class="filter-btn filter-btn-principle" onclick="setFilter('principle','1',this)">Perceivable</button>
          <button class="filter-btn filter-btn-principle" onclick="setFilter('principle','2',this)">Operable</button>
          <button class="filter-btn filter-btn-principle" onclick="setFilter('principle','3',this)">Understandable</button>
          <button class="filter-btn filter-btn-principle" onclick="setFilter('principle','4',this)">Robust</button>
        </div>
        <span class="filter-result-count" id="filter-result-count" aria-live="polite"></span>
      </div>
    </section>
    {criterion_panels}

  </main>

  <!-- ═══════ Embedded report data (for JSON download) ════════════════════ -->
  <script type="application/json" id="report-json-data">{report_json}</script>

  <!-- ═══════ Application scripts ══════════════════════════════════════════ -->
  <script>
    /* ── Filter system ───────────────────────────────────────────────────── */
    var _active = {{}};

    function _applyFilters() {{
      var panels  = document.querySelectorAll('.criterion-panel');
      var hasFilter = Object.keys(_active).some(function(k) {{ return _active[k]; }});
      var visible = 0;

      panels.forEach(function(p) {{
        var show;
        if (!hasFilter) {{
          // Restore default state: not-tested panels stay hidden
          show = p.getAttribute('data-default-hidden') !== 'true';
        }} else {{
          show = true;
          var od = p.getAttribute('data-outcome');
          var ld = p.getAttribute('data-level');
          var pd = p.getAttribute('data-principle');
          if (_active['outcome']   && od !== _active['outcome'])   show = false;
          if (_active['level']     && ld !== _active['level'])     show = false;
          if (_active['principle'] && pd !== _active['principle']) show = false;
        }}
        p.style.display = show ? '' : 'none';
        if (show) visible++;
      }});

      var rc = document.getElementById('filter-result-count');
      if (rc) {{
        rc.textContent = hasFilter
          ? (visible + ' of ' + panels.length + ' shown')
          : '';
      }}
    }}

    function setFilter(type, value, btn) {{
      if (_active[type] === value) {{
        delete _active[type];
        btn.classList.remove('filter-active');
      }} else {{
        document.querySelectorAll('.filter-btn-' + type).forEach(function(b) {{
          b.classList.remove('filter-active');
        }});
        _active[type] = value;
        btn.classList.add('filter-active');
      }}
      _applyFilters();
    }}

    /* ── JSON download ───────────────────────────────────────────────────── */
    function downloadJSON() {{
      var el = document.getElementById('report-json-data');
      if (!el) return;
      var blob = new Blob([el.textContent], {{ type: 'application/json' }});
      var url  = URL.createObjectURL(blob);
      var a    = document.createElement('a');
      a.href = url;
      a.download = 'a11y-report.json';
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      setTimeout(function() {{ URL.revokeObjectURL(url); }}, 1000);
    }}

    /* ── CSV download ────────────────────────────────────────────────────── */
    function downloadCSV() {{
      var header = [
        'ID', 'Name', 'Principle', 'Level', 'Outcome',
        'Coverage', 'Sources', 'Affected Screens', 'Affected URLs'
      ];
      var rows = [header];
      document.querySelectorAll('.criterion-panel[data-csv]').forEach(function(el) {{
        try {{
          var row = JSON.parse(el.getAttribute('data-csv'));
          rows.push(row);
        }} catch(e) {{}}
      }});
      var csv = rows.map(function(r) {{
        return r.map(function(v) {{
          return '"' + String(v == null ? '' : v).replace(/"/g, '""') + '"';
        }}).join(',');
      }}).join('\n');
      var blob = new Blob([csv], {{ type: 'text/csv;charset=utf-8;' }});
      var url  = URL.createObjectURL(blob);
      var a    = document.createElement('a');
      a.href = url;
      a.download = 'a11y-report.csv';
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      setTimeout(function() {{ URL.revokeObjectURL(url); }}, 1000);
    }}

    /* ── Init ────────────────────────────────────────────────────────────── */
    _applyFilters();
  </script>
</body>
</html>
"""
