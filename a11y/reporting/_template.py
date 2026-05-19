"""
HTML templates for the accessibility reports.

The engineer-facing report (`HTML_TEMPLATE`) is loaded from the sibling
`_report_template.html` design file. It is a single-file, JS-driven report
that consumes a JSON payload embedded in `<script id="report-data">`.
The placeholder `__REPORT_JSON__` is replaced with the serialised payload
at render time.
"""

from pathlib import Path

HTML_TEMPLATE = (Path(__file__).parent / "_report_template.html").read_text(encoding="utf-8")


STAKEHOLDER_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>A11y Stakeholder Summary &mdash; {title}</title>
  <style>
    :root {{
      --c-pass: #28A745; --c-fail: #B00020; --c-review: #FFC107;
      --c-soft: #d97706; --c-muted: #6b7280; --border: #e5e7eb;
    }}
    body {{
      font: 15px/1.55 -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif;
      background: #f5f7fa; color: #1f2937; margin: 0; padding: 32px;
    }}
    .page {{ max-width: 880px; margin: 0 auto; }}
    h1 {{ font-size: 1.65rem; margin: 0 0 4px; }}
    h2 {{ font-size: 1.1rem; margin: 28px 0 10px; }}
    .meta {{ color: var(--c-muted); margin: 0 0 24px; }}
    .panel {{
      background: #fff; border: 1px solid var(--border); border-radius: 12px;
      padding: 20px 22px; margin-bottom: 18px;
      box-shadow: 0 1px 2px rgba(0,0,0,.03);
    }}
    .counts {{ display: grid; grid-template-columns: repeat(2, 1fr); gap: 12px; }}
    .count-card {{
      padding: 14px 16px; border-radius: 10px;
      background: #fafbfc; border-left: 4px solid var(--c-muted);
    }}
    .count-card.fail {{ border-left-color: var(--c-fail); }}
    .count-card.review {{ border-left-color: var(--c-review); }}
    .count-card.soft {{ border-left-color: var(--c-soft); }}
    .count-card.pass {{ border-left-color: var(--c-pass); }}
    .count-card .num {{ font-size: 2rem; font-weight: 700; display: block; line-height: 1; }}
    .count-card .lbl {{ font-size: .82rem; color: var(--c-muted); text-transform: uppercase; letter-spacing: .04em; margin-top: 4px; display: block; }}
    .count-card .desc {{ font-size: .88rem; margin-top: 8px; color: #444; }}
    ul {{ margin: 8px 0; padding-left: 20px; }}
    li {{ margin-bottom: 4px; line-height: 1.45; }}
    code {{ background: #eef1f5; padding: 1px 5px; border-radius: 4px; font-size: .88em; }}
    .muted {{ color: var(--c-muted); }}
    .scope {{ font-size: .92rem; }}
    .scope p {{ margin: 6px 0; }}
    a {{ color: #2563eb; }}
  </style>
</head>
<body>
  <div class="page">
    <h1>{title}</h1>
    <p class="meta">Stakeholder summary &middot; {generated_at}<br>
      <a href="{detail_link}">Open full engineer report &rarr;</a>
    </p>

    <section class="panel">
      <h2>Scope</h2>
      <div class="scope">{scope_block}</div>
    </section>

    <section class="panel">
      <h2>Headline counts</h2>
      <div class="counts">{headline_counts}</div>
      <p class="muted" style="margin-top:14px;font-size:.85rem">
        These are honesty-adjusted: a passed criterion is split into "verified clean", "no applicable elements", and "scanned but inconclusive". The first is strong evidence; the others are not.
      </p>
    </section>

    <section class="panel">
      <h2>Top gaps to address</h2>
      {top_gaps}
    </section>

    <section class="panel">
      <h2>Not testable by automation</h2>
      <p class="muted">These criteria require manual review. Automation does not cover them in this run.</p>
      {not_testable}
    </section>

    <section class="panel">
      <h2>Methodology TL;DR</h2>
      <p>This report ran <strong>{custom_count}</strong> custom evaluators plus axe-core across <strong>{checkpoint_count}</strong> checkpoint(s).</p>
      {methodology_tldr}
    </section>
  </div>
</body>
</html>
"""
