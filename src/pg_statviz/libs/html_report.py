"""
pg_statviz - HTML report generation for AI analysis.

Produces one consolidated HTML per analysis module, embedding references to
the sibling chart PNGs and rendering the LLM's markdown output.
"""

__author__ = "Jimmy Angelakos"
__copyright__ = "Copyright (c) 2026 Jimmy Angelakos"
__license__ = "PostgreSQL License"

import html
import logging
import re
from pathlib import Path

_logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Tiny markdown -> HTML renderer
# ---------------------------------------------------------------------------
# Deliberately not a full CommonMark implementation -- just enough to render
# what pg_statviz's LLM prompts actually produce: a status badge, short
# paragraphs, occasional bullets/numbered lists, inline **bold** / *italic* /
# `code`, and short #/##/### headings. Anything else is passed through as
# HTML-escaped plain text inside a <p>, so we fail safely rather than loudly.

_INLINE_CODE = re.compile(r'`([^`\n]+)`')
_BOLD = re.compile(r'\*\*([^*\n]+)\*\*')
# Italic: single asterisks, but not inside a bold run (we run bold first and
# its output contains <strong>...</strong> with no raw '*').
_ITALIC = re.compile(r'(?<!\*)\*([^*\n]+)\*(?!\*)')
_HEADING = re.compile(r'^(#{1,4})\s+(.*)$')
_BULLET = re.compile(r'^\s*[-*+]\s+(.*)$')
_ORDERED = re.compile(r'^\s*\d+\.\s+(.*)$')


def _inline(text: str) -> str:
    """HTML-escape then apply inline markdown transforms."""
    t = html.escape(text)
    # Inline code first so later runs don't touch its contents. Because we
    # already escaped, the code content is safe to wrap.
    t = _INLINE_CODE.sub(r'<code>\1</code>', t)
    t = _BOLD.sub(r'<strong>\1</strong>', t)
    t = _ITALIC.sub(r'<em>\1</em>', t)
    return t


def md_to_html(md: str) -> str:
    """Render a short LLM-produced markdown blob to HTML.

    Supports: bold/italic/inline-code, # headings, bullet and ordered lists,
    blank-line-separated paragraphs. The [HEALTHY]/[WARNING] status badge
    emitted as **[HEALTHY]** / **[WARNING]** is post-processed into a styled
    <span class="status healthy|warning">...</span> so CSS can colour it.

    Not a general CommonMark renderer -- sufficient for pg_statviz's prompts.
    """
    if not md:
        return ''

    md = md.strip().replace('\r\n', '\n').replace('\r', '\n')
    blocks = re.split(r'\n\s*\n', md)
    out = []

    for block in blocks:
        lines = [ln for ln in block.split('\n') if ln.strip()]
        if not lines:
            continue

        # Single-line heading
        if len(lines) == 1:
            m = _HEADING.match(lines[0])
            if m:
                level = len(m.group(1))
                out.append(f'<h{level}>{_inline(m.group(2))}</h{level}>')
                continue

        # Unordered list: every line a bullet
        bullet_matches = [_BULLET.match(ln) for ln in lines]
        if all(bullet_matches):
            items = ''.join(f'<li>{_inline(m.group(1))}</li>'
                            for m in bullet_matches)
            out.append(f'<ul>{items}</ul>')
            continue

        # Ordered list: every line "N. ..."
        ordered_matches = [_ORDERED.match(ln) for ln in lines]
        if all(ordered_matches):
            items = ''.join(f'<li>{_inline(m.group(1))}</li>'
                            for m in ordered_matches)
            out.append(f'<ol>{items}</ol>')
            continue

        # Paragraph: join lines with spaces, one <p> per block.
        out.append(f'<p>{_inline(" ".join(lines))}</p>')

    body = '\n'.join(out)

    # Promote status badges AFTER inline rendering (bold has already become
    # <strong>). Doing this here rather than pre-pass keeps the match simple.
    body = body.replace(
        '<strong>[HEALTHY]</strong>',
        '<span class="status healthy">[HEALTHY]</span>',
    )
    body = body.replace(
        '<strong>[WARNING]</strong>',
        '<span class="status warning">[WARNING]</span>',
    )
    body = body.replace(
        '<strong>[CRITICAL]</strong>',
        '<span class="status critical">[CRITICAL]</span>',
    )
    return body


# ---------------------------------------------------------------------------
# HTML report writer
# ---------------------------------------------------------------------------

_CSS = """
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI",
         "Helvetica Neue", Arial, sans-serif;
         max-width: 960px; margin: 2em auto; padding: 0 1em;
         color: #222; line-height: 1.5; }
  header { border-bottom: 2px solid #336791; padding-bottom: .5em;
           margin-bottom: 1.5em; }
  header h1 { margin: 0; color: #336791; font-size: 1.6em; }
  header .subtitle { color: #666; font-size: .95em; margin-top: .2em; }
  section { margin: 2em 0; }
  section h2 { color: #336791; font-size: 1.25em;
               border-bottom: 1px solid #e0e0e0; padding-bottom: .2em; }
  img { max-width: 100%; height: auto; display: block;
        margin: 1em 0; border: 1px solid #e0e0e0; border-radius: 4px; }
  .analysis { margin-top: 1em; }
  .analysis p { margin: .6em 0; }
  .analysis ul, .analysis ol { margin: .6em 0 .6em 1.5em; }
  .analysis code { background: #f4f4f4; padding: .1em .3em;
                   border-radius: 3px; font-size: .9em; }
  .status { display: inline-block; padding: .15em .6em; border-radius: 4px;
            font-weight: 600; font-size: .9em; letter-spacing: .02em;
            margin-right: .4em; }
  .status.healthy { background: #d4edda; color: #155724; }
  .status.warning { background: #fff3cd; color: #856404; }
  .status.critical { background: #f8d7da; color: #721c24; }
  .missing { color: #888; font-style: italic; }
  .module-list { list-style: none; padding-left: 0; }
  .module-list li { padding: .4em 0; border-bottom: 1px solid #f0f0f0; }
  .module-list a { color: #336791; text-decoration: none; font-weight: 600; }
  .module-list a:hover { text-decoration: underline; }
  .summary { background: #f8f9fa; border-left: 4px solid #336791;
             padding: 1em; margin: 1em 0; border-radius: 4px; }
  footer { color: #888; font-size: .85em; margin-top: 3em;
           text-align: center; border-top: 1px solid #eee; padding-top: 1em; }
"""


def _render_section(section: dict) -> str:
    title = html.escape(section.get('title', ''))
    image_basename = html.escape(section.get('image_basename', ''))
    analysis_md = section.get('analysis_md')

    parts = ['<section>',
             f'  <h2>{title}</h2>',
             f'  <img src="{image_basename}" alt="{title}">']
    if analysis_md:
        parts.append(
            f'  <div class="analysis">{md_to_html(analysis_md)}</div>')
    else:
        parts.append('  <p class="missing">AI analysis unavailable '
                     'for this chart.</p>')
    parts.append('</section>')
    return '\n'.join(parts)


def _output_prefix(outputdir, info, port) -> str:
    """The shared `<dir>/pg_statviz_<host>_<port>_` path prefix used by
    every chart PNG and every HTML report."""
    head = outputdir.rstrip('/') + '/' if outputdir else ''
    host = info['hostname'].replace('/', '-')
    return f"{head}pg_statviz_{host}_{port}_"


def finalize_module_report(outputdir, info, port, module_name: str,
                           sections: list) -> None:
    """Write the consolidated per-module HTML next to the chart PNGs.

    No-op when sections is empty (which happens when --ai was off or the
    module short-circuited before generating any chart). Called once at the
    end of every leaf module; independent invocation of modules (e.g.
    `pg_statviz buf`) works unchanged -- no orchestrator coupling.
    """
    if not sections:
        return
    html_out = f"{_output_prefix(outputdir, info, port)}{module_name}.html"
    write_module_report(
        html_out,
        title=f"pg_statviz · {module_name}",
        subtitle=f"{info['hostname']}:{port}",
        sections=sections,
    )


def write_module_report(output_path, title: str, subtitle: str,
                        sections: list) -> None:
    """Write a consolidated HTML report for one analysis module.

    Args:
        output_path: Destination .html path (str or Path).
        title: Human-readable report title (e.g. "pg_statviz - buffers").
        subtitle: Sub-line under the title, typically "host:port".
        sections: List of dicts with keys:
            - 'title' (str): section heading
            - 'image_basename' (str): PNG filename in the same directory
            - 'analysis_md' (str | None): raw LLM markdown, or None if the
              AI analysis for this chart failed/was skipped

    Never raises. File-write errors are logged at ERROR level.
    """
    if not sections:
        return

    body_sections = '\n'.join(_render_section(s) for s in sections)
    esc_title = html.escape(title)
    esc_subtitle = html.escape(subtitle)

    doc = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>{esc_title}</title>
<style>{_CSS}</style>
</head>
<body>
<header>
  <h1>{esc_title}</h1>
  <div class="subtitle">{esc_subtitle}</div>
</header>
{body_sections}
<footer>Generated by pg_statviz</footer>
</body>
</html>
"""

    try:
        Path(output_path).write_text(doc, encoding='utf-8')
        _logger.info(f"HTML report saved to {output_path}")
    except OSError as e:
        _logger.error(f"Could not write {output_path}: {e}")


# ---------------------------------------------------------------------------
# Cross-module index report
# ---------------------------------------------------------------------------

# Match per-section: <h2>title</h2> ... [VERDICT] ... summary up to </p>.
# Captures the title, verdict tag, and the analysis paragraph as raw HTML
# (which is then tag-stripped to a plain-text summary).
_VERDICT_RE = re.compile(
    r'<h2>([^<]+)</h2>.*?'
    r'\[(HEALTHY|WARNING|CRITICAL)\]\s*(.+?)</p>',
    re.DOTALL,
)
_HTML_TAG_RE = re.compile(r'<[^>]+>')


def _scan_module_reports(outputdir, info, port, exclude_basenames=()):
    """Walk per-module HTMLs in the output dir and extract one finding
    per chart section: (title, verdict, summary). Returns [] when no
    reports exist (e.g. --ai was disabled).
    """
    prefix = _output_prefix(outputdir, info, port)
    sections = []
    for path in sorted(Path(prefix).parent.glob(
            f"{Path(prefix).name}*.html")):
        if path.name in exclude_basenames:
            continue
        try:
            content = path.read_text(encoding='utf-8')
        except OSError:
            continue
        for m in _VERDICT_RE.finditer(content):
            title = html.unescape(m.group(1)).strip()
            verdict = m.group(2).upper()
            raw = _HTML_TAG_RE.sub('', m.group(3))
            summary = ' '.join(html.unescape(raw).split())[:240]
            sections.append({
                'module_html': path.name,
                'title': title,
                'verdict': verdict,
                'summary': summary,
            })
    return sections


def _verdict_badge(verdict: str) -> str:
    cls = verdict.lower() if verdict.lower() in (
        'healthy', 'warning', 'critical') else 'healthy'
    return f'<span class="status {cls}">[{verdict}]</span>'


def write_index_report(output_path, title: str, subtitle: str,
                       findings: list, overview_md: str | None) -> None:
    """Write an index.html cross-module summary.

    Args:
        output_path: index.html absolute path.
        title: page title (e.g. "pg_statviz overview").
        subtitle: e.g. "host:port".
        findings: list of {'module_html', 'title', 'verdict', 'summary'}.
        overview_md: optional LLM-generated synthesis paragraph (markdown).

    Never raises.
    """
    items = []
    for f in findings:
        href = html.escape(f['module_html'])
        ftitle = html.escape(f['title'])
        badge = _verdict_badge(f['verdict'])
        snippet = html.escape(f['summary'])
        items.append(
            f'    <li>{badge}<a href="{href}">{ftitle}</a>'
            f' &mdash; <span class="snippet">{snippet}</span></li>')
    list_html = ('<ul class="module-list">\n'
                 + '\n'.join(items) + '\n  </ul>') if items else (
                     '<p class="missing">No module reports found.</p>')
    overview_html = (f'<div class="summary">{md_to_html(overview_md)}</div>'
                     if overview_md else '')
    esc_title = html.escape(title)
    esc_subtitle = html.escape(subtitle)
    doc = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>{esc_title}</title>
<style>{_CSS}</style>
</head>
<body>
<header>
  <h1>{esc_title}</h1>
  <div class="subtitle">{esc_subtitle}</div>
</header>
{overview_html}
<section>
  <h2>Per-module findings</h2>
  {list_html}
</section>
<footer>Generated by pg_statviz</footer>
</body>
</html>
"""
    try:
        Path(output_path).write_text(doc, encoding='utf-8')
        _logger.info(f"Index report saved to {output_path}")
    except OSError as e:
        _logger.error(f"Could not write {output_path}: {e}")


def finalize_index_report(outputdir, info, port, ai) -> None:
    """Scan per-module HTMLs, optionally call the LLM for an overview,
    and write index.html. Called once at the end of `analyze`.

    No-op when ai is None (no per-module reports were generated).
    """
    if not ai:
        return
    findings = _scan_module_reports(outputdir, info, port,
                                    exclude_basenames=('index.html',))
    if not findings:
        return
    # Lazy import to avoid circular dep with libs.ai (which imports nothing
    # from html_report, but keep the import here for clarity).
    from pg_statviz.libs.ai import analyze_overview
    overview_md = analyze_overview(findings, info=info, mode=ai)
    out_path = f"{_output_prefix(outputdir, info, port)}index.html"
    write_index_report(
        out_path,
        title="pg_statviz · overview",
        subtitle=f"{info['hostname']}:{port}",
        findings=findings,
        overview_md=overview_md,
    )
