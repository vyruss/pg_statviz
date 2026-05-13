import os
import tempfile
from pg_statviz.libs.html_report import (
    md_to_html, finalize_module_report, write_module_report)


def test_md_to_html_healthy_badge():
    assert ('<span class="status healthy">[HEALTHY]</span>'
            in md_to_html("**[HEALTHY]**"))


def test_md_to_html_warning_badge():
    assert ('<span class="status warning">[WARNING]</span>'
            in md_to_html("**[WARNING]**"))


def test_md_to_html_bold_and_italic():
    h = md_to_html("Some **bold** and *italic* text.")
    assert "<strong>bold</strong>" in h
    assert "<em>italic</em>" in h


def test_md_to_html_inline_code():
    assert "<code>shared_buffers</code>" in md_to_html("Set `shared_buffers`.")


def test_md_to_html_headings():
    h = md_to_html("# Title\n\n## Subtitle\n\n### Level three")
    assert "<h1>Title</h1>" in h
    assert "<h2>Subtitle</h2>" in h
    assert "<h3>Level three</h3>" in h


def test_md_to_html_unordered_list():
    h = md_to_html("- first\n- second\n- third")
    assert "<ul>" in h
    assert "<li>first</li>" in h
    assert "<li>second</li>" in h
    assert "<li>third</li>" in h
    assert "</ul>" in h


def test_md_to_html_ordered_list():
    h = md_to_html("1. first\n2. second\n3. third")
    assert "<ol>" in h
    assert "<li>first</li>" in h
    assert "</ol>" in h


def test_md_to_html_paragraphs():
    h = md_to_html("First paragraph.\n\nSecond paragraph.")
    assert "<p>First paragraph.</p>" in h
    assert "<p>Second paragraph.</p>" in h


def test_md_to_html_html_escaping():
    # Raw HTML special chars must be escaped, not passed through.
    h = md_to_html("a < b and x & y")
    assert "&lt;" in h
    assert "&amp;" in h
    assert "<b>" not in h  # would be a raw HTML tag if not escaped


def test_md_to_html_empty_input():
    assert md_to_html("") == ""
    assert md_to_html(None) == ""


def test_write_module_report_creates_file_with_sections():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "test.html")
        write_module_report(
            path, title="t", subtitle="s",
            sections=[
                {'title': 'A', 'image_basename': 'a.png',
                 'analysis_md': '**[HEALTHY]** ok'},
                {'title': 'B', 'image_basename': 'b.png',
                 'analysis_md': None},
            ],
        )
        assert os.path.exists(path)
        html = open(path, encoding='utf-8').read()
        assert '<!DOCTYPE html>' in html
        assert '<img src="a.png"' in html
        assert '<img src="b.png"' in html
        assert 'class="status healthy"' in html
        assert 'class="missing"' in html


def test_write_module_report_noop_on_empty_sections():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "test.html")
        write_module_report(path, title="t", subtitle="s", sections=[])
        assert not os.path.exists(path)


def test_finalize_module_report_builds_expected_path():
    with tempfile.TemporaryDirectory() as d:
        finalize_module_report(
            d, {'hostname': 'localhost'}, '5432', 'buf',
            sections=[{'title': 'A', 'image_basename': 'a.png',
                       'analysis_md': 'ok'}],
        )
        # File must land at <outdir>/pg_statviz_<host>_<port>_<module>.html
        expected = os.path.join(
            d, 'pg_statviz_localhost_5432_buf.html')
        assert os.path.exists(expected)


def test_finalize_module_report_handles_socket_dir_hostname():
    # Hostnames containing '/' (socket dirs) get their slashes normalised
    # so the output file is filesystem-safe.
    with tempfile.TemporaryDirectory() as d:
        finalize_module_report(
            d, {'hostname': '/var/run/postgresql'}, '5432', 'buf',
            sections=[{'title': 'A', 'image_basename': 'a.png',
                       'analysis_md': 'ok'}],
        )
        expected = os.path.join(
            d, 'pg_statviz_-var-run-postgresql_5432_buf.html')
        assert os.path.exists(expected)


def test_finalize_module_report_noop_on_empty_sections():
    with tempfile.TemporaryDirectory() as d:
        finalize_module_report(
            d, {'hostname': 'localhost'}, '5432', 'buf', sections=[])
        assert os.listdir(d) == []
