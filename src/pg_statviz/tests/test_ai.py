import pandas as pd
import pytest
from pg_statviz.libs import ai


def test_ai_providers_matches_registry_keys():
    # The public choices list (consumed by argparse) must stay in lockstep
    # with the provider registry.
    assert set(ai.AI_PROVIDERS) == set(ai._PROVIDERS.keys())


def test_default_ai_provider_is_registered():
    assert ai.DEFAULT_AI_PROVIDER in ai._PROVIDERS


@pytest.fixture
def clean_env(monkeypatch):
    """Remove any AI API keys that may leak in from the shell environment."""
    for k in ('ANTHROPIC_API_KEY', 'GOOGLE_API_KEY', 'GEMINI_API_KEY'):
        monkeypatch.delenv(k, raising=False)


@pytest.fixture
def tiny_df():
    return pd.DataFrame({'x': [1.0, 2.0, 3.0]})


def test_run_chart_analysis_noop_when_ai_none(tiny_df):
    sections = []
    ai.run_chart_analysis(
        sections, None, tiny_df, "NoAI", "desc", outfile="/tmp/fake.png")
    assert sections == []


def test_run_chart_analysis_noop_when_ai_empty_string(tiny_df):
    # Falsy but non-None: treated identically to disabled.
    sections = []
    ai.run_chart_analysis(
        sections, "", tiny_df, "NoAI", "desc", outfile="/tmp/fake.png")
    assert sections == []


def test_run_chart_analysis_appends_section_on_failure(tiny_df, clean_env):
    # With no SDK installed / no API key, the provider adapter returns None;
    # run_chart_analysis must still append a section so the HTML report
    # renders the "AI analysis unavailable" placeholder.
    sections = []
    ai.run_chart_analysis(
        sections, 'claude', tiny_df, "Test Chart", "desc",
        outfile="/tmp/fake_chart.png")
    assert len(sections) == 1
    section = sections[0]
    assert section['title'] == "Test Chart"
    assert section['image_basename'] == "fake_chart.png"
    assert section['analysis_md'] is None


def test_run_chart_analysis_basename_strips_directory(tiny_df, clean_env):
    # The HTML references the PNG by basename (same-dir <img src="...">)
    # regardless of the full path passed in.
    sections = []
    ai.run_chart_analysis(
        sections, 'claude', tiny_df, "X", "desc",
        outfile="/tmp/some/deep/path/pg_statviz_host_5432_buf.png")
    assert sections[0]['image_basename'] == "pg_statviz_host_5432_buf.png"


def test_analyze_stats_unknown_provider_returns_none(tiny_df, caplog):
    assert ai.analyze_stats(
        tiny_df, "M", "desc", mode='bogus') is None


def test_analyze_stats_dispatches_via_registry(tiny_df, monkeypatch,
                                               clean_env):
    # Replace each adapter with a sentinel-returning stub and confirm
    # analyze_stats routes by the mode argument.
    calls = []

    def fake_adapter(label):
        def _fn(df, module_name, metric_description, image_paths):
            calls.append(label)
            return f"{label}:ok"
        return _fn

    monkeypatch.setitem(ai._PROVIDERS, 'claude', {
        **ai._PROVIDERS['claude'], 'fn': fake_adapter('claude'),
        'available': lambda: True,
    })
    monkeypatch.setitem(ai._PROVIDERS, 'gemini', {
        **ai._PROVIDERS['gemini'], 'fn': fake_adapter('gemini'),
        'available': lambda: True,
    })
    monkeypatch.setitem(ai._PROVIDERS, 'local', {
        **ai._PROVIDERS['local'], 'fn': fake_adapter('local'),
        'available': lambda: True,
    })

    assert ai.analyze_stats(tiny_df, "M", mode='claude') == "claude:ok"
    assert ai.analyze_stats(tiny_df, "M", mode='gemini') == "gemini:ok"
    assert ai.analyze_stats(tiny_df, "M", mode='local') == "local:ok"
    assert calls == ['claude', 'gemini', 'local']


def test_analyze_stats_adapter_crash_returns_none(tiny_df, monkeypatch):
    # Defence-in-depth: if a future adapter forgets to catch its own
    # exceptions, analyze_stats still returns None (never raises).
    def exploding(df, module_name, metric_description, image_paths):
        raise RuntimeError("boom")

    monkeypatch.setitem(ai._PROVIDERS, 'claude', {
        **ai._PROVIDERS['claude'], 'fn': exploding,
        'available': lambda: True,
    })
    assert ai.analyze_stats(tiny_df, "M", mode='claude') is None


def test_read_images_returns_bytes(tmp_path):
    p = tmp_path / "img.png"
    p.write_bytes(b"\x89PNG\r\n fake bytes")
    result = ai._read_images([str(p)])
    assert result == [b"\x89PNG\r\n fake bytes"]


def test_read_images_skips_missing(tmp_path, caplog):
    p = tmp_path / "img.png"
    p.write_bytes(b"\x89PNG\r\n fake bytes")
    # One real file, one non-existent — should return just the one, no raise.
    result = ai._read_images([str(p), str(tmp_path / "nope.png")])
    assert result == [b"\x89PNG\r\n fake bytes"]


def test_read_images_handles_empty_and_none():
    assert ai._read_images([]) == []
    assert ai._read_images(None) == []


def test_build_user_text_includes_module_and_metric(tiny_df):
    text = ai._build_user_text("Some Module", "a description", tiny_df)
    assert "Some Module" in text
    assert "a description" in text
    assert "### Data Summary" in text
    assert "### Trend" in text


def test_build_user_text_on_non_numeric_df():
    # For config-changes-style frames the function falls back to raw rows.
    df = pd.DataFrame({'k': ['a', 'b'], 'v': ['1', '2']})
    text = ai._build_user_text("Cfg", "desc", df)
    assert "Cfg" in text
    # Non-numeric fallback produces an N/A trend line.
    assert "N/A" in text


def test_timed_context_always_logs(caplog):
    import logging
    caplog.set_level(logging.INFO, logger='pg_statviz.libs.ai')
    with ai._timed("TestLabel"):
        pass
    assert any("TestLabel" in r.message for r in caplog.records)


def test_timed_context_logs_even_on_exception(caplog):
    import logging
    caplog.set_level(logging.INFO, logger='pg_statviz.libs.ai')
    try:
        with ai._timed("TestLabel"):
            raise RuntimeError("x")
    except RuntimeError:
        pass
    assert any("TestLabel" in r.message for r in caplog.records)


def test_log_provider_error_auth_message(caplog):
    import logging
    caplog.set_level(logging.ERROR, logger='pg_statviz.libs.ai')
    ai._log_provider_error("TestProv", "TEST_KEY",
                           Exception("authentication error: 401"))
    msgs = [r.message for r in caplog.records]
    assert any("authentication" in m.lower() for m in msgs)
    assert any("TEST_KEY" in m for m in msgs)


def test_log_provider_error_rate_limit_message(caplog):
    import logging
    caplog.set_level(logging.ERROR, logger='pg_statviz.libs.ai')
    ai._log_provider_error("TestProv", "TEST_KEY",
                           Exception("429 rate_limit exceeded"))
    assert any("limit" in r.message.lower() for r in caplog.records)


def test_log_provider_error_generic_message(caplog):
    import logging
    caplog.set_level(logging.ERROR, logger='pg_statviz.libs.ai')
    ai._log_provider_error("TestProv", "TEST_KEY",
                           Exception("something unexpected"))
    assert any("TestProv" in r.message and "something unexpected" in r.message
               for r in caplog.records)
