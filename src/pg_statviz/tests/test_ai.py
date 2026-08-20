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
    for k in ('ANTHROPIC_API_KEY', 'GOOGLE_API_KEY', 'GEMINI_API_KEY',
              'OPENAI_API_KEY'):
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
        def _fn(df, module_name, metric_description, image_paths,
                info=None, settings=None, findings=None):
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
    monkeypatch.setitem(ai._PROVIDERS, 'openai', {
        **ai._PROVIDERS['openai'], 'fn': fake_adapter('openai'),
        'available': lambda: True,
    })
    monkeypatch.setitem(ai._PROVIDERS, 'local', {
        **ai._PROVIDERS['local'], 'fn': fake_adapter('local'),
        'available': lambda: True,
    })

    assert ai.analyze_stats(tiny_df, "M", mode='claude') == "claude:ok"
    assert ai.analyze_stats(tiny_df, "M", mode='gemini') == "gemini:ok"
    assert ai.analyze_stats(tiny_df, "M", mode='openai') == "openai:ok"
    assert ai.analyze_stats(tiny_df, "M", mode='local') == "local:ok"
    assert calls == ['claude', 'gemini', 'openai', 'local']


def test_analyze_stats_adapter_crash_returns_none(tiny_df, monkeypatch):
    # Defence-in-depth: if a future adapter forgets to catch its own
    # exceptions, analyze_stats still returns None (never raises).
    def exploding(df, module_name, metric_description, image_paths,
                  info=None, settings=None, findings=None):
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


# --- OpenAI-compatible provider -------------------------------------------
# Hand-rolled stand-ins for the openai SDK surface we touch. Keeping them
# local to the test file means no mock framework and no SDK install needed.

class MockOpenAIMessage:
    def __init__(self, content):
        self.content = content


class MockOpenAIChoice:
    def __init__(self, content):
        self.message = MockOpenAIMessage(content)


class MockOpenAIResponse:
    def __init__(self, content):
        self.choices = [MockOpenAIChoice(content)]


class MockOpenAICompletions:
    def __init__(self, parent):
        self.parent = parent

    def create(self, **kwargs):
        self.parent.calls.append(kwargs)
        if self.parent.raises:
            raise self.parent.raises
        return MockOpenAIResponse("**[HEALTHY]** all good")


class MockOpenAIClient:
    """Mimics openai.OpenAI(): client.chat.completions.create(...)."""

    def __init__(self, raises=None, **init_kwargs):
        self.calls = []
        self.init_kwargs = init_kwargs
        self.raises = raises
        self.chat = type('chat', (), {})()
        self.chat.completions = MockOpenAICompletions(self)


@pytest.fixture
def openai_env(monkeypatch):
    """Pretend the SDK is installed and a key is set."""
    monkeypatch.setattr(ai, 'OPENAI_AVAILABLE', True)
    monkeypatch.setenv('OPENAI_API_KEY', 'sk-test')
    monkeypatch.delenv('OPENAI_BASE_URL', raising=False)
    monkeypatch.delenv('OPENAI_MODEL', raising=False)


@pytest.fixture
def openai_client(monkeypatch, openai_env):
    """Install a mock client factory and hand the instance back."""
    holder = {}

    def factory(**kwargs):
        holder['client'] = MockOpenAIClient(**kwargs)
        return holder['client']

    monkeypatch.setattr(ai, '_openai_client', factory)
    return holder


def test_openai_is_a_registered_provider():
    assert 'openai' in ai.AI_PROVIDERS
    assert 'openai' in ai._PROVIDERS


def test_chat_providers_match_analysis_providers():
    # Overview synthesis must cover exactly the same provider set as
    # per-chart analysis, or --ai <p> would work per module and silently
    # skip the executive summary.
    assert set(ai._CHAT_PROVIDERS) == set(ai._PROVIDERS)


def test_analyze_openai_returns_none_without_sdk(tiny_df, monkeypatch):
    monkeypatch.setattr(ai, 'OPENAI_AVAILABLE', False)
    assert ai._analyze_openai(tiny_df, "M", "desc", None) is None


def test_analyze_openai_returns_none_without_key(tiny_df, monkeypatch):
    monkeypatch.setattr(ai, 'OPENAI_AVAILABLE', True)
    monkeypatch.delenv('OPENAI_API_KEY', raising=False)
    assert ai._analyze_openai(tiny_df, "M", "desc", None) is None


def test_analyze_openai_returns_completion_text(tiny_df, openai_client):
    out = ai._analyze_openai(tiny_df, "M", "desc", None)
    assert out == "**[HEALTHY]** all good"


def test_analyze_openai_uses_default_model(tiny_df, openai_client):
    ai._analyze_openai(tiny_df, "M", "desc", None)
    assert openai_client['client'].calls[0]['model'] == ai.OPENAI_MODEL


def test_analyze_openai_model_env_override(tiny_df, openai_client,
                                           monkeypatch):
    # OpenAI-compatible servers each expose their own model names, so the
    # model must be overridable without a code change.
    monkeypatch.setenv('OPENAI_MODEL', 'my-local-vlm')
    ai._analyze_openai(tiny_df, "M", "desc", None)
    assert openai_client['client'].calls[0]['model'] == 'my-local-vlm'


def test_analyze_openai_sends_system_and_user_messages(tiny_df,
                                                       openai_client):
    ai._analyze_openai(tiny_df, "ModName", "metric desc", None)
    messages = openai_client['client'].calls[0]['messages']
    assert messages[0]['role'] == 'system'
    assert 'PostgreSQL' in messages[0]['content']
    assert messages[1]['role'] == 'user'
    text_parts = [p['text'] for p in messages[1]['content']
                  if p['type'] == 'text']
    assert any('ModName' in t and 'metric desc' in t for t in text_parts)


def test_analyze_openai_embeds_images_as_data_urls(tiny_df, openai_client,
                                                   tmp_path):
    p = tmp_path / "chart.png"
    p.write_bytes(b"\x89PNG\r\nfake")
    ai._analyze_openai(tiny_df, "M", "desc", [str(p)])
    content = openai_client['client'].calls[0]['messages'][1]['content']
    images = [c for c in content if c['type'] == 'image_url']
    assert len(images) == 1
    assert images[0]['image_url']['url'].startswith(
        "data:image/png;base64,")
    # Images lead, text trails: same ordering rationale as the other
    # providers.
    assert content[-1]['type'] == 'text'


def test_analyze_openai_returns_none_on_api_error(tiny_df, monkeypatch,
                                                  openai_env):
    def factory(**kwargs):
        return MockOpenAIClient(raises=Exception("401 authentication failed"))

    monkeypatch.setattr(ai, '_openai_client', factory)
    assert ai._analyze_openai(tiny_df, "M", "desc", None) is None


def test_chat_openai_returns_text(openai_client):
    assert ai._chat_openai("sys", "user") == "**[HEALTHY]** all good"


def test_chat_openai_returns_none_without_key(monkeypatch):
    monkeypatch.setattr(ai, 'OPENAI_AVAILABLE', True)
    monkeypatch.delenv('OPENAI_API_KEY', raising=False)
    assert ai._chat_openai("sys", "user") is None


def test_analyze_overview_dispatches_to_openai(openai_client):
    out = ai.analyze_overview(
        [{'title': 'T', 'verdict': 'HEALTHY', 'summary': 's'}],
        mode='openai')
    assert out == "**[HEALTHY]** all good"
