"""
pg_statviz - stats visualization and time series analysis

AI analysis backend. Provides three provider adapters (Claude / Gemini / local
Ollama) behind a single synchronous entry point, plus a module-facing helper
that owns the per-chart ceremony so leaf modules stay focused on charts.
"""

__author__ = "Jimmy Angelakos"
__copyright__ = "Copyright (c) 2026 Jimmy Angelakos"
__license__ = "PostgreSQL License"

import base64
import logging
import os
import time
from contextlib import contextmanager
from pathlib import Path
import pandas as pd

logging.basicConfig()
_logger = logging.getLogger(__name__)
_logger.setLevel(logging.INFO)


# --- Optional SDK imports --------------------------------------------------
# Each provider is independent; a missing SDK only disables that one provider.

try:
    import anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False

try:
    from google import genai as google_genai
    from google.genai import types as google_genai_types
    GOOGLE_GENAI_AVAILABLE = True
except ImportError:
    GOOGLE_GENAI_AVAILABLE = False

try:
    import ollama
    OLLAMA_AVAILABLE = True
except ImportError:
    OLLAMA_AVAILABLE = False


# --- Defaults --------------------------------------------------------------
# All three default models must be vision-capable so they can read the chart
# PNGs alongside the textual data summary.
CLAUDE_MODEL = "claude-sonnet-4-6"
# latest free-tier-eligible Gemini (Apr 2026)
GEMINI_MODEL = "gemini-2.5-flash"
# Gemma 4 E4B: Google's current small vision-capable open model. ~4.5B
# effective params, runs in ~6 GB VRAM, strong on charts/OCR -- ideal for
# reading pg_statviz's matplotlib PNGs on a laptop GPU.
OLLAMA_MODEL = "gemma4:e4b"

# Selectable provider keys exposed on the CLI as `--ai [PROVIDER]`.
# Imported by every module so the argparse choices list stays in lockstep
# with the registry.
AI_PROVIDERS = ('claude', 'gemini', 'local')
DEFAULT_AI_PROVIDER = 'claude'

ANTHROPIC_INSTALL_GUIDE = """
AI analysis with --ai claude (default) requires the Anthropic Python SDK and
an API key. Setup:

1. Install the AI extras:
   pip install pg_statviz[ai]
   (or: pip install anthropic)

2. Get an API key at https://console.anthropic.com/
   (Anthropic offers free tier credits for new accounts.)

3. Export it before running pg_statviz:
   export ANTHROPIC_API_KEY=sk-ant-...
"""

GEMINI_INSTALL_GUIDE = """
AI analysis with --ai gemini requires the google-genai Python SDK and a
Google AI Studio API key (free tier). Setup:

1. Install the AI extras:
   pip install pg_statviz[ai]
   (or: pip install google-genai)

2. Get an API key at https://aistudio.google.com/apikey
   (Free tier covers gemini-2.5-flash and other 2.5-series models.)

3. Export it before running pg_statviz:
   export GOOGLE_API_KEY=...
"""

OLLAMA_INSTALL_GUIDE = f"""
AI analysis with --ai local requires Ollama running locally with a vision-
capable model. Setup:

1. Install Ollama:
   - Linux:   curl -fsSL https://ollama.com/install.sh | sh
   - macOS:   brew install ollama
   - Windows: Download from https://ollama.com/download

2. Start the Ollama server:
   ollama serve

3. Pull a vision-capable model (default: {OLLAMA_MODEL}):
   ollama pull {OLLAMA_MODEL}

4. Install Python client:
   pip install pg_statviz[ai]
   (or: pip install ollama)
"""

SYSTEM_PROMPT = """You are a Senior PostgreSQL DBA reviewing pg_statviz output.

You will receive, per module:
- A short metric description telling you what the data means and what
  thresholds (if any) deserve a [WARNING].
- A textual statistical summary of the time-series data.
- One or more chart images (PNG) that visualize the same data.

Use BOTH the data and the chart together to form your judgement -- the chart
shows you trends, outliers and patterns that aggregate stats can hide.

Your output MUST be:
1. A status tag on its own line: **[HEALTHY]** or **[WARNING]**.
2. Two to three sentences interpreting the data for a PostgreSQL administrator.
3. Focus on resource saturation, contention, or performance implications.

Rules:
- CUMULATIVE COUNTERS: If the metric description mentions "cumulative counter",
  rising values/peaks are NORMAL. Never warn about cumulative counter growth.
- POINT-IN-TIME: Values <1.0 are essentially zero (fractional sample averages).
- THRESHOLD REQUIRED: Only warn if the metric description specifies a threshold
  AND the data exceeds it.
- If the metric description says "IGNORE" or "Do NOT warn" about something, do
  not mention it.
- Default to [HEALTHY] unless a specific threshold is clearly violated.
"""


# --- Shared helpers --------------------------------------------------------

def _build_user_text(module_name: str, metric_description: str,
                     df: pd.DataFrame) -> str:
    """Build the textual half of the prompt (data summary + trend)."""
    numeric_df = df.select_dtypes(include=['number'])
    if not numeric_df.empty:
        summary = numeric_df.describe(
            percentiles=[0.50, 0.95]
        ).to_string(float_format="{:.2f}".format)
    else:
        # For non-numeric data (like config changes), show the raw data
        summary = df.to_string(index=False, max_rows=20)

    trend_info = []
    for col in numeric_df.columns:
        try:
            col_data = numeric_df[col].dropna()
            if len(col_data) >= 2:
                first_val = col_data.iloc[0]
                last_val = col_data.iloc[-1]
                trend_info.append(
                    f"  {col}: {first_val:.2f} -> {last_val:.2f}")
        except Exception:
            pass
    trend_summary = "\n".join(trend_info) if trend_info else "N/A"

    return f"""### Module
{module_name}

### Metric Context
{metric_description}

### Data Summary
{summary}

### Trend (first -> last value)
{trend_summary}
"""


def _read_images(image_paths) -> list[bytes]:
    """Read PNGs from disk as raw bytes. Missing files are skipped with a
    warning. Callers that need base64 (Anthropic's inline-image contract)
    encode at the one call site that cares."""
    images = []
    for p in image_paths or []:
        try:
            images.append(Path(p).read_bytes())
        except Exception as e:
            _logger.warning(f"Could not read image {p}: {e}")
    return images


@contextmanager
def _timed(label: str):
    """Time a block and log its duration at INFO. Replaces the start/elapsed
    boilerplate repeated in each provider adapter."""
    start = time.time()
    try:
        yield
    finally:
        _logger.info(f"AI analysis ({label}) completed "
                     f"in {time.time() - start:.1f}s")


def _log_provider_error(label: str, env_var_hint: str, e: Exception) -> None:
    """Categorise a cloud-provider API error and log a user-actionable line.

    Handles the auth / rate-limit / generic cases shared by Claude and Gemini.
    Ollama has its own specific error taxonomy and does not use this helper.
    """
    err = str(e).lower()
    if any(t in err for t in ("api_key", "api key", "authentication",
                              "unauthenticated", "permission_denied",
                              "401", "403")):
        hint = f" Check {env_var_hint}." if env_var_hint else ""
        _logger.error(f"{label} API authentication failed.{hint}")
    elif any(t in err for t in ("rate", "quota", "credit",
                                "resource_exhausted", "429")):
        _logger.error(f"{label} API limit reached (free tier?): {e}")
    else:
        _logger.error(f"AI analysis ({label}) failed: {e}")


# --- Provider adapters -----------------------------------------------------

def _analyze_claude(df: pd.DataFrame, module_name: str,
                    metric_description: str,
                    image_paths) -> str | None:
    """Run analysis via the Anthropic Claude API."""
    if not ANTHROPIC_AVAILABLE:
        _logger.warning("anthropic package not installed."
                        + ANTHROPIC_INSTALL_GUIDE)
        return None
    if not os.environ.get("ANTHROPIC_API_KEY"):
        _logger.error("ANTHROPIC_API_KEY env var is not set."
                      + ANTHROPIC_INSTALL_GUIDE)
        return None

    user_text = _build_user_text(module_name, metric_description, df)

    # Images first then text: Claude weights later content more strongly and
    # we want the textual instructions to lead the analysis.
    content = []
    for img in _read_images(image_paths):
        content.append({
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": "image/png",
                "data": base64.standard_b64encode(img).decode("ascii"),
            },
        })
    content.append({"type": "text", "text": user_text})

    try:
        with _timed("Claude"):
            response = anthropic.Anthropic().messages.create(
                model=CLAUDE_MODEL,
                max_tokens=16384,
                # Cache the static system prompt so repeated module calls
                # within a 5-minute window pay only once for the system
                # tokens -- keeps the free tier comfortable across analyze.
                system=[{
                    "type": "text",
                    "text": SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }],
                messages=[{"role": "user", "content": content}],
            )
        return "".join(b.text for b in response.content
                       if getattr(b, "type", None) == "text")
    except Exception as e:
        _log_provider_error("Claude", "ANTHROPIC_API_KEY", e)
        return None


def _analyze_gemini(df: pd.DataFrame, module_name: str,
                    metric_description: str,
                    image_paths) -> str | None:
    """Run analysis via the Google Gemini API (AI Studio free tier)."""
    if not GOOGLE_GENAI_AVAILABLE:
        _logger.warning("google-genai package not installed."
                        + GEMINI_INSTALL_GUIDE)
        return None
    # google-genai accepts GOOGLE_API_KEY (preferred) or GEMINI_API_KEY.
    if not (os.environ.get("GOOGLE_API_KEY")
            or os.environ.get("GEMINI_API_KEY")):
        _logger.error("GOOGLE_API_KEY (or GEMINI_API_KEY) env var is not set."
                      + GEMINI_INSTALL_GUIDE)
        return None

    user_text = _build_user_text(module_name, metric_description, df)
    # Same content ordering rationale as Claude: images then text.
    parts = [google_genai_types.Part.from_bytes(data=img,
                                                mime_type='image/png')
             for img in _read_images(image_paths)]
    parts.append(google_genai_types.Part.from_text(text=user_text))

    try:
        client = google_genai.Client()
        with _timed("Gemini"):
            response = client.models.generate_content(
                model=GEMINI_MODEL,
                contents=parts,
                config=google_genai_types.GenerateContentConfig(
                    system_instruction=SYSTEM_PROMPT,
                ),
            )
        return response.text
    except Exception as e:
        _log_provider_error("Gemini", "GOOGLE_API_KEY", e)
        return None


def _analyze_local(df: pd.DataFrame, module_name: str,
                   metric_description: str,
                   image_paths) -> str | None:
    """Run analysis via local Ollama with a vision-capable model."""
    if not OLLAMA_AVAILABLE:
        _logger.warning("ollama package not installed." + OLLAMA_INSTALL_GUIDE)
        return None

    # Ollama takes a single-string prompt + a separate `images` field rather
    # than Anthropic-style content blocks, so concatenate system + user.
    prompt = SYSTEM_PROMPT + "\n\n" + _build_user_text(
        module_name, metric_description, df)
    # The SDK accepts file paths directly and base64-encodes them internally.
    valid_images = [str(p) for p in (image_paths or []) if Path(p).is_file()]

    message = {"role": "user", "content": prompt}
    if valid_images:
        message["images"] = valid_images

    try:
        with _timed("local Ollama"):
            response = ollama.chat(model=OLLAMA_MODEL, messages=[message])
        return response['message']['content']
    except Exception as e:
        err = str(e).lower()
        # Ollama has its own specific error patterns, distinct from the
        # cloud-provider auth/rate-limit taxonomy handled by
        # _log_provider_error.
        if 'model' in err and 'not found' in err:
            _logger.error(f"Ollama model {OLLAMA_MODEL} not found. "
                          f"Run: ollama pull {OLLAMA_MODEL}")
        elif 'connection' in err or 'refused' in err:
            _logger.error("Cannot connect to Ollama server. "
                          "Is it running? Try: ollama serve")
        else:
            _log_provider_error("local Ollama", "", e)
        return None


# --- Provider registry + public API ---------------------------------------
# Single source of truth for provider dispatch. Add a new provider by adding
# its adapter above and one row here.
_PROVIDERS = {
    'claude': {
        'fn': _analyze_claude,
        'available': lambda: ANTHROPIC_AVAILABLE,
        'install_guide': ANTHROPIC_INSTALL_GUIDE,
        'sdk_pkg': 'anthropic',
        'label': 'Claude',
    },
    'gemini': {
        'fn': _analyze_gemini,
        'available': lambda: GOOGLE_GENAI_AVAILABLE,
        'install_guide': GEMINI_INSTALL_GUIDE,
        'sdk_pkg': 'google-genai',
        'label': 'Gemini',
    },
    'local': {
        'fn': _analyze_local,
        'available': lambda: OLLAMA_AVAILABLE,
        'install_guide': OLLAMA_INSTALL_GUIDE,
        'sdk_pkg': 'ollama',
        'label': 'local Ollama',
    },
}


def analyze_stats(df: pd.DataFrame, module_name: str,
                  metric_description: str = "",
                  image_paths=None,
                  mode: str = DEFAULT_AI_PROVIDER) -> str | None:
    """
    Analyze DataFrame statistics (and optional chart images) with an LLM.

    Args:
        df: DataFrame with the time-series data.
        module_name: Name of the module/chart for context.
        metric_description: Description of what the metrics represent.
        image_paths: Iterable of PNG paths to send alongside the data.
        mode: Provider key -- one of AI_PROVIDERS.

    Returns the LLM's markdown response, or None on any failure.
    Never raises -- every error path returns None and logs a clear message.
    """
    provider = _PROVIDERS.get(mode)
    if provider is None:
        _logger.error(f"Unknown AI provider '{mode}'. "
                      f"Choose one of: {', '.join(AI_PROVIDERS)}.")
        return None
    if not provider['available']():
        _logger.warning(f"{provider['sdk_pkg']} package not installed."
                        + provider['install_guide'])
        return None
    _logger.info(f"Starting AI analysis ({provider['label']}) "
                 f"for {module_name}...")
    try:
        return provider['fn'](df, module_name, metric_description, image_paths)
    except Exception as e:
        # Defence in depth: each adapter already catches; this guarantees the
        # return-None contract holds even if a future adapter forgets to.
        _logger.error(f"AI analysis ({provider['label']}) crashed: {e}")
        return None


def run_chart_analysis(report_sections: list, ai, df: pd.DataFrame,
                       title: str, metric_description: str,
                       outfile: str) -> None:
    """Run the AI analysis for one chart and append a section dict to
    report_sections. No-op when ai is None.

    This is the sole AI entry point for leaf modules -- it bundles the
    per-chart ceremony (call the provider, stash title / image basename /
    markdown) so modules stay focused on chart generation.

    Args:
        report_sections: mutable list the module uses to accumulate sections.
        ai: provider key (e.g. 'claude') or None if --ai is disabled.
        df: DataFrame to analyse.
        title: Human-readable chart title (becomes <h2> in the HTML report).
        metric_description: Per-chart context sent to the LLM.
        outfile: Absolute path of the saved PNG (basename is embedded in the
            HTML as <img src="..."> so the report loads it from the same dir).
    """
    if not ai:
        return
    md = analyze_stats(df, title, metric_description,
                       image_paths=[outfile], mode=ai)
    report_sections.append({
        'title': title,
        'image_basename': os.path.basename(outfile),
        'analysis_md': md,
    })
