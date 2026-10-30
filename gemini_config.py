"""
Shared Gemini configuration, cost controls and error classification for the Vectis pipeline.

Single source of truth for the model id so the next Google model retirement is a
one-line change (or just a `GEMINI_MODEL` env var / GitHub Actions variable).

Also provides:
- `is_fatal_ai_error`, which separates "the model/key/billing is fundamentally broken"
  (retrying cannot help -> fail the run loudly) from "one batch hiccuped" (rate limit,
  timeout, 5xx -> retry/continue).
- `classification_config()`, the single builder of the per-call GenerateContentConfig
  (JSON output, thinking OFF, output capped).
- `UsageTracker` / `check_projected_cost` / `MAX_RUN_COST_USD`, the per-run cost metering
  and spend guard.
"""
import os

# `or` (not a getenv default) so an empty GEMINI_MODEL="" in CI falls back too.
#
# Model choice (2026-10 decision): gemini-2.5-flash-lite (the cheapest) was retired
# 2026-10-16 -- the same retirement trap that caused our outage. gemini-3.8-flash is the
# priciest tier and is set to roughly double around 2027-01-01. gemini-3.1-flash-lite is
# the cheapest option with real runway.
# TODO(operator): the exact id string "gemini-3.1-flash-lite" is UNVERIFIED against the live
# API (no Gemini calls were allowed when this was set). Confirm it with check_models.py
# during the instrumented test BEFORE any bulk run. Override anytime via GEMINI_MODEL.
DEFAULT_GEMINI_MODEL = "gemini-3.1-flash-lite"
GEMINI_MODEL = os.getenv("GEMINI_MODEL") or DEFAULT_GEMINI_MODEL

# HTTP statuses where retrying the same request can never succeed:
#   401/403 bad or restricted API key, 404 model retired/not found,
#   402 prepaid billing credits depleted.
FATAL_STATUS_CODES = frozenset({401, 402, 403, 404})


class AIClassificationError(RuntimeError):
    """Raised when AI classification is fundamentally broken and the run must fail."""


def _status_code(exc: Exception):
    """Best-effort HTTP status from a google-genai APIError (or similar)."""
    for attr in ("code", "status_code"):
        value = getattr(exc, attr, None)
        if isinstance(value, int):
            return value
    return None


def is_fatal_ai_error(exc: Exception) -> bool:
    """
    True if `exc` means the model / key / billing is broken (not a transient hiccup).

    Rate limits (429), timeouts and 5xx are NOT fatal. A 400 is fatal only when it is
    Google's "API key not valid" response.
    """
    code = _status_code(exc)
    if code in FATAL_STATUS_CODES:
        return True
    text = str(exc).lower()
    if code == 400 and ("api key" in text or "api_key_invalid" in text):
        return True
    # Fallbacks for exceptions that don't expose a status code.
    return "no longer available" in text or "permission_denied" in text


def fatal_ai_banner(context: str, exc: Exception) -> str:
    """Multi-line, hard-to-miss error text (also emits a GitHub Actions annotation)."""
    bar = "!" * 72
    return (
        f"\n{bar}\n"
        f"FATAL: Gemini is unusable for {context} (model='{GEMINI_MODEL}').\n"
        f"  Error: {exc}\n"
        f"  Likely causes: model retired/renamed (404), bad/restricted key (401/403),\n"
        f"  or billing credits depleted (402). Check GEMINI_MODEL and the API key/billing.\n"
        f"  Failing the run so this is NOT silently written as Unknown/Commodity.\n"
        f"{bar}\n"
        f"::error title=Gemini AI failure::{context} failed with model "
        f"{GEMINI_MODEL}: {str(exc)[:300].replace(chr(10), ' ')}"
    )


# ---------------------------------------------------------------------------
# COST CONTROL: per-call config, usage metering, and run-cost guard
# ---------------------------------------------------------------------------
# Background: an unbounded run (thinking tokens on, no output cap, whole 90-day window
# re-classified daily) once burned ~$20 in a single run. Everything below exists so that
# can never happen blind again.

def _env_float(name: str, default: float) -> float:
    """Float env var; empty/missing/garbage falls back to `default`."""
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    return int(_env_float(name, default))


# Hard ceiling on output tokens per classification call. A 30-record ingest batch needs
# roughly 1.5-2k output tokens; 4096 leaves headroom while bounding a runaway response.
# A truncated response is unparseable JSON and is counted as a failed batch (loud, cheap).
CLASSIFICATION_MAX_OUTPUT_TOKENS = _env_int("GEMINI_MAX_OUTPUT_TOKENS", 4096)

# Thinking budget in tokens. 0 = thinking OFF (what we want for simple classification).
# Some models refuse budget 0 with a 400; a 400 is NOT treated as fatal, so it fails fast via
# the consecutive-failed-batch abort rather than burning money.
CLASSIFICATION_THINKING_BUDGET = _env_int("GEMINI_THINKING_BUDGET", 0)

_thinking_warning_printed = False


def thinking_supported() -> bool:
    """True if the installed google-genai exposes ThinkingConfig(thinking_budget=...)."""
    try:
        from google.genai import types
        cls = getattr(types, "ThinkingConfig", None)
        return cls is not None and "thinking_budget" in getattr(cls, "model_fields", {})
    except Exception:
        return False


def classification_config(**overrides):
    """
    The ONE place a Gemini classification call's GenerateContentConfig is built.

    JSON output, thinking disabled, output capped. Extra GenerateContentConfig kwargs
    (e.g. system_instruction) go in `overrides`.

    Defensive: google-genai 1.3.0 (previously pinned in requirements.txt) has a
    ThinkingConfig WITHOUT `thinking_budget`, so thinking cannot be disabled there. In that
    case we skip it, print a loud one-time warning (thinking tokens are billed as output),
    and never crash. requirements.txt now pins a version that supports it.
    """
    global _thinking_warning_printed
    from google.genai import types

    kwargs = {
        "response_mime_type": "application/json",
        "max_output_tokens": CLASSIFICATION_MAX_OUTPUT_TOKENS,
    }
    if thinking_supported():
        try:
            kwargs["thinking_config"] = types.ThinkingConfig(
                thinking_budget=CLASSIFICATION_THINKING_BUDGET)
        except Exception as e:  # pragma: no cover - defensive
            print(f"WARNING: could not build ThinkingConfig ({e}); thinking NOT disabled.")
    elif not _thinking_warning_printed:
        _thinking_warning_printed = True
        print("WARNING: installed google-genai has no ThinkingConfig.thinking_budget; "
              "thinking tokens CANNOT be disabled and will be billed. Upgrade google-genai. "
              "The measured-cost guard (MAX_RUN_COST_USD) still applies.")
    kwargs.update(overrides)
    return types.GenerateContentConfig(**kwargs)


# --- Price table -----------------------------------------------------------
# Rates for gemini-3.1-flash-lite, USD per 1M tokens: input $0.25, output $1.50.
# TODO(operator): (a) these are UNVERIFIED third-party aggregator figures -- confirm against
# https://ai.google.dev/gemini-api/docs/pricing before trusting any cost number.
# (b) On Gemini, OUTPUT tokens INCLUDE thinking tokens (thinking is billed at the output
# rate) -- which is exactly why classification_config() sets thinking_budget=0.
# If you switch GEMINI_MODEL, override these via GEMINI_PRICE_INPUT_PER_1M /
# GEMINI_PRICE_OUTPUT_PER_1M so the cost guard uses the right numbers.
PRICE_INPUT_PER_1M_USD = _env_float("GEMINI_PRICE_INPUT_PER_1M", 0.25)
PRICE_OUTPUT_PER_1M_USD = _env_float("GEMINI_PRICE_OUTPUT_PER_1M", 1.50)

# Per-run spend ceiling. Enforced on measured spend inside the AI loop (always), and on
# projected spend before the loop (skippable with ALLOW_COST_OVERRIDE=1).
MAX_RUN_COST_USD = _env_float("MAX_RUN_COST_USD", 1.00)


def allow_cost_override() -> bool:
    """ALLOW_COST_OVERRIDE=1 skips the PRE-FLIGHT projection check only (not the in-loop cap)."""
    return os.getenv("ALLOW_COST_OVERRIDE", "").strip() == "1"


def estimate_cost_usd(input_tokens: float, output_tokens: float) -> float:
    """USD for the given token counts at the configured prices."""
    return (input_tokens * PRICE_INPUT_PER_1M_USD
            + output_tokens * PRICE_OUTPUT_PER_1M_USD) / 1_000_000


class CostCapExceeded(AIClassificationError):
    """Projected or measured Gemini spend exceeded MAX_RUN_COST_USD."""


def cost_cap_banner(context: str, detail: str) -> str:
    """Multi-line, hard-to-miss cost-abort text (also a GitHub Actions annotation)."""
    bar = "$" * 72
    return (
        f"\n{bar}\n"
        f"COST GUARD: aborting {context} (model='{GEMINI_MODEL}', cap=${MAX_RUN_COST_USD:.2f}).\n"
        f"  {detail}\n"
        f"  Raise MAX_RUN_COST_USD to allow a bigger run; ALLOW_COST_OVERRIDE=1 skips only the\n"
        f"  pre-flight projection. Prices in gemini_config.py are unverified until confirmed.\n"
        f"{bar}\n"
        f"::error title=Gemini cost guard::{context}: {detail[:300]}"
    )


def check_projected_cost(context: str, num_calls: int,
                         est_input_tokens_per_call: int,
                         est_output_tokens_per_call: int) -> float:
    """
    Pre-flight guard. Projects cost = calls * (tokens/call) * price and raises
    CostCapExceeded (after printing the banner) if it exceeds MAX_RUN_COST_USD, unless
    ALLOW_COST_OVERRIDE=1. Returns the projected USD.
    """
    projected = num_calls * estimate_cost_usd(est_input_tokens_per_call,
                                              est_output_tokens_per_call)
    print(f"   Projected Gemini cost: ~${projected:.4f} for {num_calls} call(s) "
          f"(cap ${MAX_RUN_COST_USD:.2f}).")
    if projected > MAX_RUN_COST_USD:
        if allow_cost_override():
            print("   ALLOW_COST_OVERRIDE=1 set: proceeding past projected-cost cap "
                  "(measured-cost cap still enforced).")
        else:
            detail = (f"projected ${projected:.2f} for {num_calls} call(s) exceeds cap "
                      f"${MAX_RUN_COST_USD:.2f}")
            print(cost_cap_banner(context, detail))
            raise CostCapExceeded(detail)
    return projected


class UsageTracker:
    """Accumulates token usage across a run and enforces the measured-cost cap."""

    def __init__(self, context: str = "gemini run"):
        self.context = context
        self.calls = 0
        self.input_tokens = 0
        self.output_tokens = 0
        self.thinking_tokens = 0
        self.calls_without_usage = 0
        self._fallback_cost = 0.0  # estimated cost for calls whose usage_metadata was None

    def record(self, response, est_input_tokens: int = 0, est_output_tokens: int = 0) -> None:
        """
        Record one successful generate_content response. Never raises. If usage_metadata is
        missing, the call is still counted and charged at the supplied estimate so the cap
        cannot be dodged by absent metadata.
        """
        self.calls += 1
        usage = getattr(response, "usage_metadata", None)
        if usage is None:
            self.calls_without_usage += 1
            self._fallback_cost += estimate_cost_usd(est_input_tokens, est_output_tokens)
            return
        # `or 0` guards None fields; candidates_token_count (google-genai) vs
        # response_token_count (some newer/Vertex shapes).
        self.input_tokens += getattr(usage, "prompt_token_count", None) or 0
        self.output_tokens += (getattr(usage, "candidates_token_count", None)
                               or getattr(usage, "response_token_count", None) or 0)
        self.thinking_tokens += getattr(usage, "thoughts_token_count", None) or 0

    @property
    def cost_usd(self) -> float:
        # Thinking tokens are billed at the output rate.
        return (estimate_cost_usd(self.input_tokens, self.output_tokens + self.thinking_tokens)
                + self._fallback_cost)

    def check_cap(self) -> None:
        """Raise CostCapExceeded (after printing the banner) if measured spend > the cap."""
        if self.cost_usd > MAX_RUN_COST_USD:
            detail = (f"measured ${self.cost_usd:.4f} after {self.calls} call(s) exceeds cap "
                      f"${MAX_RUN_COST_USD:.2f}")
            print(cost_cap_banner(self.context, detail))
            raise CostCapExceeded(detail)

    def summary(self) -> str:
        extra = (f", {self.calls_without_usage} call(s) had no usage_metadata (estimated)"
                 if self.calls_without_usage else "")
        return (f"GEMINI COST SUMMARY [{self.context}, model={GEMINI_MODEL}]: "
                f"{self.calls} call(s), input={self.input_tokens} tok, "
                f"output={self.output_tokens} tok, thinking={self.thinking_tokens} tok, "
                f"est. cost=${self.cost_usd:.4f} (cap ${MAX_RUN_COST_USD:.2f}; "
                f"prices ${PRICE_INPUT_PER_1M_USD}/${PRICE_OUTPUT_PER_1M_USD} per 1M "
                f"in/out, unverified{extra})")
