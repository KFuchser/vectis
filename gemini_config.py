"""
Shared Gemini configuration and error classification for the Vectis pipeline.

Single source of truth for the model id so the next Google model retirement is a
one-line change (or just a `GEMINI_MODEL` env var / GitHub Actions variable).

Also provides `is_fatal_ai_error`, which separates "the model/key/billing is
fundamentally broken" (retrying cannot help -> fail the run loudly) from "one
batch hiccuped" (rate limit, timeout, 5xx -> retry/continue).
"""
import os

# `or` (not a getenv default) so an empty GEMINI_MODEL="" in CI falls back too.
DEFAULT_GEMINI_MODEL = "gemini-3.8-flash"
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
