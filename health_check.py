"""
A data integrity and system health monitoring script for the Vectis pipeline.

It connects to the Supabase database and runs a series of checks, such as verifying
daily data ingestion volume and scanning for data anomalies like "time travel" permits.

Each check is isolated: a Postgres statement timeout (57014) or any other API error in
one check is reported as "could not complete" and does not abort the remaining checks.
"""
import os
import sys
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from postgrest.exceptions import APIError
from supabase import create_client

load_dotenv()

# Windows consoles default to cp1252 and choke on emoji; guard for streams without reconfigure().
for _stream in (sys.stdout, sys.stderr):
    if hasattr(_stream, "reconfigure"):
        try:
            _stream.reconfigure(encoding="utf-8")
        except Exception:
            pass

# Ingestion is considered "flatlined" if no rows were created within this many days.
FRESHNESS_WINDOW_DAYS = 2


def _get_client():
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    if not url or not key:
        raise EnvironmentError("SUPABASE_URL / SUPABASE_KEY not set (check .env).")
    return create_client(url, key)


def _is_timeout(err: Exception) -> bool:
    """True if the error looks like a Postgres statement timeout (SQLSTATE 57014)."""
    code = getattr(err, "code", None)
    return code == "57014" or "57014" in str(err) or "statement timeout" in str(err).lower()


def _guarded(label, fn):
    """Run one check; report timeouts/API errors instead of crashing the whole scan."""
    try:
        fn()
        return True
    except APIError as e:
        if _is_timeout(e):
            print(f"   ⏱️ {label}: check could not complete (timeout, 57014).")
        else:
            print(f"   ⚠️ {label}: check could not complete (API error: {e}).")
    except Exception as e:
        if _is_timeout(e):
            print(f"   ⏱️ {label}: check could not complete (timeout).")
        else:
            print(f"   ⚠️ {label}: check could not complete ({type(e).__name__}: {e}).")
    return False


def check_pulse(supabase):
    """CHECK 1: Did we ingest anything recently? Bounded count, no ORDER BY / large scan."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=FRESHNESS_WINDOW_DAYS)).isoformat()
    # NOTE: head=True intermittently returns HTTP 500 "JSON could not be generated" with the
    # installed postgrest client, so use count='exact' with limit(1) (<=1 row + the count).
    res = supabase.table('permits') \
        .select('id', count='exact') \
        .gte('created_at', cutoff) \
        .limit(1) \
        .execute()
    recent = res.count or 0
    print(f"   - Rows created in last {FRESHNESS_WINDOW_DAYS} day(s): {recent}")

    if recent == 0:
        print(f"   🚨 CRITICAL: Flatline Alert. No rows ingested since {cutoff[:10]}.")
    else:
        print("   ✅ Pulse: Active (recent data ingested)")


def check_time_travel(supabase):
    """CHECK 2: Did any negative processing_days slip through? Existence check only."""
    res = supabase.table('permits') \
        .select('id') \
        .lt('processing_days', 0) \
        .limit(5) \
        .execute()

    if len(res.data) > 0:
        print("   ❌ FAILURE: Found records with negative duration.")
    else:
        print("   ✅ Temporal Logic: Clean")


def check_imposters(supabase):
    """CHECK 3: 'Model Home' records leaking into a non-Residential tier."""
    res = supabase.table('permits') \
        .select('id') \
        .ilike('description', '%Model Home%') \
        .neq('complexity_tier', 'Residential') \
        .limit(5) \
        .execute()

    if len(res.data) > 0:
        print("   ⚠️ WARNING: 'Model Home' records found in wrong tier.")
    else:
        print("   ✅ Imposter Protocol: Clean")


def run_health_scan():
    print("🏥 Initiating Vectis Health Scan...")
    supabase = _get_client()

    _guarded("Pulse", lambda: check_pulse(supabase))
    _guarded("Temporal Logic", lambda: check_time_travel(supabase))
    _guarded("Imposter Protocol", lambda: check_imposters(supabase))

    print("🏁 Health Scan Complete.")


if __name__ == "__main__":
    run_health_scan()
