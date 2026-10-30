"""
Vectis Ingestion Orchestrator - PRODUCTION

This script serves as the central entry point for the data pipeline.
It performs the following high-level operations:
1. Fetches permit data from all configured city "spokes" (Austin, San Antonio, Fort Worth, LA,
   Chicago, New York, San Francisco).
2. Normalizes the data into a common `PermitRecord` format.
3. Applies AI-based classification (Gemini; model set by GEMINI_MODEL) to categorize permits
   (Residential vs Commercial) based on description and valuation.
4. Uploads the cleaned data to Supabase in safe batches to avoid timeouts.

Key Configuration:
- Batch Size: 200 records (Strict limit for Supabase stability).
- Lookback Period: 3 days by default (late-posted permits). Override with `--days N` or env
  `INGEST_LOOKBACK_DAYS` (CLI wins). Use e.g. `--days 90` for a manual backfill.

Cost controls (a blind run once spent ~$20): already-classified rows are skipped (looked up
in Supabase), duplicate descriptions are classified once, thinking is off and output is
capped (gemini_config.classification_config), token usage is metered, and the run aborts if
projected/measured spend exceeds MAX_RUN_COST_USD (default $1.00; ALLOW_COST_OVERRIDE=1 skips
only the pre-flight projection). Classified rows are upserted progressively so a mid-run
abort keeps the work already paid for; re-running resumes instead of re-paying.

Failure behavior: if Gemini is fundamentally broken (model retired/404, bad key/403,
billing/402), the cost cap trips, or most AI batches fail, the run aborts and exits
non-zero, so the GitHub Actions run goes red instead of silently writing Unknown/Commodity.
Rows already classified (free pre-filter / skipped / earlier AI batches) stay saved; rows the
AI never classified are NOT written.
"""
import argparse
import os
import json
import re
import sys
import time
from typing import Callable, Dict, List, Optional, Tuple
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime, timedelta
import google.genai as genai

from gemini_config import (
    GEMINI_MODEL, AIClassificationError, is_fatal_ai_error, fatal_ai_banner,
    classification_config, check_projected_cost, UsageTracker, MAX_RUN_COST_USD,
)
from service_models import PermitRecord, ComplexityTier, ProjectCategory
from ingest_austin import get_austin_data
from ingest_san_antonio import get_san_antonio_data
from ingest_fort_worth import get_fort_worth_data
from ingest_la import get_la_data
from ingest_chicago import get_chicago_data
from ingest_new_york import get_new_york_data
from ingest_san_francisco import get_san_francisco_data

load_dotenv()

# CONFIG
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GEMINI_KEY = os.getenv("GOOGLE_API_KEY")
SOCRATA_TOKEN = os.getenv("SOCRATA_APP_TOKEN", None)

_missing = [k for k, v in {"SUPABASE_URL": SUPABASE_URL, "SUPABASE_KEY": SUPABASE_KEY, "GOOGLE_API_KEY": GEMINI_KEY}.items() if not v]
if _missing:
    raise EnvironmentError(f"Missing required environment variables: {', '.join(_missing)}. Check your .env file.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
ai_client = genai.Client(api_key=GEMINI_KEY)

# AI failure policy (see process_and_classify_permits)
AI_MAX_RETRIES = 3                 # attempts per batch for transient errors (429/5xx/timeouts)
AI_RETRY_BACKOFF_SECONDS = (2, 8)  # sleep before attempt 2, 3
AI_MAX_CONSECUTIVE_FAILED_BATCHES = 3
AI_MAX_FAIL_RATE = 0.5             # abort if more than this share of AI-bound records fail
AI_CHUNK_SIZE = 30                 # unique descriptions per Gemini call

# Lookback window. Small by default: the daily run only needs to catch late-posted permits.
DEFAULT_LOOKBACK_DAYS = 3

# Rough per-call token sizes for the PRE-FLIGHT cost projection only (measured usage is what
# the in-loop cap enforces). ~30 items x ~65 input tokens + prompt; ~30 items x ~50 output.
EST_INPUT_TOKENS_PER_CALL = 2500
EST_OUTPUT_TOKENS_PER_CALL = 2000

# Complexity tiers that mean "not classified yet". Anything else already in the DB is kept.
UNCLASSIFIED_TIERS = frozenset({"", "unknown"})
LOOKUP_CHUNK = 100                 # permit_ids per Supabase `in` query

# Per-run Gemini usage meter (reported at end of run, enforces MAX_RUN_COST_USD).
usage = UsageTracker("ingest classification")

# Residential keyword pre-filter. Matched against a normalized description (lowercased,
# hyphens/underscores/slashes/whitespace collapsed to single spaces), so "Single-Family",
# "single  family" and "SINGLE FAMILY" all hit. Word boundaries stop "adu" matching "adult".
RES_KEYWORD_RE = re.compile(
    r"\b(?:single family|sfh|sfr|detached|duplex|town ?house|town ?home|garage|adu)s?\b"
)


def _normalize_description(text: str) -> str:
    """Lowercase and collapse hyphens/underscores/slashes/whitespace to single spaces."""
    return re.sub(r"[\s\-_/]+", " ", (text or "").lower()).strip()


def _generate_with_retry(prompt: str):
    """
    Calls Gemini, retrying transient errors with backoff. Fatal errors (model 404, key 401/403,
    billing 402) are re-raised immediately since retrying cannot help.
    """
    for attempt in range(AI_MAX_RETRIES):
        try:
            response = ai_client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt,
                config=classification_config(),  # JSON, thinking off, output capped
            )
            # Meter every successful call (even if the output later fails to parse).
            usage.record(response, EST_INPUT_TOKENS_PER_CALL, EST_OUTPUT_TOKENS_PER_CALL)
            return response
        except Exception as e:
            if is_fatal_ai_error(e) or attempt == AI_MAX_RETRIES - 1:
                raise
            wait = AI_RETRY_BACKOFF_SECONDS[min(attempt, len(AI_RETRY_BACKOFF_SECONDS) - 1)]
            print(f"   ⏳ Transient AI error ({str(e)[:120]}). Retry {attempt + 1}/"
                  f"{AI_MAX_RETRIES - 1} in {wait}s...")
            time.sleep(wait)


def extract_json_from_text(text: str):
    """
    Extracts a JSON array from a string that may contain other text.

    Args:
        text: The string to search for a JSON array.

    Returns:
        A list parsed from the JSON, or an empty list if no valid JSON is found.
    """
    try:
        match = re.search(r'\[.*\]', text, re.DOTALL)
        if match:
            return json.loads(match.group())
        return json.loads(text)
    except Exception:
        return []


def _map_project_category(category_str: str, tier: ComplexityTier) -> ProjectCategory:
    """Maps the AI's free-form category string to a ProjectCategory enum value."""
    cat = (category_str or "").upper()
    if any(k in cat for k in ("TENANT", " TI", "FINISH OUT", "INTERIOR IMPROVEMENT")):
        return ProjectCategory.COMMERCIAL_TI
    if "TRADE" in cat:
        return ProjectCategory.TRADE_ONLY
    if tier == ComplexityTier.RESIDENTIAL:
        if any(k in cat for k in ("NEW", "CONSTRUCTION", "BUILD")):
            return ProjectCategory.RESIDENTIAL_NEW
        return ProjectCategory.RESIDENTIAL_ALTERATION
    if tier == ComplexityTier.COMMERCIAL:
        if any(k in cat for k in ("NEW", "CONSTRUCTION", "BUILD")):
            return ProjectCategory.COMMERCIAL_NEW
        return ProjectCategory.COMMERCIAL_TI
    return ProjectCategory.UNKNOWN


def fetch_existing_tiers(records: List[PermitRecord], client=None) -> Dict[Tuple[str, str], dict]:
    """
    Looks up what Supabase already stores for these records, as
    {(city, permit_id): {complexity_tier, project_category, ai_rationale}}.

    Used to avoid re-paying Gemini for rows classified on a previous run. Queries are scoped
    to the exact (city, permit_id) pairs in play, in chunks. A failed lookup chunk is logged
    and treated as "nothing known" (those rows go to the AI); the pre-flight cost guard still
    bounds the damage.
    """
    client = client or supabase
    by_city: Dict[str, List[str]] = {}
    for r in records:
        by_city.setdefault(r.city, []).append(r.permit_id)

    existing: Dict[Tuple[str, str], dict] = {}
    for city, ids in by_city.items():
        ids = list(dict.fromkeys(ids))
        for i in range(0, len(ids), LOOKUP_CHUNK):
            chunk = ids[i:i + LOOKUP_CHUNK]
            for attempt in range(2):
                try:
                    resp = (client.table("permits")
                            .select("city, permit_id, complexity_tier, project_category, ai_rationale")
                            .eq("city", city).in_("permit_id", chunk).execute())
                    for row in resp.data or []:
                        existing[(row["city"], row["permit_id"])] = row
                    break
                except Exception as e:
                    if attempt == 1:
                        print(f"   WARNING: existing-tier lookup failed for {city} "
                              f"({len(chunk)} ids): {e}. These rows may be re-classified.")
    return existing


def _apply_existing_classification(r: PermitRecord, row: Optional[dict]) -> bool:
    """
    If `row` (the stored DB row) holds a real tier, copy it onto `r` and return True so the
    caller skips the AI. Copying (rather than dropping the record) matters: the later upsert
    would otherwise overwrite the stored tier with Unknown.
    """
    if not row:
        return False
    tier = row.get("complexity_tier")
    if str(tier or "").strip().lower() in UNCLASSIFIED_TIERS:
        return False
    try:
        r.complexity_tier = ComplexityTier(tier)
    except ValueError:
        r.complexity_tier = tier  # legacy/classify_engine value (e.g. 'Strategic'): keep as-is
    category = row.get("project_category")
    if category:
        try:
            r.project_category = ProjectCategory(category)
        except ValueError:
            r.project_category = category
    if row.get("ai_rationale"):
        r.ai_rationale = row["ai_rationale"]
    return True


def _group_by_description(records: List[PermitRecord]) -> List[List[PermitRecord]]:
    """
    Groups records sharing a normalized description so each is classified once.
    Blank / placeholder descriptions are NOT grouped (their tier depends on valuation alone).
    """
    groups: Dict[object, List[PermitRecord]] = {}
    for r in records:
        norm = _normalize_description(r.description)
        key = norm if norm and norm != "no description" else ("__unique__", id(r))
        groups.setdefault(key, []).append(r)
    return list(groups.values())


def process_and_classify_permits(
    records: List[PermitRecord],
    persist: Optional[Callable[[List[PermitRecord]], None]] = None,
    existing_lookup: Optional[Callable[[List[PermitRecord]], Dict[Tuple[str, str], dict]]] = None,
) -> List[PermitRecord]:
    """
    Classifies permits into Residential, Commercial, or Commodity tiers.

    Uses a hybrid approach (cheapest first):
    1. A keyword/valuation pre-filter for obvious high-volume, low-value permits (free).
    2. Skip anything Supabase already holds a non-Unknown tier for (free).
    3. Gemini (GEMINI_MODEL) for the remainder, one call per batch of UNIQUE descriptions.

    Args:
        records: A list of `PermitRecord` objects to be classified.
        persist: Optional callback invoked with each set of finished records as soon as it is
            ready (free rows first, then after every AI batch) so a later abort does not
            discard work already paid for.
        existing_lookup: Optional override for the Supabase existing-tier lookup (tests).

    Returns:
        A list of `PermitRecord` objects with `complexity_tier` and `project_category` populated.

    Raises:
        AIClassificationError: Gemini is fundamentally broken (model/key/billing), several
            batches in a row failed, or more than AI_MAX_FAIL_RATE of AI-bound records failed.
            Transient single-batch errors are retried, logged and counted instead.
        CostCapExceeded (an AIClassificationError): projected or measured spend > MAX_RUN_COST_USD.
    """
    if not records:
        return []

    processed_records: List[PermitRecord] = []
    to_classify: List[PermitRecord] = []

    commodity_noise = ["pool", "spa", "sign", "fence", "roof", "siding", "demolition",
                       "irrigation", "solar", "driveway"]

    for r in records:
        desc_clean = _normalize_description(r.description)
        if r.valuation >= 25000:
            to_classify.append(r)
            continue
        if any(n in desc_clean for n in commodity_noise) or r.valuation < 5000:
            r.complexity_tier = ComplexityTier.COMMODITY
            r.project_category = ProjectCategory.RESIDENTIAL_ALTERATION
            r.ai_rationale = "Auto-filtered: Commodity threshold."
            processed_records.append(r)
        elif RES_KEYWORD_RE.search(desc_clean):
            r.complexity_tier = ComplexityTier.RESIDENTIAL
            r.project_category = ProjectCategory.RESIDENTIAL_NEW
            r.ai_rationale = "Auto-filtered: Residential keyword."
            processed_records.append(r)
        else:
            to_classify.append(r)

    # Skip rows classified on a previous run (no AI call, tier preserved).
    if to_classify:
        stored = (existing_lookup or fetch_existing_tiers)(to_classify)
        still_unclassified = []
        for r in to_classify:
            if _apply_existing_classification(r, stored.get((r.city, r.permit_id))):
                processed_records.append(r)
            else:
                still_unclassified.append(r)
        skipped = len(to_classify) - len(still_unclassified)
        if skipped:
            print(f"   Skipped {skipped} record(s) already classified in Supabase (no AI call).")
        to_classify = still_unclassified

    # Free results are safe to keep no matter what happens in the AI phase.
    if persist and processed_records:
        persist(processed_records)

    if not to_classify:
        return processed_records

    # One AI call per unique normalized description; the tier is mapped back to every row.
    groups = _group_by_description(to_classify)
    chunk_size = AI_CHUNK_SIZE
    total_batches = (len(groups) + chunk_size - 1) // chunk_size
    print(f"Sending {len(groups)} unique description(s) covering {len(to_classify)} record(s) "
          f"to Gemini ({GEMINI_MODEL}) in {total_batches} call(s)...")

    # PRE-FLIGHT cost guard: abort before spending anything if the projection is over the cap.
    check_projected_cost("ingest classification", total_batches,
                         EST_INPUT_TOKENS_PER_CALL, EST_OUTPUT_TOKENS_PER_CALL)

    ai_success_count = 0   # records
    ai_error_count = 0     # records
    failed_batches = 0
    consecutive_failed = 0

    for i in range(0, len(groups), chunk_size):
        usage.check_cap()  # in-loop guard on MEASURED spend so far (raises CostCapExceeded)

        chunk = groups[i:i + chunk_size]
        chunk_records = sum(len(g) for g in chunk)
        batch_no = i // chunk_size + 1
        batch_prompt = """
        Role: Civil Engineering classifier.
        Task: Classify these permits into: 'Commercial', 'Residential', 'Commodity'.
        Output: A pure JSON list of objects. No markdown.
        Format: [{"id": 0, "tier": "Commercial", "category": "Retail", "rationale": "..."}]
        INPUT DATA:
        """
        for idx, group in enumerate(chunk):
            rep_val = max(m.valuation for m in group)
            batch_prompt += f"\nInput ID {idx}: ${rep_val} | {group[0].description[:200]}"

        try:
            response = _generate_with_retry(batch_prompt)
            raw_json = extract_json_from_text(response.text)

            if not raw_json:
                ai_error_count += chunk_records
                failed_batches += 1
                consecutive_failed += 1
                print(f"   AI Batch {batch_no}: Empty or unparseable response. "
                      f"{chunk_records} records left as UNKNOWN.")
                if consecutive_failed >= AI_MAX_CONSECUTIVE_FAILED_BATCHES:
                    raise AIClassificationError(
                        f"{consecutive_failed} consecutive AI batches failed; aborting run.")
                continue

            consecutive_failed = 0

            classified_ids = set()
            newly_classified: List[PermitRecord] = []
            for item in raw_json:
                try:
                    record_idx = int(str(item.get("id")))
                    if 0 <= record_idx < len(chunk) and record_idx not in classified_ids:
                        members = chunk[record_idx]
                        tier_str = str(item.get("tier", "Unknown")).upper()
                        if "COMMERCIAL" in tier_str:
                            tier = ComplexityTier.COMMERCIAL
                        elif "RESIDENTIAL" in tier_str:
                            tier = ComplexityTier.RESIDENTIAL
                        elif "COMMODITY" in tier_str:
                            tier = ComplexityTier.COMMODITY
                        else:
                            tier = ComplexityTier.UNKNOWN
                        category = _map_project_category(item.get("category", ""), tier)
                        rationale = item.get("rationale", "AI Classified")
                        for target in members:
                            target.complexity_tier = tier
                            target.project_category = category
                            target.ai_rationale = rationale
                        classified_ids.add(record_idx)
                        ai_success_count += len(members)
                        if tier != ComplexityTier.UNKNOWN:
                            newly_classified.extend(members)
                except Exception as item_err:
                    print(f"   Could not apply AI result for item {item}: {item_err}")

            # Log any records the AI silently skipped
            missing = chunk_records - sum(len(chunk[k]) for k in classified_ids)
            if missing > 0:
                ai_error_count += missing
                print(f"   AI Batch {batch_no}: {missing} record(s) not returned "
                      f"by AI - left as UNKNOWN.")

            # Keep what was just paid for: a later abort must not discard it.
            if persist and newly_classified:
                persist(newly_classified)

        except AIClassificationError:
            raise
        except Exception as e:
            if is_fatal_ai_error(e):
                # Model retired / bad key / billing: every remaining batch would fail the
                # same way. Stop now and fail the run instead of dumping into Unknown.
                print(fatal_ai_banner("ingest classification", e))
                raise AIClassificationError(f"Fatal Gemini error: {e}") from e
            # Transient (already retried): log, count, and move on to the next batch.
            ai_error_count += chunk_records
            failed_batches += 1
            consecutive_failed += 1
            print(f"   AI Batch {batch_no} failed after retries ({chunk_records} records "
                  f"left as UNKNOWN): {e}")
            if consecutive_failed >= AI_MAX_CONSECUTIVE_FAILED_BATCHES:
                raise AIClassificationError(
                    f"{consecutive_failed} consecutive AI batches failed; aborting run.") from e

    usage.check_cap()  # catch an overshoot on the final call
    print(f"   AI classification complete: {ai_success_count} classified, "
          f"{ai_error_count} fell back to UNKNOWN "
          f"({failed_batches}/{total_batches} batches failed outright).")

    if ai_error_count / len(to_classify) > AI_MAX_FAIL_RATE:
        print(f"::error title=Gemini AI failure::{ai_error_count}/{len(to_classify)} AI-bound "
              f"records failed classification (model {GEMINI_MODEL})")
        raise AIClassificationError(
            f"{ai_error_count}/{len(to_classify)} AI-bound records failed "
            f"(> {AI_MAX_FAIL_RATE:.0%} threshold).")

    return processed_records + to_classify


def batch_upsert(data: List[dict], batch_size: int = 200) -> Tuple[int, int]:
    """
    Chunks data into smaller batches to ensure Supabase accepts them.

    Args:
        data: A list of dictionaries to upload to Supabase.
        batch_size: The number of records per batch. Defaults to 200 (do not exceed 500).

    Returns:
        (saved_count, failed_count)
    """
    total = len(data)
    success_count = 0
    fail_count = 0
    print(f"Uploading {total} records in safe batches of {batch_size}...")

    for i in range(0, total, batch_size):
        batch = data[i:i + batch_size]
        try:
            supabase.table("permits").upsert(batch, on_conflict="city, permit_id").execute()
            success_count += len(batch)
            print(f"   Batch {i // batch_size + 1}: Saved records {i + 1}-{min(i + batch_size, total)}")
        except Exception as e:
            fail_count += len(batch)
            print(f"   Batch {i // batch_size + 1} failed (rows {i}-{i + batch_size}): {e}")
        time.sleep(0.2)

    print(f"   Upload complete: {success_count} saved, {fail_count} failed.")
    return success_count, fail_count


class ProgressiveWriter:
    """
    Upserts records as they become final, remembering what is already saved so the same
    (city, permit_id) is never written twice in a run. A key is only marked written if its
    upload succeeded, so the end-of-run pass retries failures.
    """

    def __init__(self, upsert_fn: Optional[Callable[[List[dict]], Tuple[int, int]]] = None):
        self.written: set = set()
        self._upsert = upsert_fn or batch_upsert

    def __call__(self, records: List[PermitRecord]) -> None:
        pending: Dict[Tuple[str, str], dict] = {}
        for r in records:
            key = (r.city, r.permit_id)
            if key not in self.written:
                pending[key] = r.model_dump(mode='json', exclude={'latitude', 'longitude'})
        if not pending:
            return
        _, failed = self._upsert(list(pending.values()))
        if failed == 0:
            self.written.update(pending)


def resolve_lookback_days(argv: Optional[List[str]] = None, environ=None) -> int:
    """
    Lookback window in days. Precedence: `--days N` > env INGEST_LOOKBACK_DAYS > 3.
    Raises ValueError for a non-positive / non-integer value.
    """
    environ = os.environ if environ is None else environ
    parser = argparse.ArgumentParser(description="Vectis ingestion orchestrator")
    parser.add_argument("--days", type=int, default=None,
                        help=f"lookback window in days (default {DEFAULT_LOOKBACK_DAYS}; "
                             f"use e.g. 90 for a manual backfill)")
    args, _ = parser.parse_known_args(argv)
    days = args.days
    if days is None:
        raw = (environ.get("INGEST_LOOKBACK_DAYS") or "").strip()
        days = int(raw) if raw else DEFAULT_LOOKBACK_DAYS
    if days < 1:
        raise ValueError(f"lookback days must be >= 1 (got {days})")
    return days


def main(argv: Optional[List[str]] = None):
    """
    Main entry point. Orchestrates fetch -> classify -> upload for all cities.
    """
    try:
        lookback_days = resolve_lookback_days(argv)
    except ValueError as e:
        print(f"ABORTING: bad lookback setting: {e}")
        sys.exit(2)
    cutoff = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    print(f"Fetching Data Since: {cutoff} ({lookback_days}-day lookback; "
          f"cost cap ${MAX_RUN_COST_USD:.2f}/run)")

    all_data: List[PermitRecord] = []
    city_counts: Dict[str, int] = {}

    def ingest(label: str, fn, *args):
        for attempt in range(3):
            try:
                records = fn(*args)
                all_data.extend(records)
                city_counts[label] = len(records)
                return
            except Exception as e:
                if attempt < 2:
                    print(f"⚠️ {label} attempt {attempt + 1} failed: {e}. Retrying in 5s...")
                    time.sleep(5)
                else:
                    print(f"⚠️ {label} failed after 3 attempts: {e}")
                    city_counts[label] = 0

    ingest("Austin",        get_austin_data,        SOCRATA_TOKEN, cutoff)
    ingest("San Antonio",   get_san_antonio_data,   cutoff)
    ingest("Fort Worth",    get_fort_worth_data,    cutoff)
    ingest("Los Angeles",   get_la_data,            cutoff, SOCRATA_TOKEN)
    ingest("Chicago",       get_chicago_data,       SOCRATA_TOKEN, cutoff)
    ingest("New York",      get_new_york_data,      SOCRATA_TOKEN, cutoff)
    ingest("San Francisco", get_san_francisco_data, SOCRATA_TOKEN, cutoff)

    print("\n📊 Ingestion summary:")
    for city, count in city_counts.items():
        status = "✅" if count > 0 else "⚠️ "
        print(f"   {status} {city}: {count} records")
    print(f"   Total: {len(all_data)} records\n")

    # Time-travel guard: drop records where issued_date precedes applied_date.
    # Both dates must be present to trigger; missing either passes through.
    pre_guard = len(all_data)
    all_data = [
        r for r in all_data
        if not (r.issued_date and r.applied_date and r.issued_date < r.applied_date)
    ]
    dropped = pre_guard - len(all_data)
    if dropped:
        print(f"Time-travel guard: dropped {dropped} record(s) where issued_date < applied_date.\n")

    print(f"Classifying {len(all_data)} records...")
    writer = ProgressiveWriter()
    try:
        try:
            final_records = process_and_classify_permits(all_data, persist=writer)
        except AIClassificationError as e:
            # Rows already classified were saved progressively; unclassified rows are NOT
            # written and are re-fetched/re-classified (only those) on the next healthy run.
            print(f"\nABORTING: AI classification failed - {e}")
            print(f"   {len(writer.written)} already-classified record(s) were saved before the abort.")
            sys.exit(1)

        # Final pass writes whatever is not yet saved (e.g. rows left Unknown by failed batches).
        writer(final_records)
        if final_records:
            print(f"ORCHESTRATION COMPLETE. {len(writer.written)} record(s) saved.")
        else:
            print("No records to ingest.")
    finally:
        print(usage.summary())


if __name__ == "__main__":
    main()
