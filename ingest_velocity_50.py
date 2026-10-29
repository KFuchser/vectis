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
- Lookback Period: 90 days (configurable via `timedelta`).

Failure behavior: if Gemini is fundamentally broken (model retired/404, bad key/403,
billing/402) or most AI batches fail, the run aborts BEFORE the Supabase upsert and exits
non-zero, so the GitHub Actions run goes red instead of silently writing Unknown/Commodity.
"""
import os
import json
import re
import sys
import time
from typing import List, Dict
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime, timedelta
import google.genai as genai
from google.genai import types

from gemini_config import GEMINI_MODEL, AIClassificationError, is_fatal_ai_error, fatal_ai_banner
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
            return ai_client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt,
                config=types.GenerateContentConfig(response_mime_type="application/json")
            )
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


def process_and_classify_permits(records: List[PermitRecord]) -> List[PermitRecord]:
    """
    Classifies permits into Residential, Commercial, or Commodity tiers.

    Uses a hybrid approach:
    1. A keyword-based pre-filter for obvious high-volume, low-value permits.
    2. A call to Gemini (GEMINI_MODEL) for nuanced classification of the remainder.

    Args:
        records: A list of `PermitRecord` objects to be classified.

    Returns:
        A list of `PermitRecord` objects with `complexity_tier` and `project_category` populated.

    Raises:
        AIClassificationError: Gemini is fundamentally broken (model/key/billing), several
            batches in a row failed, or more than AI_MAX_FAIL_RATE of AI-bound records failed.
            Transient single-batch errors are retried, logged and counted instead.
    """
    if not records:
        return []

    processed_records = []
    to_classify = []

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

    if not to_classify:
        return processed_records

    print(f"🧠 Sending {len(to_classify)} records to Gemini ({GEMINI_MODEL})...")
    ai_success_count = 0
    ai_error_count = 0
    failed_batches = 0
    consecutive_failed = 0
    chunk_size = 30
    total_batches = (len(to_classify) + chunk_size - 1) // chunk_size

    for i in range(0, len(to_classify), chunk_size):
        chunk = to_classify[i:i + chunk_size]
        batch_prompt = """
        Role: Civil Engineering classifier.
        Task: Classify these permits into: 'Commercial', 'Residential', 'Commodity'.
        Output: A pure JSON list of objects. No markdown.
        Format: [{"id": 0, "tier": "Commercial", "category": "Retail", "rationale": "..."}]
        INPUT DATA:
        """
        for idx, r in enumerate(chunk):
            batch_prompt += f"\nInput ID {idx}: ${r.valuation} | {r.description[:200]}"

        try:
            response = _generate_with_retry(batch_prompt)
            raw_json = extract_json_from_text(response.text)

            if not raw_json:
                ai_error_count += len(chunk)
                failed_batches += 1
                consecutive_failed += 1
                print(f"   ⚠️ AI Batch {i // chunk_size + 1}: Empty or unparseable response. "
                      f"{len(chunk)} records left as UNKNOWN.")
                if consecutive_failed >= AI_MAX_CONSECUTIVE_FAILED_BATCHES:
                    raise AIClassificationError(
                        f"{consecutive_failed} consecutive AI batches failed; aborting run.")
                continue

            consecutive_failed = 0

            classified_ids = set()
            for item in raw_json:
                try:
                    record_idx = int(str(item.get("id")))
                    if 0 <= record_idx < len(chunk):
                        target = chunk[record_idx]
                        tier_str = str(item.get("tier", "Unknown")).upper()
                        if "COMMERCIAL" in tier_str:
                            target.complexity_tier = ComplexityTier.COMMERCIAL
                        elif "RESIDENTIAL" in tier_str:
                            target.complexity_tier = ComplexityTier.RESIDENTIAL
                        elif "COMMODITY" in tier_str:
                            target.complexity_tier = ComplexityTier.COMMODITY
                        else:
                            target.complexity_tier = ComplexityTier.UNKNOWN
                        target.project_category = _map_project_category(
                            item.get("category", ""), target.complexity_tier
                        )
                        target.ai_rationale = item.get("rationale", "AI Classified")
                        classified_ids.add(record_idx)
                        ai_success_count += 1
                except Exception as item_err:
                    print(f"   ⚠️ Could not apply AI result for item {item}: {item_err}")

            # Log any records the AI silently skipped
            missing = len(chunk) - len(classified_ids)
            if missing > 0:
                ai_error_count += missing
                print(f"   ⚠️ AI Batch {i // chunk_size + 1}: {missing} record(s) not returned "
                      f"by AI — left as UNKNOWN.")

        except AIClassificationError:
            raise
        except Exception as e:
            if is_fatal_ai_error(e):
                # Model retired / bad key / billing: every remaining batch would fail the
                # same way. Stop now and fail the run instead of dumping into Unknown.
                print(fatal_ai_banner("ingest classification", e))
                raise AIClassificationError(f"Fatal Gemini error: {e}") from e
            # Transient (already retried): log, count, and move on to the next batch.
            ai_error_count += len(chunk)
            failed_batches += 1
            consecutive_failed += 1
            print(f"   ❌ AI Batch {i // chunk_size + 1} failed after retries ({len(chunk)} records "
                  f"left as UNKNOWN): {e}")
            if consecutive_failed >= AI_MAX_CONSECUTIVE_FAILED_BATCHES:
                raise AIClassificationError(
                    f"{consecutive_failed} consecutive AI batches failed; aborting run.") from e

    print(f"   🧠 AI classification complete: {ai_success_count} classified, "
          f"{ai_error_count} fell back to UNKNOWN "
          f"({failed_batches}/{total_batches} batches failed outright).")

    if ai_error_count / len(to_classify) > AI_MAX_FAIL_RATE:
        print(f"::error title=Gemini AI failure::{ai_error_count}/{len(to_classify)} AI-bound "
              f"records failed classification (model {GEMINI_MODEL})")
        raise AIClassificationError(
            f"{ai_error_count}/{len(to_classify)} AI-bound records failed "
            f"(> {AI_MAX_FAIL_RATE:.0%} threshold).")

    return processed_records + to_classify


def batch_upsert(data: List[dict], batch_size: int = 200):
    """
    Chunks data into smaller batches to ensure Supabase accepts them.

    Args:
        data: A list of dictionaries to upload to Supabase.
        batch_size: The number of records per batch. Defaults to 200 (do not exceed 500).
    """
    total = len(data)
    success_count = 0
    fail_count = 0
    print(f"📦 Uploading {total} records in safe batches of {batch_size}...")

    for i in range(0, total, batch_size):
        batch = data[i:i + batch_size]
        try:
            supabase.table("permits").upsert(batch, on_conflict="city, permit_id").execute()
            success_count += len(batch)
            print(f"   ↳ ✅ Batch {i // batch_size + 1}: Saved records {i + 1}–{min(i + batch_size, total)}")
        except Exception as e:
            fail_count += len(batch)
            print(f"   ❌ Batch {i // batch_size + 1} failed (rows {i}–{i + batch_size}): {e}")
        time.sleep(0.2)

    print(f"   Upload complete: {success_count} saved, {fail_count} failed.")


def main():
    """
    Main entry point. Orchestrates fetch → classify → upload for all cities.
    """
    cutoff = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
    print(f"📅 Fetching Data Since: {cutoff} (90-day lookback)")

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
    try:
        final_records = process_and_classify_permits(all_data)
    except AIClassificationError as e:
        # Nothing has been written to Supabase yet; the 90-day lookback re-fetches these
        # records on the next healthy run.
        print(f"\n❌ ABORTING before upload: AI classification failed — {e}")
        sys.exit(1)

    unique_batch: Dict[str, dict] = {}
    for r in final_records:
        key = f"{r.city}_{r.permit_id}"
        unique_batch[key] = r.model_dump(mode='json', exclude={'latitude', 'longitude'})

    data_to_upsert = list(unique_batch.values())

    if data_to_upsert:
        batch_upsert(data_to_upsert)
        print("✅ ORCHESTRATION COMPLETE.")
    else:
        print("⚠️  No records to ingest.")


if __name__ == "__main__":
    main()
