"""
Vectis Ingestion Orchestrator - PRODUCTION

This script serves as the central entry point for the data pipeline.
It performs the following high-level operations:
1. Fetches permit data from all configured city "spokes" (Austin, San Antonio, Fort Worth, LA,
   Chicago, New York, San Francisco).
2. Normalizes the data into a common `PermitRecord` format.
3. Applies AI-based classification (Gemini 2.0 Flash) to categorize permits
   (Residential vs Commercial) based on description and valuation.
4. Uploads the cleaned data to Supabase in safe batches to avoid timeouts.

Key Configuration:
- Batch Size: 200 records (Strict limit for Supabase stability).
- Lookback Period: 90 days (configurable via `timedelta`).
"""
import os
import json
import re
import time
from typing import List, Dict
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime, timedelta
import google.genai as genai
from google.genai import types

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
    2. A call to Gemini 2.0 Flash for nuanced classification of the remainder.

    Args:
        records: A list of `PermitRecord` objects to be classified.

    Returns:
        A list of `PermitRecord` objects with `complexity_tier` and `project_category` populated.
    """
    if not records:
        return []

    processed_records = []
    to_classify = []

    commodity_noise = ["pool", "spa", "sign", "fence", "roof", "siding", "demolition",
                       "irrigation", "solar", "driveway"]
    res_keywords = ["single family", "sfh", "detached", "duplex", "townhouse", "garage", "adu"]

    for r in records:
        desc_clean = (r.description or "").lower()
        if r.valuation >= 25000:
            to_classify.append(r)
            continue
        if any(n in desc_clean for n in commodity_noise) or r.valuation < 5000:
            r.complexity_tier = ComplexityTier.COMMODITY
            r.project_category = ProjectCategory.RESIDENTIAL_ALTERATION
            r.ai_rationale = "Auto-filtered: Commodity threshold."
            processed_records.append(r)
        elif any(k in desc_clean for k in res_keywords):
            r.complexity_tier = ComplexityTier.RESIDENTIAL
            r.project_category = ProjectCategory.RESIDENTIAL_NEW
            r.ai_rationale = "Auto-filtered: Residential keyword."
            processed_records.append(r)
        else:
            to_classify.append(r)

    if not to_classify:
        return processed_records

    print(f"🧠 Sending {len(to_classify)} records to Gemini (2.0 Flash)...")
    ai_success_count = 0
    ai_error_count = 0
    chunk_size = 30

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
            response = ai_client.models.generate_content(
                model="gemini-2.0-flash",
                contents=batch_prompt,
                config=types.GenerateContentConfig(response_mime_type="application/json")
            )
            raw_json = extract_json_from_text(response.text)

            if not raw_json:
                ai_error_count += len(chunk)
                print(f"   ⚠️ AI Batch {i // chunk_size + 1}: Empty or unparseable response. "
                      f"{len(chunk)} records left as UNKNOWN.")
                continue

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

        except Exception as e:
            ai_error_count += len(chunk)
            print(f"   ❌ AI Batch {i // chunk_size + 1} failed ({len(chunk)} records "
                  f"left as UNKNOWN): {e}")
            # Continue to next batch rather than aborting the whole run

    print(f"   🧠 AI classification complete: {ai_success_count} classified, "
          f"{ai_error_count} fell back to UNKNOWN.")

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
    final_records = process_and_classify_permits(all_data)

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
