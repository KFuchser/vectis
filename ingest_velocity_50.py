"""
Vectis Ingestion Orchestrator - PRODUCTION

This script serves as the central entry point for the data pipeline.
It performs the following high-level operations:
1. Fetches permit data from all configured city "spokes" (Austin, San Antonio, Fort Worth, LA).
2. Normalizes the data into a common `PermitRecord` format.
3. Applies AI-based classification (Gemini 2.0 Flash) to categorize permits (Residential vs Commercial)
   based on description and valuation.
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

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
ai_client = genai.Client(api_key=GEMINI_KEY)

def extract_json_from_text(text: str):
    """
    Extracts a JSON object from a string that may contain other text.

    Args:
        text: The string to search for a JSON object.

    Returns:
        A list or dictionary parsed from the JSON, or an empty list if no valid JSON is found.
    """
    try:
        match = re.search(r'\[.*\]', text, re.DOTALL)
        if match: return json.loads(match.group())
        return json.loads(text)
    except Exception: return []

def process_and_classify_permits(records: List[PermitRecord]):
    """
    Classifies permits into Residential, Commercial, or Commodity tiers.

    This function uses a hybrid approach:
    1.  A keyword-based pre-filter for obvious high-volume, low-value permits (e.g., "pool", "roof").
    2.  A call to a Gemini 2.0 Flash model for more nuanced classification of the remaining records.

    Args:
        records: A list of `PermitRecord` objects to be classified.

    Returns:
        A list of `PermitRecord` objects with the `complexity_tier` and `project_category` fields populated.
    """
    if not records: return []
    processed_records = []
    to_classify = []
    
    # These keywords represent common, low-value permits that can be automatically classified
    # as "Commodity" to reduce the number of expensive AI calls.
    commodity_noise = ["pool", "spa", "sign", "fence", "roof", "siding", "demolition", "irrigation", "solar", "driveway"]
    
    # These keywords are strong indicators of residential projects.
    res_keywords = ["single family", "sfh", "detached", "duplex", "townhouse", "garage", "adu"]

    for r in records:
        desc_clean = (r.description or "").lower()
        # Valuation threshold of $25,000 is a heuristic to separate high-value projects
        # that require AI classification from lower-value ones.
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

    if to_classify:
        print(f"🧠 Sending {len(to_classify)} records to Gemini (2.0 Flash)...")
        chunk_size = 30
        for i in range(0, len(to_classify), chunk_size):
            chunk = to_classify[i:i + chunk_size]
            # This prompt is carefully engineered for the Gemini 2.0 Flash model.
            # - "Role: Civil Engineering classifier." sets the context for the model.
            # - "Task: Classify these permits..." clearly defines the goal.
            # - "Output: A pure JSON list of objects. No markdown." ensures a machine-readable response.
            # - The format string provides a few-shot example to guide the model's output.
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
                for item in raw_json:
                    try:
                        record_idx = int(str(item.get("id")))
                        if 0 <= record_idx < len(chunk):
                            target = chunk[record_idx]
                            tier_str = str(item.get("tier", "Unknown")).upper()
                            if "COMMERCIAL" in tier_str: target.complexity_tier = ComplexityTier.COMMERCIAL
                            elif "RESIDENTIAL" in tier_str: target.complexity_tier = ComplexityTier.RESIDENTIAL
                            elif "COMMODITY" in tier_str: target.complexity_tier = ComplexityTier.COMMODITY
                            else: target.complexity_tier = ComplexityTier.UNKNOWN
                            target.project_category = ProjectCategory.UNKNOWN 
                            target.ai_rationale = item.get("rationale", "AI Classified")
                    except Exception: continue
            except Exception as e:
                print(f"❌ AI Batch Error: {e}") 
                pass

    return processed_records + to_classify

def batch_upsert(data: List[dict], batch_size: int = 200):
    """
    Chunks data into smaller batches to ensure Supabase accepts them.

    Args:
        data: A list of dictionaries to upload to Supabase.
        batch_size: The number of records to include in each batch. Defaults to 200.
    """
    total = len(data)
    print(f"📦 Uploading {total} records in safe batches of {batch_size}...")
    
    for i in range(0, total, batch_size):
        batch = data[i:i + batch_size]
        try:
            supabase.table("permits").upsert(batch, on_conflict="city, permit_id").execute()
            print(f"   ↳ ✅ Batch {i//batch_size + 1}: Saved records {i+1} to {min(i+batch_size, total)}")
        except Exception as e:
            print(f"   ❌ Batch Failed (Rows {i}-{i+batch_size}): {e}")
        time.sleep(0.2) 

def main():
    """
    The main entry point for the ingestion script.

    This function orchestrates the fetching, processing, and uploading of permit data.
    """
    cutoff = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
    print(f"📅 Fetching Data Since: {cutoff} (90 Days)")
    
    all_data = []
    
    try: all_data.extend(get_austin_data(SOCRATA_TOKEN, cutoff))
    except Exception as e: print(f"⚠️ Austin Failed: {e}")

    try: all_data.extend(get_san_antonio_data(cutoff))
    except Exception as e: print(f"⚠️ San Antonio Failed: {e}")

    try: all_data.extend(get_fort_worth_data(cutoff))
    except Exception as e: print(f"⚠️ Fort Worth Failed: {e}")
    
    try: all_data.extend(get_la_data(cutoff, SOCRATA_TOKEN))
    except Exception as e: print(f"⚠️ LA Failed: {e}")

    try: all_data.extend(get_chicago_data(SOCRATA_TOKEN, cutoff))
    except Exception as e: print(f"⚠️ Chicago Failed: {e}")

    try: all_data.extend(get_new_york_data(SOCRATA_TOKEN, cutoff))
    except Exception as e: print(f"⚠️ New York Failed: {e}")

    try: all_data.extend(get_san_francisco_data(SOCRATA_TOKEN, cutoff))
    except Exception as e: print(f"⚠️ San Francisco Failed: {e}")

    print(f"⚙️ Processing {len(all_data)} records...")
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
        print("⚠️ No records to ingest.")

if __name__ == "__main__":
    main()