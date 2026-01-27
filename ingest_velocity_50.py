"""
Vectis Ingestion Orchestrator - 90 DAY / WEEKLY VIEW
Changes:
1. Cutoff: 90 Days (Quarterly Data).
2. AI: Gemini 2.0 Flash + Regex Parser (Production Stable).
"""
import os
import json
import re
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

load_dotenv()

# --- CONFIG ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GEMINI_KEY = os.getenv("GOOGLE_API_KEY")
SOCRATA_TOKEN = os.getenv("SOCRATA_APP_TOKEN", None)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
ai_client = genai.Client(api_key=GEMINI_KEY)

def extract_json_from_text(text: str):
    try:
        match = re.search(r'\[.*\]', text, re.DOTALL)
        if match: return json.loads(match.group())
        return json.loads(text)
    except Exception: return []

def process_and_classify_permits(records: List[PermitRecord]):
    if not records: return []
    
    processed_records = []
    to_classify = []
    
    # HEURISTICS
    commodity_noise = ["pool", "spa", "sign", "fence", "roof", "siding", "demolition", "irrigation", "solar", "driveway"]
    res_keywords = ["single family", "sfh", "detached", "duplex", "townhouse", "garage", "adu"]

    for r in records:
        desc_clean = (r.description or "").lower()

        # 1. SAFETY VALVE (>$25k) -> AI
        if r.valuation >= 25000:
            to_classify.append(r)
            continue

        # 2. COMMODITY
        if any(n in desc_clean for n in commodity_noise) or r.valuation < 5000:
            r.complexity_tier = ComplexityTier.COMMODITY
            r.project_category = ProjectCategory.RESIDENTIAL_ALTERATION 
            r.ai_rationale = "Auto-filtered: Commodity threshold."
            processed_records.append(r)
        
        # 3. RESIDENTIAL
        elif any(k in desc_clean for k in res_keywords):
            r.complexity_tier = ComplexityTier.RESIDENTIAL
            r.project_category = ProjectCategory.RESIDENTIAL_NEW
            r.ai_rationale = "Auto-filtered: Residential keyword."
            processed_records.append(r)

        else:
            to_classify.append(r)

    # 4. AI CLASSIFICATION
    if to_classify:
        print(f"🧠 Sending {len(to_classify)} records to Gemini (2.0 Flash)...")
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

def main():
    # --- 90 DAY CONFIGURATION ---
    cutoff = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
    print(f"📅 Fetching Data Since: {cutoff} (90 Days)")
    
    all_data = []

    try: all_data.extend(get_austin_data(SOCRATA_TOKEN, cutoff))
    except: print("⚠️ Austin Failed")

    all_data.extend(get_san_antonio_data(cutoff))
    all_data.extend(get_fort_worth_data(cutoff))
    
    try: all_data.extend(get_la_data(cutoff, SOCRATA_TOKEN))
    except: print("⚠️ LA Failed")

    print(f"⚙️ Processing {len(all_data)} records...")
    final_records = process_and_classify_permits(all_data)
    
    unique_batch: Dict[str, dict] = {}
    for r in final_records:
        key = f"{r.city}_{r.permit_id}"
        unique_batch[key] = r.model_dump(mode='json', exclude={'latitude', 'longitude'})

    data_to_upsert = list(unique_batch.values())

    if data_to_upsert:
        try:
            supabase.table("permits").upsert(data_to_upsert, on_conflict="city, permit_id").execute()
            print(f"✅ SUCCESS: Ingested {len(data_to_upsert)} records.")
        except Exception as e:
            print(f"❌ Database Upload Failed: {e}")
    else:
        print("⚠️ No records to ingest.")

if __name__ == "__main__":
    main()