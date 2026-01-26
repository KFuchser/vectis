"""
Vectis Ingestion Orchestrator - FINAL PRODUCTION
Fixes:
1. Model: Reverts to 'gemini-2.0-flash' (Proven to connect).
2. Logic: RESTORES the missing logic to save AI results to the database.
3. Scope: Includes Los Angeles.
"""
import os
import json
from typing import List, Dict
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime, timedelta
import google.genai as genai
from google.genai import types

# Models
from service_models import PermitRecord, ComplexityTier, ProjectCategory

# Spokes
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

    # 4. AI CLASSIFICATION (Gemini 2.0 Flash)
    if to_classify:
        print(f"🧠 Sending {len(to_classify)} records to Gemini (2.0 Flash)...")
        chunk_size = 30
        
        for i in range(0, len(to_classify), chunk_size):
            chunk = to_classify[i:i + chunk_size]
            
            batch_prompt = """
            You are a Permit Classification Engine.
            Classify into TIER: 'Commercial', 'Residential', 'Commodity'.
            Return JSON list: [{"id": 0, "tier": "Commercial", "category": "Retail", "rationale": "..."}]
            INPUT DATA:
            """
            for idx, r in enumerate(chunk):
                batch_prompt += f"\nInput ID {idx}: ${r.valuation} | {r.description[:200]}"

            try:
                # CHANGED: Back to 2.0-flash which worked for you previously
                response = ai_client.models.generate_content(
                    model="gemini-2.0-flash",
                    contents=batch_prompt,
                    config=types.GenerateContentConfig(response_mime_type="application/json")
                )
                
                try:
                    raw_json = json.loads(response.text)
                except Exception:
                    raw_json = []

                # --- CRITICAL: SAVE THE DATA ---
                # This block was missing in previous failed versions
                for item in raw_json:
                    try:
                        record_idx = int(item.get("id"))
                        if 0 <= record_idx < len(chunk):
                            target_record = chunk[record_idx]
                            
                            # Map String to Enum
                            tier_str = str(item.get("tier", "Unknown")).upper()
                            if "COMMERCIAL" in tier_str:
                                target_record.complexity_tier = ComplexityTier.COMMERCIAL
                            elif "RESIDENTIAL" in tier_str:
                                target_record.complexity_tier = ComplexityTier.RESIDENTIAL
                            elif "COMMODITY" in tier_str:
                                target_record.complexity_tier = ComplexityTier.COMMODITY
                            else:
                                target_record.complexity_tier = ComplexityTier.UNKNOWN
                                
                            target_record.project_category = ProjectCategory.UNKNOWN 
                            target_record.ai_rationale = item.get("rationale", "AI Classified")
                    except Exception:
                        continue
                # -------------------------------
                        
            except Exception as e:
                print(f"❌ AI Batch Error: {e}") 
                pass

    return processed_records + to_classify

def main():
    cutoff = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    all_data = []

    print("🛰️ Fetching Austin...")
    try:
        all_data.extend(get_austin_data(SOCRATA_TOKEN, cutoff))
    except Exception as e:
        print(f"⚠️ Austin Failed: {e}")

    print("🛰️ Fetching San Antonio...")
    all_data.extend(get_san_antonio_data(cutoff))
    
    # Keeping Fort Worth safe/simple
    print("🛰️ Fetching Fort Worth...")
    all_data.extend(get_fort_worth_data(cutoff))
    
    print("🛰️ Fetching Los Angeles...")
    try:
        all_data.extend(get_la_data(cutoff, SOCRATA_TOKEN))
    except Exception as e:
        print(f"⚠️ LA Failed: {e}")

    print(f"⚙️ Processing {len(all_data)} records...")
    final_records = process_and_classify_permits(all_data)
    
    # Deduplication & Upload
    unique_batch: Dict[str, dict] = {}
    for r in final_records:
        key = f"{r.city}_{r.permit_id}"
        # Pydantic V2 dump, excluding extra fields
        r_dict = r.model_dump(mode='json', exclude={'latitude', 'longitude'})
        unique_batch[key] = r_dict

    data_to_upsert = list(unique_batch.values())

    if data_to_upsert:
        try:
            response = supabase.table("permits").upsert(
                data_to_upsert, 
                on_conflict="city, permit_id"
            ).execute()
            print(f"✅ SUCCESS: Ingested {len(data_to_upsert)} records.")
        except Exception as e:
            print(f"❌ Database Upload Failed: {e}")
    else:
        print("⚠️ No records to ingest.")

if __name__ == "__main__":
    main()