"""
Vectis Ingestion Orchestrator - PRODUCTION FIX
Fixes:
1. Replaces the 'pass' block with actual logic to save AI results.
2. Fixes Pydantic V2 warnings by using model_dump().
3. Ensures Enums are used correctly to prevent crashing.
"""
import os
import json
from typing import List, Dict
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime, timedelta
import google.genai as genai
from google.genai import types

# Import Models
from service_models import PermitRecord, ComplexityTier, ProjectCategory

# Import Spokes
from ingest_austin import get_austin_data
from ingest_san_antonio import get_san_antonio_data
from ingest_fort_worth import get_fort_worth_data

load_dotenv()

# --- CONFIG ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GEMINI_KEY = os.getenv("GOOGLE_API_KEY")
SOCRATA_TOKEN = os.getenv("SOCRATA_APP_TOKEN", None)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
ai_client = genai.Client(api_key=GEMINI_KEY)

def process_and_classify_permits(records: List[PermitRecord]):
    """
    Triage Waterfall (V3.1):
    1. Safety Valve: Valuation >= $25k -> Always Deep AI.
    2. Commodity Filter: Low value/Minor keywords -> Auto-Commodity.
    3. Residential: Specific SFH keywords -> Auto-Residential.
    """
    if not records: return []
    
    processed_records = []
    to_classify = []
    
    # HEURISTIC KEYWORDS
    commodity_noise = ["pool", "spa", "sign", "fence", "roof", "siding", "demolition", "irrigation", "solar", "driveway"]
    res_keywords = ["single family", "sfh", "detached", "duplex", "townhouse", "garage", "adu"]

    for r in records:
        desc_clean = (r.description or "").lower()

        # --- 1. STRATEGIC SAFETY VALVE (>$25k) ---
        if r.valuation >= 25000:
            to_classify.append(r)
            continue

        # --- 2. COMMODITY HEURISTICS ---
        if any(n in desc_clean for n in commodity_noise) or r.valuation < 5000:
            # FIX: Assign Enum, not string
            r.complexity_tier = ComplexityTier.COMMODITY
            r.project_category = ProjectCategory.RESIDENTIAL_ALTERATION 
            r.ai_rationale = "Auto-filtered: Commodity threshold."
            processed_records.append(r)
        
        # --- 3. RESIDENTIAL HEURISTICS ---
        elif any(k in desc_clean for k in res_keywords):
            # FIX: Assign Enum, not string
            r.complexity_tier = ComplexityTier.RESIDENTIAL
            r.project_category = ProjectCategory.RESIDENTIAL_NEW
            r.ai_rationale = "Auto-filtered: Residential keyword."
            processed_records.append(r)

        else:
            to_classify.append(r)

    # --- 4. BATCH AI CLASSIFICATION ---
    if to_classify:
        print(f"🧠 Sending {len(to_classify)} records to Gemini...")
        chunk_size = 30
        
        for i in range(0, len(to_classify), chunk_size):
            chunk = to_classify[i:i + chunk_size]
            
            # Prompt
            batch_prompt = """
            You are a Permit Classification Engine.
            Classify into TIER: 'Commercial', 'Residential', 'Commodity'.
            
            Return JSON list: [{"id": 0, "tier": "Commercial", "category": "Retail", "rationale": "..."}]
            
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
                
                # Parse JSON
                try:
                    raw_json = json.loads(response.text)
                except Exception:
                    raw_json = []

                # --- THE MISSING LOGIC (RESTORED) ---
                for item in raw_json:
                    try:
                        record_idx = int(item.get("id"))
                        if 0 <= record_idx < len(chunk):
                            target_record = chunk[record_idx]
                            
                            # Map String to Enum (Critical for Pydantic)
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
                # ------------------------------------
                        
            except Exception as e:
                print(f"❌ AI Batch Error: {e}")
                # Don't crash, just let them remain 'Unknown'
                pass

    return processed_records + to_classify

def main():
    cutoff = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    all_data = []

    print("🛰️ Fetching Austin...")
    try:
        austin_recs = get_austin_data(SOCRATA_TOKEN, cutoff)
        all_data.extend(austin_recs)
    except Exception as e:
        print(f"⚠️ Austin Failed: {e}")

    print("🛰️ Fetching San Antonio...")
    all_data.extend(get_san_antonio_data(cutoff))
    
    print("🛰️ Fetching Fort Worth...")
    all_data.extend(get_fort_worth_data(cutoff))

    print(f"⚙️ Processing {len(all_data)} records...")
    final_records = process_and_classify_permits(all_data)
    
    # --- 5. DEDUPLICATION & SERIALIZATION (CRITICAL FIX) ---
    unique_batch: Dict[str, dict] = {}
    
    for r in final_records:
        key = f"{r.city}_{r.permit_id}"
        
        # FIX 1: Use model_dump(mode='json') for Pydantic V2 to handle Enums automatically
        # FIX 2: Explicitly exclude 'latitude' and 'longitude' to match DB Schema
        r_dict = r.model_dump(
            mode='json', 
            exclude={'latitude', 'longitude'}
        )
        unique_batch[key] = r_dict

    data_to_upsert = list(unique_batch.values())

    if data_to_upsert:
        try:
            # 6. UPSERT WITH CONFLICT HANDLING
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