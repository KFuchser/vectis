"""
Vectis Ingestion Orchestrator - FIXED
Fixes:
1. Adds correct arguments to Austin spoke call to prevent crash.
2. Implements actual logic to map AI JSON response to records (removes the 'pass').
"""
import os
import json
from typing import List
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
SOCRATA_TOKEN = os.getenv("SOCRATA_APP_TOKEN", None) # Safe fallback

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
ai_client = genai.Client(api_key=GEMINI_KEY)

def process_and_classify_permits(records: List[PermitRecord]):
    """
    Triage Waterfall:
    1. Safety Valve: Valuation >= $25k -> Always Deep AI.
    2. Commodity Filter: Low value/Minor keywords -> Auto-Commodity.
    3. Residential: Specific SFH keywords -> Auto-Residential.
    """
    if not records: return []
    
    processed_records = []
    to_classify = []
    
    # HEURISTIC LISTS
    commodity_noise = ["pool", "spa", "sign", "fence", "roof", "siding", "demolition", "irrigation", "solar", "driveway"]
    res_keywords = ["single family", "sfh", "detached", "duplex", "townhouse", "garage", "adu"]

    for r in records:
        desc_clean = (r.description or "").lower()

        # --- 1. STRATEGIC SAFETY VALVE ---
        # Any project over $25k bypasses noise filters.
        if r.valuation >= 25000:
            to_classify.append(r)
            continue

        # --- 2. COMMODITY HEURISTICS ---
        if any(n in desc_clean for n in commodity_noise) or r.valuation < 5000:
            r.complexity_tier = "Commodity"
            r.project_category = "Residential - Alteration"
            r.ai_rationale = "Auto-filtered: Commodity threshold."
            processed_records.append(r)
        
        # --- 3. RESIDENTIAL HEURISTICS ---
        elif any(k in desc_clean for k in res_keywords):
            r.complexity_tier = "Residential"
            r.project_category = "Residential - New Construction" 
            r.ai_rationale = "Auto-filtered: Residential keyword."
            processed_records.append(r)

        else:
            to_classify.append(r)

    # --- 4. BATCH AI CLASSIFICATION (FIXED) ---
    if to_classify:
        print(f"🧠 Sending {len(to_classify)} records to Gemini...")
        chunk_size = 30 # Safe batch size
        
        for i in range(0, len(to_classify), chunk_size):
            chunk = to_classify[i:i + chunk_size]
            
            # Construct Prompt
            batch_prompt = """
            You are a Permit Classification Engine.
            Classify these permits into: 'Commercial', 'Residential', or 'Commodity'.
            
            RULES:
            1. 'Commercial' = Retail, Office, Industrial, Multifamily (5+ units).
            2. 'Residential' = Single Family, Duplex.
            3. 'Commodity' = Minor repairs, signs, pools.
            
            Return a JSON LIST of objects: [{"id": 0, "tier": "Commercial", "category": "Retail", "rationale": "..."}]
            The 'id' must match the Input ID provided below.
            
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
                
                # PARSE RESPONSE (This was missing before)
                raw_json = json.loads(response.text)
                
                for item in raw_json:
                    try:
                        record_idx = int(item.get("id"))
                        if 0 <= record_idx < len(chunk):
                            target_record = chunk[record_idx]
                            target_record.complexity_tier = item.get("tier", "Unknown")
                            target_record.project_category = item.get("category", "Unknown")
                            target_record.ai_rationale = item.get("rationale", "AI Classified")
                    except (ValueError, IndexError):
                        continue
                        
            except Exception as e:
                print(f"❌ AI Batch Error: {e}")
                # Fallback for failed batch
                for r in chunk:
                    if r.complexity_tier == "Unknown": # Only overwrite if not set
                        r.complexity_tier = "Unknown" 

    return processed_records + to_classify

def main():
    cutoff = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    all_data = []

    # 1. Austin (Fixed Argument Call)
    print("🛰️ Fetching Austin...")
    try:
        # Pass Token if available, else None (spoke handles logic)
        austin_recs = get_austin_data(SOCRATA_TOKEN, cutoff)
        all_data.extend(austin_recs)
    except Exception as e:
        print(f"⚠️ Austin Failed: {e}")

    # 2. San Antonio
    print("🛰️ Fetching San Antonio...")
    all_data.extend(get_san_antonio_data(cutoff))
    
    # 3. Fort Worth
    print("🛰️ Fetching Fort Worth...")
    all_data.extend(get_fort_worth_data(cutoff))

    print(f"⚙️ Processing {len(all_data)} records...")
    final_records = process_and_classify_permits(all_data)
    
    # Push to Supabase
    data_to_upsert = [r.dict() for r in final_records]
    if data_to_upsert:
        supabase.table("permits").upsert(data_to_upsert).execute()
        print(f"✅ SUCCESS: Ingested {len(final_records)} records.")
    else:
        print("⚠️ No records to ingest.")

if __name__ == "__main__":
    main()