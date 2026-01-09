import os
import json
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime, timedelta
import google.genai as genai
from google.genai import types

# --- IMPORT THE SPOKES ---
from service_models import PermitRecord, ComplexityTier
from ingest_austin import get_austin_data
from ingest_san_antonio import get_san_antonio_data
from ingest_fort_worth import get_fort_worth_data

load_dotenv()

# --- CONFIG ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SOCRATA_TOKEN = os.getenv("SOCRATA_APP_TOKEN")
GEMINI_KEY = os.getenv("GOOGLE_API_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
ai_client = genai.Client(api_key=GEMINI_KEY)

def get_cutoff_date(days_back=90):
    return (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

# --- BATCH AI ENGINE (The Speed Solution) ---
def batch_classify_permits(records: list[PermitRecord]):
    """Processes permits in chunks of 50 to maximize speed and minimize API calls."""
    if not records: return []
    
    print(f"🧠 Starting Batch Intelligence for {len(records)} records...")
    
    # 1. Negative Constraint Filter (Immediate cost/time savings)
    noise = ["bedroom", "kitchen", "fence", "roofing", "hvac", "deck", "pool", "residential"]
    to_classify = []
    
    for r in records:
        if any(word in r.description.lower() for word in noise):
            r.complexity_tier = ComplexityTier.COMMODITY
            r.ai_rationale = "Auto-filtered: Residential noise."
        else:
            to_classify.append(r)

    print(f"⚡ {len(to_classify)} records passed filter and require AI classification.")

    # 2. Batch Processing Loop (Chunks of 50)
    chunk_size = 50
    for i in range(0, len(to_classify), chunk_size):
        chunk = to_classify[i:i + chunk_size]
        print(f"🛰️ Processing AI Chunk {i//chunk_size + 1} ({len(chunk)} permits)...")
        
        # Build a structured prompt for the batch
        batch_prompt = "Classify these construction permits as 'Strategic' (Commercial/Retail) or 'Commodity' (Minor/Residential). Return JSON list of {id, tier, reason}.\n\n"
        for idx, r in enumerate(chunk):
            batch_prompt += f"ID {idx}: {r.description[:200]}\n"

        try:
            response = ai_client.models.generate_content(
                model="gemini-2.0-flash",
                contents=batch_prompt,
                config=types.GenerateContentConfig(response_mime_type="application/json")
            )
            
            results = json.loads(response.text)
            # Ensure we are handling a list of results
            if isinstance(results, dict) and "results" in results:
                results = results["results"]

            for res in results:
                try:
                    idx = int(res.get("id"))
                    if idx < len(chunk):
                        chunk[idx].complexity_tier = ComplexityTier(res.get("tier"))
                        chunk[idx].ai_rationale = res.get("reason")
                except:
                    continue
        except Exception as e:
            print(f"⚠️ Batch AI Error: {e}")
            
    return records

# --- CORE LOGIC: DAILY DELTA ---
def process_daily_delta(new_df, supabase_client):
    if new_df.empty: return
    print(f">> 🕵️ Running Daily Delta on {len(new_df)} records...")
    
    incoming_ids = new_df['permit_id'].tolist()
    existing_map = {}
    
    for i in range(0, len(incoming_ids), 200):
        chunk = incoming_ids[i : i + 200]
        resp = supabase_client.table('permits').select('permit_id, status').in_('permit_id', chunk).execute()
        for r in resp.data:
            existing_map[r['permit_id']] = r.get('status')

    history_log = []
    for _, row in new_df.iterrows():
        old_status = existing_map.get(row['permit_id'])
        if old_status and row['status'] != old_status:
            history_log.append({
                "permit_id": row['permit_id'],
                "city": row['city'],
                "previous_status": old_status,
                "new_status": row['status'],
                "change_date": datetime.now().date().isoformat()
            })

    if history_log:
        supabase_client.table('permit_history_log').insert(history_log).execute()
        print(f">> 📝 Logged {len(history_log)} status changes.")

# --- ORCHESTRATOR ---
def sync_city(city_name, fetch_func, *args):
    threshold = get_cutoff_date(90)
    
    # 1. Fetch
    records = fetch_func(*args, threshold)
    if not records:
        print(f"⚠️ No new data for {city_name}")
        return

    # 2. Batch AI Classification
    records = batch_classify_permits(records)

    # 3. Convert to JSON/DF with null-safety
    clean_json = []
    for p in records:
        p_dict = p.model_dump(mode='json')
        # Ensure these are never None so pandas/supabase stay happy
        p_dict['complexity_tier'] = p_dict.get('complexity_tier') or "Unknown"
        p_dict['ai_rationale'] = p_dict.get('ai_rationale') or "No rationale provided."
        clean_json.append(p_dict)

    df = pd.DataFrame(clean_json).drop_duplicates(subset=['permit_id'])
    
    # 4. Delta Logic
    try:
        process_daily_delta(df, supabase)
    except Exception as e:
        print(f"⚠️ Delta Check failed: {e}")
    
    # 5. Upsert (Hardened with Exception Trigger)
    try:
        response = supabase.table('permits').upsert(clean_json, on_conflict='permit_id, city').execute()
        # Verify if Supabase actually accepted it
        if hasattr(response, 'data'):
            print(f"✅ {city_name} Sync Complete: {len(clean_json)} records.")
    except Exception as e:
        print(f"❌ CRITICAL: Supabase Upsert failed for {city_name}: {e}")
        # Raising the error here ensures the GitHub Action shows a RED X if it fails
        raise e

if __name__ == "__main__":
    print("🚀 Starting Vectis Data Factory [BATCH MODE]...")
    sync_city("Austin", get_austin_data, SOCRATA_TOKEN)
    sync_city("San Antonio", get_san_antonio_data)
    sync_city("Fort Worth", get_fort_worth_data) 
    print("🏁 All syncs complete.")