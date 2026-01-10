import os
import json
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime, timedelta
import google.genai as genai
from google.genai import types

# 1. CRITICAL: Imports must happen BEFORE functions use these names
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

# --- BATCH AI ENGINE ---
def batch_classify_permits(records: list[PermitRecord]):
    if not records: return []
    
    print(f"🧠 Starting Batch Intelligence for {len(records)} records...")
    
    noise = ["bedroom", "kitchen", "fence", "roofing", "hvac", "deck", "pool", "residential"]
    to_classify = []
    
    for r in records:
        if any(word in r.description.lower() for word in noise):
            r.complexity_tier = ComplexityTier.COMMODITY
            r.ai_rationale = "Auto-filtered: Residential noise."
        else:
            to_classify.append(r)

    print(f"⚡ {len(to_classify)} records require AI classification.")

    chunk_size = 50
    for i in range(0, len(to_classify), chunk_size):
        chunk = to_classify[i:i + chunk_size]
        print(f"🛰️ Processing AI Chunk {i//chunk_size + 1}...")
        
        batch_prompt = "Classify these permits as 'Strategic' (Commercial) or 'Commodity' (Residential). Return JSON list of {id, tier, reason}.\n\n"
        for idx, r in enumerate(chunk):
            batch_prompt += f"ID {idx}: {r.description[:200]}\n"

        try:
            response = ai_client.models.generate_content(
                model="gemini-2.0-flash",
                contents=batch_prompt,
                config=types.GenerateContentConfig(response_mime_type="application/json")
            )
            
            results = json.loads(response.text)
            data_list = results.get("results") if isinstance(results, dict) else results

            for res in data_list:
                try:
                    idx = int(res.get("id"))
                    if 0 <= idx < len(chunk):
                        tier_val = res.get("tier", "Commodity").capitalize()
                        chunk[idx].complexity_tier = ComplexityTier(tier_val)
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
    # For Fort Worth troubleshooting, we look back 365 days instead of 90
    lookback = 365 if city_name == "Fort Worth" else 90
    threshold = get_cutoff_date(lookback)
    
    records = fetch_func(*args, threshold)
    if not records:
        print(f"⚠️ No new data found for {city_name} (Lookback: {lookback} days)")
        return

    unique_records = {r.permit_id: r for r in records}
    records = list(unique_records.values())
    print(f"🧹 De-duplicated {city_name}: {len(records)} unique permits.")

    records = batch_classify_permits(records)

    clean_json = []
    for p in records:
        p_dict = p.model_dump(mode='json')
        # Fix: Reference enum value correctly
        p_dict['complexity_tier'] = p_dict.get('complexity_tier') or ComplexityTier.UNKNOWN.value
        p_dict['ai_rationale'] = p_dict.get('ai_rationale') or "No rationale provided."
        clean_json.append(p_dict)

    df = pd.DataFrame(clean_json)
    
    try:
        process_daily_delta(df, supabase)
    except Exception as e:
        print(f"⚠️ Delta Check failed for {city_name}: {e}")
    
    print(f"📤 Uploading {len(clean_json)} records to Supabase...")
    chunk_size = 500
    try:
        for i in range(0, len(clean_json), chunk_size):
            batch = clean_json[i : i + chunk_size]
            supabase.table('permits').upsert(batch, on_conflict='permit_id, city').execute()
        print(f"✅ {city_name} Sync Complete.")
    except Exception as e:
        print(f"❌ Supabase Upsert failed for {city_name}: {e}")
        raise e

if __name__ == "__main__":
    print("🚀 Starting Vectis Data Factory [BATCH MODE]...")
    sync_city("Austin", get_austin_data, SOCRATA_TOKEN)
    sync_city("San Antonio", get_san_antonio_data)
    sync_city("Fort Worth", get_fort_worth_data) 
    print("🏁 All syncs complete.")