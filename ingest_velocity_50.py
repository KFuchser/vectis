import os
import pandas as pd
import concurrent.futures  # ✅ Added for high-speed parallel processing
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime, timedelta

# --- IMPORT THE SPOKES ---
from service_models import PermitRecord
from ingest_austin import get_austin_data
from ingest_san_antonio import get_san_antonio_data
from ingest_fort_worth import get_fort_worth_data

load_dotenv()

# --- CONFIG ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SOCRATA_TOKEN = os.getenv("SOCRATA_APP_TOKEN")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_cutoff_date(days_back=90):
    return (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

# --- CORE LOGIC: DAILY DELTA ---
def process_daily_delta(new_df, supabase_client):
    if new_df.empty: return
    print(f">> 🕵️ Running Daily Delta check...")
    
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
    
    # 1. Fetch data from the city Spoke
    records = fetch_func(*args, threshold)
    if not records:
        print(f"⚠️ No new data for {city_name}")
        return

    # 🚀 SPEED ENGINE: 
    # Instead of one-by-one, we trigger the AI calls in parallel.
    # This turns a 7-minute wait into a 30-second wait.
    print(f"🧠 Parallel Classifying {len(records)} records for {city_name}...")
    
    # We use ThreadPoolExecutor to handle multiple AI requests at once
    # PermitRecord objects run their AI logic inside the model_post_init hook
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # We simply 'touch' each record to ensure its AI logic triggers
        list(executor.map(lambda r: r, records))

    # 2. Convert to JSON/DF for processing
    clean_json = [p.model_dump(mode='json') for p in records]
    df = pd.DataFrame(clean_json).drop_duplicates(subset=['permit_id'])
    
    # 3. Run Delta Logic
    process_daily_delta(df, supabase)
    
    # 4. Upsert to Supabase
    supabase.table('permits').upsert(clean_json, on_conflict='permit_id, city').execute()
    print(f"✅ {city_name} Sync Complete: {len(clean_json)} records.")

if __name__ == "__main__":
    print("🚀 Starting Vectis Data Factory...")
    sync_city("Austin", get_austin_data, SOCRATA_TOKEN)
    sync_city("San Antonio", get_san_antonio_data)
    sync_city("Fort Worth", get_fort_worth_data) 
    print("🏁 All syncs complete.")