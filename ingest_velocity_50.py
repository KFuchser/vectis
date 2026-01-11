import os
import json
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime, timedelta
import google.genai as genai
from google.genai import types

# 1. CRITICAL: Imports must happen BEFORE functions use these names
# Ensure service_models.py is updated with ProjectCategory
from service_models import PermitRecord, ComplexityTier, ProjectCategory
from ingest_austin import get_austin_data
from ingest_san_antonio import get_san_antonio_data
from ingest_fort_worth import get_fort_worth_data
from ingest_la import get_la_data

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

# --- SANITIZATION ENGINE (The "Time Travel" Patch) ---
def sanitize_record(record: PermitRecord) -> PermitRecord:
    """
    Fixes dirty data anomalies common in government feeds.
    """
    # 1. Time Travel Check (Issued Date cannot be before Applied Date)
    if record.applied_date and record.issued_date:
        try:
            applied = datetime.strptime(record.applied_date, "%Y-%m-%d")
            issued = datetime.strptime(record.issued_date, "%Y-%m-%d")
            
            if issued < applied:
                # Heuristic: It's usually a cleric swapping fields. Swap them back.
                record.applied_date, record.issued_date = record.issued_date, record.applied_date
        except ValueError:
            pass # Ignore date parse errors, Pydantic handles validation
            
    # 2. Null Description Handling
    if not record.description or record.description.strip() == "":
        record.description = "No Description Provided"
        
    return record

# --- BATCH AI ENGINE ---
# --- BATCH AI ENGINE ---
def batch_classify_permits(records: list[PermitRecord]):
    if not records: return []
    
    print(f"🧠 Starting Batch Intelligence for {len(records)} records...")
    
    noise = ["bedroom", "kitchen", "fence", "roofing", "hvac", "deck", "pool", "residential", "single family", "siding", "water heater"]
    to_classify = []
    
    for r in records:
        r = sanitize_record(r) 
        
        desc_lower = r.description.lower()
        if any(word in desc_lower for word in noise):
            r.complexity_tier = ComplexityTier.COMMODITY
            r.project_category = ProjectCategory.RESIDENTIAL_ALTERATION
            r.ai_rationale = "Auto-filtered: Residential keywords found."
        else:
            to_classify.append(r)

    print(f"⚡ {len(to_classify)} records require Deep AI classification.")

    chunk_size = 50
    for i in range(0, len(to_classify), chunk_size):
        chunk = to_classify[i:i + chunk_size]
        print(f"🛰️ Processing AI Chunk {i//chunk_size + 1}...")
        
        # PROMPT: We explicitly ask for specific JSON keys
        batch_prompt = """
        You are a Permit Classification Engine. 
        Classify these permits.
        
        RULES:
        1. Tier MUST be either 'Strategic' or 'Commodity'.
        2. Category MUST be one of: 'Residential - New Construction', 'Residential - Alteration/Addition', 'Commercial - New Construction', 'Commercial - Tenant Improvement', 'Infrastructure/Public Works'.
        
        INPUT DATA:
        """
        for idx, r in enumerate(chunk):
            batch_prompt += f"ID {idx}: Val=${r.valuation} | Desc: {r.description[:200]}\n"

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
                        # --- TRANSLATION LAYER (FIXES 'Commercial' ERROR) ---
                        raw_tier = str(res.get("tier", "Commodity")).upper()
                        
                        # Map "Commercial" or "Strategic" -> STRATEGIC
                        if "COMMERCIAL" in raw_tier or "STRATEGIC" in raw_tier:
                            chunk[idx].complexity_tier = ComplexityTier.STRATEGIC
                        else:
                            # Default to Commodity for safety
                            chunk[idx].complexity_tier = ComplexityTier.COMMODITY
                            
                        # Map Category
                        cat_val = res.get("category", "Residential - Alteration/Addition")
                        try:
                            chunk[idx].project_category = ProjectCategory(cat_val)
                        except:
                            # Fallback if AI invents a category
                            if chunk[idx].complexity_tier == ComplexityTier.STRATEGIC:
                                chunk[idx].project_category = ProjectCategory.COMMERCIAL_ALTERATION
                            else:
                                chunk[idx].project_category = ProjectCategory.RESIDENTIAL_ALTERATION
                            
                        chunk[idx].ai_rationale = res.get("reason")
                except Exception as inner_e:
                    print(f"Skipping record {idx}: {inner_e}")
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
    
    # Check existing status in batches
    for i in range(0, len(incoming_ids), 200):
        chunk = incoming_ids[i : i + 200]
        resp = supabase_client.table('permits').select('permit_id, status').in_('permit_id', chunk).execute()
        for r in resp.data:
            existing_map[r['permit_id']] = r.get('status')

    history_log = []
    for _, row in new_df.iterrows():
        old_status = existing_map.get(row['permit_id'])
        # Only log if status changed AND old status wasn't None (new record)
        if old_status and row['status'] != old_status:
            history_log.append({
                "permit_id": row['permit_id'],
                "city": row['city'],
                "previous_status": old_status,
                "new_status": row['status'],
                "change_date": datetime.now().date().isoformat()
            })

    if history_log:
        try:
            supabase_client.table('permit_history_log').insert(history_log).execute()
            print(f">> 📝 Logged {len(history_log)} status changes.")
        except Exception as e:
            print(f"⚠️ Failed to log history: {e}")

# --- ORCHESTRATOR ---
def sync_city(city_name, fetch_func, *args):
    lookback = 365 if city_name == "Fort Worth" else 90
    threshold = get_cutoff_date(lookback)
    
    records = fetch_func(*args, threshold)
    if not records:
        print(f"⚠️ No new data found for {city_name}")
        return

    unique_records = {r.permit_id: r for r in records}
    records = list(unique_records.values())
    print(f"🧹 De-duplicated {city_name}: {len(records)} unique permits.")

    # RUN INTELLIGENCE
    records = batch_classify_permits(records)

    clean_json = []
    for p in records:
        p_dict = p.model_dump(mode='json')
        
        # Serialize Enums explicitly to string values for JSON/DB
        p_dict['complexity_tier'] = p_dict['complexity_tier'].value if hasattr(p_dict['complexity_tier'], 'value') else p_dict['complexity_tier']
        p_dict['project_category'] = p_dict['project_category'].value if p_dict.get('project_category') and hasattr(p_dict['project_category'], 'value') else None
        
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
        # Don't raise, allow next city to process
        pass 

if __name__ == "__main__":
    print("🚀 Starting Vectis Data Factory [BATCH MODE - QUALITY LOCK v6.0]...")
    
    # Austin uses Socrata
    sync_city("Austin", get_austin_data, SOCRATA_TOKEN)
    
    # San Antonio uses ArcGIS
    sync_city("San Antonio", get_san_antonio_data)
    
    # Fort Worth uses ArcGIS
    sync_city("Fort Worth", get_fort_worth_data) 
    sync_city("Los Angeles", get_la_data) # <--- LA is now part of the factory
    
    print("🏁 All syncs complete.")