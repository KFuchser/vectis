"""
The primary ETL (Extract, Transform, Load) pipeline for ingesting permit data.

This script is responsible for:
1.  Fetching raw permit data from various city sources (Austin, San Antonio, Fort Worth).
2.  Cleaning and standardizing the data into the `PermitRecord` Pydantic model.
3.  Calculating the "daily delta" to identify and log status changes for existing permits.
4.  Upserting the cleaned, final records into the Supabase 'permits' table.
"""
import os
import requests
import io
import time
import pandas as pd
from sodapy import Socrata
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime, timedelta

# --- IMPORTS FROM YOUR ARCHITECTURE ---
from service_models import PermitRecord 

def get_cutoff_date(days_back=90):
    """Returns the date string (YYYY-MM-DD) for X days ago."""
    cutoff = datetime.now() - timedelta(days=days_back)
    return cutoff.strftime("%Y-%m-%d")

load_dotenv()

# --- CONFIGURATION & SECRETS ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SOCRATA_TOKEN = os.getenv("SOCRATA_APP_TOKEN")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- TIER LOGIC ---
def classify_tier(description, p_type, valuation):
    """Tags permits so we can filter 'Noise' in the Dashboard later."""
    desc = str(description).upper()
    p_type = str(p_type).upper()
    val = float(valuation or 0)
    
    # 1. Commodity
    if any(x in p_type for x in ['GARAGE', 'ELECTRICAL', 'MECHANICAL', 'PLUMBING', 'TREE', 'ALARM', 'FENCE', 'ROOF']):
        return 'Commodity'
    if any(x in desc for x in ['REPAIR', 'REPLACE', 'MAINTENANCE']):
        return 'Commodity'
        
    # 2. Strategic
    if val > 1_000_000:
        return 'Strategic'
        
    # 3. Standard
    return 'Standard'

# --- CORE LOGIC: DAILY DELTA ---
def process_daily_delta(new_df, supabase_client):
    if new_df.empty: return new_df
    print(f">> 🕵️ Running Daily Delta on {len(new_df)} records...")

    incoming_ids = list(set(new_df['permit_id'].tolist()))
    existing_map = {}
    BATCH_SIZE = 200 
    
    try:
        for i in range(0, len(incoming_ids), BATCH_SIZE):
            chunk = incoming_ids[i : i + BATCH_SIZE]
            response = supabase_client.table('permits').select('permit_id, status, city').in_('permit_id', chunk).execute()
            for r in response.data:
                existing_map[r['permit_id']] = r.get('status')
    except Exception as e:
        print(f"!! Error fetching existing records: {e}")
        return new_df

    history_log = []
    for _, row in new_df.iterrows():
        pid = row['permit_id']
        new_status = row.get('status')
        old_status = existing_map.get(pid)

        if old_status and new_status and new_status != old_status:
            print(f"   ⚡ Delta Detected [{pid}]: {old_status} -> {new_status}")
            history_log.append({
                "permit_id": pid,
                "city": row['city'],
                "previous_status": old_status,
                "new_status": new_status,
                "change_date": pd.Timestamp.now().date().isoformat()
            })

    if history_log:
        print(f">> 📝 Logging {len(history_log)} status changes to Ledger...")
        try:
            supabase_client.table('permit_history_log').insert(history_log).execute()
        except Exception as e:
            print(f"!! Error writing history log: {e}")
    else:
        print(">> No status changes detected in this batch.")

    return new_df 

# --- INGESTION FUNCTIONS ---

def ingest_austin():
    print("\n--- AUSTIN (Socrata API) ---")
    threshold = get_cutoff_date(90)
    
    for attempt in range(3):
        try:
            client = Socrata("data.austintexas.gov", SOCRATA_TOKEN, timeout=120)
            results = client.get(
                "3syk-w9eu",
                where=f"status_current in ('Issued', 'Final') AND issue_date > '{threshold}'",
                limit=5000, 
                order="issue_date DESC"
            )
            break 
        except Exception as e:
            print(f"   ⚠️ Attempt {attempt+1} failed: {e}")
            time.sleep(5)
            if attempt == 2: return []

    if not results: return []
    print(f">> Raw Austin Records: {len(results)}")

    cleaned_records = []
    for row in results:
        try:
            val_raw = row.get("total_job_valuation") or row.get("est_project_cost")
            if isinstance(val_raw, str):
                val_raw = val_raw.replace('$', '').replace(',', '')
            val_float = float(val_raw) if val_raw else 0.0

            applied_raw = row.get("applieddate")
            issued_raw = row.get("issue_date")
            applied_dt = pd.to_datetime(applied_raw).date() if applied_raw else None
            issued_dt = pd.to_datetime(issued_raw).date() if issued_raw else None
            
            if applied_dt and issued_dt and issued_dt < applied_dt:
                applied_dt, issued_dt = issued_dt, applied_dt

            permit = PermitRecord(
                city="Austin",
                permit_id=row.get("permit_number"), 
                applied_date=applied_dt, 
                issued_date=issued_dt,
                description=row.get("work_description") or row.get("permit_type_desc") or "No Description",
                valuation=val_float,
                status=row.get("status_current"),
                complexity_tier="Standard" 
            )
            cleaned_records.append(permit.model_dump(mode='json'))
        except Exception: continue
            
    return cleaned_records

def ingest_san_antonio():
    print("\n--- SAN ANTONIO (Direct CSV) ---")
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        # Fetch Metadata
        meta_endpoint = "https://data.sanantonio.gov/api/3/action/resource_show"
        params = {"id": "c21106f9-3ef5-4f3a-8604-f992b4db7512"}
        meta_resp = requests.get(meta_endpoint, params=params, headers=headers, timeout=30)
        csv_url = meta_resp.json()['result']['url']
        
        file_resp = requests.get(csv_url, headers=headers, timeout=120)
        df = pd.read_csv(io.BytesIO(file_resp.content))
        
        # Standardize Columns
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
        
        # ID Finder
        possible_id_cols = ['permit_num', 'permitnum', 'permit_#', 'permit_no', 'permitid', 'permit_number']
        id_col = next((c for c in possible_id_cols if c in df.columns), None)

        if not id_col:
            print(f"!! Error: Could not find Permit ID column. Available: {df.columns.tolist()}")
            return []
        
        print(f"   DEBUG: Mapped ID column to '{id_col}'")
        
        col_map = {
            'issue': 'date_issued', 'applied': 'date_submitted',
            'id': id_col, 'desc': 'permit_description',
            'val': 'declared_valuation', 'type': 'permit_type'
        }

        # Date Filter
        df[col_map['issue']] = pd.to_datetime(df[col_map['issue']], errors='coerce')
        threshold = pd.Timestamp(get_cutoff_date(90))
        df = df[df[col_map['issue']] >= threshold].copy()
        
        print(f">> Raw San Antonio Records (Post-Filter): {len(df)}")

        cleaned_records = []
        for _, row in df.iterrows():
            try:
                id_val = str(row.get(col_map['id']))
                if not id_val or id_val.lower() == 'nan': continue

                issued_dt = row[col_map['issue']]
                applied_dt = pd.to_datetime(row.get(col_map['applied']), errors='coerce')
                
                if pd.isnull(issued_dt): continue
                if pd.notnull(applied_dt) and (issued_dt < applied_dt):
                    issued_dt, applied_dt = applied_dt, issued_dt

                # NaN Safety
                val_raw = row.get(col_map['val'], 0)
                val_float = float(val_raw) if pd.notnull(val_raw) else 0.0

                desc = str(row.get(col_map['desc'], ''))
                p_type = str(row.get(col_map['type'], ''))
                
                tier = classify_tier(desc, p_type, val_float)

                permit = PermitRecord(
                    city="San Antonio",
                    permit_id=id_val, 
                    applied_date=applied_dt.date() if pd.notnull(applied_dt) else None,
                    issued_date=issued_dt.date(),
                    description=desc,
                    valuation=val_float,
                    status="Issued",
                    complexity_tier=tier 
                )
                cleaned_records.append(permit.model_dump(mode='json'))
            except Exception: continue
        
        return cleaned_records
    except Exception as e:
        print(f"!! San Antonio Ingestion Failed: {e}")
        return []

def ingest_fort_worth():
    print("\n--- FORT WORTH (MapIT) ---")
    base_url = "https://mapit.fortworthtexas.gov/ags/rest/services/CIVIC/Permits/MapServer/0/query"
    params = {
        "where": "1=1", "outFields": "*", "f": "json",
        "resultRecordCount": 2000, "orderByFields": "Status_Date DESC" 
    }
    threshold_str = get_cutoff_date(90)
    cutoff_date = pd.to_datetime(threshold_str).date()
    
    data = {}
    for attempt in range(3):
        try:
            response = requests.get(base_url, params=params, timeout=120)
            data = response.json()
            break
        except Exception: time.sleep(5)
    
    features = data.get('features', [])
    cleaned_records = []
    
    for feature in features:
        try:
            attr = feature.get('attributes', {})
            status = str(attr.get("Current_Status", "")).title()
            if status not in ['Issued', 'Finaled', 'Complete', 'Active']: continue 
            
            issued_dt = pd.to_datetime(attr.get("Status_Date"), unit='ms').date()
            if issued_dt < cutoff_date: continue
            
            applied_dt = pd.to_datetime(attr.get("File_Date"), unit='ms').date() if attr.get("File_Date") else None
            
            val = float(attr.get("JobValue", 0.0) or 0.0)
            tier = "Strategic" if val > 1_000_000 else "Standard"

            # --- ROBUST ID LOOKUP (The Fix) ---
            # Try Permit_Num, then Permit_No, then fallback to OBJECTID
            p_id = attr.get("Permit_Num") or attr.get("Permit_No")
            if not p_id and attr.get("OBJECTID"):
                 p_id = f"OID-{attr['OBJECTID']}"
            
            permit_id = str(p_id) if p_id else "UNK"
            # ----------------------------------

            permit = PermitRecord(
                city="Fort Worth",
                permit_id=permit_id,
                applied_date=applied_dt, issued_date=issued_dt,
                description=attr.get("B1_WORK_DESC") or "No Description",
                valuation=val,
                status=status,
                complexity_tier=tier 
            )
            cleaned_records.append(permit.model_dump(mode='json'))
        except: continue
        
    return cleaned_records

# --- MAIN CONTROLLER ---
def run_pipeline():
    try:
        def remove_duplicates(records):
            seen = set()
            unique = []
            for r in records:
                if r['permit_id'] not in seen:
                    unique.append(r)
                    seen.add(r['permit_id'])
            return unique

        def execute_city_sync(city_name, fetch_func):
            raw = fetch_func()
            if not raw: return
            
            clean = remove_duplicates(raw)
            df_delta = pd.DataFrame(clean)
            
            process_daily_delta(df_delta, supabase)
            
            supabase.table('permits').upsert(clean, on_conflict='permit_id, city').execute()
            print(f">> {city_name} Sync Complete. ({len(clean)} unique records)")

        execute_city_sync("Austin", ingest_austin)
        execute_city_sync("Fort Worth", ingest_fort_worth)
        execute_city_sync("San Antonio", ingest_san_antonio)
            
    except Exception as e:
        print(f"!! CRITICAL PIPELINE FAILURE: {e}")

if __name__ == "__main__":
    run_pipeline()