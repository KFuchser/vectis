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
from datetime import datetime

# --- IMPORTS FROM OUR NEW ARCHITECTURE ---
from service_models import PermitRecord 

from datetime import timedelta  # Make sure this is imported

def get_cutoff_date(days_back=30):
    """Returns the date string (YYYY-MM-DD) for X days ago."""
    cutoff = datetime.now() - timedelta(days=days_back)
    return cutoff.strftime("%Y-%m-%d")

load_dotenv()

# --- CONFIGURATION & SECRETS ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SOCRATA_TOKEN = os.getenv("SOCRATA_APP_TOKEN")

# Initialize Supabase
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- CORE LOGIC: DAILY DELTA (ARMORED) ---
def process_daily_delta(new_df, supabase_client):
    """
    Implements Protocol 'Total Market' Snapshot Logic.
    Compares incoming Socrata data (new_df) vs Supabase (existing).
    Logs status changes to 'permit_history_log'.
    Includes CHUNKING to prevent HTTP 400 Errors.
    """
    if new_df.empty:
        return new_df
        
    print(f">> 🕵️ Running Daily Delta on {len(new_df)} records...")

    # 1. Get Unique List of Permit IDs to check
    incoming_ids = list(set(new_df['permit_id'].tolist()))

    # 2. Fetch CURRENT state with CHUNKING (Fixes HTTP 400 Error)
    existing_map = {}
    BATCH_SIZE = 200  # Safe limit for URL length
    
    try:
        for i in range(0, len(incoming_ids), BATCH_SIZE):
            chunk = incoming_ids[i : i + BATCH_SIZE]
            
            response = supabase_client.table('permits')\
                .select('permit_id, status, city')\
                .in_('permit_id', chunk)\
                .execute()
            
            # Update our lookup map with the results from this chunk
            for r in response.data:
                existing_map[r['permit_id']] = r.get('status')
                
    except Exception as e:
        print(f"!! Error fetching existing records: {e}")
        return new_df

    # 3. Detect Changes
    history_log = []
    
    for _, row in new_df.iterrows():
        pid = row['permit_id']
        new_status = row.get('status')
        old_status = existing_map.get(pid)

        # CASE A: Status Changed (The Signal)
        if old_status and new_status and new_status != old_status:
            print(f"   ⚡ Delta Detected [{pid}]: {old_status} -> {new_status}")
            history_log.append({
                "permit_id": pid,
                "city": row['city'],
                "previous_status": old_status,
                "new_status": new_status,
                "change_date": pd.Timestamp.now().date().isoformat()
            })

    # 4. Write to History Log
    if history_log:
        print(f">> 📝 Logging {len(history_log)} status changes to Ledger...")
        try:
            supabase_client.table('permit_history_log').insert(history_log).execute()
        except Exception as e:
            print(f"!! Error writing history log: {e}")
    else:
        print(">> No status changes detected in this batch.")

    return new_df 

# --- INGESTION FUNCTIONS (ARMORED) ---

def ingest_austin():
    """
    TARGET: Austin (Benchmark)
    Includes RETRY LOGIC for timeouts.
    """
    print(">> Ingesting Target: Austin (Socrata)...")

    # DYNAMIC THRESHOLD (30 Days Lookback)
    threshold = get_cutoff_date(30) 
    print(f">> Fetching Austin data since: {threshold}")
    
    # Retry Loop
    for attempt in range(3):
        try:
            client = Socrata("data.austintexas.gov", SOCRATA_TOKEN, timeout=120) # Bumped to 120s
            results = client.get(
                "3syk-w9eu",
                # REPLACE '2025-10-01' WITH f"{threshold}"
                where=f"status_current in ('Issued', 'Final') AND issue_date > '{threshold}'",
                limit=3000, 
                order="issue_date DESC"
            )
            break # Success
        except Exception as e:
            print(f"   ⚠️ Attempt {attempt+1} failed: {e}")
            time.sleep(5)
            if attempt == 2: return [] # Give up

    if not results:
        print("!! ALERT: No records returned from Austin API.")
        return [] 

    print(f">> Raw Austin Records: {len(results)}")

    cleaned_records = []
    
    for row in results:
        try:
            # FIX: Valuation Parsing
            val_raw = row.get("total_job_valuation") or row.get("valuation")
            if isinstance(val_raw, str):
                val_raw = val_raw.replace('$', '').replace(',', '')
            val_float = float(val_raw) if val_raw else 0.0

            # FIX: Time Travel
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
                status=row.get("status_current")
            )
            cleaned_records.append(permit.model_dump(mode='json'))
        except Exception as e:
            continue
            
    print(f">> Scored Austin Records: {len(cleaned_records)}")
    return cleaned_records

def ingest_san_antonio():
    print("\n>> Ingesting Target: San Antonio (Direct CSV)...")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    

    try:
        # Retry logic handled by requests implicitly or we can wrap, 
        # but usually getting the URL is the hard part.
        meta_endpoint = "https://data.sanantonio.gov/api/3/action/resource_show"
        params = {"id": "c21106f9-3ef5-4f3a-8604-f992b4db7512"}
        
        meta_resp = requests.get(meta_endpoint, params=params, headers=headers, timeout=30)
        meta_resp.raise_for_status()
        csv_url = meta_resp.json()['result']['url']
        
        # Download with increased timeout
        file_resp = requests.get(csv_url, headers=headers, timeout=120)
        file_resp.raise_for_status()
        
        df = pd.read_csv(io.BytesIO(file_resp.content))
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
        
        col_map = {
            'issue': 'date_issued',
            'applied': 'date_submitted',
            'id': 'permit_#',
            'desc': 'work_type',
            'val': 'declared_valuation'
        }

        # Parse Dates
        df[col_map['issue']] = pd.to_datetime(df[col_map['issue']], errors='coerce')
        df[col_map['applied']] = pd.to_datetime(df[col_map['applied']], errors='coerce')
        
        # Filter
        # DYNAMIC THRESHOLD
        threshold_str = get_cutoff_date(30)
        cutoff_date = pd.Timestamp(threshold_str)
        mask = (df[col_map['issue']] >= cutoff_date)
        df = df[mask].copy()
        
        print(f">> Raw San Antonio Records (Post-Filter): {len(df)}")

        cleaned_records = []
        for _, row in df.iterrows():
            try:
                issued_dt = row[col_map['issue']]
                applied_dt = row[col_map['applied']]
                if pd.isnull(issued_dt): continue
                
                if pd.notnull(applied_dt) and (issued_dt < applied_dt):
                    issued_dt, applied_dt = applied_dt, issued_dt

                val_raw = row.get(col_map['val'], 0)
                val_float = float(val_raw) if pd.notnull(val_raw) else 0.0
                
                desc = f"{row.get('permit_type','')} - {row.get('work_type','')} ({row.get('project_name','')})".strip()
                id_val = row.get(col_map['id']) or row.get('permit_no')

                permit = PermitRecord(
                    city="San Antonio",
                    permit_id=str(id_val), 
                    applied_date=applied_dt.date() if (applied_dt and pd.notnull(applied_dt)) else None,
                    issued_date=issued_dt.date(),
                    description=desc,
                    valuation=val_float,
                    status="Issued"
                )
                cleaned_records.append(permit.model_dump(mode='json'))
            except Exception:
                continue
        
        print(f">> Scored San Antonio Records: {len(cleaned_records)}")
        return cleaned_records
    except Exception as e:
        print(f"!! San Antonio Ingestion Failed: {e}")
        return []

def ingest_fort_worth():
    print("\n>> Ingesting Target: Fort Worth (MapIT Server)...")
    base_url = "https://mapit.fortworthtexas.gov/ags/rest/services/CIVIC/Permits/MapServer/0/query"
    params = {
        "where": "1=1", "outFields": "*", "f": "json",
        "resultRecordCount": 2000, "orderByFields": "Status_Date DESC" 
    }

    # DYNAMIC THRESHOLD
    threshold_str = get_cutoff_date(30)
    cutoff_date = pd.to_datetime(threshold_str).date()
    
    # Retry Loop
    data = {}
    for attempt in range(3):
        try:
            response = requests.get(base_url, params=params, timeout=120)
            response.raise_for_status()
            data = response.json()
            break
        except Exception as e:
            print(f"   ⚠️ FW Attempt {attempt+1} failed: {e}")
            time.sleep(5)
    
    features = data.get('features', [])
    if not features:
        return []

    print(f">> Raw FW Records: {len(features)}")
    cleaned_records = []
    for feature in features:
        attr = feature.get('attributes', {})
        try:
            status = str(attr.get("Current_Status", "")).title()
            if status not in ['Issued', 'Finaled', 'Complete', 'Active']: continue 
            
            issued_dt = None
            if attr.get("Status_Date"):
                issued_dt = pd.to_datetime(attr.get("Status_Date"), unit='ms').date()
            
            applied_dt = None
            if attr.get("File_Date"):
                applied_dt = pd.to_datetime(attr.get("File_Date"), unit='ms').date()

            if issued_dt and issued_dt < cutoff_date: continue
            if issued_dt and applied_dt and issued_dt < applied_dt:
                issued_dt, applied_dt = applied_dt, issued_dt

            permit = PermitRecord(
                city="Fort Worth",
                permit_id=str(attr.get("Permit_Num") or attr.get("Permit_No") or "UNK-" + str(attr.get("OBJECTID"))),
                applied_date=applied_dt, issued_date=issued_dt,
                description=attr.get("B1_WORK_DESC") or "No Description",
                valuation=float(attr.get("JobValue", 0.0) or 0.0),
                status=status
            )
            cleaned_records.append(permit.model_dump(mode='json'))
        except Exception:
            continue
    print(f">> Scored FW Records: {len(cleaned_records)}")
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
            print(f"\n--- SYNCING {city_name.upper()} ---")
            raw = fetch_func()
            if not raw: return
            
            clean = remove_duplicates(raw)
            df_delta = pd.DataFrame(clean)
            
            # RUN DELTA LOGIC (With Chunking Fix)
            process_daily_delta(df_delta, supabase)
            
            # UPSERT
            supabase.table('permits').upsert(clean, on_conflict='permit_id, city').execute()
            print(f">> {city_name} Sync Complete. ({len(clean)} unique records)")

        execute_city_sync("Austin", ingest_austin)
        execute_city_sync("Fort Worth", ingest_fort_worth)
        execute_city_sync("San Antonio", ingest_san_antonio)
            
    except Exception as e:
        print(f"!! CRITICAL PIPELINE FAILURE: {e}")

if __name__ == "__main__":
    run_pipeline()