import os
import requests
import pandas as pd
from sodapy import Socrata
from supabase import create_client, Client
from dotenv import load_dotenv

# --- IMPORTS FROM OUR NEW ARCHITECTURE ---
from service_models import PermitRecord 

load_dotenv()

# --- CONFIGURATION & SECRETS ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SOCRATA_TOKEN = os.getenv("SOCRATA_APP_TOKEN")

# Initialize Supabase
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- INGESTION FUNCTIONS ---

def ingest_austin():
    """
    TARGET: Austin (Benchmark)
    Platform: Socrata (sodapy)
    """
    print(">> Ingesting Target: Austin (Socrata)...")
    
    client = Socrata("data.austintexas.gov", SOCRATA_TOKEN)
    
    results = client.get(
        "3syk-w9eu",
        where="status_current in ('Issued', 'Final') AND issue_date > '2025-10-01'",
        limit=2000,
        order="issue_date DESC"
    )
    
    if not results:
        print("!! ALERT: No records returned from Austin API.")
        return [] 

    print(f">> Raw Austin Records: {len(results)}")

    cleaned_records = []
    
    for row in results:
        try:
            val_raw = row.get("total_job_valuation") or row.get("valuation")
            val_float = float(val_raw) if val_raw else 0.0

            permit = PermitRecord(
                city="Austin",
                permit_id=row.get("permit_number"), 
                applied_date=row.get("applied_date"), 
                issued_date=row.get("issue_date"),
                description=row.get("work_description", row.get("permit_type_desc", "No Description")),
                valuation=val_float,
                status=row.get("status_current")
            )
            cleaned_records.append(permit.model_dump(mode='json'))
        except Exception:
            continue
            
    print(f">> Scored Austin Records: {len(cleaned_records)}")
    return cleaned_records

def ingest_fort_worth():
    """
    TARGET: Fort Worth
    Platform: ArcGIS REST API (MapIT Server)
    Layer: 0 (Permits)
    """
    print("\n>> Ingesting Target: Fort Worth (MapIT Server)...")
    
    base_url = "https://mapit.fortworthtexas.gov/ags/rest/services/CIVIC/Permits/MapServer/0/query"
    
    # 1. Query: Fetch recent records
    # We sort by Status_Date because we know that field exists now.
    params = {
        "where": "1=1",
        "outFields": "*",
        "f": "json",
        "resultRecordCount": 2000,
        "orderByFields": "Status_Date DESC" 
    }
    
    try:
        response = requests.get(base_url, params=params, timeout=20)
        response.raise_for_status()
        data = response.json()
        
        if 'error' in data:
            print(f"!! ArcGIS Error: {data['error']}")
            return []

        features = data.get('features', [])
        if not features:
            print("!! ALERT: No records found.")
            return []

        print(f">> Raw FW Records: {len(features)}")

        cleaned_records = []
        
        for feature in features:
            attr = feature.get('attributes', {})
            try:
                # --- MAPPING: The Rosetta Stone ---
                
                # 1. Status Check (Using 'Current_Status')
                status = str(attr.get("Current_Status", "")).title()
                if status not in ['Issued', 'Finaled', 'Complete', 'Active']:
                    continue # Skip unissued apps
                
                # 2. Date Parsing (ArcGIS MS Timestamps)
                # 'Status_Date' -> Issued Date
                # 'File_Date'   -> Applied Date
                
                issued_dt = None
                if attr.get("Status_Date"):
                    issued_dt = pd.to_datetime(attr.get("Status_Date"), unit='ms').date()
                
                applied_dt = None
                if attr.get("File_Date"):
                    applied_dt = pd.to_datetime(attr.get("File_Date"), unit='ms').date()

                # Filter: Keep only recent (Last 60 days)
                if issued_dt and issued_dt < pd.to_datetime('2025-10-01').date():
                    continue

                # 3. Valuation ('JobValue')
                val_raw = attr.get("JobValue", 0.0)
                
                # 4. Create Record
                permit = PermitRecord(
                    city="Fort Worth",
                    permit_id=str(attr.get("Permit_Num") or attr.get("Permit_No") or "UNK-" + str(attr.get("OBJECTID"))),
                    
                    applied_date=applied_dt,
                    issued_date=issued_dt,
                    
                    description=attr.get("B1_WORK_DESC") or "No Description",
                    valuation=float(val_raw) if val_raw else 0.0,
                    status=status
                )
                
                cleaned_records.append(permit.model_dump(mode='json'))

            except Exception:
                continue
                
        print(f">> Scored FW Records: {len(cleaned_records)}")
        return cleaned_records

    except Exception as e:
        print(f"!! Fort Worth Ingestion Failed: {e}")
        return []

# --- MAIN CONTROLLER ---

def run_pipeline():
    try:
        # --- HELPER: Deduplicator ---
        def remove_duplicates(records):
            """
            Keeps only the FIRST occurrence of a permit_id.
            Since we sort by Date DESC in the query, this keeps the newest version.
            """
            seen = set()
            unique_records = []
            for r in records:
                pid = r['permit_id']
                if pid not in seen:
                    unique_records.append(r)
                    seen.add(pid)
            return unique_records

        # --- 1. AUSTIN ---
        austin_data = ingest_austin()
        if austin_data:
            # Clean duplicates locally first
            clean_austin = remove_duplicates(austin_data)
            
            supabase.table('permits').upsert(
                clean_austin, on_conflict='permit_id, city'
            ).execute()
            print(f">> Austin Data Sync Complete. ({len(clean_austin)} unique records)")
        
        # --- 2. FORT WORTH ---
        fw_data = ingest_fort_worth()
        if fw_data:
            # Clean duplicates locally first (Fixes the Critical Failure)
            clean_fw = remove_duplicates(fw_data)
            
            supabase.table('permits').upsert(
                clean_fw, on_conflict='permit_id, city'
            ).execute()
            print(f">> Fort Worth Data Sync Complete. ({len(clean_fw)} unique records)")
            
    except Exception as e:
        print(f"!! CRITICAL PIPELINE FAILURE: {e}")

# --- IGNITION ---
if __name__ == "__main__":
    run_pipeline()