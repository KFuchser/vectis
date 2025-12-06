import os
import requests
import io
import pandas as pd
from sodapy import Socrata
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime

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
    Fixes: 'applieddate' typo and string valuation parsing.
    """
    print(">> Ingesting Target: Austin (Socrata)...")
    
    client = Socrata("data.austintexas.gov", SOCRATA_TOKEN, timeout=60)
    
    # Note: increased limit to ensure we get enough valid data
    results = client.get(
        "3syk-w9eu",
        where="status_current in ('Issued', 'Final') AND issue_date > '2025-10-01'",
        limit=3000, 
        order="issue_date DESC"
    )
    
    if not results:
        print("!! ALERT: No records returned from Austin API.")
        return [] 

    print(f">> Raw Austin Records: {len(results)}")

    cleaned_records = []
    
    for row in results:
        try:
            # FIX 1: Robust Valuation Parsing (Handle "$,")
            val_raw = row.get("total_job_valuation") or row.get("valuation")
            if isinstance(val_raw, str):
                # Remove currency symbols causing float() to crash
                val_raw = val_raw.replace('$', '').replace(',', '')
            
            val_float = float(val_raw) if val_raw else 0.0

            # FIX 2: Correct API Field Names
            # API uses 'applieddate' (no underscore) and 'issue_date' (underscore)
            applied_val = row.get("applieddate") 
            
            permit = PermitRecord(
                city="Austin",
                permit_id=row.get("permit_number"), 
                
                # Map the correct API field to our Schema field
                applied_date=applied_val, 
                issued_date=row.get("issue_date"),
                
                description=row.get("work_description") or row.get("permit_type_desc") or "No Description",
                valuation=val_float,
                status=row.get("status_current")
            )
            cleaned_records.append(permit.model_dump(mode='json'))
        except Exception as e:
            # Optional: print(f"Skipped row: {e}")
            continue
            
    print(f">> Scored Austin Records: {len(cleaned_records)}")
    return cleaned_records

def ingest_san_antonio():
    """
    TARGET: San Antonio
    Platform: CKAN (Raw CSV)
    Schema: Explicitly mapped based on Data Dictionary (Source 1).
    """
    print("\n>> Ingesting Target: San Antonio (Direct CSV + Dictionary Map)...")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    # 1. Get Dynamic URL
    meta_endpoint = "https://data.sanantonio.gov/api/3/action/resource_show"
    params = {"id": "c21106f9-3ef5-4f3a-8604-f992b4db7512"}
    
    try:
        meta_resp = requests.get(meta_endpoint, params=params, headers=headers, timeout=30)
        meta_resp.raise_for_status()
        csv_url = meta_resp.json()['result']['url']
        
        # 2. Download Content
        file_resp = requests.get(csv_url, headers=headers, timeout=60)
        file_resp.raise_for_status()
        
        # 3. Load CSV
        df = pd.read_csv(io.BytesIO(file_resp.content))
        
        # 4. Normalize Headers (Standardize based on your Data Dictionary image)
        # "DATE ISSUED" -> "date_issued"
        # "DATE SUBMITTED" -> "date_submitted"
        # "PERMIT #" -> "permit_#"
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
        
        # 5. Define The "Gold Standard" Mapping
        col_map = {
            'issue': 'date_issued',      # From Data Dictionary: "DATE ISSUED"
            'applied': 'date_submitted', # From Data Dictionary: "DATE SUBMITTED"
            'id': 'permit_#',            # From Data Dictionary: "PERMIT #"
            'desc': 'work_type',         # From Data Dictionary: "WORK TYPE"
            'val': 'declared_valuation'  # From Data Dictionary: "DECLARED VALUATION"
        }

        # 6. Parse Dates
        # Handle the specific "date_submitted" column we now know exists
        df[col_map['issue']] = pd.to_datetime(df[col_map['issue']], errors='coerce')
        df[col_map['applied']] = pd.to_datetime(df[col_map['applied']], errors='coerce')
        
        # 7. Filter: Recent & Issued
        cutoff_date = pd.Timestamp('2025-10-01')
        mask = (df[col_map['issue']] >= cutoff_date)
        df = df[mask].copy()
        
        print(f">> Raw San Antonio Records (Post-Filter): {len(df)}")

        cleaned_records = []
        
        for _, row in df.iterrows():
            try:
                # --- Dirty Date Logic ---
                issued_dt = row[col_map['issue']]
                applied_dt = row[col_map['applied']]

                if pd.isnull(issued_dt): continue
                
                # Swap logic
                if pd.notnull(applied_dt) and (issued_dt < applied_dt):
                    issued_dt, applied_dt = applied_dt, issued_dt

                # --- Valuation ---
                val_raw = row.get(col_map['val'], 0)
                val_float = float(val_raw) if pd.notnull(val_raw) else 0.0

                # --- Description ---
                # Combine "PERMIT TYPE" + "WORK TYPE" + "PROJECT NAME" for rich context
                p_type = row.get('permit_type', '')
                w_type = row.get('work_type', '')
                p_name = row.get('project_name', '')
                desc = f"{p_type} - {w_type} ({p_name})".strip()

                # --- ID ---
                # Handle the '#' in 'permit_#'
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
        # Debug helper: if it fails, show us the columns again just in case
        return []

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
        response = requests.get(base_url, params=params, timeout=60)
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
            clean_austin = remove_duplicates(austin_data)
            supabase.table('permits').upsert(
                clean_austin, on_conflict='permit_id, city'
            ).execute()
            print(f">> Austin Data Sync Complete. ({len(clean_austin)} unique records)")
        
        # --- 2. FORT WORTH ---
        fw_data = ingest_fort_worth()
        if fw_data:
            clean_fw = remove_duplicates(fw_data)
            supabase.table('permits').upsert(
                clean_fw, on_conflict='permit_id, city'
            ).execute()
            print(f">> Fort Worth Data Sync Complete. ({len(clean_fw)} unique records)")

        # --- 3. SAN ANTONIO (NEW) ---
        sa_data = ingest_san_antonio()
        if sa_data:
            clean_sa = remove_duplicates(sa_data)
            supabase.table('permits').upsert(
                clean_sa, on_conflict='permit_id, city'
            ).execute()
            print(f">> San Antonio Data Sync Complete. ({len(clean_sa)} unique records)")
            
    except Exception as e:
        print(f"!! CRITICAL PIPELINE FAILURE: {e}")

# --- IGNITION ---
if __name__ == "__main__":
    run_pipeline()