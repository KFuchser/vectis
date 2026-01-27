"""
Ingestion spoke for Fort Worth, TX.
SCHEMA LOCK: Robustly hunts for 'Applied_Date'.
"""
"""
Ingestion spoke for Fort Worth, TX.
VERIFIED SCHEMA (2026-01-27):
- Applied Date: 'File_Date' (Found via satest.py)
- Issued Date: 'Status_Date'
- Future Date Handling: Dashboard will filter > Now.
"""
import requests
from datetime import datetime
from service_models import PermitRecord, ComplexityTier

def get_fort_worth_data(cutoff_date: str) -> list[PermitRecord]:
    print(f"🤠 Starting Fort Worth Sync (Schema Verified)...")
    
    url = "https://services5.arcgis.com/3ddLCBXe1bRt7mzj/arcgis/rest/services/CFW_Open_Data_Development_Permits_View/FeatureServer/0/query"
    
    params = {
        "where": f"Status_Date >= '{cutoff_date} 00:00:00'",
        "outFields": "*",
        "outSR": "4326",
        "f": "json",
        "resultRecordCount": 2000, 
        "orderByFields": "Status_Date DESC"
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        if "features" not in data or not data["features"]:
            print(f"⚠️ Fort Worth: No records found.")
            return []

        raw_records = [f["attributes"] for f in data["features"]]
        mapped_records = []
        
        for r in raw_records:
            # 1. PARSE DATES (ArcGIS uses Unix Timestamps in milliseconds)
            def parse_ms_date(ms):
                try:
                    if ms: 
                        return datetime.fromtimestamp(ms / 1000.0).strftime('%Y-%m-%d')
                except: pass
                return None

            # --- MAPPING LOCK (Based on satest.py) ---
            # 'File_Date' is the Application Date (Jan 2025 in your logs)
            applied_iso = parse_ms_date(r.get('File_Date'))
            
            # 'Status_Date' is the Issue/Expiration Date (Aug 2026 in your logs)
            issued_iso = parse_ms_date(r.get('Status_Date'))

            if not issued_iso: continue

            # 2. OTHER FIELDS
            desc = r.get('B1_WORK_DESC') or r.get('Permit_Type') or "Unspecified"
            val = float(r.get('JobValue') or 0.0)
            pid = str(r.get('Permit_No', 'UNKNOWN'))

            record = PermitRecord(
                permit_id=pid,
                city="Fort Worth",
                status="Issued",
                applied_date=applied_iso, # Now populated!
                issued_date=issued_iso,
                description=desc,
                valuation=val,
                complexity_tier=ComplexityTier.UNKNOWN 
            )
            mapped_records.append(record)
        
        print(f"✅ Fort Worth: Retrieved {len(mapped_records)} records.")
        return mapped_records

    except Exception as e:
        print(f"❌ Fort Worth API Error: {e}")
        return []