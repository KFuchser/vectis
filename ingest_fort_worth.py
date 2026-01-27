"""
Ingestion spoke for Fort Worth, TX.
Robust date handling to ensure Velocity metrics populate.
"""
import requests
from datetime import datetime
from service_models import PermitRecord, ComplexityTier

def get_fort_worth_data(cutoff_date: str) -> list[PermitRecord]:
    print(f"🤠 Starting Fort Worth Sync (Cutoff: {cutoff_date})...")
    
    url = "https://services5.arcgis.com/3ddLCBXe1bRt7mzj/arcgis/rest/services/CFW_Open_Data_Development_Permits_View/FeatureServer/0/query"
    
    # Fetch ALL fields to ensure we don't miss the date column
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
        
        # Current time for sanity checking
        now_ts = datetime.now().timestamp()
        
        for r in raw_records:
            def get_val(key_list):
                for k in key_list:
                    if k in r: return r[k]
                    if k.upper() in r: return r[k.upper()]
                    if k.lower() in r: return r[k.lower()]
                return None

            def parse_date(ms):
                try:
                    if ms: 
                        # SANITY CHECK: Ignore dates more than 7 days in the future
                        # This fixes the "2026-08-13" expiration date bug
                        if (ms / 1000.0) > (now_ts + 7 * 86400):
                            return None
                        return datetime.fromtimestamp(ms / 1000.0).strftime('%Y-%m-%d')
                except: pass
                return None

            # Handle Dates with robustness
            issued_iso = parse_date(get_val(['Status_Date', 'STATUS_DATE']))
            applied_val = get_val(['Applied_Date', 'APPLIED_DATE', 'applied_date'])
            applied_iso = parse_date(applied_val)
            
            # If issued date was filtered out because it was in the future, skip record
            if not issued_iso and get_val(['Status_Date', 'STATUS_DATE']):
                continue

            desc = get_val(['B1_WORK_DESC', 'B1_Work_Desc']) or get_val(['Permit_Type']) or "Unspecified"
            val = float(get_val(['JobValue', 'JOBVALUE']) or 0.0)
            pid = str(get_val(['Permit_No', 'PERMIT_NO']))

            record = PermitRecord(
                permit_id=pid,
                city="Fort Worth",
                status="Issued",
                applied_date=applied_iso,
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