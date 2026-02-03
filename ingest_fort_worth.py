"""
Ingestion spoke for Fort Worth, TX (ArcGIS API).

This module handles fetching and normalizing building permit data from the City of Fort Worth.
Endpoint: ArcGIS FeatureServer

Key Logic:
- Date Parsing: Converts ArcGIS Unix timestamps (milliseconds) to ISO dates.
- Field Mapping:
  - `File_Date` -> Applied Date
  - `Status_Date` -> Issued Date
- Note: Fort Worth often publishes expiration dates in `Status_Date` that are in the future.
  These are handled downstream in the dashboard via the "Time Guard".
"""
import requests
from datetime import datetime
from service_models import PermitRecord, ComplexityTier

def get_fort_worth_data(cutoff_date: str) -> list[PermitRecord]:
    """
    Fetches and normalizes building permit data from the City of Fort Worth's ArcGIS API.

    Args:
        cutoff_date: The earliest date for which to fetch permits, in 'YYYY-MM-DD' format.

    Returns:
        A list of `PermitRecord` objects, or an empty list if an error occurs.
    """
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
            # --- Data Normalization ---
            # The following lines map the raw API response to the standardized PermitRecord model.

            def parse_ms_date(ms):
                """Converts ArcGIS Unix timestamps (milliseconds) to ISO dates."""
                try:
                    if ms: 
                        return datetime.fromtimestamp(ms / 1000.0).strftime('%Y-%m-%d')
                except: pass
                return None

            # `File_Date` corresponds to the application date.
            applied_iso = parse_ms_date(r.get('File_Date'))
            
            # `Status_Date` corresponds to the issue date or, in some cases, a future expiration date.
            # This is handled by the "Time Guard" in the main orchestrator.
            issued_iso = parse_ms_date(r.get('Status_Date'))

            if not issued_iso: continue

            # Map other fields to the PermitRecord model.
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