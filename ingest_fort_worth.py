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


def _parse_ms_date(ms):
    """Converts an ArcGIS Unix timestamp (milliseconds) to YYYY-MM-DD, or returns None."""
    try:
        if ms:
            return datetime.fromtimestamp(ms / 1000.0).strftime('%Y-%m-%d')
    except (OSError, ValueError, TypeError):
        pass
    return None

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
    
    PAGE_SIZE = 1000
    all_features = []
    offset = 0

    try:
        while True:
            params = {
                "where": f"Status_Date >= '{cutoff_date} 00:00:00'",
                "outFields": "*",
                "outSR": "4326",
                "f": "json",
                "resultRecordCount": PAGE_SIZE,
                "resultOffset": offset,
                "orderByFields": "Status_Date DESC"
            }
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            features = data.get("features", [])
            if not features:
                break

            all_features.extend(features)

            if len(features) < PAGE_SIZE:
                break

            offset += PAGE_SIZE
            print(f"   Fort Worth: fetched {len(all_features)} records so far...")

        if not all_features:
            print("Fort Worth: No records found.")
            return []

        raw_records = [f["attributes"] for f in all_features]
        mapped_records = []

        for r in raw_records:
            applied_iso = _parse_ms_date(r.get('File_Date'))
            issued_iso = _parse_ms_date(r.get('Status_Date'))

            if not issued_iso: continue

            # Map other fields to the PermitRecord model.
            desc = r.get('B1_WORK_DESC') or r.get('Permit_Type') or "Unspecified"
            val = float(r.get('JobValue') or 0.0)
            pid = str(r.get('Permit_No', 'UNKNOWN'))

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