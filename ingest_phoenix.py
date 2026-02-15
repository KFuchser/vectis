"""
Ingestion spoke for Phoenix, AZ (ArcGIS API).

This module handles fetching and normalizing building permit data from the City of Phoenix.
Endpoint: https://services.arcgis.com/pdv6O6pX596L23T6/arcgis/rest/services/Open_Data_Development_Permits/FeatureServer/0/query
"""
import requests
from datetime import datetime
from service_models import PermitRecord, ComplexityTier

def get_phoenix_data(cutoff_date: str) -> list[PermitRecord]:
    """
    Fetches and normalizes building permit data from the City of Phoenix's ArcGIS API.

    Args:
        cutoff_date: The earliest date for which to fetch permits, in 'YYYY-MM-DD' format.

    Returns:
        A list of `PermitRecord` objects, or an empty list if an error occurs.
    """
    print(f"🤠 Starting Phoenix Sync...")
    
    url = "https://services.arcgis.com/pdv6O6pX596L23T6/arcgis/rest/services/Open_Data_Development_Permits/FeatureServer/0/query"
    
    # Convert cutoff_date (YYYY-MM-DD) to Unix timestamp in milliseconds
    cutoff_dt = datetime.strptime(cutoff_date, "%Y-%m-%d")
    cutoff_timestamp_ms = int(cutoff_dt.timestamp() * 1000)

    params = {
        # "where": f"ISSUED_DATE >= {cutoff_timestamp_ms}", # Temporarily removed for debugging
        "outFields": "*",
        "outSR": "4326",
        "f": "json",
        "resultRecordCount": 2000, 
        "orderByFields": "ISSUED_DATE DESC"
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        if "features" not in data or not data["features"]:
            print(f"⚠️ Phoenix: No records found.")
            return []

        raw_records = [f["attributes"] for f in data["features"]]
        mapped_records = []
        
        for r in raw_records:
            # --- Data Normalization ---
            def parse_ms_date(ms):
                """Converts ArcGIS Unix timestamps (milliseconds) to ISO dates."""
                try:
                    if ms: 
                        return datetime.fromtimestamp(ms / 1000.0).strftime('%Y-%m-%d')
                except: pass
                return None

            applied_iso = parse_ms_date(r.get('APPLIED_DATE'))
            issued_iso = parse_ms_date(r.get('ISSUED_DATE'))

            if not issued_iso: continue

            desc = r.get('DESCRIPTION') or "Unspecified"
            val = float(r.get('TOTAL_VALUATION') or 0.0)
            pid = str(r.get('PERMIT_NUMBER', 'UNKNOWN'))

            record = PermitRecord(
                permit_id=pid,
                city="Phoenix",
                status="Issued",
                applied_date=applied_iso,
                issued_date=issued_iso,
                description=desc,
                valuation=val,
                complexity_tier=ComplexityTier.UNKNOWN 
            )
            mapped_records.append(record)
        
        print(f"✅ Phoenix: Retrieved {len(mapped_records)} records.")
        return mapped_records

    except Exception as e:
        print(f"❌ Phoenix API Error: {e}")
        return []
