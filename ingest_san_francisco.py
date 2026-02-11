"""
Ingestion spoke for San Francisco, CA (Socrata API).

This module handles fetching and normalizing building permit data from the City of San Francisco.
Endpoint: https://data.sfgov.org/resource/i98e-djp9.json

Key Logic:
- Sorts by `issued_date` DESC.
- Maps Socrata API fields to `PermitRecord` fields.
"""
import requests
from service_models import PermitRecord, ComplexityTier

def get_san_francisco_data(app_token, cutoff_date):
    """
    Fetches and normalizes building permit data from the City of San Francisco's Socrata API.

    Args:
        app_token: The Socrata application token for API authentication.
        cutoff_date: The earliest date for which to fetch permits, in 'YYYY-MM-DD' format.

    Returns:
        A list of `PermitRecord` objects, or an empty list if an error occurs.
    """
    print(f"🌉 Fetching San Francisco data since {cutoff_date}...")
    
    SAN_FRANCISCO_API_URL = "https://data.sfgov.org/resource/i98e-djp9.json"
    
    params = {
        "$where": f"issued_date >= '{cutoff_date}T00:00:00'",
        "$limit": 5000,
        "$order": "issued_date DESC",
        "$$app_token": app_token
    }
    
    try:
        response = requests.get(SAN_FRANCISCO_API_URL, params=params, timeout=30)
        
        if response.status_code != 200:
            print(f"❌ San Francisco API Error: {response.status_code}")
            return []
            
        data = response.json()
        if not data:
            print("⚠️ No San Francisco data returned.")
            return []
            
        records = []
        for item in data:
            # --- Data Normalization ---
            # The following lines map the raw API response to the standardized PermitRecord model.

            def parse_date(d):
                return d.split("T")[0] if d else None
            
            applied = parse_date(item.get("filed_date"))
            issued = parse_date(item.get("issued_date"))
            
            desc = item.get("description") or "Unspecified"
            
            try: val = float(item.get("estimated_cost", 0.0) or 0.0)
            except: val = 0.0

            r = PermitRecord(
                city="San Francisco",
                permit_id=item.get("permit_number", "UNKNOWN"),
                applied_date=applied,
                issued_date=issued,
                description=desc,
                valuation=val,
                complexity_tier=ComplexityTier.UNKNOWN,
                status=item.get("status", "Issued")
            )
            records.append(r)
            
        print(f"✅ San Francisco: Retrieved {len(records)} records.")
        return records

    except Exception as e:
        print(f"❌ San Francisco Integration Error: {e}")
        return []
