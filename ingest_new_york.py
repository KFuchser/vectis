"""
Ingestion spoke for New York, NY (Socrata API).

This module handles fetching and normalizing building permit data from the City of New York.
Endpoint: https://data.cityofnewyork.us/resource/ipu4-2q9a.json

Key Logic:
- Sorts by `issued_date` DESC.
- Maps Socrata API fields to `PermitRecord` fields.
"""
import requests
from service_models import PermitRecord, ComplexityTier

def get_new_york_data(app_token, cutoff_date):
    """
    Fetches and normalizes building permit data from the City of New York's Socrata API.

    Args:
        app_token: The Socrata application token for API authentication.
        cutoff_date: The earliest date for which to fetch permits, in 'YYYY-MM-DD' format.

    Returns:
        A list of `PermitRecord` objects, or an empty list if an error occurs.
    """
    print(f"🗽 Fetching New York data since {cutoff_date}...")
    
    NEW_YORK_API_URL = "https://data.cityofnewyork.us/resource/ipu4-2q9a.json"
    
    params = {
        "$where": f"issued_date >= '{cutoff_date}T00:00:00'",
        "$limit": 5000,
        "$order": "issued_date DESC",
        "$$app_token": app_token
    }
    
    try:
        response = requests.get(NEW_YORK_API_URL, params=params, timeout=30)
        
        if response.status_code != 200:
            print(f"❌ New York API Error: {response.status_code}")
            return []
            
        data = response.json()
        if not data:
            print("⚠️ No New York data returned.")
            return []
            
        records = []
        for item in data:
            # --- Data Normalization ---
            # The following lines map the raw API response to the standardized PermitRecord model.

            def parse_date(d):
                return d.split("T")[0] if d else None
            
            applied = parse_date(item.get("filing_date"))
            issued = parse_date(item.get("issued_date"))
            
            desc = item.get("description") or "Unspecified"
            
            try: val = float(item.get("total_estimated_cost", 0.0) or 0.0)
            except: val = 0.0

            r = PermitRecord(
                city="New York",
                permit_id=item.get("permit_si_no", "UNKNOWN"),
                applied_date=applied,
                issued_date=issued,
                description=desc,
                valuation=val,
                complexity_tier=ComplexityTier.UNKNOWN,
                status=item.get("permit_status", "Issued")
            )
            records.append(r)
            
        print(f"✅ New York: Retrieved {len(records)} records.")
        return records

    except Exception as e:
        print(f"❌ New York Integration Error: {e}")
        return []
