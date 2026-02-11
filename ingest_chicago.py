"""
Ingestion spoke for Chicago, IL (Socrata API).

This module handles fetching and normalizing building permit data from the City of Chicago.
Endpoint: https://data.cityofchicago.org/resource/ydr8-5enu.json

Key Logic:
- Sorts by `issue_date` DESC.
- Maps Socrata API fields to `PermitRecord` fields.
"""
import requests
from service_models import PermitRecord, ComplexityTier

def get_chicago_data(app_token, cutoff_date):
    """
    Fetches and normalizes building permit data from the City of Chicago's Socrata API.

    Args:
        app_token: The Socrata application token for API authentication.
        cutoff_date: The earliest date for which to fetch permits, in 'YYYY-MM-DD' format.

    Returns:
        A list of `PermitRecord` objects, or an empty list if an error occurs.
    """
    print(f"🏙️ Fetching Chicago data since {cutoff_date}...")
    
    CHICAGO_API_URL = "https://data.cityofchicago.org/resource/ydr8-5enu.json"
    
    params = {
        "$where": f"issue_date >= '{cutoff_date}T00:00:00'",
        "$limit": 5000,
        "$order": "issue_date DESC",
        "$$app_token": app_token
    }
    
    try:
        response = requests.get(CHICAGO_API_URL, params=params, timeout=30)
        
        if response.status_code != 200:
            print(f"❌ Chicago API Error: {response.status_code}")
            return []
            
        data = response.json()
        if not data:
            print("⚠️ No Chicago data returned.")
            return []
            
        records = []
        for item in data:
            # --- Data Normalization ---
            # The following lines map the raw API response to the standardized PermitRecord model.

            def parse_date(d):
                return d.split("T")[0] if d else None
            
            applied = parse_date(item.get("application_start_date"))
            issued = parse_date(item.get("issue_date"))
            
            desc = item.get("work_description") or "Unspecified"
            
            try: val = float(item.get("estimated_cost", 0.0) or 0.0)
            except: val = 0.0

            r = PermitRecord(
                city="Chicago",
                permit_id=item.get("permit_", "UNKNOWN"),
                applied_date=applied,
                issued_date=issued,
                description=desc,
                valuation=val,
                complexity_tier=ComplexityTier.UNKNOWN,
                status=item.get("permit_status", "Issued")
            )
            records.append(r)
            
        print(f"✅ Chicago: Retrieved {len(records)} records.")
        return records

    except Exception as e:
        print(f"❌ Chicago Integration Error: {e}")
        return []
