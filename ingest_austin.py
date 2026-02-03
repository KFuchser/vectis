"""
Ingestion spoke for Austin, TX (Socrata API).

This module handles fetching and normalizing building permit data from the City of Austin.
Endpoint: https://data.austintexas.gov/resource/3syk-w9eu.json

Key Logic:
- Sorts by `issue_date` DESC. Sorting by `applieddate` was found to hide recent data 
  because application dates can be significantly older than issue dates or null.
- Maps `permit_number` to `permit_id`.
"""
import requests
from service_models import PermitRecord, ComplexityTier

def get_austin_data(app_token, cutoff_date):
    """
    Fetches and normalizes building permit data from the City of Austin's Socrata API.

    Args:
        app_token: The Socrata application token for API authentication.
        cutoff_date: The earliest date for which to fetch permits, in 'YYYY-MM-DD' format.

    Returns:
        A list of `PermitRecord` objects, or an empty list if an error occurs.
    """
    print(f"🤠 Fetching Austin data since {cutoff_date}...")
    
    AUSTIN_API_URL = "https://data.austintexas.gov/resource/3syk-w9eu.json"
    
    # CRITICAL FIX: The Austin API's `applieddate` is often null or years in the past.
    # Sorting by `issue_date` is essential to get recent permits.
    params = {
        "$where": f"issue_date >= '{cutoff_date}T00:00:00'",
        "$limit": 5000,
        "$order": "issue_date DESC",
        "$$app_token": app_token
    }
    
    try:
        response = requests.get(AUSTIN_API_URL, params=params, timeout=30)
        
        if response.status_code != 200:
            print(f"❌ Austin API Error: {response.status_code}")
            return []
            
        data = response.json()
        if not data:
            print("⚠️ No Austin data returned.")
            return []
            
        records = []
        for item in data:
            # --- Data Normalization ---
            # The following lines map the raw API response to the standardized PermitRecord model.

            def parse_date(d):
                return d.split("T")[0] if d else None
            
            applied = parse_date(item.get("applieddate"))
            issued = parse_date(item.get("issue_date"))
            
            # The 'description' field is often empty; 'work_class' is a reliable fallback.
            desc = item.get("description") or item.get("work_class") or "Unspecified"
            
            # Ensure valuation is a float, defaulting to 0.0 if missing or invalid.
            try: val = float(item.get("valuation", 0.0) or 0.0)
            except: val = 0.0

            r = PermitRecord(
                city="Austin",
                permit_id=item.get("permit_number", "UNKNOWN"),
                applied_date=applied,
                issued_date=issued,
                description=desc,
                valuation=val,
                complexity_tier=ComplexityTier.UNKNOWN,
                status=item.get("status_current", "Issued")
            )
            records.append(r)
            
        print(f"✅ Austin: Retrieved {len(records)} records.")
        return records

    except Exception as e:
        print(f"❌ Austin Integration Error: {e}")
        return []