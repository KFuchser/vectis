"""
Ingestion spoke for Dallas, TX (Socrata API).

This module handles fetching and normalizing building permit data from the City of Dallas.
Endpoint: https://www.dallasopendata.com/resource/e7gq-4sah.json

Key Logic:
- Sorts by `issue_date` DESC.
- Maps Socrata API fields to `PermitRecord` fields.
"""
import requests
from service_models import PermitRecord, ComplexityTier

def get_dallas_data(app_token, cutoff_date):
    """
    Fetches and normalizes building permit data from the City of Dallas's Socrata API.

    Args:
        app_token: The Socrata application token for API authentication.
        cutoff_date: The earliest date for which to fetch permits, in 'YYYY-MM-DD' format.

    Returns:
        A list of `PermitRecord` objects, or an empty list if an error occurs.
    """
    print(f"🤠 Fetching Dallas data since {cutoff_date}...")
    
    DALLAS_API_URL = "https://www.dallasopendata.com/resource/e7gq-4sah.json"
    
    # Debugging: Temporarily set a very old cutoff_date to check if any data is returned
    debug_cutoff_date = "2000-01-01" 

    params = {
        # "$where": f"issued_date >= '{debug_cutoff_date}'", # Temporarily removed for debugging
        "$limit": 1, # Reduced for debugging
        "$order": "issued_date DESC",
        "$$app_token": app_token
    }
    
    try:
        response = requests.get(DALLAS_API_URL, params=params, timeout=30)
        
        if response.status_code != 200:
            print(f"❌ Dallas API Error: {response.status_code}")
            print(f"API Response: {response.text}") # Added for debugging
            return []
            
        data = response.json()
        if not data:
            print("⚠️ No Dallas data returned.")
            return []
            
        records = []
        for item in data:
            # --- Data Normalization ---
            def parse_date(d):
                return d.split("T")[0] if d else None
            
            # These field names are guesses based on common Socrata patterns.
            # Will need to verify if data returned or error occurs.
            applied = None # No 'application_date' found in Dallas API response
            issued = parse_date(item.get("issued_date"))
            
            desc = item.get("description") or item.get("work_description") or "Unspecified"
            
            try: val = float(item.get("valuation", 0.0) or 0.0)
            except: val = 0.0

            r = PermitRecord(
                city="Dallas",
                permit_id=item.get("permit_number", "UNKNOWN"),
                applied_date=applied,
                issued_date=issued,
                description=desc,
                valuation=val,
                complexity_tier=ComplexityTier.UNKNOWN,
                status=item.get("status", "Issued")
            )
            records.append(r)
            
        print(f"✅ Dallas: Retrieved {len(records)} records.")
        return records

    except Exception as e:
        print(f"❌ Dallas Integration Error: {e}")
        return []
