"""
Ingestion spoke for Dallas, TX (Socrata API).

This module handles fetching and normalizing building permit data from the City of Dallas.
Endpoint: https://www.dallasopendata.com/resource/e7gq-4sah.json

Key Logic:
- Sorts by `issued_date` DESC.
- Maps Socrata API fields to `PermitRecord` fields.
"""
import requests
from service_models import PermitRecord, ComplexityTier
from datetime import datetime # Import datetime for date parsing

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
    
    params = {
        # "$where": f"issued_date >= '{cutoff_date}'", # Temporarily removed for debugging
        "$limit": 1, # Reduced for debugging
        "$order": "issued_date DESC",
        "$$app_token": app_token
    }
    
    try:
        response = requests.get(DALLAS_API_URL, params=params, timeout=30)
        
        if response.status_code != 200:
            print(f"❌ Dallas API Error: {response.status_code}")
            print(f"API Response: {response.text}") 
            return []
            
        data = response.json()
        
        if not data:
            print("⚠️ No Dallas data returned.")
            return []
            
        # DEBUGGING: Print issued_date of the first record if available
        if data:
            first_record_issue_date = data[0].get("issued_date")
            print(f"DEBUG: Dallas - First record issued_date: {first_record_issue_date}")
            
        records = []
        for item in data:
            # --- Data Normalization ---
            def parse_date(d_str):
                if not d_str: return None
                try:
                    # Try parsing MM/DD/YY
                    return datetime.strptime(d_str, "%m/%d/%y").strftime("%Y-%m-%d")
                except ValueError:
                    try:
                        # Try parsing MM/DD/YYYY
                        return datetime.strptime(d_str, "%m/%d/%Y").strftime("%Y-%m-%d")
                    except ValueError:
                        # Fallback for other formats or return None
                        return None
            
            applied = None # No 'application_date' found in Dallas API response
            issued = parse_date(item.get("issued_date"))
            
            desc = item.get("work_description") or "Unspecified" # Mapped to work_description
            
            try: val = float(item.get("value", 0.0) or 0.0) # Mapped to 'value'
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
