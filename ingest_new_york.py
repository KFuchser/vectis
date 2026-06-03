"""
Ingestion spoke for New York, NY (Socrata API) using sodapy.

This module handles fetching and normalizing building permit data from the City of New York.
Endpoint: https://data.cityofnewyork.us/api/views/ipu4-2q9a/rows.json (SODA 3.0 compatible)

Key Logic:
- Uses sodapy library for robust Socrata API interaction.
- Maps Socrata API fields to `PermitRecord` fields.
"""
from sodapy import Socrata
from service_models import PermitRecord, ComplexityTier
from datetime import datetime, timedelta

def get_new_york_data(app_token, cutoff_date):
    """
    Fetches and normalizes building permit data from the City of New York's Socrata API.

    Args:
        app_token: The Socrata application token for API authentication.
        cutoff_date: The earliest date for which to fetch permits, in 'YYYY-MM-DD' format.

    Returns:
        A list of `PermitRecord` objects, or an empty list if an error occurs.
    """
    # --- HARD LIMIT: 24 MONTHS ---
    # Ensure the cutoff date is no further back than 24 months from today.
    max_history = (datetime.now() - timedelta(days=24*30)).strftime("%Y-%m-%d")
    final_cutoff = max(cutoff_date, max_history)
    
    print(f"🗽 Fetching New York data since {final_cutoff} (requested: {cutoff_date}) using sodapy...")
    
    # Socrata API endpoint details for SODA 3.0 /views API
    # Domain: data.cityofnewyork.us
    # Dataset ID: rbx6-tga4 (Newer DOB NOW data)
    client = Socrata("data.cityofnewyork.us", app_token=app_token)
    
    query_params = {
        "where": f"issued_date >= '{final_cutoff}' AND approved_date >= '{final_cutoff}'",
        "limit": 5000,
        "order": "issued_date DESC",
    }
    
    try:
        data = client.get("rbx6-tga4", **query_params)
        
        if not data:
            print("⚠️ No New York data returned.")
            return []
            
        records = []
        for item in data:
            # --- Data Normalization ---
            def parse_date(d):
                return d.split("T")[0] if d else None
            
            applied = parse_date(item.get("approved_date")) # Using approved_date as closest to applied_date
            issued = parse_date(item.get("issued_date"))
            
            desc = item.get("job_description") or "Unspecified"
            
            try: val = float(item.get("estimated_job_costs", 0.0) or 0.0) 
            except: val = 0.0

            r = PermitRecord(
                city="New York",
                permit_id=item.get("job_filing_number", "UNKNOWN"),
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

if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    load_dotenv()
    token = os.getenv("SOCRATA_APP_TOKEN")
    # Test with a very old date to verify the 24-month hard limit
    cutoff = "2000-01-01"
    results = get_new_york_data(token, cutoff)
    if results:
        print(f"Earliest record in batch: {results[-1].issued_date}")
        for r in results[:5]:
            print(r)