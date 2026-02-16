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
    print(f"🗽 Fetching New York data since {cutoff_date} using sodapy...")
    
    # Socrata API endpoint details for SODA 3.0 /views API
    # Domain: data.cityofnewyork.us
    # Dataset ID: ipu4-2q9a
    client = Socrata("data.cityofnewyork.us", app_token=app_token)
    
    # Query: SELECT * WHERE issuance_date >= 'YYYY-MM-DD' ORDER BY issuance_date DESC LIMIT 5000
    # Note: sodapy handles the '$' prefix for parameters
    query_params = {
        "where": f"issuance_date >= '{cutoff_date}'",
        "limit": 5000,
        "order": "issuance_date DESC",
    }
    
    try:
        # The SODA 3.0 /views API typically returns data directly as a list of dictionaries
        data = client.get("ipu4-2q9a", **query_params)
        
        if not data:
            print("⚠️ No New York data returned.")
            return []
            
        records = []
        for item in data:
            # --- Data Normalization ---
            # These field names are based on inspection of ipu4-2q9a dataset attributes
            def parse_date(d):
                return d.split("T")[0] if d else None
            
            applied = parse_date(item.get("filing_date"))
            issued = parse_date(item.get("issuance_date"))
            
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