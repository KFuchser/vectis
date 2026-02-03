"""
Ingestion spoke for Los Angeles, CA (Socrata API).

This module handles fetching and normalizing building permit data from the City of Los Angeles.
Endpoint: https://data.lacity.org/resource/pi9x-tg5x.json

Key Logic:
- Timeout: Set to 60 seconds because the LA Socrata endpoint is historically slow.
- Schema Limitation: The source does not reliably publish application dates, so `applied_date` is set to None.
  This means LA data contributes to Volume metrics but not Velocity (Lead Time) metrics.
"""
import requests
from service_models import PermitRecord, ComplexityTier

def get_la_data(cutoff_date, socrata_token=None):
    """
    Fetches and normalizes building permit data from the City of Los Angeles' Socrata API.

    Args:
        cutoff_date: The earliest date for which to fetch permits, in 'YYYY-MM-DD' format.
        socrata_token: The Socrata application token for API authentication.

    Returns:
        A list of `PermitRecord` objects, or an empty list if an error occurs.
    """
    print(f"🤠 Fetching LA data (Volume Only)...")
    
    LA_ENDPOINT = "https://data.lacity.org/resource/pi9x-tg5x.json"
    
    query = (
        f"$select=permit_nbr,issue_date,valuation,work_desc,status_desc"
        f"&$where=issue_date >= '{cutoff_date}T00:00:00'"
        f"&$limit=2000"
        f"&$order=issue_date DESC"
    )
    
    headers = {}
    if socrata_token:
        headers["X-App-Token"] = socrata_token
        
    try:
        # CRITICAL: Increased timeout to 60 seconds as the LA Socrata endpoint is notoriously slow.
        resp = requests.get(f"{LA_ENDPOINT}?{query}", headers=headers, timeout=60)
        if resp.status_code != 200: 
            print(f"❌ LA API Error: {resp.status_code}")
            return []
            
        data = resp.json()
        records = []
        
        for r in data:
            # --- Data Normalization ---
            # The following lines map the raw API response to the standardized PermitRecord model.

            def parse_date(d):
                return d.split("T")[0] if d else None

            # The LA API does not provide a reliable application date.
            applied = None 
            issued = parse_date(r.get("issue_date"))
            
            # Ensure valuation is a float, defaulting to 0.0 if missing or invalid.
            val_raw = r.get("valuation", "0")
            try: val = float(val_raw)
            except: val = 0.0

            records.append(PermitRecord(
                permit_id=r.get("permit_nbr"),
                city="Los Angeles",
                applied_date=applied,
                issued_date=issued,
                valuation=val,
                description=r.get("work_desc", "No Description"),
                status=r.get("status_desc", "Issued"),
                complexity_tier=ComplexityTier.UNKNOWN
            ))
            
        print(f"✅ Los Angeles: Retrieved {len(records)} records (Volume Only).")
        return records
        
    except Exception as e:
        print(f"❌ LA API Error: {e}")
        return []