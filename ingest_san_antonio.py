"""
Ingestion spoke for San Antonio, TX (CKAN API).

This module handles fetching and normalizing building permit data from the City of San Antonio.
Endpoint: https://data.sanantonio.gov/api/3/action/datastore_search

Key Logic:
- Composite ID: Combines `PERMIT #` and internal `_id` (e.g., "12345_99") to guarantee uniqueness.
  The raw API often returns duplicate permit numbers for sub-tasks, causing database collisions.
- Filters out records with missing issue dates.
"""
import requests
from service_models import PermitRecord, ComplexityTier

def get_san_antonio_data(cutoff_date: str) -> list[PermitRecord]:
    """
    Fetches and normalizes building permit data from the City of San Antonio's CKAN API.

    Args:
        cutoff_date: The earliest date for which to fetch permits, in 'YYYY-MM-DD' format.

    Returns:
        A list of `PermitRecord` objects, or an empty list if an error occurs.
    """
    print(f"🤠 Starting San Antonio Sync (Composite ID Mode)...")
    
    # RESOURCE ID: Permits Issued 2020-Present
    url = "https://data.sanantonio.gov/api/3/action/datastore_search"
    params = {
        "resource_id": "c21106f9-3ef5-4f3a-8604-f992b4db7512",
        "limit": 3000, 
        "sort": "_id desc"
    }

    try:
        response = requests.get(url, params=params, timeout=30)
        data = response.json()
        
        if not data.get("success"):
            return []

        raw_records = data["result"]["records"]
        mapped_records = []
        
        for r in raw_records:
            def parse_date(d_str):
                return str(d_str).split("T")[0] if d_str else None

            # --- Data Normalization ---
            # The following lines map the raw API response to the standardized PermitRecord model.

            # DATES
            issued_iso = parse_date(r.get("DATE ISSUED"))
            applied_iso = parse_date(r.get("DATE SUBMITTED"))

            if not issued_iso or issued_iso < cutoff_date: continue

            # CRITICAL FIX: The API often returns duplicate permit numbers for sub-tasks.
            # A composite ID (Permit # + Internal ID) is required to prevent data loss.
            permit_no = str(r.get("PERMIT #", "UNKNOWN"))
            internal_id = str(r.get("_id"))
            
            # Format: "PERMIT-NUM_SYSTEM-ID"
            unique_pid = f"{permit_no}_{internal_id}"

            # VALUATION: The API returns valuation as a currency string (e.g., "$5,000.00").
            # This needs to be cleaned and converted to a float.
            raw_val = r.get("DECLARED VALUATION")
            try:
                val = float(str(raw_val).replace("$", "").replace(",", "")) if raw_val else 0.0
            except:
                val = 0.0

            record = PermitRecord(
                permit_id=unique_pid,
                city="San Antonio",
                status="Issued",
                applied_date=applied_iso,
                issued_date=issued_iso,
                description=r.get("PROJECT NAME") or r.get("WORK TYPE") or "Unspecified",
                valuation=val,
                complexity_tier=ComplexityTier.UNKNOWN 
            )
            mapped_records.append(record)
        
        print(f"✅ San Antonio: Processed {len(mapped_records)} unique records.")
        return mapped_records

    except Exception as e:
        print(f"❌ San Antonio Error: {e}")
        return []