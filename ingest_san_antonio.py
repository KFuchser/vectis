"""
Ingestion spoke for San Antonio, TX.
VERIFIED SCHEMA (2026-01-27):
- Endpoint: CKAN datastore_search
- Resource: c21106f9-3ef5-4f3a-8604-f992b4db7512
- Applied Date: 'DATE SUBMITTED'
- Issued Date: 'DATE ISSUED'
"""
import requests
from service_models import PermitRecord, ComplexityTier

def get_san_antonio_data(cutoff_date: str) -> list[PermitRecord]:
    print(f"🤠 Starting San Antonio Sync (Verified CKAN)...")
    
    # URL and Resource ID verified by 'satest.py'
    url = "https://data.sanantonio.gov/api/3/action/datastore_search"
    resource_id = "c21106f9-3ef5-4f3a-8604-f992b4db7512"
    
    params = {
        "resource_id": resource_id,
        "limit": 3000, 
        "sort": "_id desc" # Get newest records first
    }

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if not data.get("success") or not data["result"]["records"]:
            print(f"⚠️ San Antonio: No records returned.")
            return []

        raw_records = data["result"]["records"]
        mapped_records = []
        
        for r in raw_records:
            # 1. PARSE DATES (Format: "2025-11-18")
            def parse_date(d_str):
                if not d_str: return None
                return str(d_str).split("T")[0]

            # 2. EXACT FIELD MAPPING (Based on satest.py output)
            issued_iso = parse_date(r.get("DATE ISSUED"))
            applied_iso = parse_date(r.get("DATE SUBMITTED"))

            # 3. FILTER
            # Only keep records issued after the cutoff
            if not issued_iso or issued_iso < cutoff_date:
                continue

            # 4. VALUATION
            # Handle nulls or strings like "$1,000.00"
            raw_val = r.get("DECLARED VALUATION")
            try:
                if raw_val:
                    val = float(str(raw_val).replace("$", "").replace(",", ""))
                else:
                    val = 0.0
            except:
                val = 0.0

            # 5. DESCRIPTION
            # Use Project Name, fallback to Work Type
            desc = r.get("PROJECT NAME") or r.get("WORK TYPE") or "Unspecified"

            record = PermitRecord(
                permit_id=str(r.get("PERMIT #", "UNKNOWN")),
                city="San Antonio",
                status="Issued",
                applied_date=applied_iso, # This enables Velocity calculation
                issued_date=issued_iso,
                description=desc,
                valuation=val,
                complexity_tier=ComplexityTier.UNKNOWN 
            )
            mapped_records.append(record)
        
        print(f"✅ San Antonio: Retrieved {len(mapped_records)} valid records.")
        return mapped_records

    except Exception as e:
        print(f"❌ San Antonio API Error: {e}")
        return []