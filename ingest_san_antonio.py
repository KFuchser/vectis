"""
Ingestion spoke for San Antonio, TX.
FIX: COMPOSITE ID (Permit # + System ID).
Solves the "228 Duplicates" collision issue verified by diagnostics.
"""
import requests
from service_models import PermitRecord, ComplexityTier

def get_san_antonio_data(cutoff_date: str) -> list[PermitRecord]:
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

            # DATES
            issued_iso = parse_date(r.get("DATE ISSUED"))
            applied_iso = parse_date(r.get("DATE SUBMITTED"))

            if not issued_iso or issued_iso < cutoff_date: continue

            # --- THE FIX: COMPOSITE ID ---
            # Combines Permit Number + Internal ID to guarantee uniqueness
            # Fixes the 228 duplicates you found in diagnostics
            permit_no = str(r.get("PERMIT #", "UNKNOWN"))
            internal_id = str(r.get("_id"))
            
            # Format: "PERMIT-NUM_SYSTEM-ID"
            unique_pid = f"{permit_no}_{internal_id}"

            # VALUATION (Handle messy currency strings)
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