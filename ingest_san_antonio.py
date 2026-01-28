"""
Ingestion spoke for San Antonio, TX.
LOGIC: Cloned from successful 'satest.py'.
"""
import requests
from service_models import PermitRecord, ComplexityTier

def get_san_antonio_data(cutoff_date: str) -> list[PermitRecord]:
    print(f"🤠 Starting San Antonio Sync (satest.py Clone)...")
    
    # EXACT URL and RESOURCE ID from satest.py
    url = "https://data.sanantonio.gov/api/3/action/datastore_search"
    params = {
        "resource_id": "c21106f9-3ef5-4f3a-8604-f992b4db7512",
        "limit": 3000, 
        "sort": "_id desc"
    }

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if not data.get("success"):
            print(f"⚠️ San Antonio: API reported failure.")
            return []

        raw_records = data["result"]["records"]
        print(f"   ↳ API Connection Successful. Downloaded {len(raw_records)} raw records.")

        mapped_records = []
        
        # --- DEBUG: Print first record to confirm we see what satest.py saw ---
        if raw_records:
            first = raw_records[0]
            print(f"   ↳ DEBUG SAMPLE: Issued='{first.get('DATE ISSUED')}', Applied='{first.get('DATE SUBMITTED')}'")
        # ---------------------------------------------------------------------

        for r in raw_records:
            # 1. DATE PARSING (Simple String Split)
            def parse_date(d_str):
                if not d_str: return None
                return str(d_str).split("T")[0]

            issued_iso = parse_date(r.get("DATE ISSUED"))
            applied_iso = parse_date(r.get("DATE SUBMITTED"))

            # 2. FILTERING
            if not issued_iso:
                continue
            if issued_iso < cutoff_date:
                continue

            # 3. VALUATION
            raw_val = r.get("DECLARED VALUATION")
            try:
                if raw_val:
                    val = float(str(raw_val).replace("$", "").replace(",", ""))
                else:
                    val = 0.0
            except:
                val = 0.0

            # 4. DESCRIPTION
            desc = r.get("PROJECT NAME") or r.get("WORK TYPE") or "Unspecified"

            record = PermitRecord(
                permit_id=str(r.get("PERMIT #", "UNKNOWN")),
                city="San Antonio",
                status="Issued",
                applied_date=applied_iso,
                issued_date=issued_iso,
                description=desc,
                valuation=val,
                complexity_tier=ComplexityTier.UNKNOWN 
            )
            mapped_records.append(record)
        
        print(f"✅ San Antonio: Filtered & Kept {len(mapped_records)} records (Cutoff: {cutoff_date}).")
        return mapped_records

    except Exception as e:
        print(f"❌ San Antonio Integration Error: {e}")
        return []