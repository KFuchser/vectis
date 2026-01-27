"""
Ingestion spoke for San Antonio, TX.
SCHEMA LOCK: Maps 'filing_date' -> 'applied_date'.
"""
import requests
from service_models import PermitRecord, ComplexityTier

def get_san_antonio_data(cutoff_date: str) -> list[PermitRecord]:
    print(f"🤠 Starting San Antonio Sync (Cutoff: {cutoff_date})...")
    
    # Official Dataset: DSD Permits (cfm2-35h3)
    url = "https://data.sanantonio.gov/resource/cfm2-35h3.json"
    
    params = {
        "$where": f"issued_date >= '{cutoff_date}T00:00:00'",
        "$limit": 2000,
        "$order": "issued_date DESC"
    }

    try:
        response = requests.get(url, params=params, timeout=20)
        response.raise_for_status()
        data = response.json()

        if not data:
            print(f"⚠️ San Antonio: No records found.")
            return []

        mapped_records = []
        
        for r in data:
            def parse_date(date_str):
                try:
                    if date_str: return date_str.split("T")[0]
                except: pass
                return None

            # --- MAPPING LOCK ---
            # Source: filing_date (The day the clock starts)
            applied_iso = parse_date(r.get('filing_date'))
            issued_iso = parse_date(r.get('issued_date'))

            val = float(r.get('total_project_valuation', 0.0))
            desc = r.get('work_description') or r.get('project_name') or "Unspecified"

            record = PermitRecord(
                permit_id=str(r.get('permit_number', 'UNKNOWN')),
                city="San Antonio",
                status="Issued",
                applied_date=applied_iso, 
                issued_date=issued_iso,
                description=desc,
                valuation=val,
                complexity_tier=ComplexityTier.UNKNOWN 
            )
            mapped_records.append(record)
        
        print(f"✅ San Antonio: Retrieved {len(mapped_records)} records.")
        return mapped_records

    except Exception as e:
        print(f"❌ San Antonio API Error: {e}")
        return []