"""
Ingestion spoke for San Antonio, TX.
PLATFORM MIGRATION: Socrata -> CKAN (SQL).
Fixes 404 Error by using the new resource ID.
"""
import requests
import urllib.parse
from datetime import datetime
from service_models import PermitRecord, ComplexityTier

def get_san_antonio_data(cutoff_date: str) -> list[PermitRecord]:
    print(f"🤠 Starting San Antonio Sync (CKAN Platform)...")
    
    # NEW RESOURCE ID (Permits Issued 2020-Present)
    # Replaces the dead 'cfm2-35h3' endpoint
    CKAN_ID = "c21106f9-3ef5-4f3a-8604-f992b4db7512"
    base_url = "https://data.sanantonio.gov/api/3/action/datastore_search_sql"
    
    # SQL Query: Fetch recent permits
    # We fetch by _id DESC to get the newest entries
    sql = f"""SELECT * from "{CKAN_ID}" ORDER BY "_id" DESC LIMIT 3000"""
    
    encoded_sql = urllib.parse.quote(sql)
    url = f"{base_url}?sql={encoded_sql}"

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if not data.get("success") or not data["result"]["records"]:
            print(f"⚠️ San Antonio: No records found.")
            return []

        raw_records = data["result"]["records"]
        mapped_records = []
        
        for r in raw_records:
            def parse_ckan_date(d_str):
                # Formats can vary: "2026-01-20T00:00:00" or "2026-01-20"
                if not d_str: return None
                return d_str.split("T")[0]

            # FIELD MAPPING
            # DATE ISSUED -> issued_date
            # DATE SUBMITTED -> applied_date (Velocity Key)
            issued_iso = parse_ckan_date(r.get("DATE ISSUED"))
            applied_iso = parse_ckan_date(r.get("DATE SUBMITTED"))

            # Filter by cutoff manually since we didn't do it in SQL
            if not issued_iso or issued_iso < cutoff_date:
                continue

            # Valuation cleanup
            val_str = str(r.get("DECLARED VALUATION") or "0").replace("$", "").replace(",", "")
            try: val = float(val_str)
            except: val = 0.0

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
        
        print(f"✅ San Antonio: Retrieved {len(mapped_records)} records.")
        return mapped_records

    except Exception as e:
        print(f"❌ San Antonio API Error: {e}")
        return []