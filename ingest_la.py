"""
Ingestion spoke for Los Angeles, CA.
SCHEMA LOCK: Maps 'submit_date' -> 'applied_date'.
"""
import requests
from service_models import PermitRecord, ComplexityTier

def get_la_data(cutoff_date, socrata_token=None):
    print(f"🤠 Fetching LA data since {cutoff_date}...")
    
    # LA Building and Safety - Permits Issued (pi9x-tg5x)
    LA_ENDPOINT = "https://data.lacity.org/resource/pi9x-tg5x.json"
    
    # We explicitly request submit_date
    query = (
        f"$select=permit_nbr,issue_date,submit_date,valuation,work_desc,status_desc"
        f"&$where=issue_date >= '{cutoff_date}T00:00:00'"
        f"&$limit=2000"
        f"&$order=issue_date DESC"
    )
    
    headers = {}
    if socrata_token:
        headers["X-App-Token"] = socrata_token
        
    try:
        resp = requests.get(f"{LA_ENDPOINT}?{query}", headers=headers, timeout=20)
        if resp.status_code != 200: 
            return []
            
        data = resp.json()
        records = []
        
        for r in data:
            def parse_date(d):
                return d.split("T")[0] if d else None

            # --- MAPPING LOCK ---
            applied = parse_date(r.get("submit_date"))
            issued = parse_date(r.get("issue_date"))
            
            val_raw = r.get("valuation", "0")
            try: val = float(val_raw)
            except: val = 0.0

            records.append(PermitRecord(
                permit_id=r.get("permit_nbr"),
                city="Los Angeles",
                applied_date=applied, # Often null, but correct field
                issued_date=issued,
                valuation=val,
                description=r.get("work_desc", "No Description"),
                status=r.get("status_desc", "Issued"),
                complexity_tier=ComplexityTier.UNKNOWN
            ))
            
        print(f"✅ Los Angeles: Retrieved {len(records)} records.")
        return records
        
    except Exception as e:
        print(f"❌ LA API Error: {e}")
        return []