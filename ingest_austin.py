"""
Ingestion spoke for Austin, TX.
FIX: Orders by 'issue_date' because 'applieddate' is often null/stale in recent records.
"""
import requests
from service_models import PermitRecord, ComplexityTier

def get_austin_data(app_token, cutoff_date):
    print(f"🤠 Fetching Austin data since {cutoff_date}...")
    
    AUSTIN_API_URL = "https://data.austintexas.gov/resource/3syk-w9eu.json"
    
    # CRITICAL FIX: Sort by issue_date, not applieddate
    # The Verification Report proved applieddate is often null/old
    params = {
        "$where": f"issue_date >= '{cutoff_date}T00:00:00'",
        "$limit": 5000,
        "$order": "issue_date DESC",
        "$$app_token": app_token
    }
    
    try:
        response = requests.get(AUSTIN_API_URL, params=params, timeout=30)
        
        if response.status_code != 200:
            print(f"❌ Austin API Error: {response.status_code}")
            return []
            
        data = response.json()
        if not data:
            print("⚠️ No Austin data returned.")
            return []
            
        records = []
        for item in data:
            # Date Parsing
            def parse_date(d):
                return d.split("T")[0] if d else None
            
            applied = parse_date(item.get("applieddate"))
            issued = parse_date(item.get("issue_date"))
            
            # Use Description or Work Class
            desc = item.get("description") or item.get("work_class") or "Unspecified"
            
            # Valuation
            try: val = float(item.get("valuation", 0.0) or 0.0)
            except: val = 0.0

            r = PermitRecord(
                city="Austin",
                permit_id=item.get("permit_number", "UNKNOWN"),
                applied_date=applied,
                issued_date=issued,
                description=desc,
                valuation=val,
                complexity_tier=ComplexityTier.UNKNOWN,
                status=item.get("status_current", "Issued")
            )
            records.append(r)
            
        print(f"✅ Austin: Retrieved {len(records)} records.")
        return records

    except Exception as e:
        print(f"❌ Austin Integration Error: {e}")
        return []