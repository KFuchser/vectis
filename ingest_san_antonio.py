import requests
import pandas as pd
from service_models import PermitRecord

def get_san_antonio_data(threshold):
    print("\n--- 🛰️ SAN ANTONIO SPOKE (BuildSA) ---")
    
    # San Antonio Open Data Portal API (Building Permits Issued)
    url = "https://data.sanantonio.gov/api/3/action/datastore_search"
    resource_id = "c21106f9-3ef5-4f3a-8604-f992b4db7512" # Permits Issued Dataset
    
    params = {
        'resource_id': resource_id,
        'limit': 200,
        'sort': 'DATE ISSUED desc'
    }

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        raw_records = data.get('result', {}).get('records', [])
        
        cleaned_records = []
        for raw in raw_records:
            # 2026 BuildSA Field Mapping
            # Check for 'DATE ISSUED' or 'ISSUED DATE' based on current API version
            issued_date = raw.get('DATE ISSUED') or raw.get('ISSUED_DATE')
            
            # Skip if older than threshold
            if issued_date and issued_date < threshold:
                continue

            # Construct standardized PermitRecord
            p = PermitRecord(
                city="San Antonio",
                permit_id=str(raw.get('PERMIT #') or raw.get('PERMIT_NUMBER')),
                applied_date=raw.get('APPLIED DATE'),
                issued_date=issued_date,
                # SA often stores business names in 'PROJECT NAME'
                description=str(raw.get('PROJECT NAME') or raw.get('WORK TYPE') or "No Description"),
                valuation=float(raw.get('DECLARED VALUATION') or 0),
                status=str(raw.get('STATUS') or "Issued")
            )
            cleaned_records.append(p)
            
        print(f"✅ San Antonio: Found {len(cleaned_records)} records.")
        return cleaned_records

    except Exception as e:
        print(f"❌ San Antonio Spoke Failed: {e}")
        return []