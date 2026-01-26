"""
Ingestion spoke for San Antonio, TX (BuildSA).
Connects to the City of San Antonio's Socrata open data portal to fetch recently issued building permits.
"""
import requests
from service_models import PermitRecord

def get_san_antonio_data(threshold: str) -> list[PermitRecord]:
    url = "https://data.sanantonio.gov/api/3/action/datastore_search"
    resource_id = "c21106f9-3ef5-4f3a-8604-f992b4db7512"
    params = {'resource_id': resource_id, 'limit': 300, 'sort': 'DATE ISSUED desc'}

    try:
        response = requests.get(url, params=params, timeout=30)
        data = response.json()
        raw_records = data.get('result', {}).get('records', [])
        
        cleaned = []
        for raw in raw_records:
            issued = raw.get('DATE ISSUED') or raw.get('ISSUED_DATE')
            if issued and issued < threshold: continue

            cleaned.append(PermitRecord(
                city="San Antonio",
                permit_id=str(raw.get('PERMIT #') or raw.get('PERMIT_NUMBER')),
                applied_date=raw.get('APPLIED DATE'),
                issued_date=issued,
                description=str(raw.get('PROJECT NAME') or raw.get('WORK TYPE') or "No Description"),
                valuation=float(raw.get('DECLARED VALUATION') or 0),
                status=str(raw.get('STATUS') or "Issued")
            ))
        return cleaned
    except Exception as e:
        print(f"❌ SA Error: {e}")
        return []