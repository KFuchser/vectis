import requests
import pandas as pd
from service_models import PermitRecord

def get_fort_worth_data(cutoff_date):
    base_url = "https://mapit.fortworthtexas.gov/ags/rest/services/CIVIC/Permits/MapServer/0/query"
    params = {
        "where": "1=1", "outFields": "*", "f": "json",
        "resultRecordCount": 2000, "orderByFields": "Status_Date DESC" 
    }
    response = requests.get(base_url, params=params, timeout=120)
    features = response.json().get('features', [])
    
    cleaned = []
    for feature in features:
        attr = feature.get('attributes', {})
        # Use our Pydantic model to auto-clean and auto-tier
        try:
            p = PermitRecord(
                city="Fort Worth",
                permit_id=str(attr.get("Permit_Num") or attr.get("Permit_No") or f"OID-{attr.get('OBJECTID')}") ,
                description=attr.get("B1_WORK_DESC") or "No Description",
                valuation=float(attr.get("JobValue", 0.0) or 0.0),
                status=str(attr.get("Current_Status", "")).title(),
                applied_date=pd.to_datetime(attr.get("File_Date"), unit='ms').date() if attr.get("File_Date") else None,
                issued_date=pd.to_datetime(attr.get("Status_Date"), unit='ms').date() if attr.get("Status_Date") else None
            )
            cleaned.append(p)
        except: continue
    return cleaned