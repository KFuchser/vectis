# ingest_la.py
import requests
from service_models import PermitRecord

def get_la_data(threshold):
    LA_ENDPOINT = "https://data.lacity.org/resource/pi9x-tg5x.json"
    query = (
        f"$select=permit_nbr,issue_date,permit_type,permit_sub_type,valuation,work_desc,status_desc"
        f"&$where=issue_date >= '{threshold}'"
        f"&$limit=1000"
    )
    resp = requests.get(f"{LA_ENDPOINT}?{query}")
    if resp.status_code != 200: return []
    
    records = []
    for r in resp.json():
        records.append(PermitRecord(
            permit_id=r.get("permit_nbr"),
            city="Los Angeles",
            applied_date=r.get("issue_date"),
            issued_date=r.get("issue_date"),
            valuation=float(r.get("valuation", 0)) if r.get("valuation") else 0,
            description=r.get("work_desc", "No Description"),
            status=r.get("status_desc", "Issued")
        ))
    return records