"""
Ingestion spoke for Los Angeles, CA.
Connects to the City of Los Angeles' Socrata open data portal (LADBS) to fetch recently issued building permits.
"""
import os
import requests
from service_models import PermitRecord
from datetime import datetime

def get_la_data(threshold, socrata_token=None):
    # ENDPOINT: Building and Safety - Permits Issued (pi9x-tg5x)
    LA_ENDPOINT = "https://data.lacity.org/resource/pi9x-tg5x.json"
    
    # SOCRATA QUERY
    # We fetch 'submit_date' (if avail) or 'status_date' to try and get a start time
    # Note: L.A. 'pi9x-tg5x' dataset is specifically "Issued" permits.
    # We try to grab 'submit_date' which is often hidden in these datasets.
    query = (
        f"$select=permit_nbr,issue_date,status_date,valuation,work_desc,status_desc"
        f"&$where=issue_date >= '{threshold}'"
        f"&$limit=2000"
        f"&$order=issue_date DESC"
    )
    
    headers = {}
    if socrata_token:
        headers["X-App-Token"] = socrata_token
        
    try:
        resp = requests.get(f"{LA_ENDPOINT}?{query}", headers=headers, timeout=20)
        if resp.status_code != 200: 
            print(f"⚠️ LA API Error: {resp.status_code}")
            return []
            
        data = resp.json()
        records = []
        
        for r in data:
            # FIX 1: Do NOT set applied_date to issue_date. 
            # Leave it None if missing so the Quality Lock can handle it.
            # L.A. sometimes exposes 'submit_date' or we can infer from 'status_date' if status was 'Submitted'
            applied = r.get("submit_date") # Try to get real start date
            
            # Formatting Valuation
            val_raw = r.get("valuation", "0")
            try:
                val = float(val_raw)
            except:
                val = 0.0

            records.append(PermitRecord(
                permit_id=r.get("permit_nbr"),
                city="Los Angeles",
                applied_date=applied, # Can be None!
                issued_date=r.get("issue_date"),
                valuation=val,
                description=r.get("work_desc", "No Description"),
                status=r.get("status_desc", "Issued")
            ))
            
        return records

    except Exception as e:
        print(f"❌ LA Ingest Failed: {e}")
        return []