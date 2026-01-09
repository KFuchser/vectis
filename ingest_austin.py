import time
import pandas as pd
from sodapy import Socrata
from service_models import PermitRecord

def get_austin_data(socrata_token, threshold):
    print("\n--- 🛰️ AUSTIN SPOKE (Socrata) ---")
    results = []
    for attempt in range(3):
        try:
            client = Socrata("data.austintexas.gov", socrata_token, timeout=120)
            results = client.get(
                "3syk-w9eu",
                where=f"status_current in ('Issued', 'Final') AND issue_date > '{threshold}'",
                limit=2000, 
                order="issue_date DESC"
            )
            break 
        except Exception as e:
            print(f"   ⚠️ Attempt {attempt+1} failed: {e}")
            time.sleep(5)

    cleaned_records = []
    for row in results:
        try:
            # Valuation cleaning
            val_raw = row.get("total_job_valuation") or row.get("est_project_cost") or 0
            if isinstance(val_raw, str):
                val_raw = val_raw.replace('$', '').replace(',', '')
            
            # Map to Pydantic (This triggers our Negative Constraints automatically)
            permit = PermitRecord(
                city="Austin",
                permit_id=row.get("permit_number"), 
                applied_date=row.get("applieddate"), 
                issued_date=row.get("issue_date"),
                description=row.get("work_description") or row.get("permit_type_desc") or "No Description",
                valuation=float(val_raw),
                status=row.get("status_current")
            )
            cleaned_records.append(permit)
        except: continue
            
    return cleaned_records