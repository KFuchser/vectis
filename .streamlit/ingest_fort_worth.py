import requests
import pandas as pd
from datetime import datetime
from service_models import PermitRecord

def get_fort_worth_data(threshold_str):
    print("\n--- 🛰️ FORT WORTH SPOKE (ArcGIS 2026) ---")
    
    # 2026 Production Endpoint for Development Permits
    base_url = "https://mapit.fortworthtexas.gov/ags/rest/services/Planning_Development/PlanningDevelopment/MapServer/117/query"
    
    # 1. Convert threshold (YYYY-MM-DD) to Unix Milliseconds
    # ArcGIS requires integers for date queries on this server
    threshold_dt = datetime.strptime(threshold_str, "%Y-%m-%d")
    threshold_ts = int(threshold_dt.timestamp() * 1000)

    # 2. Build the precise query
    where_clause = f"Date_Applied >= {threshold_ts} OR Date_Issued >= {threshold_ts}"
    
    params = {
        'where': where_clause,
        'outFields': 'Permit_No,B1_WORK_DESC,Date_Issued,Valuation,Permit_Status,Date_Applied',
        'f': 'json',
        'resultRecordCount': 500,
        'orderByFields': 'Date_Applied DESC'
    }

    try:
        response = requests.get(base_url, params=params, timeout=30)
        response.raise_for_status()
        features = response.json().get('features', [])
        
        cleaned_records = []
        for feat in features:
            attr = feat.get('attributes', {})
            
            # Helper: Convert Fort Worth Milliseconds to YYYY-MM-DD
            def clean_date(ts):
                if not ts or ts < 0: return None
                return pd.to_datetime(ts, unit='ms').strftime('%Y-%m-%d')

            p = PermitRecord(
                city="Fort Worth",
                permit_id=str(attr.get('Permit_No')),
                applied_date=clean_date(attr.get('Date_Applied')),
                issued_date=clean_date(attr.get('Date_Issued')),
                description=str(attr.get('B1_WORK_DESC') or "No Description"),
                valuation=float(attr.get('Valuation') or 0),
                status=str(attr.get('Permit_Status') or "Pending")
            )
            cleaned_records.append(p)

        print(f"✅ Fort Worth: Found {len(cleaned_records)} records.")
        return cleaned_records

    except Exception as e:
        print(f"❌ Fort Worth Spoke Failed: {e}")
        return []