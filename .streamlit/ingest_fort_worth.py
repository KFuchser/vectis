import requests
import pandas as pd
from datetime import datetime
from service_models import PermitRecord

def get_fort_worth_data(threshold_str):
    print(f"\n--- 🛰️ FORT WORTH SPOKE (Final Calibration) ---")
    
    # 1. THE DIRECT ENDPOINT
    url = "https://services.arcgis.com/8v7963f69S16O3z0/arcgis/rest/services/CFW_Development_Permits_Points/FeatureServer/0/query"
    
    # 2. CONVERT DATE TO MILLISECONDS
    dt_obj = datetime.strptime(threshold_str, "%Y-%m-%d")
    ms_threshold = int(dt_obj.timestamp() * 1000)

    # 3. USE POST TO BYPASS URL ERRORS
    # We send the query in the 'data' body so the URL stays clean and valid.
    payload = {
        'where': f"Date_Applied >= {ms_threshold}",
        'outFields': '*',  # Grab all to avoid field-naming errors
        'f': 'json',
        'returnGeometry': 'false',
        'resultRecordCount': 100
    }

    try:
        # Standard headers to look like a clean request
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        
        # Using .post() instead of .get() is the key to fixing 'Invalid URL'
        response = requests.post(url, data=payload, headers=headers, timeout=30)
        
        if response.status_code != 200:
            print(f"❌ Connection failed: HTTP {response.status_code}")
            return []

        data = response.json()
        
        if "error" in data:
            print(f"❌ ArcGIS Error: {data['error'].get('message')}")
            return []

        features = data.get('features', [])
        cleaned_records = []
        
        for feat in features:
            attr = feat.get('attributes', {})
            
            def clean_date(ts):
                if not ts or ts < 0: return None
                return pd.to_datetime(ts, unit='ms').strftime('%Y-%m-%d')

            # Standardizing mapping for 2026 schema
            p = PermitRecord(
                city="Fort Worth",
                permit_id=str(attr.get('Permit_No') or attr.get('OBJECTID')),
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
        print(f"❌ Spoke Error: {e}")
        return []