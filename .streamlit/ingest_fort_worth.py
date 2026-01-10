import requests
import pandas as pd
from datetime import datetime
from service_models import PermitRecord

def get_fort_worth_data(threshold_str):
    print(f"\n--- 🛰️ FORT WORTH SPOKE (Final Calibration) ---")
    
    # 1. THE VERIFIED 2026 ENDPOINT
    # Host: services.arcgis.com | Service: CFW_Development_Permits_Points | Layer: 0
    base_url = "https://services.arcgis.com/8v7963f69S16O3z0/arcgis/rest/services/CFW_Development_Permits_Points/FeatureServer/0/query"
    
    # 2. CONVERT DATE TO MILLISECONDS
    # Fort Worth's server stores dates as long integers. 
    # Example: Jan 1, 2026 -> 1735707600000
    try:
        dt_obj = datetime.strptime(threshold_str, "%Y-%m-%d")
        ms_threshold = int(dt_obj.timestamp() * 1000)
    except Exception as e:
        print(f"❌ Date conversion failed: {e}")
        return []

    # 3. BUILD THE PRECISE WHERE CLAUSE
    # No quotes around the timestamp because it is a number
    where_clause = f"Date_Applied >= {ms_threshold} OR Date_Issued >= {ms_threshold}"
    
    params = {
        'where': where_clause,
        'outFields': 'Permit_No,B1_WORK_DESC,Date_Issued,Valuation,Permit_Status,Date_Applied',
        'f': 'json',
        'resultRecordCount': 500,
        'orderByFields': 'Date_Applied DESC',
        'returnGeometry': 'false'
    }

    try:
        # Standard headers to ensure the server treats us as a browser
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) VectisDataFactory/1.0',
            'Accept': 'application/json'
        }
        
        response = requests.get(base_url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Check for ArcGIS internal errors
        if "error" in data:
            print(f"❌ ArcGIS Server Message: {data['error'].get('message')}")
            return []

        features = data.get('features', [])
        
        cleaned_records = []
        for feat in features:
            attr = feat.get('attributes', {})
            
            # ArcGIS returns milliseconds; convert back to YYYY-MM-DD
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