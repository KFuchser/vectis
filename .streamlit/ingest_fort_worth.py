import requests
import pandas as pd
from datetime import datetime
from service_models import PermitRecord

def get_fort_worth_data(threshold_str):
    print(f"\n--- 🛰️ FORT WORTH SPOKE (Final Handshake) ---")
    
    # 1. THE STRIPPED-DOWN ENDPOINT
    # We use the generic MapServer/117 which is often more 'public-friendly' than FeatureServer
    url = "https://mapit.fortworthtexas.gov/ags/rest/services/Planning_Development/PlanningDevelopment/MapServer/117/query"
    
    # 2. CONVERT TO INTEGER TIMESTAMP
    # ArcGIS 2026 back-ends are strictly numeric for date fields
    dt_obj = datetime.strptime(threshold_str, "%Y-%m-%d")
    ms_threshold = int(dt_obj.timestamp() * 1000)

    # 3. NO-SPACE QUERY PARAMETERS
    # We remove spaces in the 'where' clause to prevent URL encoding errors
    params = {
        'where': f"Date_Applied>={ms_threshold}", 
        'outFields': 'Permit_No,B1_WORK_DESC,Date_Issued,Valuation,Permit_Status,Date_Applied',
        'f': 'json',
        'returnGeometry': 'false',
        'resultRecordCount': 100
    }

    try:
        # We use a very basic User-Agent. Sometimes custom ones trigger WAF blocks.
        headers = {'User-Agent': 'python-requests/2.31.0'}
        
        # We use a GET request, but let the 'params' argument handle the encoding
        response = requests.get(url, params=params, headers=headers, timeout=30)
        
        if response.status_code != 200:
            print(f"❌ Connection failed: HTTP {response.status_code}")
            return []

        data = response.json()
        
        # Log the internal server error if it exists
        if "error" in data:
            print(f"❌ ArcGIS Server Message: {data['error'].get('message')}")
            print(f"DEBUG: Check if 'Date_Applied' field name is correct for MapServer 117.")
            return []

        features = data.get('features', [])
        cleaned_records = []
        
        for feat in features:
            attr = feat.get('attributes', {})
            
            def clean_date(ts):
                if not ts or ts < 0: return None
                return pd.to_datetime(ts, unit='ms').strftime('%Y-%m-%d')

            p = PermitRecord(
                city="Fort Worth",
                permit_id=str(attr.get('Permit_No') or "FW-UNKNOWN"),
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