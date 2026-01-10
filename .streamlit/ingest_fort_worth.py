import requests
import pandas as pd
from datetime import datetime
from service_models import PermitRecord

def get_fort_worth_data(threshold_str):
    print(f"\n--- 🛰️ FORT WORTH SPOKE (Manual Handshake) ---")
    
    # 1. DIRECT CLOUD ENDPOINT
    url = "https://services.arcgis.com/8v7963f69S16O3z0/ArcGIS/rest/services/CFW_Development_Permits_Points/FeatureServer/0/query"
    
    # 2. CONVERT DATE TO MILLISECONDS
    dt_obj = datetime.strptime(threshold_str, "%Y-%m-%d")
    ms_threshold = int(dt_obj.timestamp() * 1000)

    # 3. USE BROWSER-COMPLIANT GET PARAMETERS
    # We explicitly define every parameter to be ultra-compatible with ArcGIS 2026
    params = {
        'where': f"Date_Applied>={ms_threshold}",
        'outFields': 'Permit_No,B1_WORK_DESC,Date_Issued,Valuation,Permit_Status,Date_Applied',
        'f': 'pjson', # Request pretty-printed JSON for better server compatibility
        'returnGeometry': 'false',
        'resultRecordCount': '50', # Lower limit to ensure success
        'orderByFields': 'Date_Applied DESC'
    }

    try:
        # Standard Browser Headers
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36',
            'Accept': '*/*'
        }
        
        # Using GET with specific headers to look like a standard browser request
        response = requests.get(url, params=params, headers=headers, timeout=30)
        
        if response.status_code != 200:
            print(f"❌ Connection failed: HTTP {response.status_code}")
            return []

        data = response.json()
        
        if "error" in data:
            print(f"❌ ArcGIS Server Message: {data['error'].get('message')}")
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