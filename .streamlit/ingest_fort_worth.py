import requests
import pandas as pd
from datetime import datetime
from service_models import PermitRecord

def get_fort_worth_data(threshold_str):
    print(f"\n--- 🛰️ FORT WORTH SPOKE (Final Calibration) ---")
    
    # 1. THE VERIFIED 2026 CLOUD ENDPOINT
    url = "https://services.arcgis.com/8v7963f69S16O3z0/arcgis/rest/services/CFW_Development_Permits_Points/FeatureServer/0/query"
    
    # 2. CONVERT DATE TO MILLISECONDS
    # ArcGIS internal databases use numeric timestamps (Epoch MS)
    dt_obj = datetime.strptime(threshold_str, "%Y-%m-%d")
    ms_threshold = int(dt_obj.timestamp() * 1000)

    # 3. USE POST TO BYPASS "INVALID URL" ERRORS
    # Sending parameters in 'data' instead of 'params' ensures the URL stays clean
    payload = {
        'where': f"Date_Applied >= {ms_threshold}",
        'outFields': 'Permit_No,B1_WORK_DESC,Date_Issued,Valuation,Permit_Status,Date_Applied',
        'f': 'json',
        'returnGeometry': 'false',
        'resultRecordCount': 200,
        'orderByFields': 'Date_Applied DESC'
    }

    try:
        # Standard headers to prevent the server from flagging us as a bot
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        # We switch to .post() here. This is the "Magic Fix" for ArcGIS URL errors.
        response = requests.post(url, data=payload, headers=headers, timeout=30)
        
        if response.status_code != 200:
            print(f"❌ Connection failed: HTTP {response.status_code}")
            return []

        data = response.json()
        
        # Catch internal ArcGIS errors (like bad field names)
        if "error" in data:
            print(f"❌ ArcGIS Server Error: {data['error'].get('message')}")
            # If it says 'Invalid Field', we'll know exactly which one to fix.
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