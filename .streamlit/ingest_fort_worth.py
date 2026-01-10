import requests
import pandas as pd
from datetime import datetime
from service_models import PermitRecord

def get_fort_worth_data(threshold_str):
    print(f"\n--- 🛰️ FORT WORTH SPOKE (Manual Calibration) ---")
    
    # 1. DIRECT FEATURE LAYER ENDPOINT
    # This is the verified 2026 URI for building permits
    url = "https://services.arcgis.com/8v7963f69S16O3z0/ArcGIS/rest/services/CFW_Development_Permits_Points/FeatureServer/0/query"
    
    # 2. CONVERT DATE TO MS (Integer)
    # ArcGIS 2026 internal database uses numeric timestamps
    dt_obj = datetime.strptime(threshold_str, "%Y-%m-%d")
    ms_threshold = int(dt_obj.timestamp() * 1000)

    # 3. USE POST TO PREVENT "INVALID URL" ERRORS
    # By sending data in the body, we avoid character-encoding issues in the URL string
    payload = {
        'where': f"Date_Applied >= {ms_threshold}",
        'outFields': 'Permit_No,Full_Description,Date_Issued,Valuation,Permit_Status,Date_Applied',
        'f': 'json',
        'returnGeometry': 'false',
        'resultRecordCount': 200,
        'orderByFields': 'Date_Applied DESC'
    }

    try:
        # We use a POST request here to be more robust
        response = requests.post(url, data=payload, timeout=30)
        
        if response.status_code != 200:
            print(f"❌ Connection failed: HTTP {response.status_code}")
            return []

        data = response.json()
        
        if "error" in data:
            print(f"❌ ArcGIS Server Error: {data['error'].get('message')}")
            return []

        features = data.get('features', [])
        cleaned_records = []
        
        for feat in features:
            attr = feat.get('attributes', {})
            
            def clean_date(ts):
                if not ts or ts < 0: return None
                return pd.to_datetime(ts, unit='ms').strftime('%Y-%m-%d')

            # Note: Using .get() with fallbacks for 2026 schema changes
            p = PermitRecord(
                city="Fort Worth",
                permit_id=str(attr.get('Permit_No') or "FW-UNKNOWN"),
                applied_date=clean_date(attr.get('Date_Applied')),
                issued_date=clean_date(attr.get('Date_Issued')),
                description=str(attr.get('Full_Description') or attr.get('B1_WORK_DESC') or "No Description"),
                valuation=float(attr.get('Valuation') or 0),
                status=str(attr.get('Permit_Status') or "Pending")
            )
            cleaned_records.append(p)

        print(f"✅ Fort Worth: Found {len(cleaned_records)} records.")
        return cleaned_records

    except Exception as e:
        print(f"❌ Spoke Error: {e}")
        return []