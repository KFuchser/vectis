import requests
import pandas as pd
from service_models import PermitRecord

def get_fort_worth_data(threshold_str):
    print(f"\n--- 🛰️ FORT WORTH SPOKE (ArcGIS 2026) ---")
    
    # 2026 CALIBRATED ENDPOINT
    # This is the Open Data FeatureServer for Building Permits
    base_url = "https://services.arcgis.com/8v7963f69S16O3z0/arcgis/rest/services/CFW_Development_Permits_Points/FeatureServer/0/query"
    
    # ArcGIS 2026 SQL Dialect: Requires DATE 'YYYY-MM-DD'
    where_clause = f"Date_Applied >= DATE '{threshold_str}'"
    
    params = {
        'where': where_clause,
        'outFields': 'Permit_No,B1_WORK_DESC,Date_Issued,Valuation,Permit_Status,Date_Applied',
        'f': 'json',
        'resultRecordCount': 500,
        'orderByFields': 'Date_Applied DESC',
        'returnGeometry': 'false'
    }

    try:
        # Use a generic User-Agent to prevent ArcGIS from blocking the GitHub runner
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(base_url, params=params, headers=headers, timeout=30)
        
        # Check if we got a 404 or 403
        if response.status_code != 200:
            print(f"❌ Server rejected request: HTTP {response.status_code}")
            return []

        data = response.json()
        
        # Catch ArcGIS-specific errors returned in JSON
        if "error" in data:
            print(f"❌ ArcGIS Server Message: {data['error'].get('message')}")
            # If 'Invalid URL' happens again, it's likely the Layer ID (0)
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