import requests
import pandas as pd
from service_models import PermitRecord

def get_fort_worth_data(threshold_str):
    print(f"\n--- 🛰️ FORT WORTH SPOKE (ArcGIS 2026) ---")
    
    # Authoritative 2026 FeatureServer for Development Permits
    # Using the FeatureServer endpoint is more robust for data extraction than MapServer
    base_url = "https://services.arcgis.com/8v7963f69S16O3z0/arcgis/rest/services/CFW_Development_Permits_Points/FeatureServer/0/query"
    
    # 🛠️ THE FIX: SQL-92 DATE LITERAL
    # ArcGIS requires the DATE 'YYYY-MM-DD' prefix to treat the string as a calendar date
    where_clause = f"Date_Applied >= DATE '{threshold_str}' OR Date_Issued >= DATE '{threshold_str}'"
    
    params = {
        'where': where_clause,
        'outFields': 'Permit_No,B1_WORK_DESC,Date_Issued,Valuation,Permit_Status,Date_Applied',
        'f': 'json',
        'resultRecordCount': 500,
        'orderByFields': 'Date_Applied DESC',
        'returnGeometry': 'false' # Speeds up response by ignoring coordinates
    }

    try:
        response = requests.get(base_url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Check for server-side error messages hidden in the JSON
        if "error" in data:
            print(f"❌ ArcGIS Query Error: {data['error'].get('message')}")
            return []

        features = data.get('features', [])
        
        cleaned_records = []
        for feat in features:
            attr = feat.get('attributes', {})
            
            # Helper: ArcGIS returns integers (ms); convert to YYYY-MM-DD
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