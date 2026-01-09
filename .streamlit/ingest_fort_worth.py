import requests
import pandas as pd
from datetime import datetime
from service_models import PermitRecord

def get_fort_worth_data(threshold_str):
    print("\n--- 🛰️ FORT WORTH SPOKE (ArcGIS 2026) ---")
    
    # 1. HARDENED ENDPOINT: Using the FeatureServer alias for better query reliability
    base_url = "https://mapit.fortworthtexas.gov/ags/rest/services/Planning_Development/PlanningDevelopment/FeatureServer/117/query"
    
    # 2. Convert threshold to Unix Milliseconds (Integer only)
    threshold_dt = datetime.strptime(threshold_str, "%Y-%m-%d")
    threshold_ts = int(threshold_dt.timestamp() * 1000)

    # 3. THE "WIDE NET" QUERY
    # We add 'returnGeometry=false' to speed up the response
    where_clause = f"Date_Applied >= {threshold_ts} OR Date_Issued >= {threshold_ts}"
    
    params = {
        'where': where_clause,
        'outFields': '*',  # Temporarily grab all to ensure we don't miss fields
        'f': 'json',
        'returnGeometry': 'false',
        'resultRecordCount': 500,
        'orderByFields': 'Date_Applied DESC'
    }

    try:
        response = requests.get(base_url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Check for ArcGIS-specific error messages
        if "error" in data:
            print(f"❌ ArcGIS Server Error: {data['error'].get('message')}")
            return []

        features = data.get('features', [])
        
        cleaned_records = []
        for feat in features:
            attr = feat.get('attributes', {})
            
            # Helper: Handle ArcGIS date stamps
            def clean_date(ts):
                if not ts or ts < 0: return None
                try:
                    return pd.to_datetime(ts, unit='ms').strftime('%Y-%m-%d')
                except:
                    return None

            # 4. FIELD MAPPING (Handles Case Sensitivity)
            # FW uses different keys in different layers; we check both.
            p = PermitRecord(
                city="Fort Worth",
                permit_id=str(attr.get('Permit_No') or attr.get('PERMIT_NO') or "Unknown"),
                applied_date=clean_date(attr.get('Date_Applied') or attr.get('DATE_APPLIED')),
                issued_date=clean_date(attr.get('Date_Issued') or attr.get('DATE_ISSUED')),
                description=str(attr.get('B1_WORK_DESC') or attr.get('WORK_DESCRIPTION') or "No Description"),
                valuation=float(attr.get('Valuation') or attr.get('ESTIMATED_VALUE') or 0),
                status=str(attr.get('Permit_Status') or attr.get('STATUS') or "Pending")
            )
            cleaned_records.append(p)

        print(f"✅ Fort Worth: Found {len(cleaned_records)} records.")
        return cleaned_records

    except Exception as e:
        print(f"❌ Fort Worth Spoke Failed: {e}")
        return []