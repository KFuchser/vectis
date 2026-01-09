import requests
import pandas as pd
from service_models import PermitRecord

def get_fort_worth_data(threshold):
    print("\n--- 🛰️ FORT WORTH SPOKE (ArcGIS) ---")
    
    # ArcGIS MapServer for CFW Development Permits
    base_url = "https://mapit.fortworthtexas.gov/ags/rest/services/CIVIC/Permits/MapServer/0/query"
    
    # 2026 Filter: Catch both Applied and Issued to get a wider 'Strategic' net
    where_clause = f"Date_Applied >= '{threshold}' OR Date_Issued >= '{threshold}'"
    
    params = {
        'where': where_clause,
        'outFields': 'Permit_No,B1_WORK_DESC,Date_Issued,Valuation,Permit_Status,Date_Applied',
        'f': 'json',
        'resultRecordCount': 200,
        'orderByFields': 'Date_Applied DESC'
    }

    try:
        response = requests.get(base_url, params=params, timeout=30)
        response.raise_for_status()
        features = response.json().get('features', [])
        
        cleaned_records = []
        for feat in features:
            attr = feat.get('attributes', {})
            
            # Fort Worth uses Unix timestamps (milliseconds)
            def clean_date(ts):
                if not ts: return None
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