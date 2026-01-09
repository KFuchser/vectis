import requests
import pandas as pd
from service_models import PermitRecord

def get_fort_worth_data(threshold):
    print("\n--- 🛰️ FORT WORTH SPOKE (ArcGIS) ---")
    
    # Fort Worth ArcGIS REST API endpoint for Development Permits
    # We query for records issued after our threshold date
    base_url = "https://mapit.fortworthtexas.gov/ags/rest/services/CIVIC/Permits/MapServer/0/query"
    
    # We define the SQL-like 'where' clause for the ArcGIS engine
    # '1=1' is a fallback to ensure the query is valid if threshold logic fails
    date_query = f"Date_Issued >= '{threshold}'" if threshold else "1=1"
    
    params = {
        'where': date_query,
        'outFields': 'Permit_No,B1_WORK_DESC,Date_Issued,Valuation,Permit_Status',
        'f': 'json',           # Request JSON format
        'resultRecordCount': 1000,
        'orderByFields': 'Date_Issued DESC'
    }

    try:
        response = requests.get(base_url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        features = data.get('features', [])
        cleaned_records = []
        
        for feat in features:
            attrs = feat.get('attributes', {})
            try:
                # Fort Worth usually provides dates as Unix Timestamps (milliseconds)
                # We convert to ISO string for our Pydantic model
                raw_date = attrs.get('Date_Issued')
                issued_date = None
                if raw_date:
                    issued_date = pd.to_datetime(raw_date, unit='ms').strftime('%Y-%m-%d')

                permit = PermitRecord(
                    city="Fort Worth",
                    permit_id=str(attrs.get('Permit_No')),
                    description=str(attrs.get('B1_WORK_DESC') or "No Description"),
                    valuation=float(attrs.get('Valuation') or 0),
                    status=str(attrs.get('Permit_Status') or "Issued"),
                    issued_date=issued_date
                )
                cleaned_records.append(permit)
            except Exception as e:
                # Log specific record failures but keep the loop moving
                continue
                
        return cleaned_records

    except Exception as e:
        print(f"!! Fort Worth Spoke Failed: {e}")
        return []