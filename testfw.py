import requests
import pandas as pd
import json

# 🎯 THE TARGET (From your screenshot)
# "CFW Development Permits Table"
ARCGIS_URL = "https://services5.arcgis.com/3ddLCBXe1bRt7mzj/arcgis/rest/services/CFW_Open_Data_Development_Permits_View/FeatureServer/0/query"

def sync_fort_worth_arcgis():
    print(f"🤠 Connecting to Fort Worth ArcGIS FeatureServer...")
    
    # 1. Build Query Params (Standard ArcGIS REST Protocol)
    # We ask for '*' (All Fields) to see what we are working with.
    params = {
        "where": "1=1",           # "Give me everything" (we limit count below)
        "outFields": "*",         # Get all columns
        "outSR": "4326",          # Standard GPS coordinates
        "f": "json",              # Return JSON
        "resultRecordCount": 10,  # Limit to 10 for safety
        "orderByFields": "Permit_Issued_Date DESC" # Try to get recent ones first
    }

    try:
        # 2. Hit the Endpoint
        response = requests.get(ARCGIS_URL, params=params)
        response.raise_for_status()
        data = response.json()

        # 3. Parse Features
        if "features" in data and len(data["features"]) > 0:
            # ArcGIS hides data inside 'attributes' key
            records = [f["attributes"] for f in data["features"]]
            df = pd.DataFrame(records)
            
            print(f"\n✅ SUCCESS. Connection Established.")
            print(f"   Retrieved {len(df)} records.")
            print("-" * 50)
            print("📋 AVAILABLE COLUMNS (Copy these for your schema):")
            print(list(df.columns))
            print("-" * 50)
            
            # Check for the critical Date Column
            # Likely 'Permit_Issued_Date' or 'Issued_Date'
            date_cols = [col for col in df.columns if 'date' in col.lower()]
            print(f"📅 Date Columns Found: {date_cols}")
            
            print("\n🧪 Sample Data (First Record):")
            print(df.iloc[0])
            
            return df
        else:
            print("⚠️ Connected, but found 0 features. Check query parameters.")
            print(f"Response: {data}")
            return pd.DataFrame()

    except Exception as e:
        print(f"🔥 Error connecting to ArcGIS: {e}")
        return pd.DataFrame()

if __name__ == "__main__":
    sync_fort_worth_arcgis()