import requests
import json
from datetime import datetime

# 🚨 THE GOLDEN KEY (From your screenshot)
# Host: services5.arcgis.com (ArcGIS Online)
# Org ID: 3ddLCBXe1bRt7mzj (Fort Worth Open Data)
# Service: CFW_Open_Data_Development_Permits_View
URL = "https://services5.arcgis.com/3ddLCBXe1bRt7mzj/arcgis/rest/services/CFW_Open_Data_Development_Permits_View/FeatureServer/0/query"

def run_test():
    print(f"🔌 Connecting to OFFICIAL endpoint: {URL} ...")
    
    # Query: Get the 3 most recent permits
    params = {
        "where": "1=1",
        "outFields": "Permit_No,B1_WORK_DESC,File_Date,Permit_Type", # Fields from your screenshot
        "orderByFields": "File_Date DESC", # Order by Newest
        "resultRecordCount": 3,
        "f": "json"
    }

    try:
        r = requests.get(URL, params=params, timeout=15)
        print(f"📡 Status Code: {r.status_code}")
        
        data = r.json()
        
        if "error" in data:
            print(f"❌ API Error: {data['error']}")
            return

        features = data.get("features", [])
        print(f"✅ Success! Retrieved {len(features)} records.")

        if features:
            print("\n--- MOST RECENT RECORD ---")
            top_record = features[0]['attributes']
            print(json.dumps(top_record, indent=2))
            
            # Verify Date
            ms = top_record.get('File_Date')
            if ms:
                readable = datetime.fromtimestamp(ms / 1000)
                print(f"\n📆 Latest Permit Date: {readable}")
                # This should match your '2 days ago' observation

    except Exception as e:
        print(f"💥 Critical Failure: {e}")

if __name__ == "__main__":
    run_test()