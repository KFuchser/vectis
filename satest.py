"""
System Validator: Live Schema Inspection
Targets: Fort Worth & Los Angeles
Goal: Identify the EXACT field names for 'Applied Date' to fix Velocity calculations.
"""
import requests
import json

def check_fort_worth():
    print("\n🤠 --- INSPECTING FORT WORTH ---")
    # Fort Worth ArcGIS Endpoint
    url = "https://services5.arcgis.com/3ddLCBXe1bRt7mzj/arcgis/rest/services/CFW_Open_Data_Development_Permits_View/FeatureServer/0/query"
    params = {
        "where": "1=1",         # Get any record
        "outFields": "*",       # Get ALL fields
        "f": "json",
        "resultRecordCount": 3, # Just need a few samples
        "orderByFields": "Status_Date DESC" # Get recent ones
    }
    
    try:
        resp = requests.get(url, params=params, timeout=10)
        data = resp.json()
        
        if "features" in data and data["features"]:
            record = data["features"][0]["attributes"]
            print("✅ Connection Successful.")
            print(f"🔑 KEYS FOUND: {list(record.keys())}")
            print("\n📋 SAMPLE DATA (Dates):")
            # Print any field that looks like a date
            date_fields = {k: v for k, v in record.items() if 'Date' in k or 'DATE' in k}
            print(json.dumps(date_fields, indent=2))
        else:
            print("⚠️ Connected, but no features returned.")
            
    except Exception as e:
        print(f"❌ Fort Worth Failed: {e}")

def check_la():
    print("\n🌴 --- INSPECTING LOS ANGELES ---")
    # LA Socrata Endpoint (Permits Issued)
    url = "https://data.lacity.org/resource/pi9x-tg5x.json"
    params = {
        "$limit": 3,
        "$order": "issue_date DESC" # Get recent
    }
    
    try:
        resp = requests.get(url, params=params, timeout=10)
        data = resp.json()
        
        if data:
            record = data[0]
            print("✅ Connection Successful.")
            print(f"🔑 KEYS FOUND: {list(record.keys())}")
            print("\n📋 SAMPLE DATA (Dates):")
            # Check specifically for the elusive submit_date
            print(f"   issue_date:  {record.get('issue_date')}")
            print(f"   submit_date: {record.get('submit_date')} (Critical for Velocity)")
            print(f"   status_date: {record.get('status_date')}")
        else:
            print("⚠️ Connected, but table is empty.")

    except Exception as e:
        print(f"❌ Los Angeles Failed: {e}")

if __name__ == "__main__":
    check_fort_worth()
    check_la()