"""
A simple script for testing the business logic embedded in the Pydantic models.
It creates several instances of the `PermitRecord` model with different test data to verify field defaults and property calculations.
"""
import requests
from collections import Counter

def diagnostic():
    url = "https://data.sanantonio.gov/api/3/action/datastore_search"
    params = {"resource_id": "c21106f9-3ef5-4f3a-8604-f992b4db7512", "limit": 1000}
    
    print("🕵️ Analyzing San Antonio Data Integrity...")
    try:
        r = requests.get(url, params=params).json()
        records = r["result"]["records"]
        
        # 1. Check for Duplicate Permit Numbers
        ids = [str(rec.get("PERMIT #")) for rec in records]
        unique_ids = set(ids)
        
        # 2. Check for Nulls
        null_ids = ids.count('None')
        
        print(f"✅ Total Records Fetched: {len(records)}")
        print(f"🆔 Unique Permit IDs:     {len(unique_ids)}")
        print(f"⚠️ Duplicate IDs found:   {len(records) - len(unique_ids)}")
        print(f"🚫 Null IDs found:        {null_ids}")
        
        if len(unique_ids) < 10 and len(records) > 100:
            print("\n🔥 SMOKING GUN: Your IDs are colliding. Most records are being overwritten.")
        
    except Exception as e:
        print(f"❌ Diagnostic Failed: {e}")

if __name__ == "__main__":
    diagnostic()