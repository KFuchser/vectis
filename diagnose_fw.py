"""
A diagnostic script for scouting the Fort Worth ArcGIS MapServer endpoint.

This tool connects to the root of the Permits MapServer, fetches metadata,
and lists all available layers and their types. Its primary purpose is to
help identify the correct layer ID for data ingestion by `ingest_velocity_50.py`.
"""
import requests

def diagnose_fort_worth_schema():
    print(">> DIAGNOSTICS: Scouting Fort Worth MapServer...")
    
    # 1. Hit the Root Metadata (No /0/query yet)
    base_url = "https://mapit.fortworthtexas.gov/ags/rest/services/CIVIC/Permits/MapServer"
    
    params = {"f": "json"}
    
    try:
        response = requests.get(base_url, params=params, timeout=10)
        data = response.json()
        
        if 'error' in data:
            print(f"!! Server Error: {data['error']}")
            return

        # 2. List All Layers
        print(f"\nService Description: {data.get('description', 'N/A')}")
        print("-" * 60)
        print(f"{'ID':<5} | {'Layer Name':<40} | {'Type'}")
        print("-" * 60)
        
        for layer in data.get('layers', []):
            print(f"{layer['id']:<5} | {layer['name']:<40} | {layer['type']}")
            
        print("-" * 60)
        
        # 3. Identify the Target
        # We are looking for "Permits" or "Development Permits"
        # Once we see the list, we update ingest_velocity_50.py with the correct ID.

    except Exception as e:
        print(f"!! Connection Failed: {e}")

if __name__ == "__main__":
    diagnose_fort_worth_schema()