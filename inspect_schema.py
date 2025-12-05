import requests
import json

def inspect_layer_metadata():
    print(">> INSPECTING: Fort Worth Layer 0 Metadata...")
    
    # Hit the Layer Definition (not the query endpoint)
    base_url = "https://mapit.fortworthtexas.gov/ags/rest/services/CIVIC/Permits/MapServer/0"
    
    params = {"f": "json"}
    
    try:
        response = requests.get(base_url, params=params, timeout=10)
        data = response.json()
        
        if 'error' in data:
            print(f"!! Error: {data['error']}")
            return

        # 1. FIND THE ID FIELD (Crucial for Sorting)
        id_field = data.get('objectIdField', 'UNKNOWN')
        print(f"\n[CRITICAL] Object ID Field Name: '{id_field}'")
        
        # 2. LIST RELEVANT FIELDS
        print("\n[SCHEMA] Available Fields:")
        print(f"{'Name':<30} | {'Alias':<30} | {'Type'}")
        print("-" * 80)
        
        target_fields = ['date', 'status', 'desc', 'type', 'val', 'cost', 'permit']
        
        for field in data.get('fields', []):
            name = field['name']
            alias = field['alias']
            dtype = field['type']
            
            # Print only relevant fields to keep it readable
            if any(x in name.lower() for x in target_fields) or any(x in alias.lower() for x in target_fields):
                print(f"{name:<30} | {alias:<30} | {dtype}")

    except Exception as e:
        print(f"!! Connection Failed: {e}")

if __name__ == "__main__":
    inspect_layer_metadata()