import requests
import pandas as pd

# The confirmed "Golden Source" endpoint
BASE_URL = "https://mapit.fortworthtexas.gov/ags/rest/services/CIVIC/Permits/MapServer/0"

def fix_fort_worth_connection():
    print(f"🛰️ Initiating Handshake with Fort Worth CIVIC Layer...")

    # --- STEP 1: GET METADATA (SCHEMA DISCOVERY) ---
    try:
        # Requesting JSON without 'query' returns the layer definition (Fields, Types, etc.)
        meta_url = f"{BASE_URL}?f=json"
        r = requests.get(meta_url, timeout=10)
        r.raise_for_status()
        meta = r.json()
        
        if "error" in meta:
            print(f"❌ Server Error: {meta['error']}")
            return

        print("✅ Handshake Successful. Analyzing Schema...")
        
        # Extract Field Names
        fields = [f['name'] for f in meta.get('fields', [])]
        print(f"📋 Found {len(fields)} Columns.")
        
        # Intelligently find the "Time" column
        date_candidates = [f for f in fields if "date" in f.lower() or "time" in f.lower() or "created" in f.lower()]
        print(f"🕵️ Potential Date Columns: {date_candidates}")
        
        # Pick the best candidate (prefer 'Applied' or 'Open')
        target_date_col = next((c for c in date_candidates if 'open' in c.lower()), None)
        if not target_date_col:
             target_date_col = next((c for c in date_candidates if 'app' in c.lower()), date_candidates[0] if date_candidates else None)
        
        print(f"🎯 Locking on Time Column: '{target_date_col}'")
        
    except Exception as e:
        print(f"💥 Metadata Fetch Failed: {e}")
        return

    # --- STEP 2: EXECUTE PROOF OF LIFE QUERY ---
    if not target_date_col:
        print("❌ CRITICAL: No Date Column found. Cannot sort records.")
        return

    print(f"\n🚀 Attempting Query using verified schema...")
    query_params = {
        "where": f"{target_date_col} IS NOT NULL", # Safer than 1=1 for some DBs
        "outFields": "*",
        "resultRecordCount": 3,
        "orderByFields": f"{target_date_col} DESC", # Now we know this column exists
        "f": "json"
    }

    try:
        q_url = f"{BASE_URL}/query"
        r = requests.get(q_url, params=query_params, timeout=10)
        data = r.json()
        
        if "error" in data:
            print(f"❌ Query Failed: {data['error']}")
            # Fallback: Sometimes 'orderBy' is the issue on older servers. Retry without it.
            print("🔄 Retrying without Sort...")
            del query_params["orderByFields"]
            r = requests.get(q_url, params=query_params)
            data = r.json()

        features = data.get("features", [])
        print(f"✅ SUCCESS! Fetched {len(features)} records.")
        
        if features:
            df = pd.DataFrame([f['attributes'] for f in features])
            print("\n--- 🧬 VERIFIED DATA SAMPLE ---")
            print(df[[target_date_col] + [c for c in df.columns if c != target_date_col][:3]].head().to_markdown(index=False))

    except Exception as e:
        print(f"💥 Query Execution Failed: {e}")

if __name__ == "__main__":
    fix_fort_worth_connection()