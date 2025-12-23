import pandas as pd
from sodapy import Socrata
import os
from supabase import create_client
from tqdm import tqdm
from datetime import datetime, timedelta

# 1. Configuration
SOCRATA_DOMAIN = "data.austintexas.gov"
DATASET_SPEED = "3syk-w9eu"   # Source A: Dates & Status (Fast)
DATASET_MONEY = "y2wy-tgr5"   # Source B: Valuation (Legacy Tax Data)
APP_TOKEN = None 

# Supabase
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase = create_client(url, key)

def fetch_speed_data(client, days_back=90):
    print(f"🤠 Fetching SPEED data (Status/Dates)...")
    start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
    # We fetch the list of permits we care about
    query = f"""
    SELECT permit_number, issue_date, applieddate, status_current, permit_type_desc
    WHERE issue_date >= '{start_date}'
    LIMIT 3000
    """
    try:
        return client.get(DATASET_SPEED, query=query)
    except Exception as e:
        print(f"❌ Speed Fetch Error: {e}")
        return []

def fetch_money_data(client, permit_ids):
    print(f"💰 Fetching MONEY data from Legacy Dataset ({DATASET_MONEY})...")
    
    # We must chunk the IDs to avoid URL length limits
    chunk_size = 400
    unique_ids = list(set(permit_ids))
    money_map = {} # {permit_number: valuation}

    # Schema check: We know y2wy-tgr5 uses 'total_job_valuation' or 'valuation'
    # We will query generic '*' first for a sample to be safe, then map.
    try:
        sample = client.get(DATASET_MONEY, limit=1)
        keys = sample[0].keys() if sample else []
        # Find the money column
        val_col = next((k for k in keys if 'valuation' in k.lower() or 'cost' in k.lower()), None)
        id_col = next((k for k in keys if 'permit' in k.lower() and 'num' in k.lower()), 'permit_number')
        
        if not val_col:
            print("   ⚠️ No valuation column found in Legacy dataset.")
            return {}
        print(f"   ✅ Found Money Column: '{val_col}'")
    except:
        return {}

    # Bulk Fetch
    for i in range(0, len(unique_ids), chunk_size):
        chunk = unique_ids[i:i + chunk_size]
        ids_fmt = "'" + "','".join(chunk) + "'"
        q = f"SELECT {id_col}, {val_col} WHERE {id_col} IN ({ids_fmt})"
        
        try:
            results = client.get(DATASET_MONEY, query=q)
            for r in results:
                # Parse Money
                try:
                    raw = str(r.get(val_col, 0)).replace('$','').replace(',','')
                    money_map[r.get(id_col)] = float(raw)
                except: pass
        except: pass
        
    print(f"   💰 Matched Valuation for {len(money_map)} permits.")
    return money_map

def run_fix():
    client = Socrata(SOCRATA_DOMAIN, APP_TOKEN)
    
    # 1. Get the list of permits
    speed_data = fetch_speed_data(client)
    if not speed_data: return

    # 2. Get the money for those permits
    ids = [r['permit_number'] for r in speed_data]
    money_map = fetch_money_data(client, ids)

    # 3. Merge
    clean_rows = []
    print("🌊 Merging & Normalizing...")
    for row in speed_data:
        pid = row.get('permit_number')
        
        # The Merge Logic
        val = money_map.get(pid, 0.0) # Default to 0 if not found
        
        # Date Logic
        issued = row.get('issue_date', '').split('T')[0]
        applied = row.get('applieddate', '').split('T')[0] if row.get('applieddate') else None
        
        proc_days = None
        if issued and applied:
            try:
                d1 = datetime.strptime(issued, '%Y-%m-%d')
                d2 = datetime.strptime(applied, '%Y-%m-%d')
                proc_days = abs((d1-d2).days)
            except: pass

        clean_rows.append({
            'city': 'Austin',
            'permit_id': pid,
            'applied_date': applied,
            'issued_date': issued,
            'processing_days': proc_days,
            'description': row.get('permit_type_desc', 'Permit'),
            'valuation': val,
            'status': row.get('status_current'),
            'complexity_tier': 'Standard'
        })

    # 4. Save
    print(f"💾 Saving {len(clean_rows)} Records...")
    batch_size = 500
    for i in tqdm(range(0, len(clean_rows), batch_size)):
        batch = clean_rows[i:i + batch_size]
        try:
            supabase.table('permits').upsert(batch, on_conflict='permit_id, city').execute()
        except Exception as e:
            print(f"Error: {e}")

    print("✅ Austin Fix Finalized.")

if __name__ == "__main__":
    run_fix()