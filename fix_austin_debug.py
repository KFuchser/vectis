import pandas as pd
from sodapy import Socrata
import os
from supabase import create_client
from tqdm import tqdm
from datetime import datetime, timedelta

# 1. Configuration
SOCRATA_DOMAIN = "data.austintexas.gov"
DATASET_ID = "3syk-w9eu" 
APP_TOKEN = None 

# Supabase
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase = create_client(url, key)

def fetch_austin_permits(days_back=90):
    print(f"🤠 Fetching Austin data for last {days_back} days...")
    
    client = Socrata(SOCRATA_DOMAIN, APP_TOKEN)
    start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
    
    # SCHEMA REPAIR V4 Confirmed:
    # We fetch issue_date and applieddate to calculate velocity.
    # Valuation columns are skipped (they don't exist).
    query = f"""
    SELECT 
        permit_number, 
        permit_type_desc, 
        description, 
        issue_date,
        applieddate,
        status_current
    WHERE 
        issue_date >= '{start_date}'
    LIMIT 3000
    """
    
    try:
        results = client.get(DATASET_ID, query=query)
        print(f"   Fetched {len(results)} raw records.")
        return results
    except Exception as e:
        print(f"❌ API Error: {e}")
        return []

def normalize_record(row):
    # 1. Dates & Velocity
    issued = row.get('issue_date')
    applied = row.get('applieddate')
    
    if issued: issued = issued.split('T')[0]
    if applied: applied = applied.split('T')[0]
    
    processing_days = None
    if issued and applied:
        try:
            d_iss = datetime.strptime(issued, '%Y-%m-%d')
            d_app = datetime.strptime(applied, '%Y-%m-%d')
            days = (d_iss - d_app).days
            # Iron Dome: Fix Negative
            if days < 0:
                temp = issued
                issued = applied
                applied = temp
                days = abs(days)
            processing_days = days
        except:
            pass

    # 2. Description
    raw_desc = row.get('description', '')
    type_desc = row.get('permit_type_desc', '')
    if type_desc and type_desc not in raw_desc:
        final_desc = f"{type_desc} - {raw_desc}"
    else:
        final_desc = raw_desc

    return {
        'city': 'Austin',
        'permit_id': row.get('permit_number'),
        'applied_date': applied, 
        'issued_date': issued,
        'processing_days': processing_days,
        'description': final_desc,
        'valuation': 0.0, # Placeholder so it appears in DB
        'status': row.get('status_current'),
        'complexity_tier': 'Standard'
    }

def run_fix():
    raw_data = fetch_austin_permits()
    
    if not raw_data:
        print("❌ No data returned.")
        return

    clean_rows = []
    print("🌊 Normalizing Austin Data...")
    for row in raw_data:
        clean = normalize_record(row)
        clean_rows.append(clean)
            
    print(f"   Prepared {len(clean_rows)} records.")

    if clean_rows:
        print("💾 Saving to Supabase...")
        batch_size = 500
        for i in tqdm(range(0, len(clean_rows), batch_size)):
            batch = clean_rows[i:i + batch_size]
            try:
                # FIX: Added 'on_conflict' to tell Supabase HOW to upsert
                supabase.table('permits').upsert(
                    batch, 
                    on_conflict='permit_id, city'
                ).execute()
            except Exception as e:
                print(f"Error: {e}")
        print("✅ Austin Fix Complete. Velocity metrics should now work.")

if __name__ == "__main__":
    run_fix()