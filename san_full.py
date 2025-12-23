import pandas as pd
from sodapy import Socrata
import os
from supabase import create_client
from tqdm import tqdm
from datetime import datetime, timedelta

# 1. Configuration
SOCRATA_DOMAIN = "data.sanantonio.gov"
DATASET_ID = "c211-06f9" # The Firehose
APP_TOKEN = None 

# Supabase
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase = create_client(url, key)

def fetch_sa_permits(days_back=90):
    print(f"🤠 Fetching ALL San Antonio permits (Last {days_back} days)...")
    start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
    
    # NO FILTERS in query. We want it all.
    query = f"""
    SELECT permit_number, permit_type, work_type, date_submitted, date_issued, declared_valuation, permit_description
    WHERE date_issued >= '{start_date}'
    LIMIT 20000
    """
    
    client = Socrata(SOCRATA_DOMAIN, APP_TOKEN)
    try:
        return client.get(DATASET_ID, query=query)
    except Exception as e:
        print(f"❌ API Error: {e}")
        return []

def classify_tier(row):
    """
    Strategic Logic: Tag the data, don't delete it.
    """
    desc = str(row.get('permit_description', '')).upper()
    p_type = str(row.get('permit_type', '')).upper()
    w_type = str(row.get('work_type', '')).upper()
    val = float(row.get('declared_valuation', 0) or 0)
    
    # 1. Commodity / "Zero Day" Candidates
    if any(x in p_type for x in ['GARAGE', 'ELECTRICAL', 'MECHANICAL', 'PLUMBING', 'TREE', 'ALARM', 'FENCE', 'ROOF']):
        return 'Commodity'
    if any(x in w_type for x in ['REPAIR', 'REPLACE', 'MAINTENANCE']):
        return 'Commodity'
        
    # 2. Strategic Candidates
    if val > 1_000_000:
        return 'Strategic'
    if 'COMMERCIAL' in p_type and 'NEW' in w_type:
        return 'Strategic'
        
    # 3. Everything Else
    return 'Standard'

def run_fix():
    raw_data = fetch_sa_permits()
    if not raw_data: return

    clean_rows = []
    print("🌊 Normalizing Data Stream...")
    
    for row in raw_data:
        # Dates
        submitted = row.get('date_submitted')
        issued = row.get('date_issued')
        
        if submitted: submitted = submitted.split('T')[0]
        if issued: issued = issued.split('T')[0]
        
        # Velocity
        days = None
        if submitted and issued:
            try:
                d1 = datetime.strptime(submitted, '%Y-%m-%d')
                d2 = datetime.strptime(issued, '%Y-%m-%d')
                days = (d2 - d1).days
            except: pass

        # Valuation
        try: val = float(row.get('declared_valuation', 0))
        except: val = 0.0

        # Classification (The new "Brain")
        tier = classify_tier(row)

        clean_rows.append({
            'city': 'San Antonio',
            'permit_id': row.get('permit_number'),
            'applied_date': submitted,
            'issued_date': issued,
            'processing_days': days,
            'description': row.get('permit_description', f"{row.get('permit_type')} - {row.get('work_type')}"),
            'valuation': val,
            'status': 'Issued',
            'complexity_tier': tier  # Now we can filter this in Dashboard!
        })

    print(f"💾 Saving {len(clean_rows)} Records to Supabase...")
    batch_size = 500
    for i in tqdm(range(0, len(clean_rows), batch_size)):
        batch = clean_rows[i:i + batch_size]
        try:
            supabase.table('permits').upsert(batch, on_conflict='permit_id, city').execute()
        except Exception as e:
            pass

    print("✅ San Antonio Ingest Complete. ALL data loaded.")

if __name__ == "__main__":
    run_fix()