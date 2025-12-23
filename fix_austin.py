import pandas as pd
from sodapy import Socrata
import os
from supabase import create_client
from tqdm import tqdm
from datetime import datetime, timedelta

# 1. Configuration
SOCRATA_DOMAIN = "data.austintexas.gov"
DATASET_ID = "3syk-w9eu" # 'Issued Construction Permits'
APP_TOKEN = None # Optional, helps with throttling

# Supabase
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase = create_client(url, key)

def fetch_austin_permits(days_back=90):
    print(f"🤠 Fetching Austin data for last {days_back} days...")
    
    client = Socrata(SOCRATA_DOMAIN, APP_TOKEN)
    
    # Calculate start date
    start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
    
    # Query: Get permits issued recently
    # Note: We select specific columns to speed up the fetch
    query = f"""
    SELECT 
        permit_num, 
        permit_type_desc, 
        work_description, 
        calendar_date_issued, 
        issued_date,
        status_current, 
        total_job_valuation, 
        total_valuation_remodel, 
        est_project_cost
    WHERE 
        issued_date >= '{start_date}' 
        AND status_current = 'Active'
    LIMIT 5000
    """
    
    try:
        results = client.get(DATASET_ID, query=query)
        print(f"   Fetched {len(results)} raw records.")
        return results
    except Exception as e:
        print(f"❌ API Error: {e}")
        return []

def normalize_record(row):
    """
    The Waterfall Logic: cascading checks to find the real money.
    """
    # 1. Valuation Waterfall
    # Try 'total_job_valuation' first (Standard)
    # Then 'total_valuation_remodel' (The Austin Trap)
    # Then 'est_project_cost' (Fallback)
    val = row.get('total_job_valuation')
    if not val or float(val) == 0:
        val = row.get('total_valuation_remodel')
    if not val or float(val) == 0:
        val = row.get('est_project_cost')
        
    valuation = float(val) if val else 0.0

    # 2. Description Merge
    # Combine type and description for better context
    desc = f"{row.get('permit_type_desc', '')} - {row.get('work_description', '')}"

    # 3. Date Normalization (Handle ISO strings)
    # Austin often puts the real issue date in 'calendar_date_issued'
    issued = row.get('calendar_date_issued')
    if not issued:
        issued = row.get('issued_date')
        
    if issued:
        issued = issued.split('T')[0] # Clean '2025-10-01T00:00:00' -> '2025-10-01'

    # 4. Proxy Applied Date
    # Austin's public feed often omits 'Applied Date'. 
    # For MVP, we use Issued Date as a placeholder (Velocity = 0) or leave null.
    # We will leave it NULL so it doesn't skew our Velocity metrics with fake "0 day" approvals.
    applied = None 

    return {
        'city': 'Austin',
        'permit_id': row.get('permit_num'),
        'applied_date': applied, 
        'issued_date': issued,
        'processing_days': None, # Cannot calc velocity without applied date
        'description': desc,
        'valuation': valuation,
        'status': row.get('status_current'),
        'complexity_tier': 'Standard' # Default (Lizard Brain)
    }

def run_fix():
    # 1. Fetch
    raw_data = fetch_austin_permits()
    
    # 2. Process
    clean_rows = []
    print("🌊 Applying Valuation Waterfall...")
    for row in raw_data:
        clean = normalize_record(row)
        # Filter: Only keep records with actual value to save DB space
        if clean['valuation'] > 1000: 
            clean_rows.append(clean)
            
    print(f"   Retained {len(clean_rows)} high-value records.")

    # 3. Upsert
    print("💾 Saving to Supabase...")
    batch_size = 500
    for i in tqdm(range(0, len(clean_rows), batch_size)):
        batch = clean_rows[i:i + batch_size]
        try:
            supabase.table('permits').upsert(batch).execute()
        except Exception as e:
            print(f"Error: {e}")

    print("✅ Austin Fix Complete. Check your Dashboard.")

if __name__ == "__main__":
    run_fix()