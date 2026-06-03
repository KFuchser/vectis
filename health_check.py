"""
A data integrity and system health monitoring script for the Vectis pipeline.

It connects to the Supabase database and runs a series of checks, such as verifying
daily data ingestion volume and scanning for data anomalies like "time travel" permits.
"""
import os
from supabase import create_client
from datetime import datetime

# 1. Initialize Connection
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase = create_client(url, key)

def run_health_scan():
    print("🏥 Initiating Vectis Health Scan...")
    today = datetime.now().date()
    
    # --- CHECK 1: The "Pulse" (Did we get data?) ---
    # We check the most recent record to see if ingestion happened recently.
    recent_res = supabase.table('permits') \
        .select('created_at') \
        .order('created_at', desc=True) \
        .limit(1) \
        .execute()
        
    if not recent_res.data:
        print("   🚨 CRITICAL: No records found in database.")
    else:
        last_created = datetime.fromisoformat(recent_res.data[0]['created_at'].split('+')[0]).date()
        print(f"   - Most Recent Ingestion: {last_created}")
        
        if last_created < today:
            print(f"   🚨 CRITICAL: Flatline Alert. Last data was ingested on {last_created}.")
        else:
            print(f"   ✅ Pulse: Active (Data ingested today)")
    
    # --- CHECK 2: The "Time Travel" Regression ---
    # Did any negative processing_days slip through?
    # We check for existence of bad records rather than counting all of them.
    bad_dates_res = supabase.table('permits') \
        .select('id') \
        .lt('processing_days', 0) \
        .limit(5) \
        .execute()
    
    if len(bad_dates_res.data) > 0:
        print(f"   ❌ FAILURE: Found records with negative duration.")
    else:
        print("   ✅ Temporal Logic: Clean")

    # --- CHECK 3: The "Imposter" Leak ---
    imposters_res = supabase.table('permits') \
        .select('id') \
        .ilike('description', '%Model Home%') \
        .neq('complexity_tier', 'Residential') \
        .limit(5) \
        .execute()
    
    if len(imposters_res.data) > 0:
        print(f"   ⚠️ WARNING: 'Model Home' records found in wrong tier.")
    else:
        print("   ✅ Imposter Protocol: Clean")
        
    print("🏁 Health Scan Complete.")

if __name__ == "__main__":
    run_health_scan()