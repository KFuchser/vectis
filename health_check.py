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
    # We check if any records were updated/created in the last 24 hours.
    # Note: Since your schema uses 'created_at', we filter by that.
    # For a robust check, we might look at 'applied_date' recency, but this is a system check.
    
    # Fetch count of records created today (UTC)
    response = supabase.table('permits') \
        .select('*', count='exact', head=True) \
        .gte('created_at', f"{today} 00:00:00") \
        .execute()
        
    daily_volume = response.count
    print(f"   - Daily Ingestion Volume: {daily_volume} records")
    
    if daily_volume == 0:
        print("   🚨 CRITICAL: Flatline Alert. No data ingested today.")
        # In production, this would trigger an email/SMS via Twilio or SendGrid
    
    # --- CHECK 2: The "Time Travel" Regression ---
    # Did any negative processing_days slip through?
    bad_dates = supabase.table('permits') \
        .select('*', count='exact', head=True) \
        .lt('processing_days', 0) \
        .execute()
        
    if bad_dates.count > 0:
        print(f"   ❌ FAILURE: Found {bad_dates.count} records with negative duration.")
    else:
        print("   ✅ Temporal Logic: Clean")

    # --- CHECK 3: The "Imposter" Leak ---
    # Did 'Model Home' slip into a Strategic/Commercial tier?
    # We query for descriptions containing 'Model Home' that represent High Value logic
    # (Assuming High Value logic might be flagged elsewhere, but here we check tier consistency)
    imposters = supabase.table('permits') \
        .select('*', count='exact', head=True) \
        .ilike('description', '%Model Home%') \
        .neq('complexity_tier', 'Residential') \
        .neq('complexity_tier', 'Standard') \
        .execute()
        
    if imposters.count > 0:
        print(f"   ⚠️ WARNING: {imposters.count} 'Model Home' records found in wrong tier.")
    else:
        print("   ✅ Imposter Protocol: Clean")
        
    print("🏁 Health Scan Complete.")

if __name__ == "__main__":
    run_health_scan()