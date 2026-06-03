import os
from supabase import create_client
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

def purge_old_new_york():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    supabase = create_client(url, key)
    
    cutoff = (datetime.now() - timedelta(days=24*30)).strftime("%Y-%m-%d")
    print(f"🧹 Purging New York records older than {cutoff}...")
    
    try:
        # Purge by Issued Date
        count_res = supabase.table('permits').select('id', count='exact').eq('city', 'New York').lt('issued_date', cutoff).execute()
        print(f"   Found {count_res.count} records with old Issued Date to purge.")
        if count_res.count > 0:
            supabase.table('permits').delete().eq('city', 'New York').lt('issued_date', cutoff).execute()
            print("   ✅ Issued Date purge complete.")

        # Purge by Applied Date
        count_res_app = supabase.table('permits').select('id', count='exact').eq('city', 'New York').lt('applied_date', cutoff).execute()
        print(f"   Found {count_res_app.count} records with old Applied Date to purge.")
        if count_res_app.count > 0:
            supabase.table('permits').delete().eq('city', 'New York').lt('applied_date', cutoff).execute()
            print("   ✅ Applied Date purge complete.")
        
        print(f"✅ Full historical New York cleanup complete.")
        
    except Exception as e:
        print(f"❌ Error during purge: {e}")

if __name__ == "__main__":
    purge_old_new_york()