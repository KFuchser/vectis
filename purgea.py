import os
from supabase import create_client

# Config
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase = create_client(url, key)

def run_purge():
    print("⚠️ STARTING PURGE: Deleting ALL Austin records...")
    
    # Delete where city = 'Austin'
    # Note: Supabase delete requires a filter.
    try:
        data = supabase.table('permits').delete().eq('city', 'Austin').execute()
        print(f"✅ PURGE COMPLETE. Austin data has been wiped.")
    except Exception as e:
        print(f"❌ Error during purge: {e}")

if __name__ == "__main__":
    run_purge()