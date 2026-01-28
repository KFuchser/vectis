"""
STEP 2: SUPABASE DATA VALIDATION
Checks the physical database state to confirm ingestion success.
"""
import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

def audit_db():
    print("🕵️ Auditing Supabase Storage...")
    try:
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_KEY")
        supabase = create_client(url, key)

        # Fetch all 'city' and 'issued_date' fields (lightweight query)
        # We assume < 50,000 records for now
        res = supabase.table("permits").select("city, issued_date, permit_id").execute()
        
        df = pd.DataFrame(res.data)
        
        if df.empty:
            print("❌ DATABASE IS EMPTY.")
            return

        print("\n📊 RECORD COUNTS BY CITY:")
        print("------------------------")
        print(df['city'].value_counts())
        print("------------------------")

        # Check Date Ranges
        print("\n📅 DATE RANGES (Verify no 'Over-Filtering'):")
        for city in df['city'].unique():
            city_df = df[df['city'] == city]
            min_date = city_df['issued_date'].min()
            max_date = city_df['issued_date'].max()
            print(f"- {city}: {min_date} to {max_date}")

        # Check for San Antonio specifically
        sa_count = len(df[df['city'] == 'San Antonio'])
        if sa_count == 0:
            print("\n⚠️ ALERT: San Antonio is MISSING from the database.")
        else:
            print(f"\n✅ San Antonio is present ({sa_count} records).")

    except Exception as e:
        print(f"❌ Audit Error: {e}")

if __name__ == "__main__":
    audit_db()