"""
STEP 2: SUPABASE DATA VALIDATION
Checks the physical database state to confirm ingestion success.
"""
import os
import sys
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

# Windows consoles default to cp1252 and choke on emoji; guard for streams without reconfigure().
for _stream in (sys.stdout, sys.stderr):
    if hasattr(_stream, "reconfigure"):
        try:
            _stream.reconfigure(encoding="utf-8")
        except Exception:
            pass

# Cities configured in ingest_velocity_50.py
CITIES = ["Austin", "San Antonio", "Fort Worth", "Los Angeles",
          "Chicago", "New York", "San Francisco"]


def audit_db():
    print("🕵️ Auditing Supabase Storage...")
    try:
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_KEY")
        supabase = create_client(url, key)

        # Per-city bounded queries. The old version pulled every row in one request, which
        # PostgREST silently truncates at 1000 rows (and without ORDER BY the sample was
        # arbitrary), so counts/date ranges were wrong on a ~570k-row table. Each query
        # below returns at most 1 row (+ an exact count) and is filtered by city.
        def _date_edge(city, desc):
            res = (supabase.table("permits").select("issued_date")
                   .eq("city", city).not_.is_("issued_date", "null")
                   .order("issued_date", desc=desc).limit(1).execute())
            return res.data[0]["issued_date"] if res.data else None

        rows = []
        for city in CITIES:
            try:
                res = (supabase.table("permits").select("permit_id", count="exact")
                       .eq("city", city).limit(1).execute())
                rows.append((city, res.count or 0, _date_edge(city, False), _date_edge(city, True)))
            except Exception as e:
                print(f"⚠️ {city}: query failed ({type(e).__name__}: {e})")
                rows.append((city, None, None, None))

        df = pd.DataFrame(rows, columns=["city", "records", "min_issued", "max_issued"])

        if df["records"].fillna(0).sum() == 0:
            print("❌ DATABASE IS EMPTY (or all queries failed).")
            return

        print("\n📊 RECORD COUNTS BY CITY:")
        print("------------------------")
        print(df[["city", "records"]].to_string(index=False))
        print("------------------------")

        # Check Date Ranges
        print("\n📅 DATE RANGES (Verify no 'Over-Filtering'):")
        for _, r in df.iterrows():
            print(f"- {r['city']}: {r['min_issued']} to {r['max_issued']}")

        # Check for San Antonio specifically. A failed/timed-out query (records is NaN/None)
        # is NOT the same as a successful query that returned 0 rows.
        sa_records = df.loc[df["city"] == "San Antonio", "records"].iloc[0]
        if pd.isna(sa_records):
            print("\n⚠️ San Antonio: query failed (see error above), presence unknown.")
        elif int(sa_records) == 0:
            print("\n⚠️ ALERT: San Antonio is MISSING from the database.")
        else:
            print(f"\n✅ San Antonio is present ({int(sa_records)} records).")

    except Exception as e:
        print(f"❌ Audit Error: {e}")

if __name__ == "__main__":
    audit_db()