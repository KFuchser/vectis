"""
Test script for checking API connections and data retrieval for problematic ingestion spokes.

This script focuses on New York, Dallas, and Phoenix, bypassing AI classification and Supabase upserting
to quickly diagnose connection issues and initial data retrieval.
"""
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Import only the problematic city ingestion spokes
# Note: ingest_new_york.py and ingest_dallas.py are assumed to be reverted to original working state for this test
# (i.e., not the debugging states with removed filters or fixed old dates)
from ingest_new_york import get_new_york_data
from ingest_dallas import get_dallas_data

load_dotenv()

# CONFIG
SOCRATA_TOKEN = os.getenv("SOCRATA_APP_TOKEN", None)
# Use a generous cutoff date for testing to ensure data presence if available
TEST_CUTOFF_DATE = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d") # 1 year ago

print(f"--- Running API Connection Test Script ---")
print(f"Test Cutoff Date: {TEST_CUTOFF_DATE}")
print(f"SOCRATA_TOKEN present: {SOCRATA_TOKEN is not None}")
print("-" * 40)

def run_test_for_city(city_name, get_data_func, *args):
    print(f"Attempting to fetch data for {city_name}...")
    try:
        records = get_data_func(*args)
        if records:
            print(f"✅ {city_name}: Successfully retrieved {len(records)} records.")
        else:
            print(f"⚠️ {city_name}: No records found.")
    except Exception as e:
        print(f"❌ {city_name}: Error during data retrieval - {e}")
    print("-" * 40)

if __name__ == "__main__":
    # Test New York
    run_test_for_city("New York", get_new_york_data, SOCRATA_TOKEN, TEST_CUTOFF_DATE)
    
    # Test Dallas
    run_test_for_city("Dallas", get_dallas_data, SOCRATA_TOKEN, TEST_CUTOFF_DATE)
