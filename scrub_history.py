"""
A historical data sanitization script, also known as "Operation Deep Clean".

This script fetches all records from the 'permits' table in Supabase, applies a series of
"Iron Dome" data cleaning protocols (e.g., fixing date paradoxes, reclassifying zero-value
and imposter records), and then pushes the cleaned data back to the database.
"""
import os
import pandas as pd
import numpy as np
from supabase import create_client
from tqdm import tqdm

# 1. Initialize Connection
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase = create_client(url, key)

def fetch_all_records():
    print("📡 Fetching raw data from table 'permits'...")
    all_rows = []
    offset = 0
    limit = 1000
    
    while True:
        response = supabase.table('permits').select('*').range(offset, offset + limit - 1).execute()
        rows = response.data
        if not rows:
            break
        all_rows.extend(rows)
        offset += limit
        print(f"   ...fetched {len(all_rows)} records")
        
    return pd.DataFrame(all_rows)

def apply_iron_dome_protocols(df):
    print("🛡️ Applying Iron Dome sanitization protocols...")
    
    # --- PROTOCOL A: The "Time Travel" Patch ---
    # Convert to datetime safely
    df['applied_date'] = pd.to_datetime(df['applied_date'], errors='coerce')
    df['issued_date'] = pd.to_datetime(df['issued_date'], errors='coerce')
    
    # Identify paradoxes
    mask_time_travel = df['issued_date'] < df['applied_date']
    
    # Swap Logic
    df.loc[mask_time_travel, 'temp_date'] = df.loc[mask_time_travel, 'applied_date']
    df.loc[mask_time_travel, 'applied_date'] = df.loc[mask_time_travel, 'issued_date']
    df.loc[mask_time_travel, 'issued_date'] = df.loc[mask_time_travel, 'temp_date']
    
    # SCHEMA ALIGNMENT: Calculate 'processing_days' (not velocity_days)
    df['processing_days'] = (df['issued_date'] - df['applied_date']).dt.days
    
    # --- PROTOCOL B: The "Zero Value" Trap ---
    df['valuation'] = df['valuation'].fillna(0)
    # Cast to numeric to ensure comparison works
    df['valuation'] = pd.to_numeric(df['valuation'], errors='coerce').fillna(0)
    
    mask_zero_val = df['valuation'] <= 100 
    # Update 'complexity_tier' as per Schema
    df.loc[mask_zero_val, 'complexity_tier'] = 'Standard'
    
    # --- PROTOCOL C: The "Imposter" Regex ---
    imposter_pattern = r'(?:Model Home|Bedroom|Addition|Home Office|Single Family)'
    df['description'] = df['description'].astype(str)
    mask_imposter = df['description'].str.contains(imposter_pattern, case=False, regex=True)
    
    # Update 'complexity_tier' (reclassifying imposters as Residential)
    df.loc[mask_imposter, 'complexity_tier'] = 'Residential'
    
    # --- STATS ---
    print(f"   - Fixed {mask_time_travel.sum()} Time Travel errors.")
    print(f"   - Downgraded {mask_zero_val.sum()} Zero Value records.")
    print(f"   - Quarantined {mask_imposter.sum()} Imposter records.")
    
    # --- CLEANUP ---
    df.drop(columns=['temp_date'], inplace=True, errors='ignore')
    
    # SCHEMA ALIGNMENT: Drop 'velocity_days' if it exists to avoid confusion
    if 'velocity_days' in df.columns:
        df.drop(columns=['velocity_days'], inplace=True)
    
    # Format dates for API
    df['applied_date'] = df['applied_date'].dt.strftime('%Y-%m-%d').replace('NaT', None)
    df['issued_date'] = df['issued_date'].dt.strftime('%Y-%m-%d').replace('NaT', None)
    
    # Sanitizing NaNs for JSON compliance (Supabase rejects NaN)
    print("   - Sanitizing NaNs for JSON compliance...")
    df = df.replace({np.nan: None})
    df = df.where(pd.notnull(df), None)
    
    return df

def push_updates(df):
    print("💾 Pushing sanitized data back to 'permits'...")
    
    # Double check we aren't sending columns that don't exist
    valid_columns = [
        'id', 'city', 'permit_id', 'applied_date', 'issued_date', 
        'processing_days', 'description', 'valuation', 'status', 'complexity_tier'
    ]
    
    # Filter dataframe to only include valid columns (and ID for upsert)
    # This prevents errors if you have extra columns in your local dataframe
    df_final = df[df.columns.intersection(valid_columns)].copy()
    
    records = df_final.to_dict(orient='records')
    batch_size = 500
    
    for i in tqdm(range(0, len(records), batch_size)):
        batch = records[i:i + batch_size]
        try:
            supabase.table('permits').upsert(batch).execute()
        except Exception as e:
            print(f"❌ Batch failed: {e}")

if __name__ == "__main__":
    raw_df = fetch_all_records()
    if not raw_df.empty:
        clean_df = apply_iron_dome_protocols(raw_df)
        push_updates(clean_df)
        print("✅ Operation Deep Clean Complete.")
    else:
        print("⚠️ No records found in 'permits'.")