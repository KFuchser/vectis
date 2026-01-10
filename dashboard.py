import streamlit as st
import pandas as pd
from supabase import create_client, Client
import os
from dotenv import load_dotenv

load_dotenv()

# --- CONFIG ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

@st.cache_data(ttl=600)
def fetch_all_permit_data():
    """
    Fetches ALL records by breaking the 1,000-row Supabase limit.
    """
    if not SUPABASE_URL:
        st.error("Supabase URL missing.")
        return pd.DataFrame()

    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    all_data = []
    page_size = 1000
    current_start = 0
    
    # Progress indicator for the user
    status_text = st.empty()
    
    while True:
        status_text.text(f"📡 Syncing records {current_start} to {current_start + page_size}...")
        
        # Logic: Fetch range [start, end] inclusive
        response = supabase.table('permits').select("*").range(
            current_start, 
            current_start + page_size - 1
        ).execute()
        
        batch = response.data
        if not batch:
            break
            
        all_data.extend(batch)
        
        # If we got less than a full page, we've reached the end
        if len(batch) < page_size:
            break
            
        current_start += page_size

    status_text.empty() # Clear status when done
    return pd.DataFrame(all_data)

# --- DASHBOARD UI ---
st.title("🏛️ VECTIS INDICES")
df = fetch_all_permit_data()

if not df.empty:
    st.success(f"✅ Success: Loaded {len(df):,} total records.")
    # Show city breakdown to verify San Antonio is back
    st.write(df['city'].value_counts())
else:
    st.warning("No data found. Check your ingestion pipeline.")