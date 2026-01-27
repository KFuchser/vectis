import streamlit as st
import pandas as pd
from supabase import create_client, Client

st.set_page_config(layout="wide", page_title="Vectis X-Ray")

# --- 1. DIRECT DATA CONNECTION ---
@st.cache_data(ttl=0) # No caching
def load_raw_data():
    try:
        url = st.secrets["SUPABASE_URL"]
        key = st.secrets["SUPABASE_KEY"]
        supabase: Client = create_client(url, key)
        # Fetch everything, no filters
        response = supabase.table('permits').select("*").limit(5000).execute()
        return pd.DataFrame(response.data)
    except Exception as e:
        st.error(f"Connection Error: {e}")
        return pd.DataFrame()

st.title("🔎 Vectis Data X-Ray")

if st.button("🔄 Force Reload from Database"):
    st.cache_data.clear()
    st.rerun()

df = load_raw_data()

# --- 2. DIAGNOSTICS ---
if df.empty:
    st.error("❌ The Database is EMPTY. The ingestion script said 'Success' but saved nothing.")
else:
    st.success(f"✅ Database contains {len(df)} records.")
    
    # METRICS CHECK
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Records", len(df))
    c2.metric("Commercial", len(df[df['complexity_tier'] == 'Commercial']))
    c3.metric("Residential", len(df[df['complexity_tier'] == 'Residential']))
    c4.metric("Unknown", len(df[df['complexity_tier'] == 'Unknown']))

    # NULL CHECK (The likely culprit)
    st.subheader("⚠️ Data Health Check")
    nulls = df.isnull().sum()
    st.write(nulls[nulls > 0])

    # RAW DATA
    st.subheader("📋 Raw Data Dump (Top 50)")
    st.dataframe(df.head(50))