import streamlit as st
import pandas as pd
import altair as alt
from supabase import create_client, Client
import os
from datetime import datetime, timedelta

# --- CONFIG & THEME ---
st.set_page_config(page_title="Vectis Command Console", page_icon="🏛️", layout="wide")

# Correctly fetching secrets for Streamlit Cloud
try:
    SUPABASE_URL = st.secrets["SUPABASE_URL"]
    SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
except:
    # Local fallback for development
    from dotenv import load_dotenv
    load_dotenv()
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")

VECTIS_BLUE = "#1C2B39"   
VECTIS_BRONZE = "#C87F42" 

# --- DATA FETCH ---
@st.cache_data(ttl=600)
def fetch_strategic_data():
    if not SUPABASE_URL:
        st.error("Supabase URL missing.")
        return pd.DataFrame()
        
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    cutoff = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")
    
    all_data = []
    batch_size = 1000
    start = 0
    
    while True:
        response = supabase.table('permits').select("*").filter(
            'applied_date', 'gte', cutoff
        ).range(start, start + batch_size - 1).execute()
        
        batch = response.data
        if not batch: break
        all_data.extend(batch)
        if len(batch) < batch_size: break
        start += batch_size

    df = pd.DataFrame(all_data)
    if not df.empty:
        df['applied_date'] = pd.to_datetime(df['applied_date'], errors='coerce')
        df['issued_date'] = pd.to_datetime(df['issued_date'], errors='coerce')
        df['valuation'] = pd.to_numeric(df['valuation'], errors='coerce').fillna(0)
        df['velocity'] = (df['issued_date'] - df['applied_date']).dt.days
    return df

# --- UI LAYOUT ---
st.title("🏛️ VECTIS COMMAND CONSOLE")
st.markdown("**National Regulatory Friction Index (NRFI)** | *6-Month Strategic View*")

df = fetch_strategic_data()

if df.empty:
    st.warning("No data found in the 6-month window.")
    st.stop()

# --- SIDEBAR ---
with st.sidebar:
    st.header("Story Controls")
    cities = sorted(df['city'].unique().tolist())
    sel_cities = st.multiselect("Jurisdiction", cities, default=cities)
    min_val = st.number_input("Minimum Valuation ($)", value=10000, step=5000)
    exclude_noise = st.checkbox("Exclude Same-Day Permits", value=True)

# --- FILTERING ---
mask = (df['city'].isin(sel_cities)) & (df['valuation'] >= min_val)
if exclude_noise:
    filtered = df[mask & ((df['velocity'] > 0) | (df['velocity'].isna()))]
else:
    filtered = df[mask]

issued = filtered.dropna(subset=['velocity'])

# --- METRICS ---
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Pipeline", f"{len(filtered):,}", "Active/Closed")
c2.metric("Pipeline Value", f"${(filtered['valuation'].sum()/1000000):,.1f}M", "Total CapEx")
c3.metric("Velocity Score", f"{issued['velocity'].median():.0f} Days" if not issued.empty else "-", "Median Speed")
c4.metric("Friction Risk", f"±{issued['velocity'].std():.0f} Days" if not issued.empty else "-", "Std Dev")

st.markdown("---")

# --- CHART LAYOUT (THE FIX) ---
# We define these variables HERE so they exist for the 'with' blocks below
left_col, right_col = st.columns