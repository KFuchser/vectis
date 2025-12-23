"""
The main Streamlit dashboard for visualizing and analyzing permit data.
"""
import streamlit as st
import pandas as pd
import altair as alt
from supabase import create_client, Client
import os
from dotenv import load_dotenv

# --- 1. VECTIS BRAND CONFIG ---
st.set_page_config(
    page_title="Vectis Indices | Risk Intelligence",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- 2. CSS STYLING (The "Slate & Bronze" Identity) ---
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter+Tight:wght@400;600;800&family=JetBrains+Mono&display=swap');
    
    html, body, [class*="css"] { 
        font-family: 'Inter Tight', sans-serif; 
        color: #1C2B39; 
    }
    
    .stMetric { font-family: 'JetBrains Mono', monospace; }
    div[data-testid="stMetricValue"] { color: #C87F42 !important; font-weight: 700; }
    h1, h2, h3 { color: #1C2B39 !important; font-weight: 800; letter-spacing: -0.5px; }
    
    /* Metric Card Styling */
    div[data-testid="metric-container"] {
        background-color: #FFFFFF;
        border-left: 4px solid #C87F42;
        padding: 10px;
        box-shadow: 0px 2px 4px rgba(0,0,0,0.05);
    }
    
    section[data-testid="stSidebar"] { background-color: #F0F4F8; border-right: 1px solid #d1d5db; }
    </style>
    """, unsafe_allow_html=True)

# --- 3. DATA CONNECTION ---
try:
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
except:
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")

if not url or not key:
    st.error("Credentials missing. Check .env or Streamlit Secrets.")
    st.stop()

supabase: Client = create_client(url, key)

@st.cache_data(ttl=600)
def load_data():
    """
    Fetches data from 'permits' (The ACTUAL table).
    Includes Outlier Caps and Robust Valuation Mapping.
    """
    all_records = []
    batch_size = 1000
    offset = 0
    
    while True:
        # Fetch from 'permits'
        response = supabase.table('permits').select("*").range(offset, offset + batch_size - 1).execute()
        records = response.data
        all_records.extend(records)
        if len(records) < batch_size:
            break
        offset += batch_size

    df = pd.DataFrame(all_records)
    
    if df.empty:
        return df

    # --- 1. CLEANING & MAPPING ---
    column_map = {
        'issue_date': 'issued_date',
        'work_description': 'description',
        'est_value': 'valuation',
        'project_cost': 'valuation',
        'total_job_valuation': 'valuation' # The Critical Fix for Austin
    }
    df = df.rename(columns=column_map)

    # --- 2. ROBUST TYPE CONVERSION ---
    # Fix Dates
    df['issued_date'] = pd.to_datetime(df['issued_date'], errors='coerce')
    df['applied_date'] = pd.to_datetime(df['applied_date'], errors='coerce')
    
    # Fix Valuation (Handle '$', ',', and text)
    if 'valuation' in df.columns:
        df['valuation'] = df['valuation'].astype(str).str.replace(r'[$,]', '', regex=True)
        df['valuation'] = pd.to_numeric(df['valuation'], errors='coerce').fillna(0)
    else:
        df['valuation'] = 0.0

    # --- 3. LOGIC FILTERS ---
    # A. The "Time Traveler" Patch (No Futures)
    df = df[df['issued_date'] <= pd.Timestamp.now()]

    # B. Calculate Velocity
    df['velocity'] = (df['issued_date'] - df['applied_date']).dt.days

    # C. THE SANITY CAPS (Outlier Protection)
    # Exclude negative durations (clerical error)
    df.loc[df['velocity'] < 0, 'velocity'] = None 
    # Exclude "Ancient" holds (Outliers > 10 years likely data errors)
    df.loc[df['velocity'] > 3650, 'velocity'] = None 

    return df

# --- 4. MAIN DASHBOARD ---
def main():
    st.title("VECTIS INDICES")
    st.markdown("**National Regulatory Friction Index (NRFI)** | *Live Beta*")
    st.markdown("---")

    with st.spinner('Accessing Data Factory...'):
        df = load_data()

    if df.empty:
        st.warning("System Online. Awaiting Data Ingestion...")
        st.stop()

    # --- SIDEBAR CONTROLS ---
    st.sidebar.header("Configuration")
    
    # 1. City Filter
    cities = sorted(df['city'].unique().tolist())
    selected_cities = st.sidebar.multiselect("Jurisdiction", cities, default=cities)
    
    # 2. Date Filter
    if not df['