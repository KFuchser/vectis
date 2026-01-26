"""
A Streamlit-based dashboard for visualizing permit data from the Supabase database.

It provides filtering controls and displays metrics and charts, such as a time-series
analysis of permit processing velocity across different cities.
"""
import streamlit as st
import pandas as pd
import altair as alt
from supabase import create_client, Client

st.set_page_config(layout="wide", page_title="Vectis Command Console")

@st.cache_data(ttl=600)
def load_data():
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    supabase: Client = create_client(url, key)
    response = supabase.table('permits').select("*").execute()
    df = pd.DataFrame(response.data)
    
    if not df.empty:
        df['issue_date'] = pd.to_datetime(df['issued_date'], errors='coerce')
        df['applied_date'] = pd.to_datetime(df['applied_date'], errors='coerce')
        df['velocity'] = (df['issue_date'] - df['applied_date']).dt.days
    return df

df_raw = load_data()

# --- VECTIS SIDEBAR CONTROLS ---
st.sidebar.title("🎛️ Command Controls")

# 1. Valuation Floor (The UI Valve)
min_val = st.sidebar.slider("Min Valuation ($)", 0, 1000000, 0, step=10000)

# 2. Tier Selection (V3.0 Taxonomy)
available_tiers = ["Commercial", "Residential", "Commodity", "Unknown"]
selected_tiers = st.sidebar.multiselect("Tiers", available_tiers, default=["Commercial", "Residential"])

# 3. City Selection
cities = df_raw['city'].unique().tolist()
selected_cities = st.sidebar.multiselect("Cities", cities, default=cities)

# Apply Filter
df_filtered = df_raw[
    (df_raw['valuation'] >= min_val) & 
    (df_raw['complexity_tier'].isin(selected_tiers)) &
    (df_raw['city'].isin(selected_cities))
]

# --- METRICS & CHARTS ---
st.title("🏛️ National Regulatory Friction Index")
c1, c2, c3 = st.columns(3)
c1.metric("Total Records", len(df_filtered))
c2.metric("Median Velocity", f"{df_filtered['velocity'].median():.0f} Days")
c3.metric("Total Value", f"${df_filtered['valuation'].sum()/1e6:.1f}M")

st.dataframe(df_filtered.sort_values('issue_date', ascending=False), use_container_width=True)