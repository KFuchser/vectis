import streamlit as st
import pandas as pd
import altair as alt
from supabase import create_client, Client
import os
from datetime import datetime, timedelta

# --- CONFIG & THEME ---
st.set_page_config(page_title="Vectis Command Console", page_icon="🏛️", layout="wide")
SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]

VECTIS_BLUE = "#1C2B39"   # Slate Blue
VECTIS_BRONZE = "#C87F42" # Architectural Bronze

# --- OPTIMIZED DATA FETCH ---
@st.cache_data(ttl=600)
def fetch_strategic_data():
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # 1. Define the 6-Month Window
    cutoff = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")
    
    # 2. Batched Fetch (Pagination) to handle the San Antonio "Firehose"
    all_data = []
    batch_size = 1000
    start = 0
    
    while True:
        # Query: Applied in last 6 months
        response = supabase.table('permits').select("*").filter(
            'applied_date', 'gte', cutoff
        ).range(start, start + batch_size - 1).execute()
        
        batch = response.data
        if not batch: break
        all_data.extend(batch)
        if len(batch) < batch_size: break
        start += batch_size

    df = pd.DataFrame(all_data)
    if df.empty: return df

    # Normalization
    df['applied_date'] = pd.to_datetime(df['applied_date'], errors='coerce')
    df['issued_date'] = pd.to_datetime(df['issued_date'], errors='coerce')
    df['valuation'] = pd.to_numeric(df['valuation'], errors='coerce').fillna(0)
    df['velocity'] = (df['issued_date'] - df['applied_date']).dt.days
    
    return df

# --- UI LOGIC ---
st.title("🏛️ VECTIS COMMAND CONSOLE")
st.markdown("**National Regulatory Friction Index (NRFI)** | *6-Month Performance View*")

df = fetch_strategic_data()

# --- SIDEBAR FILTERS ---
with st.sidebar:
    st.header("Story Controls")
    cities = sorted(df['city'].unique().tolist())
    sel_cities = st.multiselect("Jurisdiction", cities, default=cities)
    
    # Precision Input for Valuation (Fixed)
    min_val = st.number_input("Minimum Valuation ($)", value=10000, step=5000)
    
    # The Noise Filter (Default ON for clean storytelling)
    exclude_noise = st.checkbox("Exclude Same-Day Permits", value=True)

# --- FILTERING ---
mask = (df['city'].isin(sel_cities)) & (df['valuation'] >= min_val)
if exclude_noise:
    # Filter 0-day velocity from metrics, keep Active permits (NaN)
    filtered = df[mask & ((df['velocity'] > 0) | (df['velocity'].isna()))]
else:
    filtered = df[mask]

issued = filtered.dropna(subset=['velocity'])

# --- METRICS ROW ---
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Pipeline", f"{len(filtered):,}", "Units")
c2.metric("Pipeline Value", f"${(filtered['valuation'].sum()/1000000):,.1f}M", "Total CapEx")
c3.metric("Velocity Score", f"{issued['velocity'].median():.0f} Days" if not issued.empty else "-", "Median Speed")
c4.metric("Friction Risk", f"±{issued['velocity'].std():.0f} Days" if not issued.empty else "-", "Uncertainty (Std Dev)")

st.markdown("---")

# --- LEADERBOARD & TRENDS ---
col_l, col_r = st.columns([2, 1])

with col_l:
    st.subheader("📉 Bureaucracy Leaderboard")
    if not issued.empty:
        stats = issued.groupby('city')['velocity'].agg(['median', 'std', 'count']).reset_index()
        stats.columns = ['Jurisdiction', 'Speed (Days)', 'Risk (±Days)', 'Volume']
        st.dataframe(stats, use_container_width=True, hide_index=True)
        
    st.subheader("📈 Velocity Trends")
    if not issued.empty:
        chart_df = issued.copy()
        chart_df['week'] = chart_df['issued_date'].dt.to_period('W').astype(str)
        trend = chart_df.groupby(['week', 'city'])['velocity'].median().reset_index()
        line = alt.Chart(trend).mark_line(point=True).encode(
            x='week', y='velocity', color='city', tooltip=['city', 'week', 'velocity']
        ).properties(height=300)
        st.altair_chart(line, use_container_width=True)

with col_r:
    st.subheader("📊 Complexity Tier")
    # Only show meaningful tiers
    valid_tiers = filtered[filtered['complexity_tier'] != "Unknown"]
    if not valid_tiers.empty:
        tier_counts = valid_tiers['complexity_tier'].value_counts().reset_index()
        pie = alt.Chart(tier_counts).mark_arc(outerRadius=100).encode(
            theta="count", color=alt.Color("complexity_tier", scale=alt.Scale(range=[VECTIS_BRONZE, VECTIS_BLUE]))
        )
        st.altair_chart(pie, use_container_width=True)
    else:
        st.info("Pending AI Classification...")