import streamlit as st
import pandas as pd
import altair as alt
from supabase import create_client, Client
import os
from datetime import datetime, timedelta

# --- CONFIG & THEME ---
st.set_page_config(page_title="Vectis Command Console", page_icon="🏛️", layout="wide")

try:
    SUPABASE_URL = st.secrets["SUPABASE_URL"]
    SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
except:
    from dotenv import load_dotenv
    load_dotenv()
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# VECTIS BRAND PALETTE
VECTIS_BLUE = "#1C2B39"   # Primary (Austin)
VECTIS_BRONZE = "#C87F42" # Secondary (San Antonio)
VECTIS_RED = "#D32F2F"    # New: Los Angeles (To highlight the "Friction" contrast)
VECTIS_YELLOW = "#F2C94C" # Tier: Residential
VECTIS_GREY = "#D1D5DB"   # Tier: Awaiting Analysis

# --- DATA FACTORY ---
@st.cache_data(ttl=600)
def fetch_strategic_data():
    if not SUPABASE_URL:
        st.error("Missing Supabase Credentials.")
        return pd.DataFrame()
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # We pull the last 180 days to see the LA impact
    cutoff = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")
    
    response = supabase.table('permits').select("*").filter(
        'applied_date', 'gte', cutoff
    ).execute()
    
    df = pd.DataFrame(response.data)
    if not df.empty:
        df['applied_date'] = pd.to_datetime(df['applied_date'], errors='coerce')
        df['issued_date'] = pd.to_datetime(df['issued_date'], errors='coerce')
        df['valuation'] = pd.to_numeric(df['valuation'], errors='coerce').fillna(0)
        df['velocity'] = (df['issued_date'] - df['applied_date']).dt.days
        
        # Normalize Tiers for the UI
        df['complexity_tier'] = df['complexity_tier'].fillna("Awaiting Analysis")
    return df

# --- UI START ---
st.title("🏛️ VECTIS COMMAND CONSOLE")
st.markdown("**National Regulatory Friction Index (NRFI)** | *Austin • San Antonio • Los Angeles*")

df = fetch_strategic_data()

if df.empty:
    st.warning("No data found in the current window.")
    st.stop()

# --- SIDEBAR ---
with st.sidebar:
    st.header("Story Controls")
    cities = sorted(df['city'].unique().tolist())
    sel_cities = st.multiselect("Jurisdictions", cities, default=cities)
    
    tiers = ['Strategic', 'Commodity', 'Awaiting Analysis']
    sel_tiers = st.multiselect("Complexity Tiers", tiers, default=tiers)
    
    exclude_noise = st.checkbox("Exclude Same-Day Permits", value=True)

# --- FILTER LOGIC ---
mask = (df['city'].isin(sel_cities)) & (df['complexity_tier'].isin(sel_tiers))
filtered = df[mask]
if exclude_noise:
    filtered = filtered[((filtered['velocity'] > 0) | (filtered['velocity'].isna()))]

issued = filtered.dropna(subset=['velocity'])

# --- KPI ROW ---
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Pipeline", f"{len(filtered):,}")
c2.metric("Pipeline Value", f"${(filtered['valuation'].sum()/1000000):,.1f}M")
c3.metric("Avg Velocity", f"{issued['velocity'].mean():.1f} Days" if not issued.empty else "-")
c4.metric("Friction Variance", f"±{issued['velocity'].std():.1f} Days" if not issued.empty else "-")

st.markdown("---")

# --- CHARTS ---
left_col, right_col = st.columns([2, 1])

with left_col:
    st.subheader("📈 Velocity Trends by City")
    if not issued.empty:
        # Create a weekly trend
        trend_df = issued.copy()
        trend_df['week'] = trend_df['issued_date'].dt.to_period('W').astype(str)
        trend = trend_df.groupby(['week', 'city'])['velocity'].median().reset_index()
        
        # Color encoding to explicitly handle LA
        city_colors = alt.Scale(
            domain=['Austin', 'San Antonio', 'Los Angeles'],
            range=[VECTIS_BLUE, VECTIS_BRONZE, VECTIS_RED]
        )
        
        line_chart = alt.Chart(trend).mark_line(point=True).encode(
            x=alt.X('week:O', title='Week Issued'),
            y=alt.Y('velocity:Q', title='Median Days to Issue'),
            color=alt.Color('city:N', scale=city_colors),
            tooltip=['city', 'week', 'velocity']
        ).properties(height=400).interactive()
        
        st.altair_chart(line_chart, use_container_width=True)

with right_col:
    st.subheader("📊 Mix of Complexity")
    tier_counts = filtered['complexity_tier'].value_counts().reset_index()
    tier_counts.columns = ['tier', 'count']
    
    tier_colors = alt.Scale(
        domain=['Strategic', 'Commodity', 'Awaiting Analysis'],
        range=[VECTIS_BRONZE, VECTIS_BLUE, VECTIS_GREY]
    )
    
    donut = alt.Chart(tier_counts).mark_arc(innerRadius=50).encode(
        theta=alt.Theta(field="count", type="quantitative"),
        color=alt.Color("tier:N", scale=tier_colors),
        tooltip=['tier', 'count']
    ).properties(height=350)
    
    st.altair_chart(donut, use_container_width=True)

# --- LEADERBOARD ---
st.subheader("🏛️ Jurisdiction Friction Leaderboard")
if not issued.empty:
    leaderboard = issued.groupby('city').agg({
        'velocity': ['median', 'std'],
        'permit_id': 'count'
    }).reset_index()
    leaderboard.columns = ['City', 'Median Days', 'Risk (Std Dev)', 'Volume']
    st.dataframe(leaderboard.sort_values('Median Days'), use_container_width=True, hide_index=True)