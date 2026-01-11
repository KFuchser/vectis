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

VECTIS_BLUE = "#1C2B39"   
VECTIS_BRONZE = "#C87F42" 
VECTIS_RED = "#D32F2F"    # LA
VECTIS_GREY = "#4A5568"   

# --- DATA FACTORY ---
@st.cache_data(ttl=600)
def fetch_strategic_data():
    if not SUPABASE_URL:
        st.error("Missing Supabase Credentials.")
        return pd.DataFrame()
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # Ensure window is wide enough for all newly ingested data
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
        
        # CRITICAL FIX: Ensure every record has a Tier so it isn't dropped by groupby
        df['complexity_tier'] = df['complexity_tier'].fillna("Unknown").replace("", "Unknown")
        df['project_category'] = df['project_category'].fillna("Unclassified")
    return df

# --- UI START ---
st.title("🏛️ VECTIS INDICES")
st.markdown("**National Regulatory Friction Index (NRFI)** | *Live Beta*")

df = fetch_strategic_data()

if df.empty:
    st.warning("No data found.")
    st.stop()

# --- SIDEBAR: DYNAMIC RESTORATION ---
with st.sidebar:
    st.header("Configuration")
    
    # Dynamic Jurisdictions - Automatically detects Los Angeles
    available_cities = sorted(df['city'].unique().tolist())
    sel_cities = st.multiselect("Jurisdiction", available_cities, default=available_cities)
    
    # Dynamic Tiers - Automatically detects Residential, Strategic, etc.
    available_tiers = sorted(df['complexity_tier'].unique().tolist())
    sel_tiers = st.multiselect("AI Complexity Tier", available_tiers, default=available_tiers)

    # Precision numeric input - preserves the "previous version" behavior
    min_val = st.number_input("Minimum Project Valuation ($)", value=0, step=10000)
    
    exclude_noise = st.checkbox("Exclude Same-Day Issuance", value=True)

# --- GLOBAL FILTER LOGIC ---
mask = (
    (df['city'].isin(sel_cities)) & 
    (df['complexity_tier'].isin(sel_tiers)) & 
    (df['valuation'] >= min_val)
)
filtered = df[mask]

if exclude_noise:
    filtered = filtered[((filtered['velocity'] > 0) | (filtered['velocity'].isna()))]

# Keep dropna=False so 'Unknown' tiers still appear in metrics
issued = filtered.dropna(subset=['velocity'])

# --- KPI ROW ---
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Volume", f"{len(filtered):,}", "Active Permits")
c2.metric("Pipeline Value", f"${(filtered['valuation'].sum()/1000000):,.1f}M", "Total CapEx")
c3.metric("Velocity Score", f"{issued['velocity'].median():.0f} Days" if not issued.empty else "---", "Median Time to Issue")
c4.metric("Friction Risk", f"±{issued['velocity'].std():.0f} Days" if not issued.empty else "---", "Uncertainty (Std Dev)")

st.markdown("---")

# --- CHARTS ---
col_left, col_right = st.columns([2, 1])

with col_left:
    st.subheader("🏛️ Bureaucracy Leaderboard")
    if not filtered.empty:
        # dropna=False ensures cities with missing metrics still show up
        stats = filtered.groupby('city', dropna=False).agg({
            'velocity': ['median', 'std'],
            'permit_id': 'count'
        }).reset_index()
        stats.columns = ['Jurisdiction', 'Speed (Lower is Better)', 'Uncertainty (Std Dev)', 'Sample Size']
        st.dataframe(stats, use_container_width=True, hide_index=True)

    st.subheader("📉 Velocity Trends")
    if not issued.empty:
        trend_df = issued.copy()
        trend_df['week'] = trend_df['issued_date'].dt.to_period('W').astype(str)
        trend = trend_df.groupby(['week', 'city'])['velocity'].median().reset_index()
        
        line = alt.Chart(trend).mark_line(point=True).encode(
            x=alt.X('week:O', title='Week of Issuance'),
            y=alt.Y('velocity:Q', title='Median Days to Issue'),
            color=alt.Color('city:N', scale=alt.Scale(domain=['Austin', 'San Antonio', 'Los Angeles', 'Fort Worth'], 
                                                     range=[VECTIS_BLUE, VECTIS_BRONZE, VECTIS_RED, '#A0A0A0'])),
            tooltip=['city', 'week', 'velocity']
        ).properties(height=400).interactive() # Re-enabled Zoom/Pan
        st.altair_chart(line, use_container_width=True)

with col_right:
    st.subheader("🧠 AI Complexity Mix")
    tier_counts = filtered['complexity_tier'].value_counts().reset_index()
    tier_counts.columns = ['tier', 'count']
    
    # Donut chart with zoom interaction
    pie = alt.Chart(tier_counts).mark_pie(innerRadius=60).encode(
        theta=alt.Theta(field="count", type="quantitative"),
        color=alt.Color("tier:N", title="Tier"),
        tooltip=['tier', 'count']
    ).properties(height=350).interactive()
    st.altair_chart(pie, use_container_width=True)

    st.subheader("🔎 Category Drill-Down")
    cat_counts = filtered['project_category'].value_counts().reset_index()
    cat_counts.columns = ['category', 'count']
    
    bar = alt.Chart(cat_counts).mark_bar().encode(
        y=alt.Y("category:N", title=None),
        x=alt.X("count:Q", title=None),
        color=alt.value(VECTIS_BLUE)
    ).properties(height=300)
    st.altair_chart(bar, use_container_width=True)