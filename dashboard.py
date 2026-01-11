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
VECTIS_RED = "#D32F2F"    
VECTIS_GREY = "#D1D5DB"

# --- DATA FACTORY ---
@st.cache_data(ttl=600)
def fetch_strategic_data():
    if not SUPABASE_URL:
        st.error("Missing Supabase Credentials.")
        return pd.DataFrame()
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # Increased window to ensure we catch all of LA's recent ingestion
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
        
        # FIX: Fill missing complexity tiers so they don't get filtered out
        df['complexity_tier'] = df['complexity_tier'].fillna("Awaiting Analysis")
    return df

# --- UI START ---
st.title("🏛️ VECTIS COMMAND CONSOLE")
st.markdown("**National Regulatory Friction Index (NRFI)**")

df = fetch_strategic_data()

if df.empty:
    st.warning("No data found in the current window.")
    st.stop()

# --- SIDEBAR: DYNAMIC FILTERS ---
with st.sidebar:
    st.header("Story Controls")
    
    # Dynamic selection of cities found in the DB
    available_cities = sorted(df['city'].unique().tolist())
    sel_cities = st.multiselect("Jurisdictions", available_cities, default=available_cities)
    
    # DYNAMIC TIER FILTER: This will now show "Residential" if it exists in the data
    available_tiers = sorted(df['complexity_tier'].unique().tolist())
    sel_tiers = st.multiselect("Complexity Tiers", available_tiers, default=available_tiers)
    
    # PRECISION NUMERIC FILTER
    min_val = st.number_input("Minimum Valuation ($)", value=0, step=10000)
    
    exclude_noise = st.checkbox("Exclude Same-Day Permits", value=True)
    
    st.divider()
    st.markdown(f"**City Legend:** \n🔵 Austin | 🟠 San Antonio | 🔴 Los Angeles")

# --- GLOBAL FILTER LOGIC ---
mask = (
    (df['city'].isin(sel_cities)) & 
    (df['complexity_tier'].isin(sel_tiers)) & 
    (df['valuation'] >= min_val)
)
filtered = df[mask]

if exclude_noise:
    # We keep NaN velocity (pending permits) but drop 0-day ones
    filtered = filtered[((filtered['velocity'] > 0) | (filtered['velocity'].isna()))]

issued = filtered.dropna(subset=['velocity'])

# --- KPI ROW ---
c1, c2, c3, c4 = st.columns(4)
c1.metric("Selected Volume", f"{len(filtered):,}")
c2.metric("Pipeline Value", f"${(filtered['valuation'].sum()/1000000):,.1f}M")
c3.metric("Velocity Score", f"{issued['velocity'].median():.0f} Days" if not issued.empty else "---")
c4.metric("Friction Risk", f"±{issued['velocity'].std():.0f} Days" if not issued.empty else "---")

st.markdown("---")

# --- CHART LAYOUT ---
left_col, right_col = st.columns([2, 1])

with left_col:
    st.subheader("📈 Velocity Trends (Scroll to Zoom)")
    if not issued.empty:
        trend_df = issued.copy()
        trend_df['week'] = trend_df['issued_date'].dt.to_period('W').astype(str)
        trend = trend_df.groupby(['week', 'city'])['velocity'].median().reset_index()
        
        city_colors = alt.Scale(
            domain=['Austin', 'San Antonio', 'Los Angeles', 'Fort Worth'],
            range=[VECTIS_BLUE, VECTIS_BRONZE, VECTIS_RED, '#A0A0A0']
        )
        
        line = alt.Chart(trend).mark_line(point=True).encode(
            x=alt.X('week:O', title='Week Issued'),
            y=alt.Y('velocity:Q', title='Median Days'),
            color=alt.Color('city:N', scale=city_colors),
            tooltip=['city', 'week', 'velocity']
        ).properties(height=400).interactive()
        
        st.altair_chart(line, use_container_width=True)

with right_col:
    st.subheader("📊 Tier Distribution")
    if not filtered.empty:
        tier_counts = filtered['complexity_tier'].value_counts().reset_index()
        tier_counts.columns = ['tier', 'count']
        
        donut = alt.Chart(tier_counts).mark_arc(innerRadius=50).encode(
            theta=alt.Theta(field="count", type="quantitative"),
            color=alt.Color("tier:N", title="Tier"),
            tooltip=['tier', 'count']
        ).properties(height=350).interactive()
        
        st.altair_chart(donut, use_container_width=True)

# --- LEADERBOARD: THE LA FIX ---
st.subheader("🏛️ Jurisdiction Friction Leaderboard")
if not filtered.empty:
    # Use dropna=False to ensure cities with missing metrics still show up
    stats = filtered.groupby('city', dropna=False).agg({
        'velocity': ['median', 'std'],
        'permit_id': 'count'
    }).reset_index()
    
    stats.columns = ['Jurisdiction', 'Median Days', 'Risk (±Days)', 'Volume']
    
    # Display the table with formatting
    st.dataframe(
        stats.style.format({
            'Median Days': '{:.0f}', 
            'Risk (±Days)': '±{:.0f}', 
            'Volume': '{:,}'
        }), 
        use_container_width=True, 
        hide_index=True
    )