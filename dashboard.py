import streamlit as st
import pandas as pd
import altair as alt
from supabase import create_client, Client
from datetime import datetime

st.set_page_config(layout="wide", page_title="Vectis Command Console")

# --- 1. STYLING ---
st.markdown("""
    <style>
    .stApp { background-color: #F8F9FA; }
    div[data-testid="metric-container"] {
        background-color: #FFFFFF;
        border-left: 5px solid #C87F42;
        padding: 15px;
        border-radius: 5px;
        box-shadow: 0 2px 5px rgba(0,0,0,0.05);
    }
    h1, h2, h3 { font-family: 'Arial', sans-serif; color: #1C2B39; }
    </style>
    """, unsafe_allow_html=True)

# --- 2. DATA LOADING (FIXED) ---
@st.cache_data(ttl=600)
def load_data():
    try:
        url = st.secrets["SUPABASE_URL"]
        key = st.secrets["SUPABASE_KEY"]
        supabase: Client = create_client(url, key)
        
        # CRITICAL FIX 1: Use .range(0, 5000) to break the 1000-row default limit
        response = supabase.table('permits')\
            .select("*")\
            .order('issued_date', desc=True)\
            .range(0, 5000)\
            .execute()
            
        df = pd.DataFrame(response.data)
        
        if not df.empty:
            df['issue_date'] = pd.to_datetime(df['issued_date'], errors='coerce')
            df['applied_date'] = pd.to_datetime(df['applied_date'], errors='coerce')
            
            # CRITICAL FIX 2: Filter out FUTURE dates (Time Travel Bug)
            # This prevents dates like '2026-08-13' from ruining the chart scale
            now = pd.Timestamp.now()
            df = df[df['issue_date'] <= now]
            
            # Calculate Velocity
            df['velocity'] = (df['issue_date'] - df['applied_date']).dt.days
            
        return df
    except Exception as e:
        return pd.DataFrame()

# --- SIDEBAR ---
st.sidebar.title("Vectis Command")

if st.sidebar.button("🔄 Force Refresh"):
    st.cache_data.clear()
    st.rerun()

df_raw = load_data()

# FILTERS
min_val = st.sidebar.slider("Valuation Floor ($)", 0, 1000000, 0, step=10000)
all_tiers = ["Commercial", "Residential", "Commodity", "Unknown"]
selected_tiers = st.sidebar.multiselect("Complexity Tiers", all_tiers, default=all_tiers)
cities = sorted(df_raw['city'].unique().tolist()) if not df_raw.empty else []
selected_cities = st.sidebar.multiselect("Jurisdictions", cities, default=cities)

if not df_raw.empty:
    df = df_raw[
        (df_raw['valuation'] >= min_val) & 
        (df_raw['complexity_tier'].isin(selected_tiers)) &
        (df_raw['city'].isin(selected_cities))
    ].copy()
else:
    df = pd.DataFrame()

# --- MAIN DASHBOARD ---
st.title("🏛️ National Regulatory Friction Index")

if df.empty:
    st.warning("No records found.")
    st.stop()

# METRICS
real_projects = df[df['velocity'] >= 0]
median_vel = real_projects['velocity'].median() if not real_projects.empty else 0

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Volume", len(df)) # Should now exceed 1000
c2.metric("Median Lead Time", f"{median_vel:.0f} Days")
c3.metric("Pipeline Value", f"${df['valuation'].sum()/1e6:.1f}M")
c4.metric("High Friction (>180d)", len(df[df['velocity'] > 180]))

st.divider()

# CHARTS
c_left, c_right = st.columns([2, 1])

with c_left:
    st.subheader("📉 Weekly Velocity Trends")
    chart_df = df.dropna(subset=['issue_date', 'velocity'])
    chart_df = chart_df[chart_df['velocity'] >= 0]
    
    if not chart_df.empty:
        # Weekly Grouping
        chart_df['week'] = chart_df['issue_date'].dt.to_period('W').apply(lambda r: r.start_time)
        
        line = alt.Chart(chart_df).mark_line(point=True).encode(
            x=alt.X('week:T', title='Week Of', axis=alt.Axis(format='%b %d')),
            y=alt.Y('median(velocity):Q', title='Median Days'),
            color='city:N',
            tooltip=['city', 'week', 'median(velocity)', 'count()']
        ).properties(height=350).interactive()
        st.altair_chart(line, use_container_width=True)
    else:
        st.info("No velocity data available.")

with c_right:
    st.subheader("🏷️ Tier Breakdown")
    bar = alt.Chart(df).mark_arc(innerRadius=50).encode(
        theta=alt.Theta("count():Q"),
        color=alt.Color("complexity_tier:N"),
        tooltip=["complexity_tier", "count()"]
    ).properties(height=350)
    st.altair_chart(bar, use_container_width=True)

# DATA TABLE
st.subheader("📋 Recent Permit Manifest")
st.dataframe(
    df[['city', 'complexity_tier', 'valuation', 'velocity', 'description', 'issue_date']]
    .sort_values('issue_date', ascending=False)
    .head(100),
    use_container_width=True
)