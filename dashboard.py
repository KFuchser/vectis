"""
A Streamlit-based dashboard for visualizing permit data from the Supabase database.

It provides filtering controls and displays metrics and charts, such as a time-series
analysis of permit processing velocity across different cities.
"""
"""
Vectis Command Console - V3.0 Taxonomy Patch
Mission: Visualize 3-Tier Permit Data (Commodity, Residential, Commercial).
Strategy: UI-layer filtering for Valuation and Jurisdiction.
"""
import streamlit as st
import pandas as pd
import altair as alt
from supabase import create_client, Client

# --- PAGE SETUP ---
st.set_page_config(layout="wide", page_title="Vectis Command Console")
st.markdown("""
    <style>
    .metric-card { background-color: #F0F4F8; border-left: 5px solid #C87F42; padding: 15px; border-radius: 5px; }
    .stMetric { background-color: #ffffff; padding: 10px; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }
    </style>
    """, unsafe_allow_html=True)

# --- PERMISSIVE DATA LOADER ---
@st.cache_data(ttl=600)
def load_data():
    try:
        # Pulling from Streamlit Secrets for Solopreneur Stack security
        url = st.secrets["SUPABASE_URL"]
        key = st.secrets["SUPABASE_KEY"]
        supabase: Client = create_client(url, key)
        
        # 1. Fetch ALL Data (We filter in the UI now)
        response = supabase.table('permits').select("*").execute()
        if not response.data: 
            return pd.DataFrame()
        
        df = pd.DataFrame(response.data)

        # 2. Schema Normalization
        if 'issued_date' in df.columns: 
            df = df.rename(columns={'issued_date': 'issue_date'})
        
        # Convert dates and handle NaT
        date_cols = ['issue_date', 'applied_date']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        # 3. Velocity Calculation (Friction Metric)
        if 'issue_date' in df.columns and 'applied_date' in df.columns:
            df['velocity'] = (df['issue_date'] - df['applied_date']).dt.days
            # Filter out "Time Travel" errors for visualization
            df.loc[df['velocity'] < 0, 'velocity'] = None

        return df
    except Exception as e:
        st.error(f"Database Connection Failed: {e}")
        return pd.DataFrame()

df_raw = load_data()

if df_raw.empty:
    st.warning("No data found in Supabase. Run ingestion_velocity_50.py first.")
    st.stop()

# --- SIDEBAR: THE COMMAND VALVE ---
st.sidebar.image("https://via.placeholder.com/150x50?text=VECTIS+INDICES", use_column_width=True)
st.sidebar.title("🎛️ Data Controls")

# A. City Multi-select
cities = sorted(df_raw['city'].unique().tolist())
selected_cities = st.sidebar.multiselect("Jurisdictions", cities, default=cities)

# B. Taxonomy Tier Filter (The V3.0 Patch)
tiers = ["Commercial", "Residential", "Commodity", "Unknown"]
selected_tiers = st.sidebar.multiselect("Complexity Tiers", tiers, default=["Commercial", "Residential"])

# C. Valuation Slider (The "San Antonio" UI Valve)
# Setting default to $50k as per original Velocity requirements, but allowing 0 to see all.
max_val_found = int(df_raw['valuation'].max()) if not df_raw.empty else 1000000
min_val = st.sidebar.slider("Min Valuation ($)", 0, 500000, 50000, step=5000)

# Apply Global Filters
mask = (
    df_raw['city'].isin(selected_cities) & 
    df_raw['complexity_tier'].isin(selected_tiers) & 
    (df_raw['valuation'] >= min_val)
)
df = df_raw[mask].copy()

# --- MAIN DASHBOARD ---
st.title("🏛️ National Regulatory Friction Index")
st.subheader("Q1 Bureaucracy Leaderboard (Stable)")

# --- METRIC ROW ---
m1, m2, m3, m4 = st.columns(4)
with m1:
    st.metric("Total Permits", len(df))
with m2:
    avg_velocity = df['velocity'].median() if 'velocity' in df.columns else 0
    st.metric("Median Lead Time", f"{avg_velocity:.0f} Days")
with m3:
    total_value = df['valuation'].sum() / 1_000_000
    st.metric("Total Pipeline Value", f"${total_value:.1f}M")
with m4:
    friction_count = len(df[df['velocity'] > 180]) if 'velocity' in df.columns else 0
    st.metric("High Friction (>180d)", friction_count)

# --- CHART: VELOCITY TRENDS ---
st.markdown("### 📉 Permitting Velocity by Jurisdiction")
if 'issue_date' in df.columns and not df.empty:
    # Prepare trend data
    chart_df = df.dropna(subset=['issue_date', 'velocity'])
    chart_df = chart_df[chart_df['velocity'] >= 0]
    
    if not chart_df.empty:
        # Resample to monthly median
        chart_df['month'] = chart_df['issue_date'].dt.to_period('M').apply(lambda r: r.start_time)
        trend = chart_df.groupby(['city', 'month'])['velocity'].median().reset_index()
        
        line_chart = alt.Chart(trend).mark_line(point=True).encode(
            x=alt.X('month:T', title='Issue Month'),
            y=alt.Y('velocity:Q', title='Median Days to Issue'),
            color=alt.Color('city:N', scale=alt.Scale(scheme='tableau10')),
            tooltip=['city', 'month', 'velocity']
        ).properties(height=400).interactive()
        
        st.altair_chart(line_chart, use_container_width=True)
    else:
        st.info("Insufficient date data to plot velocity trends.")

# --- CHART: TAXONOMY DISTRIBUTION ---
c1, c2 = st.columns(2)
with c1:
    st.markdown("### 🏷️ Tier Distribution")
    tier_chart = alt.Chart(df).mark_bar().encode(
        x=alt.X('count():Q', title='Permit Count'),
        y=alt.Y('complexity_tier:N', sort='-x', title='Tier'),
        color=alt.Color('complexity_tier:N', legend=None)
    ).properties(height=300)
    st.altair_chart(tier_chart, use_container_width=True)

with c2:
    st.markdown("### 💰 Value by Jurisdiction")
    val_chart = alt.Chart(df).mark_arc(innerRadius=50).encode(
        theta=alt.Theta(field="valuation", type="quantitative", aggregate="sum"),
        color=alt.Color(field="city", type="nominal"),
        tooltip=['city', 'valuation']
    ).properties(height=300)
    st.altair_chart(val_chart, use_container_width=True)

# --- DATA INSPECTION TABLE ---
st.markdown("### 📋 Detailed Records")
# Formatting valuation for readability
display_df = df[['city', 'complexity_tier', 'valuation', 'velocity', 'description', 'issue_date']].copy()
display_df['valuation'] = display_df['valuation'].apply(lambda x: f"${x:,.2f}")
st.dataframe(
    display_df.sort_values('issue_date', ascending=False).head(100),
    use_container_width=True
)

st.sidebar.markdown("---")
st.sidebar.caption(f"Vectis Intelligence Engine v3.0 | Records Loaded: {len(df_raw)}")