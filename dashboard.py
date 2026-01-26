import streamlit as st
import pandas as pd
import altair as alt
from supabase import create_client, Client

st.set_page_config(layout="wide", page_title="Vectis Command Console")

# --- 1. ARCHITECTURAL BRANDING RESTORATION ---
st.markdown("""
    <style>
    /* Main Background */
    .stApp { background-color: #F8F9FA; }
    
    /* Metric Cards - Slate Blue & Bronze */
    div[data-testid="metric-container"] {
        background-color: #FFFFFF;
        border-left: 5px solid #C87F42; /* Bronze */
        padding: 15px;
        border-radius: 5px;
        box-shadow: 0 2px 5px rgba(0,0,0,0.05);
    }
    
    /* Headers */
    h1, h2, h3 { font-family: 'Arial', sans-serif; color: #1C2B39; } /* Slate Blue */
    </style>
    """, unsafe_allow_html=True)

@st.cache_data(ttl=600)
def load_data():
    try:
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
    except Exception as e:
        return pd.DataFrame()

df_raw = load_data()

# --- SIDEBAR CONTROLS ---
st.sidebar.header("Vectis Command")

# A. Valuation Valve (Default to 0 to find missing data)
min_val = st.sidebar.slider("Valuation Floor ($)", 0, 1000000, 0, step=10000)

# B. Tier Filter (Explicitly include UNKNOWN to debug)
all_tiers = ["Commercial", "Residential", "Commodity", "Unknown"]
# Default to showing everything except Commodity to start
selected_tiers = st.sidebar.multiselect("Complexity Tiers", all_tiers, default=["Commercial", "Residential", "Unknown"])

# C. City Filter
cities = sorted(df_raw['city'].unique().tolist()) if not df_raw.empty else []
selected_cities = st.sidebar.multiselect("Jurisdictions", cities, default=cities)

# Apply Filters
if not df_raw.empty:
    df = df_raw[
        (df_raw['valuation'] >= min_val) & 
        (df_raw['complexity_tier'].isin(selected_tiers)) &
        (df_raw['city'].isin(selected_cities))
    ].copy()
else:
    df = pd.DataFrame()

# --- DASHBOARD ---
st.title("🏛️ National Regulatory Friction Index")

if df.empty:
    st.warning("No records found. Check filters or run ingestion.")
    st.stop()

# 1. METRICS ROW
c1, c2, c3, c4 = st.columns(4)
c1.metric("Pipeline Volume", len(df))
c2.metric("Median Velocity", f"{df['velocity'].median():.0f} Days")
c3.metric("Total Valuation", f"${df['valuation'].sum()/1e6:.1f}M")
c4.metric("High Friction (>180d)", len(df[df['velocity'] > 180]))

st.divider()

# 2. CHARTS ROW
col_chart1, col_chart2 = st.columns([2, 1])

with col_chart1:
    st.subheader("📉 Velocity Trends (Speed to Permit)")
    # Filter out bad dates for the chart
    chart_df = df.dropna(subset=['issue_date', 'velocity'])
    if not chart_df.empty:
        chart_df['month'] = chart_df['issue_date'].dt.to_period('M').astype(str)
        
        # Simple line chart
        line = alt.Chart(chart_df).mark_line(point=True).encode(
            x=alt.X('month:T', title='Month'),
            y=alt.Y('median(velocity):Q', title='Median Days'),
            color='city:N',
            tooltip=['city', 'month', 'median(velocity)']
        ).properties(height=350)
        st.altair_chart(line, use_container_width=True)
    else:
        st.info("Insufficient date data for trends.")

with col_chart2:
    st.subheader("🏷️ Tier Breakdown")
    bar = alt.Chart(df).mark_arc(innerRadius=50).encode(
        theta=alt.Theta("count():Q"),
        color=alt.Color("complexity_tier:N"),
        tooltip=["complexity_tier", "count()"]
    ).properties(height=350)
    st.altair_chart(bar, use_container_width=True)

# 3. DATA TABLE
st.subheader("📋 Permit Manifest")
st.dataframe(
    df[['city', 'complexity_tier', 'valuation', 'velocity', 'description', 'issue_date']].sort_values('valuation', ascending=False).head(100),
    use_container_width=True
)