import streamlit as st
import pandas as pd
import altair as alt
from supabase import create_client, Client
import os
from datetime import datetime, timedelta

# --- CONFIG & THEME ---
st.set_page_config(page_title="Vectis Command Console", page_icon="🏛️", layout="wide")

# Handling secrets for both local and Cloud deployment
try:
    SUPABASE_URL = st.secrets["SUPABASE_URL"]
    SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
except:
    from dotenv import load_dotenv
    load_dotenv()
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Vectis Architectural Brand Colors
VECTIS_BLUE = "#1C2B39"   # Slate Blue (Commodity)
VECTIS_BRONZE = "#C87F42" # Architectural Bronze (Strategic)
VECTIS_YELLOW = "#F2C94C" # Zoning Yellow (Residential)
VECTIS_GREY = "#D1D5DB"   # Vellum Grey (Unknown/Pending)

# --- DATA FACTORY ---
@st.cache_data(ttl=600)
def fetch_strategic_data():
    if not SUPABASE_URL:
        st.error("Missing Supabase Credentials.")
        return pd.DataFrame()
        
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # 6-Month Rolling Window
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
        # Ensure tier has a value for the chart
        df['complexity_tier'] = df['complexity_tier'].fillna("Unknown")
    return df

# --- UI START ---
st.title("🏛️ VECTIS COMMAND CONSOLE")
st.markdown("**National Regulatory Friction Index (NRFI)** | *6-Month Strategic View*")

df = fetch_strategic_data()

if df.empty:
    st.warning("The 6-month data window is currently empty.")
    st.stop()

# --- SIDEBAR & LEGEND ---
with st.sidebar:
    st.header("Story Controls")
    cities = sorted(df['city'].unique().tolist())
    sel_cities = st.multiselect("Jurisdiction", cities, default=cities)
    min_val = st.number_input("Minimum Valuation ($)", value=10000, step=5000)
    exclude_noise = st.checkbox("Exclude Same-Day Permits", value=True, help="Hides 'instant' approvals to reveal true review times.")
    
    st.divider()
    st.header("Tier Definitions")
    st.markdown(f"""
    * **Strategic ({VECTIS_BRONZE}):** High-impact commercial, retail, & multi-family.
    * **Residential ({VECTIS_YELLOW}):** Single-family dwellings & major additions.
    * **Commodity ({VECTIS_BLUE}):** Trade permits, signage, & minor maintenance.
    * **Unknown ({VECTIS_GREY}):** Specialty trades or data awaiting AI classification.
    """)
    st.caption(f"Syncing {len(df):,} total records...")

# --- FILTER LOGIC ---
mask = (df['city'].isin(sel_cities)) & (df['valuation'] >= min_val)
if exclude_noise:
    filtered = df[mask & ((df['velocity'] > 0) | (df['velocity'].isna()))]
else:
    filtered = df[mask]

issued = filtered.dropna(subset=['velocity'])

# --- KPI ROW ---
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Pipeline", f"{len(filtered):,}", "Records")
c2.metric("Pipeline Value", f"${(filtered['valuation'].sum()/1000000):,.1f}M", "Total CapEx")
c3.metric("Velocity Score", f"{issued['velocity'].median():.0f} Days" if not issued.empty else "-", "Median Speed")
c4.metric("Friction Risk", f"±{issued['velocity'].std():.0f} Days" if not issued.empty else "-", "Std Dev")

st.markdown("---")

# --- CHART LAYOUT ---
left_col, right_col = st.columns([2, 1])

with left_col:
    st.subheader("📉 Bureaucracy Leaderboard")
    if not issued.empty:
        stats = issued.groupby('city')['velocity'].agg(['median', 'std', 'count']).reset_index()
        stats.columns = ['Jurisdiction', 'Speed (Days)', 'Risk (±Days)', 'Volume']
        st.dataframe(stats.style.format({'Speed (Days)': '{:.0f}', 'Risk (±Days)': '±{:.0f}'}), 
                     use_container_width=True, hide_index=True)
        
    st.subheader("📈 Velocity Trends")
    if not issued.empty:
        chart_df = issued.copy()
        chart_df['week'] = chart_df['issued_date'].dt.to_period('W').astype(str)
        trend = chart_df.groupby(['week', 'city'])['velocity'].median().reset_index()
        
        line = alt.Chart(trend).mark_line(point=True).encode(
            x=alt.X('week', title='Week of Issuance'),
            y=alt.Y('velocity', title='Median Days to Issue'),
            color=alt.Color('city', scale=alt.Scale(range=[VECTIS_BLUE, VECTIS_BRONZE, '#A0A0A0'])),
            tooltip=['city', 'week', 'velocity']
        ).properties(height=350).interactive()
        
        st.altair_chart(line, use_container_width=True)

with right_col:
    st.subheader("📊 Complexity Tier")
    st.markdown("*Mix of development impact types.*")
    
    tier_counts = filtered['complexity_tier'].value_counts().reset_index()
    tier_counts.columns = ['tier', 'count']
    
    # Consistent Zoning-Standard Palette
    color_scale = alt.Scale(
        domain=['Strategic', 'Residential', 'Commodity', 'Unknown'],
        range=[VECTIS_BRONZE, VECTIS_YELLOW, VECTIS_BLUE, VECTIS_GREY]
    )
    
    pie = alt.Chart(tier_counts).mark_arc(outerRadius=100, innerRadius=50).encode(
        theta="count", 
        color=alt.Color("tier", scale=color_scale, legend=None),
        tooltip=['tier', 'count']
    ).interactive()
    
    st.altair_chart(pie, use_container_width=True)
    
    # Transparency Context
    unknown_count = len(filtered[filtered['complexity_tier'] == 'Unknown'])
    if unknown_count > 0:
        st.info(f"**Data Note:** {unknown_count:,} permits are 'Unknown'. These represent specialty trades or permits pending AI classification.")
    else:
        st.success("All current records classified.")