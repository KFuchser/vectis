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
VECTIS_YELLOW = "#F2C94C" 
VECTIS_GREY = "#D1D5DB"

# --- DATA FACTORY ---
@st.cache_data(ttl=600)
def fetch_strategic_data():
    if not SUPABASE_URL:
        st.error("Missing Supabase Credentials.")
        return pd.DataFrame()
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
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
        
        # --- NORMALIZATION ---
        df['complexity_tier'] = df['complexity_tier'].astype(str).str.strip()
        def clean_tier(val):
            val = val.capitalize()
            if val in ['Strategic', 'Residential', 'Commodity']:
                return val
            return 'Unknown'
        df['complexity_tier'] = df['complexity_tier'].apply(clean_tier)
        
    return df

# --- UI START ---
st.title("🏛️ VECTIS COMMAND CONSOLE")
st.markdown("**National Regulatory Friction Index (NRFI)** | *6-Month Strategic View*")

df = fetch_strategic_data()

if df.empty:
    st.warning("The 6-month data window is currently empty.")
    st.stop()

# --- SIDEBAR: DYNAMIC FILTERS ---
with st.sidebar:
    st.header("Story Controls")
    
    # 1. Jurisdiction Filter
    cities = sorted(df['city'].unique().tolist())
    sel_cities = st.multiselect("Jurisdiction", cities, default=cities)
    
    # 2. NEW: Tier Filter (The Story Switch)
    all_tiers = ['Strategic', 'Residential', 'Commodity', 'Unknown']
    sel_tiers = st.multiselect("Complexity Tiers", all_tiers, default=all_tiers)
    
    # 3. Financial/Noise Filters
    min_val = st.number_input("Minimum Valuation ($)", value=10000, step=5000)
    exclude_noise = st.checkbox("Exclude Same-Day Permits", value=True)
    
    st.divider()
    st.header("Legend")
    st.markdown(f"""
    * **Strategic ({VECTIS_BRONZE})**
    * **Residential ({VECTIS_YELLOW})**
    * **Commodity ({VECTIS_BLUE})**
    * **Unknown ({VECTIS_GREY})**
    """)

# --- GLOBAL FILTER LOGIC ---
mask = (
    (df['city'].isin(sel_cities)) & 
    (df['complexity_tier'].isin(sel_tiers)) & 
    (df['valuation'] >= min_val)
)

if exclude_noise:
    filtered = df[mask & ((df['velocity'] > 0) | (df['velocity'].isna()))]
else:
    filtered = df[mask]

issued = filtered.dropna(subset=['velocity'])

# --- KPI ROW ---
c1, c2, c3, c4 = st.columns(4)
c1.metric("Selected Volume", f"{len(filtered):,}", "Records")
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
            y=alt.Y('velocity', title='Median Days'),
            color=alt.Color('city', scale=alt.Scale(range=[VECTIS_BLUE, VECTIS_BRONZE, '#A0A0A0'])),
            tooltip=['city', 'week', 'velocity']
        ).properties(height=350).interactive()
        st.altair_chart(line, use_container_width=True)

with right_col:
    st.subheader("📊 Category Mix")
    
    # Pre-aggregate for solid circle
    tier_counts = filtered['complexity_tier'].value_counts().reset_index()
    tier_counts.columns = ['tier', 'count']
    
    color_scale = alt.Scale(
        domain=['Strategic', 'Residential', 'Commodity', 'Unknown'],
        range=[VECTIS_BRONZE, VECTIS_YELLOW, VECTIS_BLUE, VECTIS_GREY]
    )
    
    pie = alt.Chart(tier_counts).mark_arc(outerRadius=100, innerRadius=50).encode(
        theta=alt.Theta(field="count", type="quantitative"),
        color=alt.Color("tier", scale=color_scale, legend=None),
        tooltip=['tier', 'count']
    ).properties(height=350).interactive()
    
    st.altair_chart(pie, use_container_width=True)
    
    # Context note
    if len(sel_tiers) < 4:
        st.info(f"Filtering for: {', '.join(sel_tiers)}")