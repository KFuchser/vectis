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
VECTIS_BLUE = "#1C2B39"   
VECTIS_BRONZE = "#C87F42" 
VECTIS_RED = "#D32F2F"    # LA
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
    
    response = supabase.table('permits').select("*").filter(
        'applied_date', 'gte', cutoff
    ).execute()
    
    df = pd.DataFrame(response.data)
    if not df.empty:
        df['applied_date'] = pd.to_datetime(df['applied_date'], errors='coerce')
        df['issued_date'] = pd.to_datetime(df['issued_date'], errors='coerce')
        df['valuation'] = pd.to_numeric(df['valuation'], errors='coerce').fillna(0)
        df['velocity'] = (df['issued_date'] - df['applied_date']).dt.days
        df['complexity_tier'] = df['complexity_tier'].fillna("Awaiting Analysis")
    return df

# --- UI START ---
st.title("🏛️ VECTIS COMMAND CONSOLE")
st.markdown("**National Regulatory Friction Index (NRFI)** | *6-Month Strategic View*")

df = fetch_strategic_data()

if df.empty:
    st.warning("No data found.")
    st.stop()

# --- SIDEBAR: RESTORED FILTERS ---
with st.sidebar:
    st.header("Story Controls")
    
    cities = sorted(df['city'].unique().tolist())
    sel_cities = st.multiselect("Jurisdictions", cities, default=cities)
    
    all_tiers = ['Strategic', 'Commodity', 'Awaiting Analysis']
    sel_tiers = st.multiselect("Complexity Tiers", all_tiers, default=all_tiers)
    
    # RESTORED: Valuation Filter
    min_val, max_val = int(df['valuation'].min()), int(df['valuation'].max())
    sel_valuation = st.slider("Valuation Filter ($)", min_val, 1000000, (10000, 1000000))
    
    exclude_noise = st.checkbox("Exclude Same-Day Permits", value=True)
    
    st.divider()
    st.header("Legend")
    st.markdown(f"""
    * **Austin:** {VECTIS_BLUE}
    * **San Antonio:** {VECTIS_BRONZE}
    * **Los Angeles:** {VECTIS_RED}
    """)

# --- GLOBAL FILTER LOGIC ---
mask = (
    (df['city'].isin(sel_cities)) & 
    (df['complexity_tier'].isin(sel_tiers)) & 
    (df['valuation'] >= sel_valuation[0]) &
    (df['valuation'] <= sel_valuation[1])
)

filtered = df[mask]
if exclude_noise:
    filtered = filtered[((filtered['velocity'] > 0) | (filtered['velocity'].isna()))]

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
    st.subheader("📈 Velocity Trends (Interactive)")
    if not issued.empty:
        chart_df = issued.copy()
        chart_df['week'] = chart_df['issued_date'].dt.to_period('W').astype(str)
        trend = chart_df.groupby(['week', 'city'])['velocity'].median().reset_index()
        
        city_colors = alt.Scale(
            domain=['Austin', 'San Antonio', 'Los Angeles', 'Fort Worth'],
            range=[VECTIS_BLUE, VECTIS_BRONZE, VECTIS_RED, '#A0A0A0']
        )
        
        # RESTORED: Zoomable Line Chart
        line = alt.Chart(trend).mark_line(point=True).encode(
            x=alt.X('week:O', title='Week Issued'),
            y=alt.Y('velocity:Q', title='Median Days'),
            color=alt.Color('city:N', scale=city_colors),
            tooltip=['city', 'week', 'velocity']
        ).properties(height=400).interactive() # .interactive() enables Zoom/Pan
        
        st.altair_chart(line, use_container_width=True)

with right_col:
    st.subheader("📊 Category Mix")
    tier_counts = filtered['complexity_tier'].value_counts().reset_index()
    tier_counts.columns = ['tier', 'count']
    
    tier_colors = alt.Scale(
        domain=['Strategic', 'Commodity', 'Awaiting Analysis'],
        range=[VECTIS_BRONZE, VECTIS_BLUE, VECTIS_GREY]
    )
    
    pie = alt.Chart(tier_counts).mark_arc(outerRadius=100, innerRadius=50).encode(
        theta=alt.Theta(field="count", type="quantitative"),
        color=alt.Color("tier:N", scale=tier_colors),
        tooltip=['tier', 'count']
    ).properties(height=350).interactive()
    
    st.altair_chart(pie, use_container_width=True)

# --- LEADERBOARD ---
st.subheader("📉 Bureaucracy Leaderboard")
if not issued.empty:
    stats = issued.groupby('city')['velocity'].agg(['median', 'std', 'count']).reset_index()
    stats.columns = ['Jurisdiction', 'Speed (Days)', 'Risk (±Days)', 'Volume']
    st.dataframe(stats.style.format({'Speed (Days)': '{:.0f}', 'Risk (±Days)': '±{:.0f}'}), 
                 use_container_width=True, hide_index=True)