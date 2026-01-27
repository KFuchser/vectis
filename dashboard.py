import streamlit as st
import pandas as pd
import altair as alt
from supabase import create_client, Client

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

# --- 2. DATA LOADING (THE FIX) ---
@st.cache_data(ttl=600)
def load_data():
    try:
        url = st.secrets["SUPABASE_URL"]
        key = st.secrets["SUPABASE_KEY"]
        supabase: Client = create_client(url, key)
        
        # CRITICAL FIX: 
        # 1. Order by 'issued_date' DESC so we see the NEWEST data we just ingested.
        # 2. Limit to 10,000 to bypass the default 1,000 record cap.
        response = supabase.table('permits')\
            .select("*")\
            .order('issued_date', desc=True)\
            .limit(10000)\
            .execute()
            
        df = pd.DataFrame(response.data)
        
        if not df.empty:
            df['issue_date'] = pd.to_datetime(df['issued_date'], errors='coerce')
            df['applied_date'] = pd.to_datetime(df['applied_date'], errors='coerce')
            # Calculate Velocity
            df['velocity'] = (df['issue_date'] - df['applied_date']).dt.days
        return df
    except Exception as e:
        st.error(f"Data Load Error: {e}")
        return pd.DataFrame()

# --- SIDEBAR ---
st.sidebar.title("Vectis Command")

if st.sidebar.button("🔄 Force Refresh"):
    st.cache_data.clear()
    st.rerun()

df_raw = load_data()

# FILTERS
min_val = st.sidebar.slider("Valuation Floor ($)", 0, 1000000, 0, step=10000)

# FIX: Default to ALL tiers so you see the volume immediately
all_tiers = ["Commercial", "Residential", "Commodity", "Unknown"]
selected_tiers = st.sidebar.multiselect("Complexity Tiers", all_tiers, default=all_tiers)

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

# --- MAIN DASHBOARD ---
st.title("🏛️ National Regulatory Friction Index")

if df.empty:
    st.warning("No records found. The database connection works, but filters match nothing.")
    st.stop()

# METRICS
# Filter out "Same Day" (0 velocity) permits for a realistic Lead Time metric
real_projects = df[df['velocity'] > 0]
median_vel = real_projects['velocity'].median() if not real_projects.empty else 0

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Volume (Visible)", len(df))
c2.metric("True Lead Time", f"{median_vel:.0f} Days", help="Excludes same-day permits")
c3.metric("Pipeline Value", f"${df['valuation'].sum()/1e6:.1f}M")
c4.metric("High Friction (>180d)", len(df[df['velocity'] > 180]))

st.divider()

# CHARTS
c_left, c_right = st.columns([2, 1])

with c_left:
    st.subheader("📉 Velocity Trends (Real Projects Only)")
    if not real_projects.empty:
        real_projects['month'] = real_projects['issue_date'].dt.to_period('M').astype(str)
        line = alt.Chart(real_projects).mark_line(point=True).encode(
            x=alt.X('month:T', title='Month'),
            y=alt.Y('median(velocity):Q', title='Median Days'),
            color='city:N',
            tooltip=['city', 'month', 'median(velocity)']
        ).properties(height=350)
        st.altair_chart(line, use_container_width=True)
    else:
        st.info("No velocity data > 0 days found.")

with c_right:
    st.subheader("🏷️ Tier Breakdown")
    bar = alt.Chart(df).mark_arc(innerRadius=50).encode(
        theta=alt.Theta("count():Q"),
        color=alt.Color("complexity_tier:N"),
        tooltip=["complexity_tier", "count()"]
    ).properties(height=350)
    st.altair_chart(bar, use_container_width=True)

# DATA TABLE
st.subheader("📋 Recent Permit Manifest (2026)")
st.dataframe(
    df[['city', 'complexity_tier', 'valuation', 'velocity', 'description', 'issue_date']]
    .sort_values('issue_date', ascending=False) # Show newest first
    .head(100),
    use_container_width=True
)