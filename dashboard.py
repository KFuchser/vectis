import streamlit as st
import pandas as pd
import altair as alt
from supabase import create_client, Client

# --- CONFIGURATION ---
st.set_page_config(layout="wide", page_title="Vectis Command Console")
DAILY_CARRY_COST = 500  # The "Vectis Tax" Basis

# --- STYLING (Slate & Bronze) ---
st.markdown("""
    <style>
    /* Metric Cards */
    div[data-testid="stMetricValue"] {
        font-size: 24px;
        color: #1C2B39; /* Slate Blue */
    }
    .metric-card {
        background-color: #F0F4F8;
        border-left: 5px solid #C87F42; /* Bronze */
        padding: 15px;
    }
    /* Headers */
    h1, h2, h3 {
        color: #1C2B39;
        font-family: 'Inter', sans-serif;
    }
    </style>
    """, unsafe_allow_html=True)

# --- STRICT DATA LOADER ---
@st.cache_data(ttl=600)
def load_data():
    try:
        # 1. Initialize Connection
        url = st.secrets["SUPABASE_URL"]
        key = st.secrets["SUPABASE_KEY"]
        supabase: Client = create_client(url, key)
        
        # 2. Fetch Data (Table: 'permits')
        # We pull everything to ensure we have the context for filtering
        response = supabase.table('permits').select("*").execute()
        
        if not response.data:
            return pd.DataFrame()
            
        df = pd.DataFrame(response.data)

        # 3. TYPE ENFORCEMENT (Aligning to Schema)
        
        # Dates: 'issue_date' is our anchor.
        if 'issue_date' in df.columns:
            df['issue_date'] = pd.to_datetime(df['issue_date'], errors='coerce')
        
        # Velocity: We use 'processing_days' as the Single Source of Truth.
        # We do NOT calculate it here. If the DB says it's NULL, it's NULL.
        if 'processing_days' in df.columns:
            df['velocity'] = pd.to_numeric(df['processing_days'], errors='coerce')
        else:
            # Critical Error if column missing - but we handle gracefully for UI
            df['velocity'] = 0 
            st.error("Schema Mismatch: Column 'processing_days' not found in table 'permits'.")

        # Valuation: Standardize to numeric
        if 'valuation' in df.columns:
            df['valuation'] = pd.to_numeric(df['valuation'], errors='coerce').fillna(0)
        else:
            df['valuation'] = 0

        # Tier: Handle missing tiers
        if 'complexity_tier' not in df.columns:
            df['complexity_tier'] = 'Standard'
        df['complexity_tier'] = df['complexity_tier'].fillna('Standard')

        # 4. DATA HYGIENE (The "Quality Lock")
        # We drop records where velocity is NULL or negative. 
        # We do not show "0 day" glitches or broken data.
        df = df.dropna(subset=['velocity'])
        df = df[df['velocity'] >= 0]
        
        return df

    except Exception as e:
        st.error(f"Database Connection Error: {e}")
        return pd.DataFrame()

df = load_data()

# --- SIDEBAR: STRATEGIC FILTERS ---
st.sidebar.title("VECTIS INDICES")
st.sidebar.markdown("`v3.0 | SOURCE: public.permits`")
st.sidebar.divider()

if df.empty:
    st.warning("No data stream detected from Supabase.")
    st.stop()

st.sidebar.header("🔍 Signal Filters")

# 1. Complexity Filter (The "Retail Tranche")
# Default: EXCLUDE 'Commodity' to hide "Garage Sales" and "Water Heaters"
all_tiers = df['complexity_tier'].unique().tolist()
default_tiers = [t for t in all_tiers if t != 'Commodity']
# Failsafe: If dataset is ALL commodity, select everything
if not default_tiers: 
    default_tiers = all_tiers

selected_tiers = st.sidebar.multiselect(
    "Permit Class",
    options=all_tiers,
    default=default_tiers,
    help="Deselect 'Commodity' to see the true regulatory friction for commercial/strategic projects."
)

# 2. Date Filter (Optional - Rolling Window)
# min_date = df['issue_date'].min()
# max_date = df['issue_date'].max()

# Apply Filters
df_filtered = df[df['complexity_tier'].isin(selected_tiers)]

if df_filtered.empty:
    st.info("No records match the selected filters.")
    st.stop()

# --- HERO SECTION: BUREAUCRACY LEADERBOARD ---
st.markdown("### 🏛️ Bureaucracy Leaderboard")
st.markdown("#### *The Cost of Delay (Median Processing Days)*")

# 1. Group & Calculate
leaderboard = df_filtered.groupby('city').agg(
    median_days=('velocity', 'median'),
    volume=('velocity', 'count'),
    pipeline_val=('valuation', 'sum')
).reset_index()

# 2. Benchmark Logic (Find the fastest)
benchmark_speed = leaderboard['median_days'].min()
leaderboard['delay'] = leaderboard['median_days'] - benchmark_speed

# 3. The "Vectis Tax" Calculation
leaderboard['tax'] = leaderboard['delay'] * DAILY_CARRY_COST

# 4. Sort (Fastest first)
leaderboard = leaderboard.sort_values('median_days')

# 5. Render
st.dataframe(
    leaderboard,
    use_container_width=True,
    hide_index=True,
    column_config={
        "city": st.column_config.TextColumn("Jurisdiction"),
        "median_days": st.column_config.NumberColumn(
            "Median Velocity", 
            format="%d days",
            help="Source: 'processing_days' column from DB"
        ),
        "volume": st.column_config.NumberColumn("Volume (n)"),
        "delay": st.column_config.NumberColumn(
            "Delay vs Best", 
            format="+%d days"
        ),
        "tax": st.column_config.ProgressColumn(
            "Cost of Delay ($)",
            help=f"Implied liability at ${DAILY_CARRY_COST}/day vs. benchmark.",
            format="$%d",
            min_value=0,
            max_value=int(leaderboard['tax'].max() * 1.25) if leaderboard['tax'].max() > 0 else 1000,
        ),
    }
)

st.divider()

# --- SECTION 2: VELOCITY MATRIX (The "Multi-Class" View) ---
st.markdown("### 🧩 Velocity Matrix")
st.caption("Median Days to Issue by City & Complexity Tier")

# Pivot data for heatmap
matrix_data = df_filtered.groupby(['city', 'complexity_tier'])['velocity'].median().reset_index()

# Altair Heatmap
base = alt.Chart(matrix_data).encode(
    x=alt.X('complexity_tier', title='Permit Class'),
    y=alt.Y('city', title='Jurisdiction')
)

heatmap = base.mark_rect().encode(
    color=alt.Color('velocity', title='Days', scale={'scheme': 'orangered'}),
    tooltip=['city', 'complexity_tier', 'velocity']
)

text = base.mark_text().encode(
    text=alt.Text('velocity', format='.0f'),
    color=alt.value('black') # Force black text for contrast
)

st.altair_chart(heatmap + text, use_container_width=True)

# --- SECTION 3: TRENDS (Are they getting better?) ---
c1, c2 = st.columns([2, 1])

with c1:
    st.markdown("### 📉 Velocity Trends (Monthly)")
    if 'issue_date' in df_filtered.columns:
        # Aggregate by Month to smooth noise
        chart_data = df_filtered.copy()
        chart_data['issue_month'] = chart_data['issue_date'].dt.to_period('M').apply(lambda r: r.start_time)
        
        chart_agg = chart_data.groupby(['city', 'issue_month'])['velocity'].median().reset_index()
        
        line_chart = alt.Chart(chart_agg).mark_line(point=True).encode(
            x=alt.X('issue_month', title='Month', axis=alt.Axis(format='%b %Y')),
            y=alt.Y('velocity', title='Median Days'),
            color=alt.Color('city', legend=alt.Legend(title="Jurisdiction")),
            tooltip=['city', 'issue_month', 'velocity']
        ).properties(height=300)
        
        st.altair_chart(line_chart, use_container_width=True)

with c2:
    st.markdown("### 📊 Market Mix")
    mix_data = df_filtered['complexity_tier'].value_counts().reset_index()
    mix_data.columns = ['Tier', 'Count']
    
    donut = alt.Chart(mix_data).mark_arc(innerRadius=50).encode(
        theta=alt.Theta(field="Count", type="quantitative"),
        color=alt.Color(field="Tier", type="nominal", scale={'range': ['#C87F42', '#1C2B39', '#8899A6']}),
        tooltip=['Tier', 'Count']
    ).properties(height=300)
    st.altair_chart(donut, use_container_width=True)

# --- FOOTER ---
st.divider()
m1, m2, m3 = st.columns(3)
with m1:
    st.metric("Total Permits (Filtered)", f"{len(df_filtered)}")
with m2:
    val_millions = df_filtered['valuation'].sum() / 1_000_000
    st.metric("Pipeline Value", f"${val_millions:.1f}M")
with m3:
    risk_std = df_filtered['velocity'].std()
    st.metric("Risk Index (σ)", f"±{risk_std:.0f} Days")