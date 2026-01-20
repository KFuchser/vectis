import streamlit as st
import pandas as pd
import altair as alt
from supabase import create_client, Client

# --- CONFIGURATION ---
st.set_page_config(layout="wide", page_title="Vectis Command Console")
DAILY_CARRY_COST = 500 

# --- STYLING ---
st.markdown("""
    <style>
    .metric-card { background-color: #F0F4F8; border-left: 5px solid #C87F42; padding: 15px; }
    h1, h2, h3 { color: #1C2B39; font-family: 'Inter', sans-serif; }
    </style>
    """, unsafe_allow_html=True)

# --- STRICT DATA LOADER (Schema Aligned) ---
@st.cache_data(ttl=600)
def load_data():
    try:
        url = st.secrets["SUPABASE_URL"]
        key = st.secrets["SUPABASE_KEY"]
        supabase: Client = create_client(url, key)
        
        # 1. Fetch Data
        response = supabase.table('permits').select("*").execute()
        if not response.data: return pd.DataFrame()
        df = pd.DataFrame(response.data)

        # 2. SCHEMA ALIGNMENT
        if 'issued_date' in df.columns:
            df = df.rename(columns={'issued_date': 'issue_date'})
            
        # 3. TYPE CONVERSION
        if 'issue_date' in df.columns:
            df['issue_date'] = pd.to_datetime(df['issue_date'], errors='coerce')
        if 'applied_date' in df.columns:
            df['applied_date'] = pd.to_datetime(df['applied_date'], errors='coerce')

        # 4. VELOCITY LOGIC
        if 'processing_days' in df.columns:
            df['velocity'] = pd.to_numeric(df['processing_days'], errors='coerce')
        else:
            df['velocity'] = None

        # Fallback Calculation
        if 'issue_date' in df.columns and 'applied_date' in df.columns:
            mask_calc = df['velocity'].isna() & df['issue_date'].notna() & df['applied_date'].notna()
            df.loc[mask_calc, 'velocity'] = (df.loc[mask_calc, 'issue_date'] - df.loc[mask_calc, 'applied_date']).dt.days

        # 5. STRICT FILTERING
        df = df.dropna(subset=['velocity'])
        df = df[df['velocity'] > 0] # Exclude OTC/Instant
        df = df[(df['velocity'] >= 0) & (df['velocity'] < 3650)]

        # 6. Metadata Cleanup
        if 'complexity_tier' not in df.columns: 
            df['complexity_tier'] = 'Unknown'
        df['complexity_tier'] = df['complexity_tier'].fillna('Unknown')
        
        # Valuation Normalization
        val_cols = ['valuation', 'job_value', 'est_project_cost', 'jobvalue']
        found_val = False
        for c in val_cols:
            if c in df.columns:
                df['valuation'] = pd.to_numeric(df[c], errors='coerce').fillna(0)
                found_val = True
                break
        if not found_val: df['valuation'] = 0

        return df

    except Exception as e:
        st.error(f"Data Pipeline Error: {e}")
        return pd.DataFrame()

df = load_data()

# --- SIDEBAR ---
st.sidebar.title("VECTIS INDICES")
st.sidebar.caption("v4.1 | VISUAL RESTORE")

if df.empty:
    st.warning("No valid data found. Check Database.")
    st.stop()

# Filter: Tiers
all_tiers = sorted(df['complexity_tier'].unique().tolist())
default_tiers = [t for t in all_tiers if t != 'Commodity']
if not default_tiers: default_tiers = all_tiers

selected_tiers = st.sidebar.multiselect("Permit Class", all_tiers, default=default_tiers)
df_filtered = df[df['complexity_tier'].isin(selected_tiers)]

if df_filtered.empty:
    st.info("No records match filters.")
    st.stop()

# --- HERO: LEADERBOARD ---
st.markdown("### 🏛️ Bureaucracy Leaderboard")
st.markdown("_Median Days to Issue (Excluding OTC/Instant)_")

leaderboard = df_filtered.groupby('city').agg(
    median_days=('velocity', 'median'),
    volume=('velocity', 'count'),
    total_value=('valuation', 'sum')
).reset_index().sort_values('median_days')

benchmark = leaderboard['median_days'].min()
leaderboard['delay'] = leaderboard['median_days'] - benchmark
leaderboard['tax'] = leaderboard['delay'] * DAILY_CARRY_COST

st.dataframe(
    leaderboard,
    use_container_width=True,
    hide_index=True,
    column_config={
        "city": "Jurisdiction",
        "median_days": st.column_config.NumberColumn("Median Velocity", format="%d days"),
        "volume": "Volume (n)",
        "delay": st.column_config.NumberColumn("Delay vs Best", format="+%d days"),
        "tax": st.column_config.ProgressColumn("Cost of Delay ($)", format="$%d", min_value=0, max_value=int(leaderboard['tax'].max()*1.1) if leaderboard['tax'].max() > 0 else 1000),
    }
)

st.divider()

# --- SECTION 2: CHARTS (RESTORED) ---
c1, c2 = st.columns([2, 1])

with c1:
    st.markdown("### 📉 Velocity Trends")
    # Prepare Time Series Data
    if 'issue_date' in df_filtered.columns:
        chart_data = df_filtered.copy()
        chart_data = chart_data.dropna(subset=['issue_date'])
        
        # Aggregate by Month for cleaner lines
        chart_data['issue_month'] = chart_data['issue_date'].dt.to_period('M').apply(lambda r: r.start_time)
        
        trend = chart_data.groupby(['city', 'issue_month'])['velocity'].median().reset_index()
        
        line_chart = alt.Chart(trend).mark_line(point=True).encode(
            x=alt.X('issue_month', title='Month', axis=alt.Axis(format='%b %Y')),
            y=alt.Y('velocity', title='Median Days to Issue'),
            color=alt.Color('city', legend=alt.Legend(title="Jurisdiction")),
            tooltip=['city', 'issue_month', 'velocity']
        ).properties(height=350).interactive()
        
        st.altair_chart(line_chart, use_container_width=True)
    else:
        st.info("Trend chart unavailable (Missing Issue Date)")

with c2:
    st.markdown("### 📊 Market Mix")
    mix = df_filtered['complexity_tier'].value_counts().reset_index()
    mix.columns = ['Tier', 'Count']
    
    donut = alt.Chart(mix).mark_arc(innerRadius=50).encode(
        theta=alt.Theta(field="Count", type="quantitative"),
        color=alt.Color(field="Tier", type="nominal", scale={'scheme': 'tableau10'}),
        tooltip=['Tier', 'Count']
    ).properties(height=350)
    
    st.altair_chart(donut, use_container_width=True)

# --- SECTION 3: MATRIX ---
st.divider()
st.markdown("### 🧩 Velocity Matrix (Detailed Breakdown)")
matrix = df_filtered.groupby(['city', 'complexity_tier'])['velocity'].median().reset_index()

heatmap = alt.Chart(matrix).mark_rect().encode(
    x='complexity_tier',
    y='city',
    color=alt.Color('velocity', scale={'scheme': 'orangered'}),
    tooltip=['city', 'complexity_tier', 'velocity']
).properties(height=300)

text = heatmap.mark_text().encode(
    text=alt.Text('velocity', format='.0f'),
    color=alt.value('black')
)
st.altair_chart(heatmap + text, use_container_width=True)

# --- FOOTER ---
st.divider()
m1, m2, m3 = st.columns(3)
with m1: st.metric("Active Permits", f"{len(df_filtered)}")
with m2: st.metric("Pipeline Value", f"${df_filtered['valuation'].sum()/1_000_000:.1f}M")
with m3: st.metric("Data Source", "Live Supabase Feed")