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
    /* Force table header to match brand */
    th { color: #1C2B39 !important; }
    </style>
    """, unsafe_allow_html=True)

# --- STRICT DATA LOADER ---
@st.cache_data(ttl=600)
def load_data():
    try:
        url = st.secrets["SUPABASE_URL"]
        key = st.secrets["SUPABASE_KEY"]
        supabase: Client = create_client(url, key)
        
        # Pull core fields needed for the leaderboard
        response = supabase.table('permits').select("*").execute()
        
        if not response.data: return pd.DataFrame()
        
        df = pd.DataFrame(response.data)

        # --- THE TRUTH LOGIC ---
        
        # 1. Normalize Dates
        df['issue_date'] = pd.to_datetime(df['issue_date'], errors='coerce')
        df['applied_date'] = pd.to_datetime(df['applied_date'], errors='coerce')
        
        # 2. Establish Velocity (Source of Truth = processing_days)
        # We prioritize the database column. 
        if 'processing_days' in df.columns:
            df['velocity'] = pd.to_numeric(df['processing_days'], errors='coerce')
        else:
            # Fallback only if column doesn't exist
            df['velocity'] = (df['issue_date'] - df['applied_date']).dt.days

        # 3. CRITICAL FIX: The "Zero Trust" Filter
        # If we don't know the speed, we DROP the record. 
        # We NEVER fillna(0) anymore. That was the bug.
        initial_count = len(df)
        df = df.dropna(subset=['velocity'])
        
        # 4. Remove Negative Aliens & 10-Year Errors
        # We filter out clerical errors (-5 days) and absurd outliers (>10 years)
        df = df[(df['velocity'] >= 0) & (df['velocity'] < 3650)]
        
        # 5. Standardization
        if 'complexity_tier' not in df.columns: df['complexity_tier'] = 'Standard'
        df['complexity_tier'] = df['complexity_tier'].fillna('Standard')
        
        if 'valuation' not in df.columns: df['valuation'] = 0
        df['valuation'] = df['valuation'].fillna(0)
        
        return df

    except Exception as e:
        st.error(f"Data Connection Error: {e}")
        return pd.DataFrame()

df = load_data()

# --- SIDEBAR CONTROLS ---
st.sidebar.title("VECTIS INDICES")
st.sidebar.caption("v2.2 | LOGIC: STRICT DROPNA")
st.sidebar.divider()

if df.empty:
    st.warning("No valid data stream.")
    st.stop()

# Filter: Complexity Tier (The "Retail Tranche")
all_tiers = df['complexity_tier'].unique().tolist()
# Default: Filter OUT Commodity to show the 'Strategic' signal by default
default_tiers = [t for t in all_tiers if t != 'Commodity']
if not default_tiers: default_tiers = all_tiers

selected_tiers = st.sidebar.multiselect(
    "Permit Class", 
    options=all_tiers, 
    default=default_tiers
)
df_filtered = df[df['complexity_tier'].isin(selected_tiers)]

# --- HERO SECTION: THE LEADERBOARD ---
st.markdown("### 🏛️ Bureaucracy Leaderboard")
st.markdown("_Median Days to Issue (Excluding Instant/Invalid Records)_")

if not df_filtered.empty:
    # Group by City to get the median
    leaderboard = df_filtered.groupby('city').agg(
        median_days=('velocity', 'median'),
        volume=('velocity', 'count'),
        pipeline_val=('valuation', 'sum')
    ).reset_index()

    # The "Vectis Tax" Calculation
    benchmark = leaderboard['median_days'].min()
    leaderboard['delay'] = leaderboard['median_days'] - benchmark
    leaderboard['tax'] = leaderboard['delay'] * DAILY_CARRY_COST

    # Sort by Velocity (Fastest at top)
    leaderboard = leaderboard.sort_values('median_days')

    # Display "Bloomberg" Style
    st.dataframe(
        leaderboard,
        use_container_width=True,
        hide_index=True,
        column_config={
            "city": st.column_config.TextColumn("Jurisdiction"),
            "median_days": st.column_config.NumberColumn(
                "Median Velocity", 
                format="%d days",
                help="Median time from Application to Issuance"
            ),
            "volume": st.column_config.NumberColumn("Volume (n)"),
            "delay": st.column_config.NumberColumn(
                "Delay vs Best", 
                format="+%d days"
            ),
            "tax": st.column_config.ProgressColumn(
                "Cost of Delay ($)",
                format="$%d",
                min_value=0,
                max_value=int(leaderboard['tax'].max() * 1.1) if leaderboard['tax'].max() > 0 else 1000,
                help=f"Implied carrying cost at ${DAILY_CARRY_COST}/day"
            ),
        }
    )
else:
    st.info("No records match the current filters.")

st.divider()

# --- SECTION 2: MATRIX VIEW (The "Multi-Class" Request) ---
st.markdown("### 🧩 Velocity Matrix")
st.caption("Breakdown by City & Class (Strategic vs Standard vs Commodity)")

# We pivot the data to show City rows vs Tier columns
matrix_data = df.groupby(['city', 'complexity_tier'])['velocity'].median().reset_index()

# Altair Heatmap for quick visual scanning
heatmap = alt.Chart(matrix_data).mark_rect().encode(
    x=alt.X('complexity_tier', title='Permit Class'),
    y=alt.Y('city', title='Jurisdiction'),
    color=alt.Color('velocity', title='Days', scale={'scheme': 'orangered'}),
    tooltip=['city', 'complexity_tier', 'velocity']
).properties(height=300)

# Text overlay for the heatmap
text = heatmap.mark_text().encode(
    text=alt.Text('velocity', format='.0f'),
    color=alt.value('black')
)

st.altair_chart(heatmap + text, use_container_width=True)

# --- FOOTER METRICS ---
st.divider()
c1, c2, c3 = st.columns(3)
with c1: st.metric("Active Permits (Filtered)", f"{len(df_filtered)}")
with c2: st.metric("Pipeline Value", f"${df_filtered['pipeline_val'].sum()/1_000_000:.1f}M")
with c3: st.metric("Data Health", f"{len(df)} Valid Records")