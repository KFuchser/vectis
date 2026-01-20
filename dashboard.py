import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime
from supabase import create_client, Client

# --- CONFIGURATION & CONSTANTS ---
st.set_page_config(
    page_title="Vectis Command Console",
    layout="wide",
    initial_sidebar_state="expanded"
)

# The "Vectis Tax" Basis: Daily carrying cost of a stalled commercial site
DAILY_CARRY_COST = 500 

# --- CSS INJECTION (Slate & Bronze Identity) ---
st.markdown("""
    <style>
    .metric-card {
        background-color: #F0F4F8; /* Vellum */
        border-left: 5px solid #C87F42; /* Bronze */
        padding: 15px;
        border-radius: 5px;
    }
    h1, h2, h3 {
        color: #1C2B39; /* Slate Blue */
        font-family: 'Inter', sans-serif;
    }
    </style>
    """, unsafe_allow_html=True)

# --- LIVE DATA FACTORY (SUPABASE) ---
@st.cache_data(ttl=600) # Cache for 10 mins
def load_data():
    try:
        # 1. Initialize Connection
        url = st.secrets["SUPABASE_URL"]
        key = st.secrets["SUPABASE_KEY"]
        supabase: Client = create_client(url, key)
        
        # 2. Fetch Data (Table: 'permits')
        # We grab all columns to ensure we catch 'processing_days' and 'complexity_tier'
        response = supabase.table('permits').select("*").execute()
        
        if not response.data:
            return pd.DataFrame() 
            
        df = pd.DataFrame(response.data)

        # 3. SCHEMA NORMALIZER (The Fix)
        # We map known potential variations to the dashboard's expected names
        # This prevents crashes if 'job_value' is named 'valuation' or 'est_project_cost'
        column_map = {
            'status_date': 'issue_date',
            'jobvalue': 'valuation',
            'job_value': 'valuation',
            'total_valuation': 'valuation',
            'est_project_cost': 'valuation',
            'jurisdiction': 'city'
        }
        df = df.rename(columns=column_map)

        # 4. Type Enforcement & Velocity Logic
        # Handle Date Conversion
        if 'issue_date' in df.columns:
            df['issue_date'] = pd.to_datetime(df['issue_date'], errors='coerce')
        else:
            # Fallback for charting if issue_date is missing entirely
            df['issue_date'] = pd.to_datetime(datetime.now())

        # VELOCITY LOGIC: Prefer 'processing_days' column if it exists
        if 'processing_days' in df.columns:
             df['velocity_days'] = pd.to_numeric(df['processing_days'], errors='coerce')
        else:
             # Fallback calculation: Issue - Applied (if applied exists)
             if 'applied_date' in df.columns:
                df['applied_date'] = pd.to_datetime(df['applied_date'], errors='coerce')
                df['velocity_days'] = (df['issue_date'] - df['applied_date']).dt.days
             else:
                df['velocity_days'] = 0 # Default if both missing
        
        # Clean up NaNs in velocity to prevent math errors
        df['velocity_days'] = df['velocity_days'].fillna(0)

        # 5. Data Sanitation
        # Valuation
        if 'valuation' in df.columns:
            df['valuation'] = pd.to_numeric(df['valuation'], errors='coerce').fillna(0)
        else:
            df['valuation'] = 0 # Safety net

        # Complexity Tier
        if 'complexity_tier' not in df.columns:
            df['complexity_tier'] = 'Standard' # Default fallback
        else:
            df['complexity_tier'] = df['complexity_tier'].fillna('Standard')
        
        return df

    except Exception as e:
        st.error(f"Data Pipeline Error: {e}")
        return pd.DataFrame()

df = load_data()

# --- SIDEBAR: SIGNAL FILTERS ---
st.sidebar.title("VECTIS INDICES")
st.sidebar.markdown("`v2.1.4 | LIVE FEED`")
st.sidebar.divider()

if not df.empty:
    st.sidebar.header("🔍 Signal Filters")

    # DIRECTIVE: Default View filters out 'Commodity' noise
    if 'complexity_tier' in df.columns:
        tier_options = df['complexity_tier'].unique().tolist()
        # Default to excluding Commodity
        default_tiers = [t for t in tier_options if t != 'Commodity']
        # If dataset is ONLY commodity, show everything
        if not default_tiers: 
            default_tiers = tier_options

        selected_tiers = st.sidebar.multiselect(
            "Complexity Tranche",
            options=tier_options,
            default=default_tiers,
            help="Default view excludes 'Commodity' to focus on Strategic activity."
        )
        # Apply Filter
        df_filtered = df[df['complexity_tier'].isin(selected_tiers)]
    else:
        # Pass through if column missing
        df_filtered = df
else:
    st.sidebar.warning("No data stream detected.")
    df_filtered = pd.DataFrame()

# --- MAIN DASHBOARD ---

if not df_filtered.empty:
    # --- SECTION 1: THE BUREAUCRACY LEADERBOARD (HERO) ---
    st.markdown("### 🏛️ Bureaucracy Leaderboard")
    st.markdown("#### *The Cost of Delay (Market Velocity)*")

    # Grouping Logic (Safety check for 'city' column)
    group_col = 'city' if 'city' in df_filtered.columns else df_filtered.columns[0]

    # 1. Calculate Median Velocity per City
    leaderboard = df_filtered.groupby(group_col)['velocity_days'].median().reset_index()
    leaderboard.columns = ['Jurisdiction', 'Median Days']

    # 2. Identify Benchmark (Fastest City)
    if not leaderboard.empty:
        benchmark_speed = leaderboard['Median Days'].min()
        leaderboard['Benchmark Delta'] = leaderboard['Median Days'] - benchmark_speed

        # 3. DIRECTIVE: Calculate "The Vectis Tax"
        leaderboard['Implied Tax'] = leaderboard['Benchmark Delta'] * DAILY_CARRY_COST

        # 4. Sort and Format
        leaderboard = leaderboard.sort_values('Median Days')

        # 5. Render Dataframe with Visual Bars
        st.dataframe(
            leaderboard,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Jurisdiction": st.column_config.TextColumn("Jurisdiction"),
                "Median Days": st.column_config.NumberColumn(
                    "Median Velocity", 
                    format="%d days",
                    help="Time from Application to Issuance (50th Percentile)"
                ),
                "Benchmark Delta": st.column_config.NumberColumn(
                    "Delay vs Best", 
                    format="+%d days",
                ),
                "Implied Tax": st.column_config.ProgressColumn(
                    "The Cost of Delay ($)",
                    help=f"Estimated liability at ${DAILY_CARRY_COST}/day vs. benchmark.",
                    format="$%d",
                    min_value=0,
                    max_value=int(leaderboard['Implied Tax'].max() * 1.25) if leaderboard['Implied Tax'].max() > 0 else 1000,
                ),
            }
        )
        st.caption(f"Benchmark Basis: Fastest jurisdiction in cohort. Cost Basis: ${DAILY_CARRY_COST}/day.")
    st.divider()

    # --- SECTION 2: VELOCITY TRENDS (CHARTS) ---
    c1, c2 = st.columns([2, 1])

    with c1:
        st.markdown("### 📉 Velocity Trends")
        # Altair time-series chart
        chart_data = df_filtered.copy()
        
        if 'issue_date' in chart_data.columns:
            # Aggregate by week to smooth lines
            chart_data['issue_week'] = chart_data['issue_date'].dt.to_period('W').apply(lambda r: r.start_time)
            chart_agg = chart_data.groupby([group_col, 'issue_week'])['velocity_days'].mean().reset_index()
            
            chart = alt.Chart(chart_agg).mark_line(point=True).encode(
                x=alt.X('issue_week', title='Issuance Week', axis=alt.Axis(format='%b %d')),
                y=alt.Y('velocity_days', title='Avg Days to Issue'),
                color=alt.Color(group_col, scale={'range': ['#1C2B39', '#C87F42', '#A3A3A3']}, legend=alt.Legend(title="Jurisdiction")),
                tooltip=[group_col, 'velocity_days', 'issue_week']
            ).properties(height=350)
            
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("Insufficient date data to render trends.")

    with c2:
        st.markdown("### 📊 Market Mix")
        if 'complexity_tier' in df_filtered.columns:
            mix_data = df_filtered['complexity_tier'].value_counts().reset_index()
            mix_data.columns = ['Tier', 'Count']
            
            pie = alt.Chart(mix_data).mark_arc(innerRadius=50).encode(
                theta=alt.Theta(field="Count", type="quantitative"),
                color=alt.Color(field="Tier", type="nominal", scale={'range': ['#C87F42', '#1C2B39', '#8899A6']}),
                tooltip=['Tier', 'Count']
            ).properties(height=350)
            st.altair_chart(pie, use_container_width=True)

    # --- SECTION 3: METRIC CARDS (FOOTER) ---
    st.divider()
    st.markdown("### 📡 Live Feed Status")

    m1, m2, m3, m4 = st.columns(4)

    with m1:
        st.metric("Active Permits", f"{len(df_filtered)}")
    with m2:
        if 'valuation' in df_filtered.columns:
            val_millions = df_filtered['valuation'].sum() / 1_000_000
            st.metric("Pipeline Value", f"${val_millions:.1f}M")
    with m3:
        risk_std = df_filtered['velocity_days'].std()
        if pd.isna(risk_std):
            risk_std = 0
        st.metric("Risk Index (σ)", f"±{risk_std:.0f} Days")
    with m4:
        st.metric("Data Freshness", "Supabase Live")

else:
    st.info("Waiting for data... Table 'permits' appears empty or connection failed.")
    # Debugging helper (Optional - shows raw columns if load fails)
    # try:
    #     url = st.secrets["SUPABASE_URL"]
    #     key = st.secrets["SUPABASE_KEY"]
    #     sb = create_client(url, key)
    #     raw = sb.table('permits').select("*").limit(1).execute()
    #     st.write("Available Columns:", list(raw.data[0].keys()) if raw.data else "None")
    # except: pass