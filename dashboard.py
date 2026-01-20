import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime

# --- CONFIGURATION & CONSTANTS ---
st.set_page_config(
    page_title="Vectis Command Console",
    layout="wide",
    initial_sidebar_state="expanded"
)

# The "Vectis Tax" Basis: Daily carrying cost of a stalled commercial site
# Source: Vectis Ops Directive (Assumed $500/day for Retail/Standard sites)
DAILY_CARRY_COST = 500 

# --- CSS INJECTION (Slate & Bronze Identity) ---
[cite_start]# [cite: 27, 28] Enforcing the strict color palette: Slate Blue #1C2B39, Bronze #C87F42
st.markdown("""
    <style>
    /* Global Font & Background overrides could go here */
    .metric-card {
        background-color: #F0F4F8; /* Vellum */
        border-left: 5px solid #C87F42; /* Bronze */
        padding: 15px;
        border-radius: 5px;
    }
    .big-stat {
        font-family: 'Inter Tight', sans-serif; 
        font-size: 26px; 
        font-weight: 700; 
        color: #1C2B39; /* Slate Blue */
    }
    .sub-stat {
        font-family: 'JetBrains Mono', monospace;
        font-size: 14px;
        color: #666;
    }
    </style>
    """, unsafe_allow_html=True)

# --- DATA FACTORY (Mock for MVP / Replace with Supabase) ---
@st.cache_data
def load_data():
    # In production: response = supabase.table('vectis_permits').select("*").execute()
    [cite_start]# [cite: 390] Using schema defined in Master Context (vectis_permits)
    data = [
        # Fort Worth (The Benchmark - Fast)
        {'city': 'Fort Worth', 'permit_id': 'FW-001', 'velocity_days': 14, 'complexity_tier': 'Standard', 'valuation': 120000, 'issue_date': '2025-12-01'},
        {'city': 'Fort Worth', 'permit_id': 'FW-002', 'velocity_days': 12, 'complexity_tier': 'Strategic', 'valuation': 5000000, 'issue_date': '2025-12-05'},
        {'city': 'Fort Worth', 'permit_id': 'FW-003', 'velocity_days': 45, 'complexity_tier': 'Commodity', 'valuation': 5000, 'issue_date': '2025-11-20'},
        {'city': 'Fort Worth', 'permit_id': 'FW-004', 'velocity_days': 18, 'complexity_tier': 'Standard', 'valuation': 85000, 'issue_date': '2025-12-10'},
        
        # San Antonio (The Lag - Slow)
        {'city': 'San Antonio', 'permit_id': 'SA-101', 'velocity_days': 110, 'complexity_tier': 'Standard', 'valuation': 150000, 'issue_date': '2025-10-15'},
        {'city': 'San Antonio', 'permit_id': 'SA-102', 'velocity_days': 180, 'complexity_tier': 'Strategic', 'valuation': 4500000, 'issue_date': '2025-09-01'},
        {'city': 'San Antonio', 'permit_id': 'SA-103', 'velocity_days': 95, 'complexity_tier': 'Standard', 'valuation': 75000, 'issue_date': '2025-11-05'},
        {'city': 'San Antonio', 'permit_id': 'SA-104', 'velocity_days': 60, 'complexity_tier': 'Commodity', 'valuation': 4000, 'issue_date': '2025-12-12'},

        # Austin (The Volume - Mixed)
        {'city': 'Austin', 'permit_id': 'AU-555', 'velocity_days': 45, 'complexity_tier': 'Standard', 'valuation': 200000, 'issue_date': '2025-11-25'},
        {'city': 'Austin', 'permit_id': 'AU-556', 'velocity_days': 65, 'complexity_tier': 'Strategic', 'valuation': 8000000, 'issue_date': '2025-11-10'},
        {'city': 'Austin', 'permit_id': 'AU-557', 'velocity_days': 30, 'complexity_tier': 'Standard', 'valuation': 50000, 'issue_date': '2025-12-02'},
    ]
    df = pd.DataFrame(data)
    df['issue_date'] = pd.to_datetime(df['issue_date'])
    return df

df = load_data()

# --- SIDEBAR: SIGNAL FILTERS ---
st.sidebar.title("VECTIS INDICES")
st.sidebar.markdown("`v1.0.4 | STATUS: LIVE`")
st.sidebar.divider()

st.sidebar.header("🔍 Signal Filters")

[cite_start]# [cite: 420, 569] DIRECTIVE: Default View must filter noise.
# We explicitly separate 'Strategic/Standard' from 'Commodity' to hide "Garage Sales".
tier_options = df['complexity_tier'].unique().tolist()
default_tiers = [t for t in tier_options if t != 'Commodity']

selected_tiers = st.sidebar.multiselect(
    "Complexity Tranche",
    options=tier_options,
    default=default_tiers,
    help="Default view excludes 'Commodity' (Fences, Roofs) to focus on Commercial/Strategic activity."
)

# Apply Filter
df_filtered = df[df['complexity_tier'].isin(selected_tiers)]

# --- SECTION 1: THE BUREAUCRACY LEADERBOARD (HERO) ---
[cite_start]# [cite: 284] DIRECTIVE: Move Leaderboard to Hero position ("Compare & Shame").
st.markdown("### 🏛️ Bureaucracy Leaderboard")
st.markdown("#### *The Cost of Delay (Market Velocity)*")

# 1. Calculate Median Velocity per City
leaderboard = df_filtered.groupby('city')['velocity_days'].median().reset_index()
leaderboard.columns = ['Jurisdiction', 'Median Days']

# 2. Identify Benchmark (Fastest City)
benchmark_speed = leaderboard['Median Days'].min()
leaderboard['Benchmark Delta'] = leaderboard['Median Days'] - benchmark_speed

# 3. DIRECTIVE: Calculate "The Vectis Tax"
# Formula: (City_Median - Benchmark_Median) * $500/day
leaderboard['Implied Tax'] = leaderboard['Benchmark Delta'] * DAILY_CARRY_COST

# 4. Sort and Format
leaderboard = leaderboard.sort_values('Median Days')

# 5. Render Dataframe with Visual Bars
st.dataframe(
    leaderboard,
    use_container_width=True,
    hide_index=True,
    column_config={
        "Jurisdiction": st.column_config.TextColumn("Jurisdiction", help="City or County Authority"),
        "Median Days": st.column_config.NumberColumn(
            "Median Velocity", 
            format="%d days",
            help="Time from Application to Issuance (50th Percentile)"
        ),
        "Benchmark Delta": st.column_config.NumberColumn(
            "Delay vs Best", 
            format="+%d days",
            help="Additional days compared to the fastest jurisdiction in the cohort."
        ),
        "Implied Tax": st.column_config.ProgressColumn(
            "The Cost of Delay ($)",
            help=f"Estimated carrying cost liability at ${DAILY_CARRY_COST}/day vs. benchmark.",
            format="$%d",
            min_value=0,
            max_value=int(leaderboard['Implied Tax'].max() * 1.25), # Scale buffer
        ),
    }
)

st.caption(f"Benchmark Basis: Fastest jurisdiction in cohort. Cost Basis: ${DAILY_CARRY_COST}/day (avg commercial carry).")
st.divider()

# --- SECTION 2: VELOCITY TRENDS (CHARTS) ---
c1, c2 = st.columns([2, 1])

with c1:
    st.markdown("### 📉 Velocity Trends")
    [cite_start]# [cite: 383] Altair time-series chart
    chart_data = df_filtered.groupby(['city', 'issue_date'])['velocity_days'].mean().reset_index()
    
    chart = alt.Chart(chart_data).mark_line(point=True).encode(
        x=alt.X('issue_date', title='Issuance Week', axis=alt.Axis(format='%b %d')),
        y=alt.Y('velocity_days', title='Days to Issue'),
        color=alt.Color('city', scale={'range': ['#1C2B39', '#C87F42', '#A3A3A3']}, legend=alt.Legend(title="Jurisdiction")),
        tooltip=['city', 'velocity_days', 'issue_date']
    ).properties(height=350)
    
    st.altair_chart(chart, use_container_width=True)

with c2:
    st.markdown("### 📊 Market Mix")
    # Simple donut chart of Permit Types
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
    [cite_start]# [cite: 382] Pipeline Value calculation
    val_millions = df_filtered['valuation'].sum() / 1_000_000
    st.metric("Pipeline Value", f"${val_millions:.1f}M")
with m3:
    [cite_start]# [cite: 267] Variance/Risk Metric
    risk_std = df_filtered['velocity_days'].std()
    st.metric("Risk Index (σ)", f"±{risk_std:.0f} Days")
with m4:
    st.metric("Data Freshness", "Live Stream")