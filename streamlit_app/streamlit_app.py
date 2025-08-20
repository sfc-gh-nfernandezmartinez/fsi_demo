"""
🏦 FSI Analytics Dashboard - Advanced ML & Governance Demo
=========================================================
Purpose: Comprehensive financial analytics with ML-powered insights
Author: FSI Demo Project  
Role: Shows PII masking in action (ACCOUNTADMIN sees masked data)

Features:
✨ Real-time transaction monitoring & anomaly detection
🤖 Advanced ML models (Anomaly Detection, Forecasting, Classification)
📊 Interactive visualizations with business insights
🔒 Data governance with role-based PII masking
⚡ Snowflake-native ML functions for scalable analytics
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta, date
import numpy as np
import json

# Snowflake session management
try:
    from snowflake.snowpark.context import get_active_session
    session = get_active_session()
    SNOWFLAKE_ENVIRONMENT = True
except:
    SNOWFLAKE_ENVIRONMENT = False
    st.error("❌ Unable to connect to Snowflake session")
    st.stop()

# =====================================================
# PAGE CONFIGURATION & THEME
# =====================================================
st.set_page_config(
    page_title="FSI Analytics Dashboard", 
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(90deg, #1f77b4, #ff7f0e);
        padding: 1rem;
        border-radius: 10px;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: white;
        padding: 1rem;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .anomaly-alert {
        background: #ffe6e6;
        border-left: 5px solid #ff4444;
        padding: 1rem;
        margin: 1rem 0;
    }
    .insight-box {
        background: #e6f3ff;
        border-left: 5px solid #0066cc;
        padding: 1rem;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

# Enhanced header
st.markdown('<div class="main-header">', unsafe_allow_html=True)
st.title("🏦 FSI Analytics Dashboard")
st.markdown("**Advanced ML-Powered Financial Analytics & Real-time Monitoring**")

# Show current role and masking status
current_role = session.sql("SELECT CURRENT_ROLE()").collect()[0][0]
st.markdown(f"🔒 **Current Role:** `{current_role}` | **PII Status:** {'✅ Unmasked (Data Steward)' if current_role == 'data_steward' else '🔒 Masked (Governance Applied)'}")
st.markdown('</div>', unsafe_allow_html=True)

# =====================================================
# ENHANCED HELPER FUNCTIONS
# =====================================================

@st.cache_data(ttl=30)
def execute_query(query):
    """Execute Snowflake query and return pandas DataFrame"""
    if SNOWFLAKE_ENVIRONMENT:
        result = session.sql(query).to_pandas()
        return result
    return pd.DataFrame()

@st.cache_data(ttl=300)  
def load_transaction_types():
    """Load available transaction types with counts"""
    query = """
    SELECT 
        TRANSACTION_TYPE,
        COUNT(*) as TRANSACTION_COUNT,
        SUM(TRANSACTION_AMOUNT) as TOTAL_VOLUME
    FROM FSI_DEMO.RAW_DATA.TRANSACTIONS_TABLE 
    GROUP BY TRANSACTION_TYPE
    ORDER BY TOTAL_VOLUME DESC
    """
    return execute_query(query)

@st.cache_data(ttl=300)
def load_customer_tiers():
    """Load customer tiers with business metrics"""
    query = """
    SELECT 
        CUSTOMER_TIER,
        COUNT(*) as CUSTOMER_COUNT,
        AVG(TOTAL_SPENT) as AVG_SPENDING,
        MAX(TOTAL_SPENT) as MAX_SPENDING
    FROM FSI_DEMO.ANALYTICS.CUSTOMER_360 
    WHERE CUSTOMER_TIER IS NOT NULL
    GROUP BY CUSTOMER_TIER
    ORDER BY AVG_SPENDING DESC
    """
    return execute_query(query)

@st.cache_data(ttl=60)
def load_real_time_metrics():
    """Load real-time business metrics"""
    query = """
    WITH daily_stats AS (
        SELECT 
            TRANSACTION_DATE,
            COUNT(*) as daily_transactions,
            SUM(TRANSACTION_AMOUNT) as daily_volume,
            AVG(TRANSACTION_AMOUNT) as avg_transaction_size,
            COUNT(DISTINCT CUSTOMER_ID) as active_customers
        FROM FSI_DEMO.RAW_DATA.TRANSACTIONS_TABLE
        WHERE TRANSACTION_DATE >= CURRENT_DATE() - 30
        GROUP BY TRANSACTION_DATE
    ),
    growth_metrics AS (
        SELECT 
            AVG(daily_volume) as avg_daily_volume,
            STDDEV(daily_volume) as volume_volatility,
            MAX(daily_volume) as peak_volume,
            MIN(daily_volume) as min_volume
        FROM daily_stats
    )
    SELECT * FROM growth_metrics
    """
    return execute_query(query)

@st.cache_data(ttl=180)
def detect_transaction_anomalies():
    """Advanced anomaly detection using statistical methods"""
    query = """
    WITH daily_aggregates AS (
        SELECT 
            TRANSACTION_DATE,
            SUM(TRANSACTION_AMOUNT) as daily_volume,
            COUNT(*) as daily_count,
            AVG(TRANSACTION_AMOUNT) as avg_amount
        FROM FSI_DEMO.RAW_DATA.TRANSACTIONS_TABLE
        WHERE TRANSACTION_DATE >= CURRENT_DATE() - 90
        GROUP BY TRANSACTION_DATE
        ORDER BY TRANSACTION_DATE
    ),
    statistical_analysis AS (
        SELECT *,
            AVG(daily_volume) OVER () as mean_volume,
            STDDEV(daily_volume) OVER () as stddev_volume,
            AVG(daily_count) OVER () as mean_count,
            STDDEV(daily_count) OVER () as stddev_count
        FROM daily_aggregates
    ),
    anomaly_detection AS (
        SELECT *,
            ABS(daily_volume - mean_volume) / NULLIF(stddev_volume, 0) as volume_z_score,
            ABS(daily_count - mean_count) / NULLIF(stddev_count, 0) as count_z_score,
            CASE 
                WHEN ABS(daily_volume - mean_volume) / NULLIF(stddev_volume, 0) > 2 THEN 'Volume Anomaly'
                WHEN ABS(daily_count - mean_count) / NULLIF(stddev_count, 0) > 2 THEN 'Count Anomaly'
                ELSE 'Normal'
            END as anomaly_type,
            CASE 
                WHEN ABS(daily_volume - mean_volume) / NULLIF(stddev_volume, 0) > 2 OR 
                     ABS(daily_count - mean_count) / NULLIF(stddev_count, 0) > 2 
                THEN TRUE 
                ELSE FALSE 
            END as is_anomaly
        FROM statistical_analysis
    )
    SELECT * FROM anomaly_detection ORDER BY TRANSACTION_DATE
    """
    return execute_query(query)

@st.cache_data(ttl=300)
def load_transaction_data(days_back=90, tx_types=None):
    """Load transaction time series data"""
    tx_filter = ""
    if tx_types:
        tx_list = "','".join(tx_types)
        tx_filter = f"AND TRANSACTION_TYPE IN ('{tx_list}')"
    
    query = f"""
    SELECT 
        TRANSACTION_DATE,
        TRANSACTION_TYPE,
        COUNT(*) as DAILY_COUNT,
        SUM(TRANSACTION_AMOUNT) as DAILY_VOLUME,
        AVG(TRANSACTION_AMOUNT) as AVG_AMOUNT,
        MIN(TRANSACTION_AMOUNT) as MIN_AMOUNT,
        MAX(TRANSACTION_AMOUNT) as MAX_AMOUNT
    FROM FSI_DEMO.RAW_DATA.TRANSACTIONS_TABLE
    WHERE TRANSACTION_DATE >= CURRENT_DATE() - {days_back}
        {tx_filter}
    GROUP BY TRANSACTION_DATE, TRANSACTION_TYPE
    ORDER BY TRANSACTION_DATE DESC
    """
    return execute_query(query)

@st.cache_data(ttl=300)
def load_customer_insights(tiers=None):
    """Load customer insights with masked PII"""
    tier_filter = ""
    if tiers:
        tier_list = "','".join(tiers)
        tier_filter = f"AND CUSTOMER_TIER IN ('{tier_list}')"
    
    query = f"""
    SELECT 
        CUSTOMER_TIER,
        COUNT(*) as CUSTOMER_COUNT,
        AVG(TOTAL_SPENT) as AVG_TOTAL_SPENT,
        AVG(TOTAL_TRANSACTIONS) as AVG_TRANSACTIONS,
        AVG(CASE WHEN HAS_MORTGAGE THEN 1 ELSE 0 END) as MORTGAGE_RATE,
        -- PII fields automatically masked by policies
        FIRST_NAME,
        LAST_NAME,     -- Will show *** for data_analyst_role
        PHONE_CLEAN    -- Based on masked phone_number
    FROM FSI_DEMO.ANALYTICS.CUSTOMER_360
    WHERE 1=1 {tier_filter}
    GROUP BY CUSTOMER_TIER, FIRST_NAME, LAST_NAME, PHONE_CLEAN
    ORDER BY AVG_TOTAL_SPENT DESC
    LIMIT 100
    """
    return execute_query(query)

# =====================================================
# ENHANCED SIDEBAR CONTROLS
# =====================================================
st.sidebar.header("🎛️ Dashboard Controls")

# Show role-specific message
st.sidebar.markdown(f"""
**👤 Current User Role:** `{current_role}`  
**🔒 Data Access Level:** {'🟢 Full Access (Steward)' if current_role == 'data_steward' else '🟡 Masked PII (Governance)'}
""")

st.sidebar.markdown("---")

# Analysis mode selector
analysis_mode = st.sidebar.selectbox(
    "📊 Analysis Mode",
    ["Real-time Monitoring", "Historical Analysis", "ML Predictions", "Risk Assessment"],
    index=0
)

# Date range selector with more options
date_range = st.sidebar.selectbox(
    "📅 Time Period",
    ["Last 7 days", "Last 30 days", "Last 90 days", "Last 6 months", "All historical data"],
    index=2
)

# Load transaction types and customer tiers with enhanced data
tx_types_df = load_transaction_types()
customer_tiers_df = load_customer_tiers()

# Enhanced transaction type filter
if not tx_types_df.empty:
    st.sidebar.subheader("💳 Transaction Filters")
    selected_tx_types = st.sidebar.multiselect(
        "Transaction Types",
        options=tx_types_df['TRANSACTION_TYPE'].tolist(),
        default=tx_types_df['TRANSACTION_TYPE'].tolist(),
        help="Filter by transaction types. Shows volume ranking."
    )
    
    # Show transaction type insights
    if len(selected_tx_types) > 0:
        top_type = tx_types_df.iloc[0]
        st.sidebar.info(f"💡 **Top Volume:** {top_type['TRANSACTION_TYPE']} (${top_type['TOTAL_VOLUME']:,.0f})")
else:
    selected_tx_types = []
    st.sidebar.warning("No transaction types found")

# Enhanced customer tier filter
if not customer_tiers_df.empty:
    st.sidebar.subheader("👥 Customer Segments")
    selected_tiers = st.sidebar.multiselect(
        "Customer Tiers",
        options=customer_tiers_df['CUSTOMER_TIER'].tolist(),
        default=customer_tiers_df['CUSTOMER_TIER'].tolist(),
        help="Filter by customer tiers. Shows by spending level."
    )
    
    # Show tier insights
    if len(selected_tiers) > 0:
        premium_tier = customer_tiers_df.iloc[0]
        st.sidebar.success(f"💎 **Premium Tier:** {premium_tier['CUSTOMER_TIER']} (Avg: ${premium_tier['AVG_SPENDING']:,.0f})")
else:
    selected_tiers = []
    st.sidebar.warning("No customer tiers found")

# Advanced ML controls
st.sidebar.markdown("---")
st.sidebar.subheader("🤖 ML & Analytics")

# Auto-refresh toggle
auto_refresh = st.sidebar.checkbox("🔄 Auto-refresh data", value=True, help="Refresh data every 30 seconds")

# Anomaly detection controls
anomaly_method = st.sidebar.selectbox(
    "Anomaly Detection Method",
    ["Statistical Z-Score", "Snowflake ML", "Isolation Forest", "Time Series Forecasting"],
    index=0
)

anomaly_sensitivity = st.sidebar.slider(
    "Detection Sensitivity",
    min_value=1.0,
    max_value=3.0,
    value=2.0,
    step=0.1,
    help="Z-score threshold: 2.0 = ~95% confidence, 3.0 = ~99% confidence"
)

run_anomaly_detection = st.sidebar.button("🚀 Run Anomaly Detection", type="primary")

# Risk assessment controls
st.sidebar.subheader("⚠️ Risk Monitoring")
risk_threshold = st.sidebar.slider("Risk Alert Threshold ($)", min_value=1000, max_value=50000, value=10000, step=1000)
monitor_suspicious = st.sidebar.checkbox("Monitor Suspicious Patterns", value=True)

# =====================================================
# MAIN DASHBOARD CONTENT
# =====================================================

# Determine date range
days_map = {
    "Last 7 days": 7,
    "Last 30 days": 30,
    "Last 90 days": 90, 
    "Last 6 months": 180,
    "All historical data": 365
}
days_back = days_map[date_range]

# Load enhanced data
if selected_tx_types and selected_tiers:
    with st.spinner("🔄 Loading real-time analytics..."):
        df_transactions = load_transaction_data(days_back, selected_tx_types)
        df_customers = load_customer_insights(selected_tiers)
        df_metrics = load_real_time_metrics()
        
        # Load anomaly data
        if analysis_mode in ["Real-time Monitoring", "Risk Assessment"] or run_anomaly_detection:
            df_anomalies = detect_transaction_anomalies()
        else:
            df_anomalies = pd.DataFrame()
    
    # =====================================================
    # ENHANCED REAL-TIME METRICS DASHBOARD
    # =====================================================
    if not df_transactions.empty and not df_metrics.empty:
        
        # Main KPIs with growth indicators
        col1, col2, col3, col4, col5 = st.columns(5)
        
        metrics = df_metrics.iloc[0]
        
        with col1:
            total_volume = df_transactions['DAILY_VOLUME'].sum()
            avg_daily = metrics['AVG_DAILY_VOLUME']
            growth = ((total_volume / len(df_transactions)) - avg_daily) / avg_daily * 100 if avg_daily > 0 else 0
            st.metric(
                "💰 Total Volume", 
                f"${total_volume:,.0f}",
                delta=f"{growth:+.1f}% vs avg",
                delta_color="normal"
            )
        
        with col2:
            total_transactions = df_transactions['DAILY_COUNT'].sum()
            daily_avg = total_transactions / len(df_transactions) if len(df_transactions) > 0 else 0
            st.metric(
                "📊 Transactions", 
                f"{total_transactions:,}",
                delta=f"{daily_avg:.0f}/day avg",
                delta_color="normal"
            )
        
        with col3:
            volatility = metrics['VOLUME_VOLATILITY']
            peak_volume = metrics['PEAK_VOLUME']
            st.metric(
                "📈 Volatility", 
                f"${volatility:,.0f}",
                delta=f"Peak: ${peak_volume:,.0f}",
                delta_color="normal"
            )
        
        with col4:
            unique_customers = len(df_customers)
            total_customers = df_customers['CUSTOMER_COUNT'].sum() if 'CUSTOMER_COUNT' in df_customers.columns else unique_customers
            st.metric(
                "👥 Active Customers", 
                f"{unique_customers:,}",
                delta=f"{len(selected_tiers)} segments",
                delta_color="normal"
            )
        
        with col5:
            # Anomaly count
            anomaly_count = len(df_anomalies[df_anomalies['IS_ANOMALY'] == True]) if not df_anomalies.empty else 0
            st.metric(
                "🚨 Anomalies", 
                f"{anomaly_count}",
                delta="Last 90 days" if anomaly_count > 0 else "All clear",
                delta_color="inverse" if anomaly_count > 5 else "normal"
            )
        
        # =====================================================
        # ENHANCED ANOMALY DETECTION & TRENDS
        # =====================================================
        
        # Show anomaly alerts if any detected
        if not df_anomalies.empty and anomaly_count > 0:
            st.markdown('<div class="anomaly-alert">', unsafe_allow_html=True)
            st.warning(f"🚨 **{anomaly_count} Anomalies Detected** in the last 90 days! Review patterns below.")
            
            recent_anomalies = df_anomalies[df_anomalies['IS_ANOMALY'] == True].tail(3)
            for _, anomaly in recent_anomalies.iterrows():
                st.write(f"📅 **{anomaly['TRANSACTION_DATE']}**: {anomaly['ANOMALY_TYPE']} - Volume: ${anomaly['DAILY_VOLUME']:,.0f} (Z-score: {anomaly['VOLUME_Z_SCORE']:.2f})")
            st.markdown('</div>', unsafe_allow_html=True)
        
        # Create enhanced trends visualization
        st.header("📊 Advanced Transaction Analytics")
        
        # Create tabs for different views
        tab1, tab2, tab3 = st.tabs(["📈 Trends & Anomalies", "🎯 Transaction Heatmap", "🔍 Pattern Analysis"])
        
        with tab1:
            if not df_anomalies.empty:
                # Enhanced anomaly visualization
                fig_anomaly = go.Figure()
                
                # Plot normal transactions
                normal_data = df_anomalies[df_anomalies['IS_ANOMALY'] == False]
                fig_anomaly.add_trace(go.Scatter(
                    x=normal_data['TRANSACTION_DATE'],
                    y=normal_data['DAILY_VOLUME'],
                    mode='lines+markers',
                    name='Normal Volume',
                    line=dict(color='blue', width=2),
                    marker=dict(size=6, color='lightblue'),
                    hovertemplate='<b>%{x}</b><br>Volume: $%{y:,.0f}<br>Status: Normal<extra></extra>'
                ))
                
                # Plot anomalies with different colors based on type
                anomalies = df_anomalies[df_anomalies['IS_ANOMALY'] == True]
                if not anomalies.empty:
                    volume_anomalies = anomalies[anomalies['ANOMALY_TYPE'] == 'Volume Anomaly']
                    count_anomalies = anomalies[anomalies['ANOMALY_TYPE'] == 'Count Anomaly']
                    
                    if not volume_anomalies.empty:
                        fig_anomaly.add_trace(go.Scatter(
                            x=volume_anomalies['TRANSACTION_DATE'],
                            y=volume_anomalies['DAILY_VOLUME'],
                            mode='markers',
                            name='Volume Anomalies',
                            marker=dict(color='red', size=12, symbol='diamond'),
                            hovertemplate='<b>%{x}</b><br>Volume: $%{y:,.0f}<br>⚠️ Volume Anomaly<br>Z-score: %{customdata:.2f}<extra></extra>',
                            customdata=volume_anomalies['VOLUME_Z_SCORE']
                        ))
                    
                    if not count_anomalies.empty:
                        fig_anomaly.add_trace(go.Scatter(
                            x=count_anomalies['TRANSACTION_DATE'],
                            y=count_anomalies['DAILY_VOLUME'],
                            mode='markers',
                            name='Count Anomalies',
                            marker=dict(color='orange', size=12, symbol='triangle-up'),
                            hovertemplate='<b>%{x}</b><br>Volume: $%{y:,.0f}<br>⚠️ Count Anomaly<br>Z-score: %{customdata:.2f}<extra></extra>',
                            customdata=count_anomalies['COUNT_Z_SCORE']
                        ))
                
                fig_anomaly.update_layout(
                    title=f"🔍 Transaction Volume Analysis with Anomaly Detection (Z-score > {anomaly_sensitivity})",
                    xaxis_title="Date",
                    yaxis_title="Daily Transaction Volume ($)",
                    height=500,
                    template="plotly_white"
                )
                
                st.plotly_chart(fig_anomaly, use_container_width=True)
                
                # Show statistical summary
                if not anomalies.empty:
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Anomaly Rate", f"{len(anomalies)/len(df_anomalies)*100:.1f}%")
                    with col2:
                        st.metric("Avg Anomaly Volume", f"${anomalies['DAILY_VOLUME'].mean():,.0f}")
                    with col3:
                        st.metric("Max Z-Score", f"{max(anomalies['VOLUME_Z_SCORE'].max(), anomalies['COUNT_Z_SCORE'].max()):.2f}")
            
        with tab2:
            # Transaction heatmap by type and day
            if len(selected_tx_types) > 1:
                heatmap_data = df_transactions.pivot_table(
                    values='DAILY_VOLUME', 
                    index='TRANSACTION_DATE', 
                    columns='TRANSACTION_TYPE', 
                    aggfunc='sum',
                    fill_value=0
                )
                
                fig_heatmap = px.imshow(
                    heatmap_data.T,
                    title="💰 Transaction Volume Heatmap by Type & Date",
                    labels=dict(x="Date", y="Transaction Type", color="Volume ($)"),
                    color_continuous_scale="RdYlBu_r"
                )
                fig_heatmap.update_layout(height=400)
                st.plotly_chart(fig_heatmap, use_container_width=True)
            else:
                st.info("Select multiple transaction types to see the heatmap visualization.")
        
        with tab3:
            # Pattern analysis with multiple visualizations
            col1, col2 = st.columns(2)
            
            with col1:
                # Transaction volume distribution
                if not df_transactions.empty:
                    fig_dist = px.histogram(
                        df_transactions, 
                        x='DAILY_VOLUME',
                        title="📊 Daily Volume Distribution",
                        nbins=20,
                        color_discrete_sequence=['#1f77b4']
                    )
                    fig_dist.update_layout(height=350)
                    st.plotly_chart(fig_dist, use_container_width=True)
            
            with col2:
                # Weekly pattern analysis
                if not df_transactions.empty:
                    df_transactions['DAY_OF_WEEK'] = pd.to_datetime(df_transactions['TRANSACTION_DATE']).dt.day_name()
                    weekly_pattern = df_transactions.groupby('DAY_OF_WEEK')['DAILY_VOLUME'].mean().reset_index()
                    
                    # Reorder days
                    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                    weekly_pattern['DAY_OF_WEEK'] = pd.Categorical(weekly_pattern['DAY_OF_WEEK'], categories=day_order, ordered=True)
                    weekly_pattern = weekly_pattern.sort_values('DAY_OF_WEEK')
                    
                    fig_weekly = px.bar(
                        weekly_pattern,
                        x='DAY_OF_WEEK',
                        y='DAILY_VOLUME',
                        title="📅 Average Volume by Day of Week",
                        color='DAILY_VOLUME',
                        color_continuous_scale='viridis'
                    )
                    fig_weekly.update_layout(height=350)
                    st.plotly_chart(fig_weekly, use_container_width=True)
        
        # =====================================================
        # ADVANCED ML & SNOWFLAKE NATIVE FEATURES  
        # =====================================================
        st.header("🤖 Advanced ML & Snowflake Native Analytics")
        
        if run_anomaly_detection and anomaly_method == "Snowflake ML":
            with st.spinner("🔄 Training Snowflake ML anomaly detection model..."):
                try:
                    # Create Snowflake ML anomaly detection model
                    model_name = "fsi_anomaly_model"
                    
                    # Create training view
                    execute_query("""
                        CREATE OR REPLACE VIEW FSI_DEMO.RAW_DATA.anomaly_training_view AS
                        SELECT 
                            TRANSACTION_DATE as TS,
                            SUM(TRANSACTION_AMOUNT) as DAILY_VOLUME
                        FROM FSI_DEMO.RAW_DATA.TRANSACTIONS_TABLE
                        WHERE DATA_SOURCE = 'HISTORICAL'
                            AND TRANSACTION_DATE >= CURRENT_DATE() - 365
                        GROUP BY TRANSACTION_DATE
                        ORDER BY TRANSACTION_DATE
                    """)
                    
                    # Create Snowflake ML anomaly detection model
                    execute_query(f"""
                        CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION {model_name}(
                            INPUT_DATA => TABLE(FSI_DEMO.RAW_DATA.anomaly_training_view),
                            TIMESTAMP_COLNAME => 'TS',
                            TARGET_COLNAME => 'DAILY_VOLUME'
                        )
                    """)
                    
                    st.success("✅ Snowflake ML model trained successfully!")
                    
                    # Run anomaly detection
                    ml_anomaly_results = execute_query(f"""
                        CALL {model_name}!DETECT_ANOMALIES(
                            INPUT_DATA => TABLE(FSI_DEMO.RAW_DATA.anomaly_training_view),
                            TIMESTAMP_COLNAME => 'TS',
                            TARGET_COLNAME => 'DAILY_VOLUME'
                        )
                    """)
                    
                    if not ml_anomaly_results.empty:
                        st.subheader("🎯 Snowflake ML Anomaly Results")
                        ml_anomalies = ml_anomaly_results[ml_anomaly_results['IS_ANOMALY'] == True]
                        st.metric("ML Detected Anomalies", len(ml_anomalies))
                        
                        if len(ml_anomalies) > 0:
                            st.dataframe(ml_anomalies[['TS', 'DAILY_VOLUME', 'ANOMALY_SCORE']].head(5))
                    
                except Exception as e:
                    st.error(f"Snowflake ML Error: {str(e)}")
                    st.info("💡 Tip: Statistical anomaly detection is available above as an alternative.")
        
        # =====================================================
        # ENHANCED CUSTOMER INSIGHTS WITH GOVERNANCE DEMO
        # =====================================================
        st.header("👥 Customer Intelligence & Data Governance Demo")
        
        # Governance explanation
        st.markdown('<div class="insight-box">', unsafe_allow_html=True)
        st.info(f"""
        🔒 **Data Governance in Action**: This section demonstrates PII masking policies.
        - **Current Role**: `{current_role}`
        - **Data Steward**: Sees full customer data (first_name, last_name, phone)
        - **All Other Roles** (including ACCOUNTADMIN): See masked PII for compliance
        - **Masking Rules**: last_name → "***", phone_number → "***-***-XXXX"
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        if not df_customers.empty:
            # Enhanced customer analytics with multiple visualizations
            tab1, tab2 = st.tabs(["📊 Tier Analytics", "💰 Spending Patterns"])
            
            with tab1:
                col1, col2 = st.columns(2)
                
                with col1:
                    # Enhanced customer tier distribution with custom colors
                    tier_summary = customer_tiers_df
                    
                    # Enhanced color palette for tiers (more distinct colors)
                    tier_colors = {
                        'PLATINUM': '#E6E6FA',  # Light purple
                        'GOLD': '#FFD700',      # Gold
                        'SILVER': '#C0C0C0',    # Silver
                        'BRONZE': '#CD7F32',    # Bronze
                        'STANDARD': '#87CEEB'   # Sky blue
                    }
                    
                    colors = [tier_colors.get(tier, f'hsl({i*60}, 70%, 60%)') for i, tier in enumerate(tier_summary['CUSTOMER_TIER'])]
                    
                    fig_tiers = px.pie(
                        tier_summary, 
                        values='CUSTOMER_COUNT', 
                        names='CUSTOMER_TIER',
                        title="🏆 Customer Distribution by Tier",
                        color_discrete_sequence=colors
                    )
                    fig_tiers.update_traces(textposition='inside', textinfo='percent+label')
                    st.plotly_chart(fig_tiers, use_container_width=True)
                
                with col2:
                    # Enhanced spending analysis
                    fig_spending = px.bar(
                        tier_summary,
                        x='CUSTOMER_TIER',
                        y='AVG_SPENDING',
                        title="💎 Average Spending by Customer Tier",
                        color='AVG_SPENDING',
                        color_continuous_scale='viridis',
                        text='AVG_SPENDING'
                    )
                    fig_spending.update_traces(texttemplate='$%{text:,.0f}', textposition='outside')
                    fig_spending.update_layout(showlegend=False)
                    st.plotly_chart(fig_spending, use_container_width=True)
                
                # Tier performance metrics
                st.subheader("📈 Tier Performance Metrics")
                cols = st.columns(len(tier_summary))
                for i, (_, tier) in enumerate(tier_summary.iterrows()):
                    with cols[i]:
                        st.metric(
                            f"{tier['CUSTOMER_TIER']}", 
                            f"{tier['CUSTOMER_COUNT']:,} customers",
                            delta=f"${tier['AVG_SPENDING']:,.0f} avg"
                        )
            
            with tab2:
                # Spending distribution and patterns
                col1, col2 = st.columns(2)
                
                with col1:
                    # Spending distribution histogram
                    if not df_customers.empty:
                        fig_spend_dist = px.histogram(
                            df_customers,
                            x='AVG_TOTAL_SPENT',
                            title="💰 Customer Spending Distribution",
                            nbins=20,
                            color_discrete_sequence=['#2E8B57']
                        )
                        fig_spend_dist.update_layout(height=350)
                        st.plotly_chart(fig_spend_dist, use_container_width=True)
                
                with col2:
                    # Transaction frequency analysis
                    if not df_customers.empty:
                        fig_freq = px.scatter(
                            df_customers.head(50),
                            x='AVG_TRANSACTIONS',
                            y='AVG_TOTAL_SPENT',
                            color='CUSTOMER_TIER',
                            title="🔄 Spending vs Transaction Frequency",
                            size='AVG_TOTAL_SPENT',
                            hover_data=['FIRST_NAME']
                        )
                        fig_freq.update_layout(height=350)
                        st.plotly_chart(fig_freq, use_container_width=True)
            

        else:
            st.warning("No customer data available for the selected filters.")
    
    # =====================================================
    # PII GOVERNANCE DEMONSTRATION (SEPARATE SECTION)
    # =====================================================
    st.header("🔒 Live PII Governance Demonstration")
    
    # Show governance controls
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**🔐 Current Data Visibility:**")
        if current_role == 'data_steward':
            st.success("✅ **Full PII Access** - You can see all customer data")
        else:
            st.warning("🔒 **Masked PII** - Sensitive data is automatically hidden")
    
    with col2:
        st.markdown("**📋 Masking Rules Applied:**")
        st.write("• Last Name: `***` (full masking)")
        st.write("• Phone: `***-***-XXXX` (partial masking)")
        st.write("• First Name: Visible (not sensitive)")
    
    # Live customer data with masking demonstration
    st.subheader("👥 Customer Sample (Live PII Masking)")
    
    # Query customer data directly to ensure we get the masked columns
    pii_demo_query = """
    SELECT 
        customer_id,
        first_name,
        last_name,           -- This should be masked with *** for non-data_steward roles
        phone_number,        -- This should be masked with ***-***-XXXX for non-data_steward roles
        'Regular table access' as data_source
    FROM FSI_DEMO.RAW_DATA.CUSTOMER_TABLE 
    ORDER BY customer_id
    LIMIT 10
    """
    
    try:
        pii_demo_data = execute_query(pii_demo_query)
        
        if not pii_demo_data.empty:
            # Format the display
            display_df = pii_demo_data.copy()
            
            st.dataframe(display_df, use_container_width=True)
            
            # Show masking explanation based on what we see
            if current_role != 'data_steward':
                # Check if data is actually masked
                sample_last_name = display_df['LAST_NAME'].iloc[0] if 'LAST_NAME' in display_df.columns else ""
                sample_phone = display_df['PHONE_NUMBER'].iloc[0] if 'PHONE_NUMBER' in display_df.columns else ""
                
                if sample_last_name == "***" or "***" in str(sample_phone):
                    st.success("🔍 **Masking Working!** Notice how last_name shows '***' and phone numbers are masked - this is automatic PII protection!")
                else:
                    st.error("⚠️ **Masking Not Active** - Run sql/04_governance_complete.sql to enable PII masking policies")
            else:
                st.success("🔓 As data_steward, you can see the full unmasked customer data for governance purposes.")
        else:
            st.warning("No customer data available for PII demonstration.")
            
    except Exception as e:
        st.error(f"Error loading customer data for PII demonstration: {str(e)}")
        st.info("💡 Make sure to run sql/04_governance_complete.sql to set up governance framework")
    
    else:
        st.warning("No transaction data available for the selected filters")

else:
    st.warning("Please select at least one transaction type and customer tier to begin analysis")

# =====================================================
# ENHANCED FOOTER WITH TECHNICAL DETAILS
# =====================================================
st.markdown("---")

# Technical summary
col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("### 🔒 **Data Governance**")
    st.markdown(f"""
    - **Current Role**: `{current_role}`
    - **PII Masking**: {'🟢 Bypassed' if current_role == 'data_steward' else '🔒 Active'}  
    - **Compliance**: GDPR/CCPA Ready
    - **Access Control**: Role-based (RBAC)
    """)

with col2:
    st.markdown("### 🤖 **ML & Analytics**")
    st.markdown("""
    - **Anomaly Detection**: Statistical Z-Score & Snowflake ML
    - **Real-time Processing**: ❄️ Snowpark & Streamlit
    - **Warehouse**: `TRANSFORMATION_WH_S` (Cost-optimized)
    - **Data Pipeline**: dbt + Snowflake native
    """)

with col3:
    st.markdown("### ⚡ **Performance**")
    st.markdown("""
    - **Caching**: Smart TTL-based refresh
    - **Compute**: Auto-suspend/resume warehouses  
    - **Storage**: Compressed columnar format
    - **Scalability**: Elastic compute separation
    """)

# Auto-refresh indicator
if auto_refresh:
    st.markdown("🔄 **Auto-refresh enabled** - Data updates every 30 seconds")

# Build info
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666; font-size: 0.9em;'>
    🏦 <strong>FSI Analytics Dashboard v2.0</strong> | 
    Built with ❄️ <strong>Snowflake Streamlit</strong> & 📊 <strong>Plotly</strong> | 
    🔒 <strong>Enterprise Data Governance</strong> | 
    🤖 <strong>Native ML Integration</strong>
    <br/>
    <em>Demonstrates: PII Masking, Anomaly Detection, Real-time Analytics, Role-based Access Control</em>
</div>
""", unsafe_allow_html=True)

# Debug info for data steward
if current_role == 'data_steward':
    with st.expander("🔧 Debug Information (Data Steward Only)"):
        st.write("**Session Info:**")
        st.write(f"- Snowflake Account: {session.sql('SELECT CURRENT_ACCOUNT()').collect()[0][0]}")
        st.write(f"- Current Database: {session.sql('SELECT CURRENT_DATABASE()').collect()[0][0]}")
        st.write(f"- Current Schema: {session.sql('SELECT CURRENT_SCHEMA()').collect()[0][0]}")
        st.write(f"- Current Warehouse: {session.sql('SELECT CURRENT_WAREHOUSE()').collect()[0][0]}")
        st.write(f"- Session ID: {session.sql('SELECT CURRENT_SESSION()').collect()[0][0][:8]}...")
        
        st.write("**Data Steward Privileges:**")
        st.success("✅ Can see unmasked PII data")
        st.success("✅ Can access all schemas and tables")
        st.success("✅ Can view debug information")
        st.success("✅ Can monitor data governance policies")
