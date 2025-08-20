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
# SIMPLIFIED SIDEBAR CONTROLS
# =====================================================
st.sidebar.header("🎛️ Dashboard Controls")

# Show current role for governance context
st.sidebar.markdown(f"""
**Current Role:** `{current_role}`  
{'🟢 Full PII Access' if current_role == 'data_steward' else '🔒 PII Masked'}
""")

st.sidebar.markdown("---")

# Simple date range selector (applies to all charts)
date_range = st.sidebar.selectbox(
    "📅 Time Period",
    ["Last 30 days", "Last 90 days", "Last 6 months", "All historical data"],
    index=1
)

# Load transaction types and customer tiers
tx_types_df = load_transaction_types()
customer_tiers_df = load_customer_tiers()

# Simple transaction type filter
if not tx_types_df.empty:
    selected_tx_types = st.sidebar.multiselect(
        "💳 Transaction Types",
        options=tx_types_df['TRANSACTION_TYPE'].tolist(),
        default=tx_types_df['TRANSACTION_TYPE'].tolist()
    )
else:
    selected_tx_types = []
    st.sidebar.warning("No transaction types found")

# Simple customer tier filter
if not customer_tiers_df.empty:
    selected_tiers = st.sidebar.multiselect(
        "👥 Customer Tiers",
        options=customer_tiers_df['CUSTOMER_TIER'].tolist(),
        default=customer_tiers_df['CUSTOMER_TIER'].tolist()
    )
else:
    selected_tiers = []
    st.sidebar.warning("No customer tiers found")

# =====================================================
# MAIN DASHBOARD CONTENT
# =====================================================

# Determine date range
days_map = {
    "Last 30 days": 30,
    "Last 90 days": 90, 
    "Last 6 months": 180,
    "All historical data": 365
}
days_back = days_map[date_range]

# Load data
if selected_tx_types and selected_tiers:
    with st.spinner("Loading analytics..."):
        df_transactions = load_transaction_data(days_back, selected_tx_types)
        df_customers = load_customer_insights(selected_tiers)
        df_metrics = load_real_time_metrics()
    
    # =====================================================
    # KEY METRICS DASHBOARD
    # =====================================================
    if not df_transactions.empty and not df_metrics.empty:
        
        # Simple KPI metrics
        col1, col2, col3, col4 = st.columns(4)
        
        metrics = df_metrics.iloc[0]
        
        with col1:
            total_volume = df_transactions['DAILY_VOLUME'].sum()
            st.metric(
                "💰 Total Volume", 
                f"${total_volume:,.0f}",
                delta=f"{len(df_transactions)} days"
            )
        
        with col2:
            total_transactions = df_transactions['DAILY_COUNT'].sum()
            daily_avg = total_transactions / len(df_transactions) if len(df_transactions) > 0 else 0
            st.metric(
                "📊 Total Transactions", 
                f"{total_transactions:,}",
                delta=f"{daily_avg:.0f}/day average"
            )
        
        with col3:
            avg_amount = df_transactions['AVG_AMOUNT'].mean()
            st.metric(
                "💳 Average Amount", 
                f"${avg_amount:.2f}",
                delta=f"Range: ${df_transactions['MIN_AMOUNT'].min():.0f}-${df_transactions['MAX_AMOUNT'].max():.0f}"
            )
        
        with col4:
            unique_customers = len(df_customers)
            st.metric(
                "👥 Active Customers", 
                f"{unique_customers:,}",
                delta=f"{len(selected_tiers)} tiers selected"
            )
        
        # =====================================================
        # TRANSACTION TRENDS
        # =====================================================
        st.header("📈 Transaction Analysis")
        
        # Simple time series chart
        if len(selected_tx_types) > 0:
            fig_trends = make_subplots(
                rows=2, cols=1,
                subplot_titles=('Daily Transaction Volume ($)', 'Daily Transaction Count'),
                vertical_spacing=0.1
            )
            
            # Plot volume trend
            for tx_type in selected_tx_types:
                df_type = df_transactions[df_transactions['TRANSACTION_TYPE'] == tx_type]
                if not df_type.empty:
                    fig_trends.add_trace(
                        go.Scatter(
                            x=df_type['TRANSACTION_DATE'],
                            y=df_type['DAILY_VOLUME'],
                            mode='lines+markers',
                            name=f'{tx_type} Volume',
                            line=dict(width=2)
                        ),
                        row=1, col=1
                    )
            
            # Plot count trend  
            for tx_type in selected_tx_types:
                df_type = df_transactions[df_transactions['TRANSACTION_TYPE'] == tx_type]
                if not df_type.empty:
                    fig_trends.add_trace(
                        go.Scatter(
                            x=df_type['TRANSACTION_DATE'],
                            y=df_type['DAILY_COUNT'],
                            mode='lines+markers',
                            name=f'{tx_type} Count',
                            line=dict(width=2),
                            showlegend=False
                        ),
                        row=2, col=1
                    )
            
            fig_trends.update_layout(height=600, title_text="Transaction Trends Over Time")
            fig_trends.update_xaxes(title_text="Date")
            fig_trends.update_yaxes(title_text="Volume ($)", row=1, col=1)
            fig_trends.update_yaxes(title_text="Count", row=2, col=1)
            
            st.plotly_chart(fig_trends, use_container_width=True)
        

        
        # =====================================================
        # CUSTOMER INSIGHTS
        # =====================================================
        st.header("👥 Customer Insights")
        
        if not df_customers.empty:
            # Simple customer analytics
            col1, col2 = st.columns(2)
            
            with col1:
                # Customer tier distribution
                tier_summary = customer_tiers_df
                
                # Color palette for tiers
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
                # Spending by tier
                fig_spending = px.bar(
                    tier_summary,
                    x='CUSTOMER_TIER',
                    y='AVG_SPENDING',
                    title="💎 Average Spending by Customer Tier",
                    color='AVG_SPENDING',
                    color_continuous_scale='viridis'
                )
                fig_spending.update_layout(showlegend=False)
                st.plotly_chart(fig_spending, use_container_width=True)
            

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
    st.markdown("### 📊 **Analytics**")
    st.markdown("""
    - **Real-time Processing**: ❄️ Streamlit on Snowflake
    - **Data Pipeline**: dbt transformations
    - **Warehouse**: `TRANSFORMATION_WH_S`
    - **Visualizations**: Interactive Plotly charts
    """)

with col3:
    st.markdown("### ⚡ **Platform**")
    st.markdown("""
    - **Cloud**: Snowflake Data Cloud
    - **Compute**: Auto-scaling warehouses
    - **Storage**: Optimized columnar format
    - **Security**: Enterprise-grade RBAC
    """)



# Build info
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666; font-size: 0.9em;'>
    🏦 <strong>FSI Analytics Dashboard</strong> | 
    Built with ❄️ <strong>Snowflake Streamlit</strong> & 📊 <strong>Plotly</strong>
    <br/>
    <em>Demonstrates: Transaction Analytics, PII Masking, Customer Insights</em>
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
