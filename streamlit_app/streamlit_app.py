"""
FSI Analytics Dashboard - Snowflake Streamlit App
==================================================
Purpose: Interactive analytics dashboard with ML-powered anomaly detection
Author: FSI Demo Project  
Role: Created by data_analyst_role (PII automatically masked)

Features:
- Time series transaction analysis
- ML-based anomaly detection using Snowflake ML
- Customer insights with automatic PII masking
- Interactive visualizations with Plotly
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import numpy as np

# Snowflake session management
try:
    from snowflake.snowpark.context import get_active_session
    session = get_active_session()
    SNOWFLAKE_ENVIRONMENT = True
except:
    SNOWFLAKE_ENVIRONMENT = False
    st.error("Unable to connect to Snowflake session")
    st.stop()

# =====================================================
# PAGE CONFIGURATION
# =====================================================
st.set_page_config(
    page_title="FSI Analytics Dashboard", 
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# App title and description
st.title("🏦 FSI Analytics Dashboard")
st.markdown("**Real-time Transaction Analysis with ML-Powered Anomaly Detection**")
st.markdown("*Built on Snowflake • Data governance with automatic PII masking*")

# =====================================================
# HELPER FUNCTIONS
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
    """Load available transaction types"""
    query = """
    SELECT DISTINCT TRANSACTION_TYPE 
    FROM FSI_DEMO.RAW_DATA.TRANSACTIONS_TABLE 
    ORDER BY TRANSACTION_TYPE
    """
    return execute_query(query)

@st.cache_data(ttl=300)
def load_customer_tiers():
    """Load available customer tiers"""
    query = """
    SELECT DISTINCT CUSTOMER_TIER 
    FROM FSI_DEMO.ANALYTICS.CUSTOMER_360 
    WHERE CUSTOMER_TIER IS NOT NULL
    ORDER BY CUSTOMER_TIER
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
# SIDEBAR CONTROLS
# =====================================================
st.sidebar.header("📊 Dashboard Controls")

# Date range selector
date_range = st.sidebar.selectbox(
    "📅 Analysis Period",
    ["Last 30 days", "Last 90 days", "Last 6 months", "All historical data"],
    index=1
)

# Load transaction types and customer tiers
tx_types_df = load_transaction_types()
customer_tiers_df = load_customer_tiers()

# Transaction type filter
if not tx_types_df.empty:
    selected_tx_types = st.sidebar.multiselect(
        "💳 Transaction Types",
        options=tx_types_df['TRANSACTION_TYPE'].tolist(),
        default=tx_types_df['TRANSACTION_TYPE'].tolist()
    )
else:
    selected_tx_types = []
    st.sidebar.warning("No transaction types found")

# Customer tier filter
if not customer_tiers_df.empty:
    selected_tiers = st.sidebar.multiselect(
        "👥 Customer Tiers",
        options=customer_tiers_df['CUSTOMER_TIER'].tolist(),
        default=customer_tiers_df['CUSTOMER_TIER'].tolist()
    )
else:
    selected_tiers = []
    st.sidebar.warning("No customer tiers found")

# Anomaly detection controls
st.sidebar.subheader("🔍 Anomaly Detection")
anomaly_sensitivity = st.sidebar.slider(
    "Detection Sensitivity",
    min_value=0.1,
    max_value=0.9,
    value=0.2,
    step=0.1,
    help="Lower values = more sensitive (more anomalies detected)"
)

run_anomaly_detection = st.sidebar.button("🚀 Run Anomaly Detection", type="primary")

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
    with st.spinner("Loading transaction data..."):
        df_transactions = load_transaction_data(days_back, selected_tx_types)
        df_customers = load_customer_insights(selected_tiers)
    
    # =====================================================
    # KEY METRICS ROW
    # =====================================================
    if not df_transactions.empty:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_volume = df_transactions['DAILY_VOLUME'].sum()
            st.metric(
                "💰 Total Volume", 
                f"${total_volume:,.0f}",
                delta=f"{len(df_transactions)} data points"
            )
        
        with col2:
            total_transactions = df_transactions['DAILY_COUNT'].sum()
            st.metric(
                "📊 Total Transactions", 
                f"{total_transactions:,}",
                delta=f"Avg: {total_transactions/len(df_transactions):.0f}/day" if len(df_transactions) > 0 else "No data"
            )
        
        with col3:
            avg_amount = df_transactions['AVG_AMOUNT'].mean()
            st.metric(
                "💳 Avg Transaction", 
                f"${avg_amount:.2f}",
                delta=f"Range: ${df_transactions['MIN_AMOUNT'].min():.0f}-${df_transactions['MAX_AMOUNT'].max():.0f}"
            )
        
        with col4:
            unique_customers = len(df_customers)
            st.metric(
                "👥 Active Customers", 
                f"{unique_customers:,}",
                delta=f"{len(selected_tiers)} tiers"
            )
        
        # =====================================================
        # TRANSACTION TRENDS ANALYSIS
        # =====================================================
        st.header("📈 Transaction Trends Analysis")
        
        if len(selected_tx_types) > 0:
            # Time series chart
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
        # ANOMALY DETECTION SECTION
        # =====================================================
        st.header("🔍 ML-Powered Anomaly Detection")
        
        if run_anomaly_detection:
            with st.spinner("Training anomaly detection model..."):
                try:
                    # Create anomaly detection model
                    model_name = "fsi_anomaly_model"
                    
                    # First, create training view
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
                    
                    # Create anomaly detection model
                    execute_query(f"""
                        CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION {model_name}(
                            INPUT_DATA => TABLE(FSI_DEMO.RAW_DATA.anomaly_training_view),
                            TIMESTAMP_COLNAME => 'TS',
                            TARGET_COLNAME => 'DAILY_VOLUME'
                        )
                    """)
                    
                    st.success("✅ Anomaly detection model trained successfully!")
                    
                    # Run anomaly detection on recent data
                    anomaly_results = execute_query(f"""
                        CALL {model_name}!DETECT_ANOMALIES(
                            INPUT_DATA => TABLE(FSI_DEMO.RAW_DATA.anomaly_training_view),
                            TIMESTAMP_COLNAME => 'TS',
                            TARGET_COLNAME => 'DAILY_VOLUME'
                        )
                    """)
                    
                    # Display anomaly results
                    st.subheader("🚨 Detected Anomalies")
                    
                    if len(anomaly_results) > 0:
                        # Create anomaly visualization
                        fig_anomaly = go.Figure()
                        
                        # Plot normal data points
                        normal_data = anomaly_results[anomaly_results['IS_ANOMALY'] == False]
                        if not normal_data.empty:
                            fig_anomaly.add_trace(go.Scatter(
                                x=normal_data['TS'],
                                y=normal_data['DAILY_VOLUME'],
                                mode='lines+markers',
                                name='Normal',
                                line=dict(color='blue', width=2),
                                marker=dict(size=6)
                            ))
                        
                        # Plot anomalies
                        anomalies = anomaly_results[anomaly_results['IS_ANOMALY'] == True]
                        if not anomalies.empty:
                            fig_anomaly.add_trace(go.Scatter(
                                x=anomalies['TS'],
                                y=anomalies['DAILY_VOLUME'],
                                mode='markers',
                                name='Anomalies',
                                marker=dict(color='red', size=12, symbol='x')
                            ))
                        
                        fig_anomaly.update_layout(
                            title="Transaction Volume Anomaly Detection",
                            xaxis_title="Date",
                            yaxis_title="Daily Volume ($)",
                            height=500
                        )
                        
                        st.plotly_chart(fig_anomaly, use_container_width=True)
                        
                        # Show anomaly summary
                        num_anomalies = len(anomalies)
                        st.metric("🚨 Anomalies Detected", num_anomalies)
                        
                        if num_anomalies > 0:
                            st.write("**Anomalous dates:**")
                            st.dataframe(
                                anomalies[['TS', 'DAILY_VOLUME', 'ANOMALY_SCORE']].head(10),
                                use_container_width=True
                            )
                    else:
                        st.info("No anomalies detected in the analyzed period.")
                        
                except Exception as e:
                    st.error(f"Error in anomaly detection: {str(e)}")
                    st.info("This might be due to insufficient historical data or model training requirements.")
        
        # =====================================================
        # CUSTOMER INSIGHTS SECTION  
        # =====================================================
        st.header("👥 Customer Insights (PII Masked)")
        
        if not df_customers.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                # Customer tier distribution
                tier_summary = df_customers.groupby('CUSTOMER_TIER').agg({
                    'CUSTOMER_COUNT': 'sum',
                    'AVG_TOTAL_SPENT': 'mean'
                }).reset_index()
                
                fig_tiers = px.pie(
                    tier_summary, 
                    values='CUSTOMER_COUNT', 
                    names='CUSTOMER_TIER',
                    title="Customer Distribution by Tier"
                )
                st.plotly_chart(fig_tiers, use_container_width=True)
            
            with col2:
                # Spending by tier
                fig_spending = px.bar(
                    tier_summary,
                    x='CUSTOMER_TIER',
                    y='AVG_TOTAL_SPENT',
                    title="Average Spending by Customer Tier"
                )
                st.plotly_chart(fig_spending, use_container_width=True)
            
            # Customer sample with masked PII
            st.subheader("📋 Customer Sample (PII Automatically Masked)")
            st.info("🔒 Note: last_name and phone numbers are automatically masked for data_analyst_role")
            
            # Show sample customers with masked data
            sample_customers = df_customers.head(10)[['CUSTOMER_TIER', 'FIRST_NAME', 'LAST_NAME', 'PHONE_CLEAN', 'AVG_TOTAL_SPENT']]
            st.dataframe(sample_customers, use_container_width=True)
        else:
            st.warning("No customer data available for selected tiers")
    
    else:
        st.warning("No transaction data available for the selected filters")

else:
    st.warning("Please select at least one transaction type and customer tier")

# =====================================================
# FOOTER
# =====================================================
st.markdown("---")
st.markdown("""
**🔒 Data Governance:** This dashboard demonstrates Snowflake's automatic PII masking.  
Customer last names and phone numbers are masked for `data_analyst_role` but visible to `data_steward`.  

**🤖 ML Technology:** Anomaly detection powered by Snowflake ML Functions.  
**⚡ Performance:** Running on `TRANSFORMATION_WH_S` warehouse for cost-optimized analytics.
""")

st.markdown("*Built with ❄️ Snowflake Streamlit & 📊 Plotly*")
