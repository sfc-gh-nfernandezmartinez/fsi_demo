"""
🏦 FSI Analytics Dashboard
=========================
Purpose: Financial analytics with governance demo
Author: FSI Demo Project  
Role: Shows PII masking in action (ACCOUNTADMIN sees masked data)
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from snowflake.snowpark.context import get_active_session
import datetime
from datetime import timedelta

# =====================================================
# SESSION & CONFIG
# =====================================================

def get_session():
    """Get Snowflake session with error handling"""
    try:
        return get_active_session()
    except Exception as e:
        st.error(f"Failed to connect to Snowflake: {e}")
        st.stop()

# =====================================================
# PAGE CONFIG
# =====================================================

st.set_page_config(
    page_title="FSI Analytics Dashboard",
    page_icon="🏦", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# Get session
session = get_session()

# =====================================================
# HEADER
# =====================================================

st.title("🏦 FSI Analytics Dashboard")
st.markdown("**Financial Services Analytics & Data Governance Demo**")
st.markdown("---")

# =====================================================
# SIDEBAR CONTROLS
# =====================================================

st.sidebar.header("📊 Filters")

# Time Period Filter
time_options = {
    "Last 7 Days": 7,
    "Last 30 Days": 30, 
    "Last 90 Days": 90,
    "Last Year": 365
}

selected_period = st.sidebar.selectbox(
    "🗓️ Time Period",
    options=list(time_options.keys()),
    index=1  # Default to 30 days
)

days_back = time_options[selected_period]

# Transaction Type Filter  
transaction_types = st.sidebar.multiselect(
    "💳 Transaction Types",
    options=["leisure", "lifestyle", "business", "healthcare", "education"],
    default=["leisure", "lifestyle", "business"]
)

# Convert to SQL format
tx_types_sql = "', '".join(transaction_types)

# =====================================================
# DATA LOADING
# =====================================================

@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_transaction_data(days_back, tx_types_sql):
    """Load transaction data with filters"""
    query = f"""
    SELECT 
        TRANSACTION_ID,
        CUSTOMER_ID,
        TRANSACTION_TYPE,
        TRANSACTION_AMOUNT as AMOUNT,
        TRANSACTION_DATE,
        DATA_SOURCE as DESCRIPTION
    FROM FSI_DEMO.TRANSFORMED.STG_TRANSACTIONS
    WHERE TRANSACTION_DATE >= DATEADD('day', -{days_back}, CURRENT_DATE())
    AND TRANSACTION_TYPE IN ('{tx_types_sql}')
    ORDER BY TRANSACTION_DATE DESC
    LIMIT 10000
    """
    
    try:
        return session.sql(query).to_pandas()
    except Exception as e:
        st.error(f"Error loading transaction data: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_customer_summary():
    """Load customer summary metrics"""
    query = """
    SELECT 
        COUNT(DISTINCT c.CUSTOMER_ID) as TOTAL_CUSTOMERS,
        COUNT(DISTINCT CASE WHEN t.TRANSACTION_DATE >= DATEADD('day', -30, CURRENT_DATE()) 
              THEN c.CUSTOMER_ID END) as ACTIVE_CUSTOMERS_30D,
        SUM(CASE WHEN t.TRANSACTION_DATE >= DATEADD('day', -30, CURRENT_DATE()) 
            THEN t.TRANSACTION_AMOUNT ELSE 0 END) as TOTAL_VOLUME_30D,
        COUNT(CASE WHEN t.TRANSACTION_DATE >= DATEADD('day', -30, CURRENT_DATE()) 
              THEN t.TRANSACTION_ID END) as TOTAL_TRANSACTIONS_30D
    FROM FSI_DEMO.ANALYTICS.CUSTOMER_360 c
    LEFT JOIN FSI_DEMO.TRANSFORMED.STG_TRANSACTIONS t ON c.CUSTOMER_ID = t.CUSTOMER_ID
    """
    
    try:
        return session.sql(query).to_pandas()
    except Exception as e:
        st.error(f"Error loading customer summary: {e}")
        return pd.DataFrame()

# Load data
df_transactions = load_transaction_data(days_back, tx_types_sql)
df_summary = load_customer_summary()

# =====================================================
# KEY METRICS
# =====================================================

if not df_summary.empty:
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Total Customers", 
            f"{df_summary['TOTAL_CUSTOMERS'].iloc[0]:,}"
        )
    
    with col2:
        st.metric(
            "Active Customers (30d)", 
            f"{df_summary['ACTIVE_CUSTOMERS_30D'].iloc[0]:,}"
        )
    
    with col3:
        st.metric(
            "Transaction Volume (30d)", 
            f"${df_summary['TOTAL_VOLUME_30D'].iloc[0]:,.0f}"
        )
    
    with col4:
        st.metric(
            "Total Transactions (30d)", 
            f"{df_summary['TOTAL_TRANSACTIONS_30D'].iloc[0]:,}"
        )

st.markdown("---")

# =====================================================
# VISUALIZATIONS
# =====================================================

col1, col2 = st.columns(2)

# Transaction Volume by Type
if not df_transactions.empty:
    with col1:
        st.subheader("💰 Transaction Volume by Type")
        
        volume_by_type = df_transactions.groupby('TRANSACTION_TYPE')['AMOUNT'].sum().reset_index()
        
        fig_pie = px.pie(
            volume_by_type, 
            values='AMOUNT', 
            names='TRANSACTION_TYPE',
            color_discrete_sequence=['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']
        )
        fig_pie.update_layout(height=400)
        st.plotly_chart(fig_pie, use_container_width=True)

    # Daily Transaction Trends
    with col2:
        st.subheader("📈 Daily Transaction Trends")
        
        df_transactions['TRANSACTION_DATE'] = pd.to_datetime(df_transactions['TRANSACTION_DATE'])
        daily_trends = df_transactions.groupby('TRANSACTION_DATE').agg({
            'AMOUNT': 'sum',
            'TRANSACTION_ID': 'count'
        }).reset_index()
        
        fig_line = px.line(
            daily_trends, 
            x='TRANSACTION_DATE', 
            y='AMOUNT',
            title="Daily Transaction Volume"
        )
        fig_line.update_layout(height=400)
        st.plotly_chart(fig_line, use_container_width=True)

# =====================================================
# PII GOVERNANCE DEMO
# =====================================================

st.markdown("---")
st.header("🔒 PII Governance Demo")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Customer Data (Live PII Masking)")
    
    # Query customer data to show masking in action
    customer_query = """
    SELECT 
        CUSTOMER_ID,
        FIRST_NAME,
        LAST_NAME,
        PHONE_NUMBER,
        'Standard' as CUSTOMER_TIER
    FROM FSI_DEMO.RAW_DATA.CUSTOMER_TABLE 
    LIMIT 10
    """
    
    try:
        df_customers = session.sql(customer_query).to_pandas()
        st.dataframe(df_customers, use_container_width=True)
        
        # Show masking status
        if not df_customers.empty:
            if df_customers['LAST_NAME'].iloc[0] == '***':
                st.success("✅ PII Masking Active - Data is properly masked for current role")
            else:
                st.info("ℹ️ PII Masking Inactive - Current role has unmasked access")
                
    except Exception as e:
        st.error(f"Error loading customer data: {e}")

with col2:
    st.subheader("Governance Information")
    
    st.markdown("""
    **Current Role-Based Access:**
    - `data_steward`: Sees unmasked PII
    - `data_analyst_role`: Sees masked PII  
    - `ACCOUNTADMIN`: Sees masked PII
    
    **Masking Policies Applied:**
    - `LAST_NAME`: Masked as '***'
    - `PHONE_NUMBER`: Masked as 'XXX-XXX-XXXX'
    
    **Data Classification:**
    - 🔴 Sensitive: Last Name, Phone Number
    - 🟡 Internal: Customer ID, Tier
    - 🟢 Public: First Name, General Stats
    """)

# =====================================================
# RECENT TRANSACTIONS TABLE
# =====================================================

st.markdown("---")
st.subheader("📋 Recent Transactions")

if not df_transactions.empty:
    # Display recent transactions
    display_df = df_transactions.head(20)[['TRANSACTION_ID', 'CUSTOMER_ID', 'TRANSACTION_TYPE', 'AMOUNT', 'TRANSACTION_DATE', 'DESCRIPTION']]
    st.dataframe(display_df, use_container_width=True)
else:
    st.warning("No transaction data available for the selected filters.")

# =====================================================
# FOOTER
# =====================================================

st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666; font-size: 0.8em;'>
🏦 FSI Analytics Dashboard | Built with Streamlit in Snowflake | Data Governance & Analytics Demo
</div>
""", unsafe_allow_html=True)