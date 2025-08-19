#!/bin/bash

# =====================================================
# FSI DEMO - COMPLETE DEPLOYMENT SCRIPT
# =====================================================
# Purpose: Deploy entire FSI demo to Snowflake account
# Usage: ./deploy_to_snowflake.sh
# Prerequisites: Snowflake CLI configured with connection


echo "🏦 FSI Demo - Deploying to Snowflake..."
echo "======================================================"

# Check if Snowflake CLI is available
if ! command -v snow &> /dev/null; then
    echo "❌ Error: Snowflake CLI not found. Please install it first."
    exit 1
fi

# Check connection
echo "🔍 Testing Snowflake connection..."
if ! snow connection test nfm_demo_keypair &> /dev/null; then
    echo "❌ Error: Cannot connect to Snowflake. Check your connection configuration."
    exit 1
fi

echo "✅ Connection successful!"

# Step 1: Deploy governance setup
echo ""
echo "🛡️ Step 1: Setting up data governance and PII masking..."
snow sql --connection nfm_demo_keypair --filename fsi_demo/sql/03_governance_setup.sql

if [ $? -eq 0 ]; then
    echo "✅ Governance setup completed successfully!"
else
    echo "❌ Error in governance setup. Please check the output above."
    exit 1
fi

# Step 2: Upload Streamlit files  
echo ""
echo "📤 Step 2: Uploading Streamlit app files..."

# Switch to data_analyst_role and create stage
snow sql --connection nfm_demo_keypair --query "
USE ROLE data_analyst_role;
USE DATABASE FSI_DEMO; 
USE SCHEMA ANALYTICS;
CREATE OR REPLACE STAGE fsi_streamlit_stage;
"

# Upload main app file
echo "Uploading fsi_analytics_dashboard.py..."
snow sql --connection nfm_demo_keypair --query "
USE ROLE data_analyst_role;
USE DATABASE FSI_DEMO;
USE SCHEMA ANALYTICS;
PUT file://$(pwd)/fsi_demo/streamlit/fsi_analytics_dashboard.py @fsi_streamlit_stage/streamlit/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
"

# Upload environment file
echo "Uploading environment.yml..."
snow sql --connection nfm_demo_keypair --query "
USE ROLE data_analyst_role; 
USE DATABASE FSI_DEMO;
USE SCHEMA ANALYTICS;
PUT file://$(pwd)/fsi_demo/streamlit/environment.yml @fsi_streamlit_stage/streamlit/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
"

echo "✅ Files uploaded successfully!"

# Step 3: Create Streamlit app
echo ""
echo "🚀 Step 3: Creating Streamlit application..."
snow sql --connection nfm_demo_keypair --query "
USE ROLE data_analyst_role;
USE DATABASE FSI_DEMO;
USE SCHEMA ANALYTICS;

CREATE OR REPLACE STREAMLIT fsi_analytics_dashboard
    ROOT_LOCATION = '@fsi_streamlit_stage/streamlit'
    MAIN_FILE = 'fsi_analytics_dashboard.py'
    QUERY_WAREHOUSE = 'ANALYTICS_WH_S'
    COMMENT = 'FSI Analytics Dashboard - Transaction analysis with ML anomaly detection';

GRANT USAGE ON STREAMLIT fsi_analytics_dashboard TO ROLE data_analyst_role;
"

if [ $? -eq 0 ]; then
    echo "✅ Streamlit app created successfully!"
else
    echo "❌ Error creating Streamlit app. Please check the output above."
    exit 1
fi

# Step 4: Verification
echo ""
echo "🔍 Step 4: Verifying deployment..."
snow sql --connection nfm_demo_keypair --query "
USE ROLE data_analyst_role;
USE DATABASE FSI_DEMO;
USE SCHEMA ANALYTICS;

-- List uploaded files
LIST @fsi_streamlit_stage/streamlit;

-- Show Streamlit apps
SHOW STREAMLITS;
"

echo ""
echo "🎉 DEPLOYMENT COMPLETE!"
echo "======================================================"
echo ""
echo "📱 Access your Streamlit app:"
echo "  1. Login to your Snowflake account"
echo "  2. Navigate to: Data > Apps > Streamlit Apps"  
echo "  3. Click on: fsi_analytics_dashboard"
echo ""
echo "🛡️ Test governance controls:"
echo "  Run: snow sql --filename fsi_demo/sql/04_governance_controls.sql"
echo ""
echo "🔧 App details:"
echo "  • Name: fsi_analytics_dashboard"
echo "  • Role: data_analyst_role (PII masked)"
echo "  • Warehouse: ANALYTICS_WH_S"
echo "  • Schema: FSI_DEMO.ANALYTICS"
echo ""
echo "✨ Features available:"
echo "  • Interactive transaction trends"
echo "  • ML-powered anomaly detection"  
echo "  • Customer insights (PII masked)"
echo "  • Real-time visualizations"
echo ""
echo "Happy analyzing! 🚀"
