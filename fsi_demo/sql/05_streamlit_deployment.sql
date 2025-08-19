-- =====================================================
-- FSI DEMO - STREAMLIT APP DEPLOYMENT  
-- =====================================================
-- Purpose: Deploy FSI Analytics Dashboard to Snowflake
-- Usage: Run as data_analyst_role after governance setup
-- Author: FSI Demo Project


-- =====================================================
-- 1. PREPARATION - SWITCH TO CORRECT ROLE
-- =====================================================

-- Use data_analyst_role to create the app (this determines app permissions)
USE ROLE data_analyst_role;
USE WAREHOUSE ANALYTICS_WH_S;
USE DATABASE FSI_DEMO;
USE SCHEMA ANALYTICS;


-- =====================================================
-- 2. CREATE STAGE FOR STREAMLIT FILES
-- =====================================================

-- Create internal stage to hold Streamlit app files
CREATE OR REPLACE STAGE fsi_streamlit_stage
COMMENT = 'Internal stage for FSI Analytics Dashboard Streamlit app files';

-- Check stage is created
LIST @fsi_streamlit_stage;


-- =====================================================
-- 3. UPLOAD STREAMLIT FILES TO STAGE
-- =====================================================
-- Note: Files must be uploaded using PUT commands from local machine
-- These commands need to be run from your local environment via Snowflake CLI

/*
-- Run these PUT commands from your local terminal using Snowflake CLI:

snow sql --connection nfm_demo_keypair --query "USE ROLE data_analyst_role; USE DATABASE FSI_DEMO; USE SCHEMA ANALYTICS;"

-- Upload main Streamlit app
PUT file://fsi_demo/streamlit/fsi_analytics_dashboard.py @fsi_streamlit_stage/streamlit/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- Upload environment file  
PUT file://fsi_demo/streamlit/environment.yml @fsi_streamlit_stage/streamlit/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

*/


-- =====================================================
-- 4. CREATE STREAMLIT APPLICATION
-- =====================================================

-- Create the Streamlit app pointing to the staged files
CREATE OR REPLACE STREAMLIT fsi_analytics_dashboard
    ROOT_LOCATION = '@fsi_streamlit_stage/streamlit'
    MAIN_FILE = 'fsi_analytics_dashboard.py'
    QUERY_WAREHOUSE = 'ANALYTICS_WH_S'
    COMMENT = 'FSI Analytics Dashboard - Transaction analysis with ML anomaly detection. Created by data_analyst_role with automatic PII masking.';


-- =====================================================
-- 5. GRANT ACCESS TO STREAMLIT APP
-- =====================================================

-- Grant usage to data_analyst_role (creator already has access)
GRANT USAGE ON STREAMLIT fsi_analytics_dashboard TO ROLE data_analyst_role;

-- Grant access to other roles if needed
-- GRANT USAGE ON STREAMLIT fsi_analytics_dashboard TO ROLE data_steward;


-- =====================================================
-- 6. VERIFICATION
-- =====================================================

-- Show the created Streamlit app
SHOW STREAMLITS;

-- Get app details
DESC STREAMLIT fsi_analytics_dashboard;

-- Get the app URL for access
SELECT 
    'App Created Successfully!' as status,
    'fsi_analytics_dashboard' as app_name,
    'Navigate to: Data > Streamlit Apps in Snowflake UI' as access_instructions,
    'Created by: data_analyst_role (PII automatically masked)' as governance_note;


-- =====================================================
-- DEPLOYMENT COMPLETE
-- =====================================================
-- Next steps:
-- 1. Verify files uploaded: LIST @fsi_streamlit_stage/streamlit;
-- 2. Access app: Snowflake UI > Data > Apps > Streamlit Apps > fsi_analytics_dashboard
-- 3. Test governance: Notice PII masking in customer data
-- 4. Test ML: Run anomaly detection on transaction data
-- =====================================================
