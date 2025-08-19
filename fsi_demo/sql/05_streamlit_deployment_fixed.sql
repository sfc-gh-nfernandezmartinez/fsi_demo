-- =====================================================
-- FSI DEMO - CORRECTED STREAMLIT APP DEPLOYMENT  
-- =====================================================
-- Purpose: Deploy FSI Analytics Dashboard to Snowflake (Native Streamlit)
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
-- 2. CREATE STAGE FOR STREAMLIT FILES (CORRECTED)
-- =====================================================

-- Create internal stage to hold Streamlit app files
CREATE OR REPLACE STAGE fsi_streamlit_stage
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Internal stage for FSI Analytics Dashboard Streamlit app files';

-- Verify stage creation
SHOW STAGES LIKE 'fsi_streamlit_stage';


-- =====================================================
-- 3. UPLOAD FILES TO STAGE (MANUAL STEP)
-- =====================================================
-- IMPORTANT: These PUT commands must be run from your LOCAL terminal
-- Copy and paste these commands one by one in your terminal:

/*

snow sql --connection nfm_demo_keypair --query "
USE ROLE data_analyst_role;
USE DATABASE FSI_DEMO;
USE SCHEMA ANALYTICS;
PUT file://fsi_demo/streamlit/fsi_analytics_dashboard.py @fsi_streamlit_stage AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://fsi_demo/streamlit/environment.yml @fsi_streamlit_stage AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
"

*/


-- =====================================================
-- 4. VERIFY FILE UPLOAD
-- =====================================================

-- Check uploaded files
LIST @fsi_streamlit_stage;


-- =====================================================
-- 5. CREATE STREAMLIT APPLICATION (CORRECTED SYNTAX)
-- =====================================================

-- Create the Streamlit app with correct syntax
CREATE STREAMLIT fsi_analytics_dashboard
    ROOT_LOCATION = '@fsi_streamlit_stage'
    MAIN_FILE = 'fsi_analytics_dashboard.py'
    QUERY_WAREHOUSE = 'ANALYTICS_WH_S'
    COMMENT = 'FSI Analytics Dashboard - Transaction analysis with ML anomaly detection';


-- =====================================================
-- 6. GRANT PERMISSIONS
-- =====================================================

-- Grant usage to the creating role
GRANT USAGE ON STREAMLIT fsi_analytics_dashboard TO ROLE data_analyst_role;

-- Grant to additional roles if needed
-- GRANT USAGE ON STREAMLIT fsi_analytics_dashboard TO ROLE data_steward;


-- =====================================================
-- 7. VERIFICATION
-- =====================================================

-- Show all Streamlit apps in current schema
SHOW STREAMLITS;

-- Describe the specific app
DESC STREAMLIT fsi_analytics_dashboard;

-- Get app information
SELECT 
    'Streamlit App Created Successfully!' as status,
    'fsi_analytics_dashboard' as app_name,
    'FSI_DEMO.ANALYTICS' as location,
    'data_analyst_role' as owner_role;


-- =====================================================
-- 8. ACCESS INSTRUCTIONS
-- =====================================================

/*

TO ACCESS YOUR STREAMLIT APP:

1. Login to your Snowflake account (web UI)
2. Navigate to: Projects > Streamlit
   OR: Data > Apps > Streamlit
3. Look for: fsi_analytics_dashboard
4. Click to open the app

Alternative: Direct URL access through Snowflake UI

*/

