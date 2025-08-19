-- Fix Streamlit App Ownership for PII Masking
-- =============================================
-- Purpose: Transfer ownership to data_analyst_role so PII masking policies work
-- Note: Run this after deploying the Streamlit app with SnowCLI

USE DATABASE FSI_DEMO;
USE WAREHOUSE TRANSFORMATION_WH_S;

-- Ensure data_analyst_role exists and has necessary privileges
GRANT USAGE ON DATABASE FSI_DEMO TO ROLE data_analyst_role;
GRANT USAGE ON SCHEMA ANALYTICS TO ROLE data_analyst_role;
GRANT USAGE ON SCHEMA RAW_DATA TO ROLE data_analyst_role;
GRANT USAGE ON SCHEMA TRANSFORMED TO ROLE data_analyst_role;

-- Grant warehouse usage for the app
GRANT USAGE ON WAREHOUSE TRANSFORMATION_WH_S TO ROLE data_analyst_role;

-- Grant read access to all necessary tables
GRANT SELECT ON ALL TABLES IN SCHEMA RAW_DATA TO ROLE data_analyst_role;
GRANT SELECT ON ALL VIEWS IN SCHEMA TRANSFORMED TO ROLE data_analyst_role;
GRANT SELECT ON ALL TABLES IN SCHEMA ANALYTICS TO ROLE data_analyst_role;

-- Grant access to the stage used by Streamlit
GRANT USAGE ON STAGE fsi_demo_stage TO ROLE data_analyst_role;

-- Transfer ownership of the Streamlit app to data_analyst_role
-- This ensures the app runs with data_analyst_role privileges and sees masked PII
ALTER STREAMLIT FSI_DEMO.ANALYTICS.fsi_analytics_dashboard SET OWNER = data_analyst_role;

-- Verify the ownership change
SHOW STREAMLIT LIKE 'fsi_analytics_dashboard' IN SCHEMA FSI_DEMO.ANALYTICS;

-- Test: Check if the current role can see masked data
-- (Run this as data_analyst_role to verify masking works)
/*
USE ROLE data_analyst_role;
SELECT 
    first_name,
    last_name,      -- Should show *** for data_analyst_role
    phone_clean     -- Should show partial masking like xxx-xxx-1234
FROM FSI_DEMO.ANALYTICS.customer_360 
LIMIT 5;
*/
