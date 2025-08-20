-- Clean up duplicate Streamlit apps
USE DATABASE FSI_DEMO;
USE WAREHOUSE TRANSFORMATION_WH_S;

-- Show existing Streamlit apps
SHOW STREAMLIT;

-- Drop old/duplicate Streamlit apps (keep only the one in ANALYTICS schema)
-- Adjust these names based on what you see in SHOW STREAMLIT results:

-- Drop any apps in RAW_DATA schema
DROP STREAMLIT IF EXISTS FSI_DEMO.RAW_DATA.fsi_analytics_dashboard;
DROP STREAMLIT IF EXISTS FSI_DEMO.RAW_DATA.fsi_analytics_dashboard_no_env;

-- Drop any apps in PUBLIC schema  
DROP STREAMLIT IF EXISTS FSI_DEMO.PUBLIC.fsi_analytics_dashboard;
DROP STREAMLIT IF EXISTS FSI_DEMO.PUBLIC.fsi_analytics_dashboard_no_env;

-- Keep only: FSI_DEMO.ANALYTICS.fsi_analytics_dashboard

-- Verify only one app remains
SHOW STREAMLIT;

SELECT 'Cleanup complete - should only see FSI_DEMO.ANALYTICS.fsi_analytics_dashboard' as status;
