-- =====================================================
-- FSI DEMO - DATA GOVERNANCE & PII MASKING SETUP
-- =====================================================
-- Purpose: Create data steward role and implement PII masking policies
-- Author: FSI Demo Project
-- Usage: Run as ACCOUNTADMIN or SYSADMIN


-- =====================================================
-- 1. CREATE DATA STEWARD ROLE
-- =====================================================

-- Create dedicated data steward role (separate from SYSADMIN)
CREATE ROLE IF NOT EXISTS data_steward
COMMENT = 'Role for data stewards with access to unmasked PII data for governance purposes';

-- Grant role hierarchy
GRANT ROLE data_steward TO ROLE SYSADMIN;

-- Grant database and schema privileges
GRANT USAGE ON DATABASE FSI_DEMO TO ROLE data_steward;
GRANT ALL ON SCHEMA FSI_DEMO.RAW_DATA TO ROLE data_steward;
GRANT ALL ON SCHEMA FSI_DEMO.TRANSFORMED TO ROLE data_steward;
GRANT ALL ON SCHEMA FSI_DEMO.ANALYTICS TO ROLE data_steward;

-- Grant warehouse access for governance tasks
GRANT USAGE ON WAREHOUSE ANALYTICS_WH_S TO ROLE data_steward;
GRANT USAGE ON WAREHOUSE DEV_WH_XS TO ROLE data_steward;


-- =====================================================
-- 2. CREATE PII MASKING POLICIES
-- =====================================================

-- Full masking policy for last names
CREATE OR REPLACE MASKING POLICY mask_last_name AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('data_steward', 'ACCOUNTADMIN') THEN val
    ELSE '***'
  END
COMMENT = 'Full masking of last names - only data_steward and ACCOUNTADMIN can see actual values';

-- Partial masking policy for phone numbers (show last 4 digits)
CREATE OR REPLACE MASKING POLICY mask_phone_partial AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('data_steward', 'ACCOUNTADMIN') THEN val
    ELSE CONCAT('***-***-', RIGHT(val, 4))
  END
COMMENT = 'Partial masking of phone numbers - shows last 4 digits only for non-privileged roles';


-- =====================================================
-- 3. APPLY MASKING POLICIES TO TABLES
-- =====================================================

-- Apply masking to CUSTOMER_TABLE in RAW_DATA
ALTER TABLE FSI_DEMO.RAW_DATA.CUSTOMER_TABLE 
  MODIFY COLUMN last_name SET MASKING POLICY mask_last_name;

ALTER TABLE FSI_DEMO.RAW_DATA.CUSTOMER_TABLE 
  MODIFY COLUMN phone_number SET MASKING POLICY mask_phone_partial;

-- Note: Masking policies automatically propagate to views and downstream objects
-- This means dbt models in TRANSFORMED and ANALYTICS will inherit the masking


-- =====================================================
-- 4. VERIFICATION QUERIES
-- =====================================================

-- Check masking policies exist
SHOW MASKING POLICIES;

-- Verify policy application
SELECT 
    table_name,
    column_name,
    masking_policy_name
FROM FSI_DEMO.INFORMATION_SCHEMA.POLICY_REFERENCES
WHERE policy_kind = 'MASKING_POLICY'
ORDER BY table_name, column_name;

-- Test masking behavior (run as different roles to see different results)
-- This query will show different results based on current role
SELECT 
    'Current Role: ' || CURRENT_ROLE() as context,
    customer_id,
    first_name,
    last_name,     -- Masked for data_analyst_role, visible for data_steward
    phone_number   -- Partially masked for data_analyst_role, visible for data_steward
FROM FSI_DEMO.RAW_DATA.CUSTOMER_TABLE 
LIMIT 5;


-- =====================================================
-- 5. GRANT PERMISSIONS FOR STREAMLIT APP
-- =====================================================

-- Ensure data_analyst_role can create and use Streamlit apps
GRANT CREATE STREAMLIT ON SCHEMA FSI_DEMO.ANALYTICS TO ROLE data_analyst_role;
GRANT USAGE ON STREAMLIT ALL ON SCHEMA FSI_DEMO.ANALYTICS TO ROLE data_analyst_role;

-- Grant ML function usage for anomaly detection
GRANT USAGE ON DATABASE FSI_DEMO TO ROLE data_analyst_role;
GRANT CREATE MODEL ON SCHEMA FSI_DEMO.ANALYTICS TO ROLE data_analyst_role;


-- =====================================================
-- DEPLOYMENT COMPLETE
-- =====================================================
-- Next steps:
-- 1. Test masking with: USE ROLE data_analyst_role; then query CUSTOMER_TABLE
-- 2. Test steward access: USE ROLE data_steward; then query CUSTOMER_TABLE  
-- 3. Deploy Streamlit app using data_analyst_role
-- =====================================================
