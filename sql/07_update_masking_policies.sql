-- =====================================================
-- UPDATE MASKING POLICIES - ACCOUNTADMIN ALSO MASKED
-- =====================================================
-- Purpose: Update PII masking so ONLY data_steward can see unmasked data
-- All other roles (including ACCOUNTADMIN) see masked PII
-- Author: FSI Demo Project
-- Usage: Run as ACCOUNTADMIN or SYSADMIN

USE DATABASE FSI_DEMO;
USE WAREHOUSE TRANSFORMATION_WH_S;

-- =====================================================
-- 1. UPDATE MASKING POLICIES (STRONGER GOVERNANCE)
-- =====================================================

-- Update last name masking policy - ONLY data_steward sees unmasked
CREATE OR REPLACE MASKING POLICY mask_last_name AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() = 'data_steward' THEN val
    ELSE '***'
  END
COMMENT = 'Full masking of last names - ONLY data_steward can see actual values, ACCOUNTADMIN sees masked';

-- Update phone masking policy - ONLY data_steward sees unmasked  
CREATE OR REPLACE MASKING POLICY mask_phone_partial AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() = 'data_steward' THEN val
    ELSE CONCAT('***-***-', RIGHT(val, 4))
  END
COMMENT = 'Partial masking of phone numbers - ONLY data_steward sees full numbers, all others see last 4 digits';

-- =====================================================
-- 2. VERIFY POLICIES ARE APPLIED
-- =====================================================

-- Check masking policies exist
SHOW MASKING POLICIES;

-- Verify policy application to tables
SELECT 
    table_name,
    column_name,
    masking_policy_name,
    policy_kind
FROM FSI_DEMO.INFORMATION_SCHEMA.POLICY_REFERENCES
WHERE policy_kind = 'MASKING_POLICY'
ORDER BY table_name, column_name;

-- =====================================================
-- 3. TEST MASKING WITH DIFFERENT ROLES
-- =====================================================

-- Test as current role (should show masked if not data_steward)
SELECT 
    'Testing with role: ' || CURRENT_ROLE() as test_context,
    customer_id,
    first_name,
    last_name,     -- Should be *** unless role is data_steward
    phone_number   -- Should be ***-***-XXXX unless role is data_steward
FROM FSI_DEMO.RAW_DATA.CUSTOMER_TABLE 
LIMIT 3;

-- =====================================================
-- 4. SHOW ROLE MASKING COMPARISON
-- =====================================================

-- Instructions for testing
SELECT '=== TESTING INSTRUCTIONS ===' as instructions
UNION ALL
SELECT 'Run the following to test different roles:'
UNION ALL
SELECT ''
UNION ALL  
SELECT '-- Test as ACCOUNTADMIN (should see masked):'
UNION ALL
SELECT 'USE ROLE ACCOUNTADMIN;'
UNION ALL
SELECT 'SELECT first_name, last_name, phone_number FROM FSI_DEMO.RAW_DATA.CUSTOMER_TABLE LIMIT 3;'
UNION ALL
SELECT ''
UNION ALL
SELECT '-- Test as data_steward (should see unmasked):'
UNION ALL
SELECT 'USE ROLE data_steward;'
UNION ALL
SELECT 'SELECT first_name, last_name, phone_number FROM FSI_DEMO.RAW_DATA.CUSTOMER_TABLE LIMIT 3;'
UNION ALL
SELECT ''
UNION ALL
SELECT '-- Test as data_analyst_role (should see masked):'
UNION ALL
SELECT 'USE ROLE data_analyst_role;'
UNION ALL
SELECT 'SELECT first_name, last_name, phone_number FROM FSI_DEMO.RAW_DATA.CUSTOMER_TABLE LIMIT 3;';

-- =====================================================
-- 5. GRANT NECESSARY PERMISSIONS TO ROLES
-- =====================================================

-- Ensure data_steward has all necessary access
GRANT USAGE ON DATABASE FSI_DEMO TO ROLE data_steward;
GRANT USAGE ON ALL SCHEMAS IN DATABASE FSI_DEMO TO ROLE data_steward;
GRANT SELECT ON ALL TABLES IN DATABASE FSI_DEMO TO ROLE data_steward;
GRANT SELECT ON ALL VIEWS IN DATABASE FSI_DEMO TO ROLE data_steward;

-- Ensure data_analyst_role can use the Streamlit app
GRANT USAGE ON DATABASE FSI_DEMO TO ROLE data_analyst_role;
GRANT USAGE ON SCHEMA FSI_DEMO.ANALYTICS TO ROLE data_analyst_role;
GRANT USAGE ON SCHEMA FSI_DEMO.RAW_DATA TO ROLE data_analyst_role;
GRANT USAGE ON SCHEMA FSI_DEMO.TRANSFORMED TO ROLE data_analyst_role;
GRANT SELECT ON ALL TABLES IN SCHEMA FSI_DEMO.RAW_DATA TO ROLE data_analyst_role;
GRANT SELECT ON ALL VIEWS IN SCHEMA FSI_DEMO.TRANSFORMED TO ROLE data_analyst_role;
GRANT SELECT ON ALL TABLES IN SCHEMA FSI_DEMO.ANALYTICS TO ROLE data_analyst_role;
GRANT USAGE ON WAREHOUSE TRANSFORMATION_WH_S TO ROLE data_analyst_role;

-- =====================================================
-- GOVERNANCE UPDATE COMPLETE
-- =====================================================
-- Summary:
-- ✅ Updated masking policies to be more restrictive
-- ✅ ONLY data_steward can see unmasked PII data
-- ✅ ACCOUNTADMIN now sees masked data (stronger governance)
-- ✅ All other roles see masked data
-- ✅ Streamlit app will show governance in action
-- =====================================================

SELECT 
    '🔒 GOVERNANCE UPDATE COMPLETE' as status,
    'ONLY data_steward can see unmasked PII' as policy,
    'ACCOUNTADMIN now sees masked data' as enhancement,
    'Test different roles to verify' as next_step;
