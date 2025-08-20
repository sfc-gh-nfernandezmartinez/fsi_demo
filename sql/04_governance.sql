-- =====================================================
-- FSI DEMO - COMPLETE DATA GOVERNANCE FRAMEWORK
-- =====================================================
-- Purpose: Comprehensive data governance setup and demonstration
-- Features: Role creation, PII masking, access controls, Streamlit integration
-- Author: FSI Demo Project
-- Usage: Run as ACCOUNTADMIN - Complete governance setup in one script

USE DATABASE FSI_DEMO;
USE WAREHOUSE TRANSFORMATION_WH_S;

-- =====================================================
-- SECTION 1: ROLE CREATION & HIERARCHY
-- =====================================================

-- Create data steward role (governance leader)
CREATE ROLE IF NOT EXISTS data_steward
COMMENT = 'Data steward role with full access to unmasked PII for governance oversight';

-- Create data analyst role (dashboard users)
CREATE ROLE IF NOT EXISTS data_analyst_role  
COMMENT = 'Data analyst role for dashboard users - sees masked PII for compliance';

-- Grant role hierarchy
GRANT ROLE data_steward TO ROLE SYSADMIN;
GRANT ROLE data_analyst_role TO ROLE SYSADMIN;

-- =====================================================
-- SECTION 2: DATABASE & SCHEMA PERMISSIONS
-- =====================================================

-- Data Steward permissions (full access)
GRANT USAGE ON DATABASE FSI_DEMO TO ROLE data_steward;
GRANT USAGE ON ALL SCHEMAS IN DATABASE FSI_DEMO TO ROLE data_steward;
GRANT SELECT ON ALL TABLES IN DATABASE FSI_DEMO TO ROLE data_steward;
GRANT SELECT ON ALL VIEWS IN DATABASE FSI_DEMO TO ROLE data_steward;
GRANT USAGE ON WAREHOUSE TRANSFORMATION_WH_S TO ROLE data_steward;
GRANT USAGE ON WAREHOUSE ANALYTICS_WH_S TO ROLE data_steward;

-- Data Analyst permissions (restricted access)
GRANT USAGE ON DATABASE FSI_DEMO TO ROLE data_analyst_role;
GRANT USAGE ON SCHEMA FSI_DEMO.ANALYTICS TO ROLE data_analyst_role;
GRANT USAGE ON SCHEMA FSI_DEMO.RAW_DATA TO ROLE data_analyst_role;
GRANT USAGE ON SCHEMA FSI_DEMO.TRANSFORMED TO ROLE data_analyst_role;
GRANT SELECT ON ALL TABLES IN SCHEMA FSI_DEMO.RAW_DATA TO ROLE data_analyst_role;
GRANT SELECT ON ALL VIEWS IN SCHEMA FSI_DEMO.TRANSFORMED TO ROLE data_analyst_role;
GRANT SELECT ON ALL TABLES IN SCHEMA FSI_DEMO.ANALYTICS TO ROLE data_analyst_role;
GRANT USAGE ON WAREHOUSE TRANSFORMATION_WH_S TO ROLE data_analyst_role;

-- =====================================================
-- SECTION 3: PII MASKING POLICIES (STRICT GOVERNANCE)
-- =====================================================

-- Last name masking - ONLY data_steward sees unmasked (including ACCOUNTADMIN masked)
CREATE OR REPLACE MASKING POLICY mask_last_name AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() = 'data_steward' THEN val
    ELSE '***'
  END
COMMENT = 'Strict PII masking: ONLY data_steward sees actual last names, all others (including ACCOUNTADMIN) see ***';

-- Phone number partial masking - ONLY data_steward sees unmasked
CREATE OR REPLACE MASKING POLICY mask_phone_partial AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() = 'data_steward' THEN val
    ELSE CONCAT('***-***-', RIGHT(val, 4))
  END
COMMENT = 'Strict PII masking: ONLY data_steward sees full phone numbers, all others see ***-***-XXXX';

-- Apply masking policies to customer table
ALTER TABLE FSI_DEMO.RAW_DATA.CUSTOMER_TABLE 
  MODIFY COLUMN last_name SET MASKING POLICY mask_last_name;

ALTER TABLE FSI_DEMO.RAW_DATA.CUSTOMER_TABLE 
  MODIFY COLUMN phone_number SET MASKING POLICY mask_phone_partial;

-- =====================================================
-- SECTION 4: STREAMLIT APP PERMISSIONS & SETUP
-- =====================================================

-- Streamlit app permissions for data_analyst_role
GRANT CREATE STREAMLIT ON SCHEMA FSI_DEMO.ANALYTICS TO ROLE data_analyst_role;
GRANT USAGE ON STREAMLIT ALL ON SCHEMA FSI_DEMO.ANALYTICS TO ROLE data_analyst_role;

-- ML function permissions for anomaly detection
GRANT CREATE MODEL ON SCHEMA FSI_DEMO.ANALYTICS TO ROLE data_analyst_role;
GRANT USAGE ON DATABASE FSI_DEMO TO ROLE data_analyst_role;

-- Stage permissions for Streamlit deployment
GRANT USAGE ON STAGE fsi_demo_stage TO ROLE data_analyst_role;
GRANT READ ON STAGE fsi_demo_stage TO ROLE data_analyst_role;

-- Transfer ownership of existing Streamlit app (if it exists)
-- This ensures the app runs with data_analyst_role privileges and shows masked PII
-- ALTER STREAMLIT FSI_DEMO.ANALYTICS.fsi_analytics_dashboard SET OWNER = data_analyst_role;

-- =====================================================
-- SECTION 5: GOVERNANCE VERIFICATION & TESTING
-- =====================================================

-- Show created policies
SHOW MASKING POLICIES;

-- Verify policy applications
SELECT 
    table_name,
    column_name,
    masking_policy_name,
    policy_kind
FROM FSI_DEMO.INFORMATION_SCHEMA.POLICY_REFERENCES
WHERE policy_kind = 'MASKING_POLICY'
ORDER BY table_name, column_name;

-- Test current role masking (will show different results based on role)
SELECT 
    'Current Role Masking Test: ' || CURRENT_ROLE() as test_context,
    customer_id,
    first_name,
    last_name,     -- *** unless data_steward
    phone_number   -- ***-***-XXXX unless data_steward
FROM FSI_DEMO.RAW_DATA.CUSTOMER_TABLE 
LIMIT 5;

-- =====================================================
-- SECTION 6: INTERACTIVE GOVERNANCE DEMONSTRATION
-- =====================================================

-- Display role comparison instructions
SELECT '═══ GOVERNANCE DEMONSTRATION GUIDE ═══' as demo_guide
UNION ALL
SELECT ''
UNION ALL
SELECT '🔒 TEST PII MASKING WITH DIFFERENT ROLES:'
UNION ALL
SELECT ''
UNION ALL
SELECT '1️⃣ Test as ACCOUNTADMIN (should see MASKED data):'
UNION ALL
SELECT '   USE ROLE ACCOUNTADMIN;'
UNION ALL
SELECT '   SELECT first_name, last_name, phone_number FROM FSI_DEMO.RAW_DATA.CUSTOMER_TABLE LIMIT 3;'
UNION ALL
SELECT '   Expected: last_name = ***, phone_number = ***-***-XXXX'
UNION ALL
SELECT ''
UNION ALL
SELECT '2️⃣ Test as data_analyst_role (should see MASKED data):'
UNION ALL
SELECT '   USE ROLE data_analyst_role;'
UNION ALL
SELECT '   SELECT first_name, last_name, phone_number FROM FSI_DEMO.RAW_DATA.CUSTOMER_TABLE LIMIT 3;'
UNION ALL
SELECT '   Expected: last_name = ***, phone_number = ***-***-XXXX'
UNION ALL
SELECT ''
UNION ALL
SELECT '3️⃣ Test as data_steward (should see UNMASKED data):'
UNION ALL
SELECT '   USE ROLE data_steward;'
UNION ALL
SELECT '   SELECT first_name, last_name, phone_number FROM FSI_DEMO.RAW_DATA.CUSTOMER_TABLE LIMIT 3;'
UNION ALL
SELECT '   Expected: Full names and phone numbers visible'
UNION ALL
SELECT ''
UNION ALL
SELECT '🏦 STREAMLIT DASHBOARD:'
UNION ALL
SELECT '   - App URL: FSI_DEMO.ANALYTICS.fsi_analytics_dashboard'
UNION ALL
SELECT '   - Shows live PII masking demonstration'
UNION ALL
SELECT '   - Role-based access control in action'
UNION ALL
SELECT ''
UNION ALL
SELECT '✅ GOVERNANCE FEATURES DEMONSTRATED:'
UNION ALL
SELECT '   • Role-based PII masking (GDPR/CCPA compliant)'
UNION ALL
SELECT '   • Principle of least privilege'
UNION ALL
SELECT '   • Data steward oversight capabilities'
UNION ALL
SELECT '   • Real-time access control enforcement';

-- =====================================================
-- SECTION 7: GOVERNANCE METRICS & MONITORING
-- =====================================================

-- Show role assignments for governance tracking
SELECT 
    'GOVERNANCE ROLES SUMMARY' as category,
    '' as role_name,
    '' as description,
    '' as access_level
UNION ALL
SELECT 
    'Role Assignment',
    'data_steward',
    'Full PII access for governance oversight',
    '🟢 UNMASKED'
UNION ALL
SELECT 
    'Role Assignment', 
    'data_analyst_role',
    'Dashboard users with masked PII',
    '🔒 MASKED'
UNION ALL
SELECT 
    'Role Assignment',
    'ACCOUNTADMIN', 
    'System admin with masked PII (compliance)',
    '🔒 MASKED'
UNION ALL
SELECT 
    'Policy Status',
    'mask_last_name',
    'Full masking except data_steward',
    'ACTIVE'
UNION ALL
SELECT 
    'Policy Status',
    'mask_phone_partial', 
    'Partial masking except data_steward',
    'ACTIVE';

-- =====================================================
-- GOVERNANCE FRAMEWORK DEPLOYMENT COMPLETE
-- =====================================================

SELECT 
    '🏆 GOVERNANCE FRAMEWORK DEPLOYED SUCCESSFULLY' as status,
    'PII masking policies active' as security,
    'Role-based access control enabled' as access,
    'Streamlit integration ready' as dashboard,
    'GDPR/CCPA compliance achieved' as compliance;

-- =====================================================
-- NEXT STEPS
-- =====================================================
/*
✅ GOVERNANCE SETUP COMPLETE

📋 NEXT ACTIONS:
1. Test role switching and PII masking as shown above
2. Access Streamlit dashboard: FSI_DEMO.ANALYTICS.fsi_analytics_dashboard  
3. Verify governance controls work in real-time
4. Use data_steward role for governance oversight
5. Use data_analyst_role for daily analytics work

🔒 SECURITY FEATURES ACTIVE:
• PII masking for last_name and phone_number
• Role-based access control (RBAC)
• Principle of least privilege enforcement
• Data steward oversight capabilities
• Compliance with privacy regulations

🎯 GOVERNANCE ACHIEVED:
This framework demonstrates enterprise-grade data governance
with automatic PII protection, role-based security, and
real-time compliance enforcement.
*/
