-- =====================================================
-- FSI DEMO - INTERACTIVE GOVERNANCE CONTROLS DEMO
-- =====================================================
-- Purpose: Demonstrate PII masking policies in action across different roles
-- Usage: Run sections interactively to show governance capabilities
-- Author: FSI Demo Project


-- =====================================================
-- DEMO PART 1: PII MASKING IN ACTION
-- =====================================================

-- Start with current role context
SELECT 'Starting Demo - Current Role: ' || CURRENT_ROLE() as demo_status;

-- -----------------------------------------------------
-- Step 1: View data as DATA_ANALYST (PII Masked)
-- -----------------------------------------------------
USE ROLE data_analyst_role;
USE WAREHOUSE ANALYTICS_WH_S;

SELECT 
    '👀 DATA ANALYST VIEW (PII Masked)' as view_type,
    customer_id,
    first_name,
    last_name,     -- Should show: ***
    phone_number,  -- Should show: ***-***-1234 
    has_mortgage,
    created_at
FROM FSI_DEMO.RAW_DATA.CUSTOMER_TABLE 
LIMIT 10;

-- -----------------------------------------------------  
-- Step 2: View data as DATA_STEWARD (PII Visible)
-- -----------------------------------------------------
USE ROLE data_steward;

SELECT 
    '🔓 DATA STEWARD VIEW (PII Visible)' as view_type,
    customer_id,
    first_name,
    last_name,     -- Should show: actual last name
    phone_number,  -- Should show: full phone number
    has_mortgage,
    created_at
FROM FSI_DEMO.RAW_DATA.CUSTOMER_TABLE 
LIMIT 10;

-- -----------------------------------------------------
-- Step 3: Show masking consistency across schemas
-- -----------------------------------------------------

-- Check TRANSFORMED schema (dbt staging models)
SELECT 
    '📊 TRANSFORMED SCHEMA (Inherits Masking)' as view_type,
    customer_id,
    first_name,
    last_name,     -- Masking policy propagates
    phone_clean,   -- Based on masked phone_number
    has_mortgage
FROM FSI_DEMO.TRANSFORMED.stg_customers 
LIMIT 5;

-- Check ANALYTICS schema (dbt mart models)  
SELECT 
    '📈 ANALYTICS SCHEMA (Inherits Masking)' as view_type,
    customer_id,
    first_name,
    last_name,     -- Masking policy propagates
    customer_tier,
    total_transactions,
    total_spent
FROM FSI_DEMO.ANALYTICS.customer_360 
LIMIT 5;


-- =====================================================
-- DEMO PART 2: MASKING POLICY DETAILS
-- =====================================================

-- Show all masking policies in the account
SHOW MASKING POLICIES;

-- Get detailed policy definitions
DESC MASKING POLICY mask_last_name;
DESC MASKING POLICY mask_phone_partial;

-- Show where masking policies are applied
SELECT 
    policy_db,
    policy_schema,
    policy_name,
    ref_database_name,
    ref_schema_name,
    ref_entity_name as table_name,
    ref_column_name as column_name,
    policy_kind
FROM FSI_DEMO.INFORMATION_SCHEMA.POLICY_REFERENCES
WHERE policy_kind = 'MASKING_POLICY'
ORDER BY ref_entity_name, ref_column_name;


-- =====================================================
-- DEMO PART 3: ROLE-BASED ACCESS TESTING
-- =====================================================

-- Test with data_analyst_role
USE ROLE data_analyst_role;
SELECT 
    CURRENT_ROLE() as current_role,
    'Testing PII masking...' as test_purpose,
    customer_id,
    last_name,      -- Should be masked
    phone_number    -- Should be partially masked
FROM FSI_DEMO.RAW_DATA.CUSTOMER_TABLE 
WHERE customer_id = 1001;

-- Test with data_steward role  
USE ROLE data_steward;
SELECT 
    CURRENT_ROLE() as current_role,
    'Testing PII visibility...' as test_purpose,
    customer_id,
    last_name,      -- Should be visible
    phone_number    -- Should be fully visible
FROM FSI_DEMO.RAW_DATA.CUSTOMER_TABLE 
WHERE customer_id = 1001;

-- Test with ACCOUNTADMIN (also has access)
USE ROLE ACCOUNTADMIN;
SELECT 
    CURRENT_ROLE() as current_role,
    'Testing admin access...' as test_purpose,
    customer_id,
    last_name,      -- Should be visible
    phone_number    -- Should be fully visible
FROM FSI_DEMO.RAW_DATA.CUSTOMER_TABLE 
WHERE customer_id = 1001;


-- =====================================================
-- DEMO PART 4: GOVERNANCE INSIGHTS
-- =====================================================

-- Show data lineage - masking follows the data
USE ROLE data_steward;

-- Count customers by tier (business insight)
SELECT 
    customer_tier,
    COUNT(*) as customer_count,
    AVG(total_spent) as avg_spending
FROM FSI_DEMO.ANALYTICS.customer_360
GROUP BY customer_tier
ORDER BY avg_spending DESC;

-- Show transaction patterns (no PII involved)
SELECT 
    transaction_type,
    COUNT(*) as transaction_count,
    SUM(transaction_amount) as total_volume,
    AVG(transaction_amount) as avg_amount
FROM FSI_DEMO.RAW_DATA.TRANSACTIONS_TABLE
WHERE transaction_date >= CURRENT_DATE() - 30
GROUP BY transaction_type
ORDER BY total_volume DESC;


-- =====================================================
-- DEMO PART 5: POLICY MANAGEMENT (OPTIONAL)
-- =====================================================
-- Uncomment these commands to demonstrate policy management
-- WARNING: Only run if you want to temporarily remove/restore masking

/*
-- Remove masking policy (temporarily)
USE ROLE ACCOUNTADMIN;
ALTER TABLE FSI_DEMO.RAW_DATA.CUSTOMER_TABLE 
  MODIFY COLUMN last_name UNSET MASKING POLICY;

-- Query to show unmasked data
USE ROLE data_analyst_role;
SELECT customer_id, last_name FROM FSI_DEMO.RAW_DATA.CUSTOMER_TABLE LIMIT 3;

-- Restore masking policy
USE ROLE ACCOUNTADMIN;  
ALTER TABLE FSI_DEMO.RAW_DATA.CUSTOMER_TABLE 
  MODIFY COLUMN last_name SET MASKING POLICY mask_last_name;

-- Query to confirm masking is restored
USE ROLE data_analyst_role;
SELECT customer_id, last_name FROM FSI_DEMO.RAW_DATA.CUSTOMER_TABLE LIMIT 3;
*/


-- =====================================================
-- DEMO SUMMARY
-- =====================================================
SELECT 
    '✅ Governance Demo Complete!' as status,
    'PII masking policies are active and working correctly' as result,
    'data_analyst_role sees masked PII, data_steward sees actual PII' as key_takeaway;
