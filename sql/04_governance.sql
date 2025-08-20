-- =====================================================
-- FSI DEMO - COMPLETE DATA GOVERNANCE FRAMEWORK
-- =====================================================
-- Purpose: Comprehensive data governance setup and demonstration
-- Features: Role creation, PII masking, access controls, Streamlit integration
-- Author: FSI Demo Project
-- Usage: Run as ACCOUNTADMIN - Complete governance setup in one script

USE ROLE ACCOUNTADMIN; 
USE DATABASE FSI_DEMO;
USE SCHEMA RAW_DATA;
USE WAREHOUSE DEV_WH_XS;

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


-- Data Steward permissions (full access)
GRANT USAGE ON DATABASE FSI_DEMO TO ROLE data_steward;
GRANT USAGE ON ALL SCHEMAS IN DATABASE FSI_DEMO TO ROLE data_steward;
GRANT SELECT ON ALL TABLES IN DATABASE FSI_DEMO TO ROLE data_steward;
GRANT SELECT ON ALL VIEWS IN DATABASE FSI_DEMO TO ROLE data_steward;
GRANT USAGE ON WAREHOUSE TRANSFORMATION_WH_S TO ROLE data_steward;
GRANT USAGE ON WAREHOUSE ANALYTICS_WH_S TO ROLE data_steward;




-- =====================================================
-- SECTION 2: PII MASKING POLICIES (STRICT GOVERNANCE)
-- =====================================================


-- Last name masking - ONLY data_steward sees unmasked (including ACCOUNTADMIN masked)
CREATE OR REPLACE MASKING POLICY mask_last_name AS (val STRING) RETURNS STRING ->
  CASE
    WHEN UPPER(CURRENT_ROLE()) = 'DATA_STEWARD' THEN val
    ELSE '***'
  END
COMMENT = 'Strict PII masking: ONLY data_steward sees actual last names, all others (including ACCOUNTADMIN) see ***';

-- Phone number partial masking - ONLY data_steward sees unmasked
CREATE OR REPLACE MASKING POLICY mask_phone_partial AS (val STRING) RETURNS STRING ->
  CASE
    WHEN UPPER(CURRENT_ROLE()) = 'DATA_STEWARD' THEN val
    ELSE CONCAT('***-***-', RIGHT(val, 4))
  END
COMMENT = 'Strict PII masking: ONLY data_steward sees full phone numbers, all others see ***-***-XXXX';


-- ALTER TABLE FSI_DEMO.RAW_DATA.CUSTOMER_TABLE
--   MODIFY COLUMN last_name UNSET MASKING POLICY;
-- ALTER TABLE FSI_DEMO.RAW_DATA.CUSTOMER_TABLE
--   MODIFY COLUMN phone_number UNSET MASKING POLICY;
-- ALTER ICEBERG TABLE FSI_DEMO.RAW_DATA.CUSTOMER_TABLE_ICEBERG
--   MODIFY COLUMN last_name UNSET MASKING POLICY;
-- ALTER ICEBERG TABLE FSI_DEMO.RAW_DATA.CUSTOMER_TABLE_ICEBERG
--   MODIFY COLUMN phone_number UNSET MASKING POLICY;


-- Apply masking policies to customer table
ALTER TABLE FSI_DEMO.RAW_DATA.CUSTOMER_TABLE 
  MODIFY COLUMN last_name SET MASKING POLICY mask_last_name;

ALTER TABLE FSI_DEMO.RAW_DATA.CUSTOMER_TABLE 
  MODIFY COLUMN phone_number SET MASKING POLICY mask_phone_partial;


-- iceberg
-- Apply masking policies to customer table
ALTER ICEBERG TABLE FSI_DEMO.RAW_DATA.CUSTOMER_TABLE_ICEBERG 
  MODIFY COLUMN last_name SET MASKING POLICY mask_last_name;

ALTER ICEBERG TABLE FSI_DEMO.RAW_DATA.CUSTOMER_TABLE_ICEBERG 
  MODIFY COLUMN phone_number SET MASKING POLICY mask_phone_partial;

-- Show created policies
SHOW MASKING POLICIES;

use role data_steward;
select * from FSI_DEMO.RAW_DATA.CUSTOMER_TABLE;
select * from FSI_DEMO.RAW_DATA.CUSTOMER_TABLE_ICEBERG;


use role data_analyst_role;

select * from FSI_DEMO.RAW_DATA.CUSTOMER_TABLE;
select * from FSI_DEMO.RAW_DATA.CUSTOMER_TABLE_ICEBERG;

-- =====================================================
-- SECTION 3: data quality and data metric functions
-- =====================================================
-- Data quality uses data metric functions (DMFs), which include Snowflake-provided system DMFs and user-defined DMFs, 
-- to monitor the state and integrity of your data. You can use DMFs to measure key metrics, such as, but not limited to, 
-- freshness and counts that measure duplicates, NULLs, rows, and unique values.

USE ROLE ACCOUNTADMIN;
GRANT EXECUTE DATA METRIC FUNCTION ON ACCOUNT TO ROLE data_steward;

use role data_steward;
SELECT snowflake.core.blank_count (SELECT transaction_type FROM FSI_DEMO.RAW_DATA.TRANSACTIONS_TABLE);

SELECT *
  FROM TABLE(SYSTEM$DATA_METRIC_SCAN(
    REF_ENTITY_NAME  => 'FSI_DEMO.RAW_DATA.TRANSACTIONS_TABLE',
    METRIC_NAME  => 'snowflake.core.blank_count',
    ARGUMENT_NAME => 'transaction_type'
   ));


-- =====================================================
-- SECTION 4: audit history
-- =====================================================
USE ROLE data_steward;

SELECT user_name
       , query_id
       , query_start_time
       , direct_objects_accessed
       , base_objects_accessed
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
ORDER BY 1, 3 desc;