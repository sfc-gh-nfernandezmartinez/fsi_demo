-- =====================================================
-- FSI Demo - Data Ingestion Setup SQL Worksheet
-- =====================================================
-- Purpose: Complete ingestion workflow for historical and real-time data
-- Historical: Batch load from AWS S3 (JSON format)
-- Real-time: Snowpipe Streaming via Python simulator

USE ROLE data_engineer_role;
USE WAREHOUSE INGESTION_WH_XS;
USE DATABASE FSI_DEMO;
USE SCHEMA RAW_DATA;

-- =====================================================
-- 1. MORTGAGE TABLE - FOUNDATIONAL LOAN DATA
-- =====================================================

-- Create mortgage table based on CSV structure
DROP TABLE IF EXISTS MORTGAGE_TABLE;

CREATE TABLE MORTGAGE_TABLE (
    loan_id VARCHAR(255) PRIMARY KEY,
    week_start_date DATE NOT NULL,
    week INT NOT NULL,
    ts VARCHAR(50),  -- Storing as VARCHAR due to MM:SS.s format
    loan_type_name VARCHAR(255) NOT NULL,
    loan_purpose_name VARCHAR(255) NOT NULL,
    applicant_income_000s NUMBER(10,2) NOT NULL,
    loan_amount_000s NUMBER(10,2) NOT NULL,
    county_name VARCHAR(255) NOT NULL,
    mortgageresponse INT NOT NULL,
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
) COMMENT = 'Mortgage data from AWS S3 - foundational loan information';

-- =====================================================
-- 2. CUSTOMER TABLE - REGULAR TABLE (ICEBERG PENDING PERMISSIONS)
-- =====================================================

-- Create customer table (will migrate to Iceberg once external volume permissions resolved)
DROP TABLE IF EXISTS CUSTOMER_TABLE;

CREATE TABLE CUSTOMER_TABLE (
    customer_id NUMBER(38,0) PRIMARY KEY,
    loan_id VARCHAR(255) NOT NULL,
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL,
    phone_number VARCHAR(255) NOT NULL,
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
) COMMENT = 'Customer data - will migrate to Iceberg format once external volume permissions are configured';

-- Note: Iceberg table creation requires external volume write permissions
-- Future migration command (once permissions resolved):
-- CREATE ICEBERG TABLE CUSTOMER_TABLE_ICEBERG (...) EXTERNAL_VOLUME = 'vol_fsi_demo' CATALOG = 'SNOWFLAKE' BASE_LOCATION = 'customer_data';

-- =====================================================
-- 3. ENHANCED TRANSACTIONS TABLE WITH SOURCE TRACKING
-- =====================================================

-- Drop and recreate table with source tracking for easy cleanup
DROP TABLE IF EXISTS TRANSACTIONS_TABLE;

CREATE TABLE TRANSACTIONS_TABLE (
    transaction_id NUMBER(38,0) PRIMARY KEY,
    customer_id NUMBER(38,0) NOT NULL,
    transaction_date DATE NOT NULL,
    transaction_amount NUMBER(10,2) NOT NULL,
    transaction_type VARCHAR(255) NOT NULL,
    data_source VARCHAR(50) NOT NULL DEFAULT 'HISTORICAL',  -- 'HISTORICAL' or 'STREAMING'
    ingestion_timestamp TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
) COMMENT = 'Transaction data with source tracking for easy real-time data cleanup';

-- =====================================================
-- 4. DATA INGESTION - MORTGAGE DATA FROM AWS S3
-- =====================================================

-- Load mortgage data from CSV file (EXISTING in AWS S3)
COPY INTO FSI_DEMO.RAW_DATA.MORTGAGE_TABLE (
    week_start_date, week, loan_id, ts, loan_type_name, 
    loan_purpose_name, applicant_income_000s, loan_amount_000s, 
    county_name, mortgageresponse
) FROM (
    SELECT 
        $1::DATE,                    -- WEEK_START_DATE
        $2::INT,                     -- WEEK  
        $3::VARCHAR,                 -- LOAN_ID
        $4::VARCHAR,                 -- TS
        $5::VARCHAR,                 -- LOAN_TYPE_NAME
        $6::VARCHAR,                 -- LOAN_PURPOSE_NAME
        $7::NUMBER(10,2),            -- APPLICANT_INCOME_000S
        $8::NUMBER(10,2),            -- LOAN_AMOUNT_000S
        $9::VARCHAR,                 -- COUNTY_NAME
        $10::INT                     -- MORTGAGERESPONSE
    FROM @FSI_DEMO.RAW_DATA.STG_EXT_AWS/Mortgage_Data.csv
) 
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1);

-- =====================================================
-- 5. DATA INGESTION - TRANSACTIONS HISTORICAL DATA
-- =====================================================

-- Using existing STG_EXT_AWS stage (no need to recreate)
-- Stage already configured for AWS S3 access

-- Historical transaction data loading query (TESTED AND WORKING)
COPY INTO FSI_DEMO.RAW_DATA.TRANSACTIONS_TABLE (
    transaction_id, customer_id, transaction_date,
    transaction_amount, transaction_type, data_source
) FROM (
    SELECT 
        $1:transaction_id::NUMBER,
        $1:customer_id::NUMBER,
        $1:transaction_date::DATE,
        $1:transaction_amount::NUMBER(10,2),
        $1:transaction_type::VARCHAR,
        $1:data_source::VARCHAR
    FROM @FSI_DEMO.RAW_DATA.STG_EXT_AWS/historical_365_days.json
) FILE_FORMAT = (TYPE = 'JSON');

-- =====================================================
-- 5B. DATA INGESTION - CUSTOMER DATA (ICEBERG)
-- =====================================================

-- Customer data loading (via internal stage)
-- Step 1: Create internal stage and JSON file format
CREATE OR REPLACE STAGE customer_stage COMMENT = 'Internal stage for customer data JSON upload';
CREATE OR REPLACE FILE FORMAT json_format TYPE = 'JSON' COMMENT = 'JSON file format for customer data';

-- Step 2: Upload via PUT command (to internal stage)
-- PUT file:///path/to/customer_data.json @customer_stage;

-- Step 3: Load into customer table
INSERT INTO FSI_DEMO.RAW_DATA.CUSTOMER_TABLE (
    customer_id, loan_id, first_name, last_name, phone_number
)
SELECT 
    $1:customer_id::NUMBER(38,0),
    $1:loan_id::VARCHAR(255),
    $1:first_name::VARCHAR(255),
    $1:last_name::VARCHAR(255),
    $1:phone_number::VARCHAR(255)
FROM @customer_stage/customer_data.json.gz
(FILE_FORMAT => json_format);

-- Step 4: Update customer loan_ids to match actual mortgage data
UPDATE CUSTOMER_TABLE 
SET loan_id = CASE 
    WHEN customer_id BETWEEN 1001 AND 1025 THEN '157154'  -- Rensselaer County
    WHEN customer_id BETWEEN 1026 AND 1050 THEN '176033'  -- Queens County
    WHEN customer_id BETWEEN 1051 AND 1075 THEN '361354'  -- Ulster County
    WHEN customer_id BETWEEN 1076 AND 1100 THEN '363343'  -- Erie County
END;

-- =====================================================
-- 6. REAL-TIME STREAMING SETUP (DIRECT INSERT)
-- =====================================================

-- Real-time streaming uses direct INSERTs via Python connector
-- No additional stages or pipes needed for this implementation
-- Data flows directly: Python Generator → Snowflake INSERT → TRANSACTIONS_TABLE

-- Streaming transactions are identified by data_source = 'STREAMING'
-- and have ingestion_timestamp for tracking

-- =====================================================
-- 7. GRANT PERMISSIONS (SIMPLIFIED)
-- =====================================================

-- Permissions for direct INSERT streaming (already covered in foundation setup)
-- GRANT USAGE ON WAREHOUSE INGESTION_WH_XS TO ROLE data_engineer_role;
-- GRANT OPERATE ON WAREHOUSE INGESTION_WH_XS TO ROLE data_engineer_role;
-- Additional permissions already granted in 01_foundation_setup.sql

-- =====================================================
-- 8. MONITORING AND MANAGEMENT QUERIES
-- =====================================================

-- Check mortgage data ingestion
SELECT 
    COUNT(*) as total_mortgages,
    COUNT(DISTINCT loan_id) as unique_loans,
    COUNT(DISTINCT county_name) as unique_counties,
    MIN(week_start_date) as earliest_week,
    MAX(week_start_date) as latest_week,
    ROUND(AVG(loan_amount_000s), 2) as avg_loan_amount
FROM MORTGAGE_TABLE;

-- Check customer data (Iceberg table)
SELECT 
    COUNT(*) as total_customers,
    COUNT(DISTINCT loan_id) as customers_with_loans,
    COUNT(DISTINCT SUBSTR(phone_number, 1, 3)) as unique_area_codes
FROM CUSTOMER_TABLE;

-- Verify data relationships
SELECT 
    'CUSTOMERS_WITH_MORTGAGES' as metric,
    COUNT(*) as count
FROM CUSTOMER_TABLE c
INNER JOIN MORTGAGE_TABLE m ON c.loan_id = m.loan_id
UNION ALL
SELECT 
    'CUSTOMERS_WITH_TRANSACTIONS' as metric,
    COUNT(DISTINCT t.customer_id) as count
FROM TRANSACTIONS_TABLE t
INNER JOIN CUSTOMER_TABLE c ON t.customer_id = c.customer_id;

-- =====================================================
-- 9. MONITORING AND MANAGEMENT QUERIES (TRANSACTIONS)
-- =====================================================

-- Check ingestion status
SELECT 
    data_source,
    COUNT(*) as record_count,
    MIN(transaction_date) as earliest_date,
    MAX(transaction_date) as latest_date,
    ROUND(SUM(transaction_amount), 2) as total_amount,
    COUNT(DISTINCT customer_id) as unique_customers
FROM TRANSACTIONS_TABLE
GROUP BY data_source
ORDER BY data_source;

-- Check today's streaming data
SELECT 
    COUNT(*) as streaming_records_today,
    ROUND(SUM(transaction_amount), 2) as streaming_amount_today,
    MIN(ingestion_timestamp) as first_stream,
    MAX(ingestion_timestamp) as last_stream
FROM TRANSACTIONS_TABLE 
WHERE data_source = 'STREAMING' 
  AND DATE(ingestion_timestamp) = CURRENT_DATE();

-- =====================================================
-- 10. CLEANUP OPERATIONS
-- =====================================================

-- Delete today's streaming data (for demo reset)
-- DELETE FROM TRANSACTIONS_TABLE 
-- WHERE data_source = 'STREAMING' 
--   AND DATE(ingestion_timestamp) = CURRENT_DATE();

-- Delete all streaming data (keep historical)
-- DELETE FROM TRANSACTIONS_TABLE WHERE data_source = 'STREAMING';

-- Reset entire table (use with caution)
-- TRUNCATE TABLE TRANSACTIONS_TABLE;

-- =====================================================
-- 11. SAMPLE DATA QUERIES
-- =====================================================

-- Sample mortgage data with customer info
SELECT 
    m.loan_id,
    c.customer_id,
    c.first_name,
    c.last_name,
    m.loan_type_name,
    m.loan_purpose_name,
    m.loan_amount_000s,
    m.county_name
FROM MORTGAGE_TABLE m
LEFT JOIN CUSTOMER_TABLE c ON m.loan_id = c.loan_id
ORDER BY m.loan_amount_000s DESC
LIMIT 10;

-- Customer transaction summary
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    c.loan_id,
    COUNT(t.transaction_id) as transaction_count,
    ROUND(SUM(t.transaction_amount), 2) as total_amount
FROM CUSTOMER_TABLE c
LEFT JOIN TRANSACTIONS_TABLE t ON c.customer_id = t.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.loan_id
ORDER BY transaction_count DESC
LIMIT 10;

-- =====================================================
-- 12. TRANSACTION-SPECIFIC SAMPLE QUERIES
-- =====================================================




-- =====================================================
-- 13. PERFORMANCE MONITORING
-- =====================================================

-- Check warehouse utilization
SHOW WAREHOUSES LIKE 'INGESTION_WH_%';

-- Check streaming connection status (Python connector handles this)
-- Real-time streaming status monitored via application metrics

-- Check recent copy history
SELECT 
    table_name,
    last_load_time,
    status,
    row_count,
    row_parsed,
    error_count
FROM INFORMATION_SCHEMA.COPY_HISTORY 
WHERE table_name = 'TRANSACTIONS_TABLE'
ORDER BY last_load_time DESC
LIMIT 10;

-- =====================================================
-- NOTES:
-- =====================================================
-- 1. Historical data assumed to be in AWS S3 as compressed JSON
-- 2. Real-time streaming uses Snowpipe Streaming API via Python
-- 3. data_source column enables easy separation and cleanup
-- 4. All operations use INGESTION_WH_XS for cost optimization
-- 5. Permissions granted to data_engineer_role for automation
-- =====================================================
