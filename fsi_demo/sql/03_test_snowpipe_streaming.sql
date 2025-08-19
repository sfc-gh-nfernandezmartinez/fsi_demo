-- =====================================================
-- Test Snowpipe Streaming with CDCSimulatorApp
-- =====================================================

USE ROLE data_engineer_role;
USE WAREHOUSE INGESTION_WH_XS;
USE DATABASE FSI_DEMO;
USE SCHEMA RAW_DATA;

-- =====================================================
-- 1. CREATE TEST TABLE FOR CDC SIMULATOR
-- =====================================================

-- Create a test table that matches the CDCSimulatorApp expected format
-- Based on the Java code, it expects a single RECORD_CONTENT column for JSON
DROP TABLE IF EXISTS CDC_STREAMING_TABLE;
CREATE TABLE CDC_STREAMING_TABLE (
    RECORD_CONTENT VARIANT
) COMMENT = 'FSI CDC Streaming Table - optimized for Snowpipe Streaming demos';

-- =====================================================
-- 2. VERIFY EXISTING DATA  
-- =====================================================

-- Verify existing historical data is intact
SELECT 
    data_source,
    COUNT(*) as record_count,
    COUNT(DISTINCT customer_id) as unique_customers,
    MIN(transaction_date) as earliest_date,
    MAX(transaction_date) as latest_date
FROM TRANSACTIONS_TABLE 
GROUP BY data_source;

-- =====================================================
-- 3. GRANT PERMISSIONS FOR JAVA CLIENT
-- =====================================================

-- Grant necessary permissions for the CDC simulator
GRANT INSERT ON TABLE CDC_STREAMING_TABLE TO ROLE data_engineer_role;
GRANT SELECT ON TABLE CDC_STREAMING_TABLE TO ROLE data_engineer_role;

-- Also grant on our main table for future testing
GRANT INSERT ON TABLE TRANSACTIONS_TABLE TO ROLE data_engineer_role;

-- =====================================================
-- 4. MONITORING QUERIES
-- =====================================================

-- Check CDC streaming table
SELECT COUNT(*) as cdc_records FROM CDC_STREAMING_TABLE;

-- View recent CDC records
SELECT 
    RECORD_CONTENT:transaction:action::STRING as action,
    RECORD_CONTENT:transaction:record_after:customer_id::NUMBER as customer_id,
    RECORD_CONTENT:transaction:record_after:transaction_type::STRING as transaction_type,
    RECORD_CONTENT:transaction:record_after:transaction_amount::NUMBER as amount,
    RECORD_CONTENT:transaction:committed_at::NUMBER as committed_at
FROM CDC_STREAMING_TABLE 
ORDER BY RECORD_CONTENT:transaction:transaction_id::NUMBER DESC
LIMIT 10;

-- Parse CDC JSON structure
SELECT 
    RECORD_CONTENT:object::STRING as object_type,
    RECORD_CONTENT:transaction:transaction_id::NUMBER as transaction_id,
    RECORD_CONTENT:transaction:action::STRING as action,
    RECORD_CONTENT:transaction:committed_at::NUMBER as committed_at,
    INGESTION_TIMESTAMP
FROM CDC_STREAMING_TABLE 
ORDER BY INGESTION_TIMESTAMP DESC 
LIMIT 10;

-- Cleanup test data (run if needed)
-- TRUNCATE TABLE CDC_STREAMING_TABLE;
