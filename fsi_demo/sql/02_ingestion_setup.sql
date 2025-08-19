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
-- 1. ENHANCED TRANSACTIONS TABLE WITH SOURCE TRACKING
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
-- 2. HISTORICAL DATA INGESTION (AWS S3)
-- =====================================================

-- Create external stage for AWS S3 historical data
CREATE OR REPLACE STAGE historical_data_stage
    URL = 's3://your-bucket/fsi-demo/historical/'
    -- CREDENTIALS = (AWS_KEY_ID = 'your_key' AWS_SECRET_KEY = 'your_secret')
    FILE_FORMAT = (
        TYPE = 'JSON'
        COMPRESSION = 'GZIP'
    )
    COMMENT = 'External stage for historical transaction data from AWS S3';

-- Historical data loading query (run once)
-- COPY INTO TRANSACTIONS_TABLE (
--     transaction_id, customer_id, transaction_date, 
--     transaction_amount, transaction_type, data_source
-- ) FROM (
--     SELECT 
--         $1:transaction_id::NUMBER,
--         $1:customer_id::NUMBER,
--         $1:transaction_date::DATE,
--         $1:transaction_amount::NUMBER(10,2),
--         $1:transaction_type::VARCHAR,
--         'HISTORICAL'
--     FROM @historical_data_stage
-- )
-- FILE_FORMAT = (TYPE = 'JSON');

-- =====================================================
-- 3. REAL-TIME STREAMING SETUP (SNOWPIPE STREAMING)
-- =====================================================

-- Create internal stage for real-time streaming buffer
CREATE OR REPLACE STAGE streaming_stage
    COMMENT = 'Internal stage for real-time transaction streaming buffer';

-- Create pipe for auto-ingestion of streaming data
CREATE OR REPLACE PIPE streaming_pipe
    AUTO_INGEST = FALSE  -- We'll use Snowpipe Streaming API, not file-based
    AS
    COPY INTO TRANSACTIONS_TABLE (
        transaction_id, customer_id, transaction_date,
        transaction_amount, transaction_type, data_source
    ) FROM (
        SELECT 
            $1:transaction_id::NUMBER,
            $1:customer_id::NUMBER,
            $1:transaction_date::DATE,
            $1:transaction_amount::NUMBER(10,2),
            $1:transaction_type::VARCHAR,
            'STREAMING'
        FROM @streaming_stage
    )
    FILE_FORMAT = (TYPE = 'JSON');

-- =====================================================
-- 4. GRANT PERMISSIONS FOR STREAMING
-- =====================================================

-- Grant necessary permissions for Snowpipe Streaming
GRANT USAGE ON WAREHOUSE INGESTION_WH_XS TO ROLE data_engineer_role;
GRANT OPERATE ON WAREHOUSE INGESTION_WH_XS TO ROLE data_engineer_role;
GRANT ALL ON STAGE streaming_stage TO ROLE data_engineer_role;
GRANT ALL ON PIPE streaming_pipe TO ROLE data_engineer_role;

-- =====================================================
-- 5. MONITORING AND MANAGEMENT QUERIES
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
-- 6. CLEANUP OPERATIONS
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
-- 7. SAMPLE DATA QUERIES
-- =====================================================

-- View recent streaming transactions
SELECT 
    transaction_id,
    customer_id,
    transaction_date,
    transaction_amount,
    transaction_type,
    data_source,
    ingestion_timestamp
FROM TRANSACTIONS_TABLE 
WHERE data_source = 'STREAMING'
ORDER BY ingestion_timestamp DESC
LIMIT 10;

-- Compare historical vs streaming patterns
SELECT 
    data_source,
    transaction_type,
    COUNT(*) as transaction_count,
    ROUND(AVG(transaction_amount), 2) as avg_amount,
    ROUND(STDDEV(transaction_amount), 2) as amount_stddev
FROM TRANSACTIONS_TABLE
GROUP BY data_source, transaction_type
ORDER BY data_source, transaction_count DESC;

-- Anomaly detection (amounts > $10k)
SELECT 
    transaction_id,
    customer_id,
    transaction_amount,
    transaction_type,
    data_source,
    ingestion_timestamp
FROM TRANSACTIONS_TABLE 
WHERE transaction_amount > 10000
ORDER BY transaction_amount DESC;

-- =====================================================
-- 8. PERFORMANCE MONITORING
-- =====================================================

-- Check warehouse utilization
SHOW WAREHOUSES LIKE 'INGESTION_WH_%';

-- Check pipe status
SELECT 
    SYSTEM$PIPE_STATUS('streaming_pipe') as pipe_status;

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
