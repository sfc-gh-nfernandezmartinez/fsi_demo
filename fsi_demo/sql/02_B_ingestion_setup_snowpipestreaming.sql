-- =====================================================
-- FSI Demo - Snowpipe Streaming Infrastructure Setup
-- =====================================================
-- Purpose: Alternative high-throughput streaming setup using Snowpipe Streaming API
-- Use Case: Production-scale streaming (100+ TPS) vs current direct INSERT approach

USE ROLE ACCOUNTADMIN;  -- Required for streaming infrastructure
USE DATABASE FSI_DEMO;
USE SCHEMA RAW_DATA;

-- =====================================================
-- 1. STREAMING INFRASTRUCTURE REQUIREMENTS
-- =====================================================

-- Check Snowflake edition (Business Critical+ required for Snowpipe Streaming)
SELECT CURRENT_VERSION() as snowflake_version,
       CURRENT_EDITION() as edition;

-- Note: Snowpipe Streaming requires Business Critical or Enterprise edition
-- If you're on Standard edition, this setup won't work

-- =====================================================
-- 2. DEDICATED STREAMING WAREHOUSE
-- =====================================================

-- Create optimized warehouse for streaming workloads
CREATE OR REPLACE WAREHOUSE STREAMING_WH_XS
WITH 
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 30  -- Suspend after 30 seconds of inactivity
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 1
    COMMENT = 'Dedicated warehouse for Snowpipe streaming operations - auto-scales for high throughput';

-- Grant permissions to streaming roles
GRANT USAGE ON WAREHOUSE STREAMING_WH_XS TO ROLE data_engineer_role;
GRANT OPERATE ON WAREHOUSE STREAMING_WH_XS TO ROLE data_engineer_role;

-- =====================================================
-- 3. STREAMING TABLE ENHANCEMENTS
-- =====================================================

-- Add streaming-specific metadata columns to existing table
ALTER TABLE TRANSACTIONS_TABLE 
ADD COLUMN IF NOT EXISTS stream_offset NUMBER COMMENT 'Snowpipe streaming offset for deduplication';

ALTER TABLE TRANSACTIONS_TABLE 
ADD COLUMN IF NOT EXISTS batch_id VARCHAR(255) COMMENT 'Streaming batch identifier for monitoring';

ALTER TABLE TRANSACTIONS_TABLE 
ADD COLUMN IF NOT EXISTS streaming_timestamp TIMESTAMP_LTZ COMMENT 'Timestamp when streamed (vs ingestion_timestamp)';

-- =====================================================
-- 4. STREAMING CHANNEL CREATION
-- =====================================================

-- Create streaming channel for high-throughput ingestion
-- Note: This requires Business Critical+ edition
-- CREATE OR REPLACE STREAM CHANNEL fsi_transactions_channel
--     TABLE_NAME = 'FSI_DEMO.RAW_DATA.TRANSACTIONS_TABLE'
--     COMMENT = 'High-throughput streaming channel for FSI transactions';

-- Alternative: Use Snowpipe with auto-ingest for Standard edition
CREATE OR REPLACE PIPE fsi_streaming_pipe
    AUTO_INGEST = TRUE
    AWS_SNS_TOPIC = 'arn:aws:sns:us-east-1:your-account:snowpipe-notifications'
    AS
    COPY INTO TRANSACTIONS_TABLE (
        transaction_id, customer_id, transaction_date,
        transaction_amount, transaction_type, data_source,
        batch_id, streaming_timestamp
    ) FROM (
        SELECT 
            $1:transaction_id::NUMBER,
            $1:customer_id::NUMBER,
            $1:transaction_date::DATE,
            $1:transaction_amount::NUMBER(10,2),
            $1:transaction_type::VARCHAR,
            $1:data_source::VARCHAR,
            $1:batch_id::VARCHAR,
            $1:streaming_timestamp::TIMESTAMP_LTZ
        FROM @STG_EXT_AWS
    )
    FILE_FORMAT = (TYPE = 'JSON')
    COMMENT = 'Auto-ingestion pipe for streaming JSON files';

-- =====================================================
-- 5. STREAMING STAGE FOR BATCHED FILES
-- =====================================================

-- Create dedicated stage for streaming file uploads (if using file-based streaming)
CREATE OR REPLACE STAGE streaming_files_stage
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for streaming JSON files with auto-ingest capabilities';

-- Grant permissions for streaming operations
GRANT READ, WRITE ON STAGE streaming_files_stage TO ROLE data_engineer_role;
GRANT OWNERSHIP ON PIPE fsi_streaming_pipe TO ROLE data_engineer_role;

-- =====================================================
-- 6. STREAMING USER & AUTHENTICATION
-- =====================================================

-- Create dedicated streaming service user (if not exists)
CREATE USER IF NOT EXISTS fsi_streaming_service
    PASSWORD = 'temp_password_change_immediately'
    DEFAULT_ROLE = 'data_engineer_role'
    DEFAULT_WAREHOUSE = 'STREAMING_WH_XS'
    DEFAULT_DATABASE = 'FSI_DEMO'
    DEFAULT_SCHEMA = 'RAW_DATA'
    COMMENT = 'Service user for Snowpipe streaming operations';

-- Grant streaming-specific permissions
GRANT ROLE data_engineer_role TO USER fsi_streaming_service;

-- Grant pipe permissions for streaming
GRANT USAGE ON PIPE fsi_streaming_pipe TO ROLE data_engineer_role;
GRANT MONITOR ON PIPE fsi_streaming_pipe TO ROLE data_engineer_role;

-- =====================================================
-- 7. STREAMING MONITORING VIEWS
-- =====================================================

-- Create view for streaming performance monitoring
CREATE OR REPLACE VIEW streaming_performance_monitor AS
SELECT 
    -- Time-based metrics
    DATE_TRUNC('minute', streaming_timestamp) as minute,
    COUNT(*) as transactions_per_minute,
    COUNT(DISTINCT batch_id) as unique_batches,
    AVG(transaction_amount) as avg_amount,
    
    -- Streaming metadata
    data_source,
    MIN(streaming_timestamp) as batch_start,
    MAX(streaming_timestamp) as batch_end,
    
    -- Performance metrics
    DATEDIFF('second', MIN(streaming_timestamp), MAX(streaming_timestamp)) as batch_duration_seconds
FROM TRANSACTIONS_TABLE 
WHERE data_source = 'STREAMING'
  AND streaming_timestamp IS NOT NULL
  AND streaming_timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
GROUP BY DATE_TRUNC('minute', streaming_timestamp), data_source
ORDER BY minute DESC;

-- Grant view access
GRANT SELECT ON VIEW streaming_performance_monitor TO ROLE data_engineer_role;
GRANT SELECT ON VIEW streaming_performance_monitor TO ROLE data_analyst_role;

-- =====================================================
-- 8. STREAMING CONFIGURATION TABLE
-- =====================================================

-- Create configuration table for streaming parameters
CREATE OR REPLACE TABLE streaming_config (
    config_key VARCHAR(255) PRIMARY KEY,
    config_value VARCHAR(1000),
    config_type VARCHAR(50),
    description VARCHAR(500),
    updated_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
) COMMENT = 'Configuration parameters for streaming operations';

-- Insert default streaming configurations
INSERT INTO streaming_config VALUES 
('batch_size', '1000', 'integer', 'Number of records per streaming batch', CURRENT_TIMESTAMP()),
('flush_interval_seconds', '10', 'integer', 'Seconds between batch flushes', CURRENT_TIMESTAMP()),
('max_retries', '3', 'integer', 'Maximum retry attempts for failed streams', CURRENT_TIMESTAMP()),
('enable_monitoring', 'true', 'boolean', 'Enable streaming performance monitoring', CURRENT_TIMESTAMP()),
('warehouse_size', 'X-SMALL', 'string', 'Warehouse size for streaming operations', CURRENT_TIMESTAMP());

-- =====================================================
-- 9. PERFORMANCE TESTING QUERIES
-- =====================================================

-- Test streaming performance
SELECT 
    'Streaming Performance Test' as test_name,
    COUNT(*) as total_streaming_records,
    MIN(streaming_timestamp) as first_stream,
    MAX(streaming_timestamp) as last_stream,
    DATEDIFF('second', MIN(streaming_timestamp), MAX(streaming_timestamp)) as total_duration_seconds,
    ROUND(COUNT(*) / NULLIF(DATEDIFF('second', MIN(streaming_timestamp), MAX(streaming_timestamp)), 0), 2) as avg_tps
FROM TRANSACTIONS_TABLE 
WHERE data_source = 'STREAMING'
  AND streaming_timestamp IS NOT NULL;

-- Compare streaming vs direct insert performance
SELECT 
    data_source,
    COUNT(*) as record_count,
    MIN(COALESCE(streaming_timestamp, ingestion_timestamp)) as first_record,
    MAX(COALESCE(streaming_timestamp, ingestion_timestamp)) as last_record,
    ROUND(AVG(transaction_amount), 2) as avg_amount
FROM TRANSACTIONS_TABLE 
GROUP BY data_source
ORDER BY data_source;

-- Monitor pipe status (for file-based streaming)
SELECT SYSTEM$PIPE_STATUS('fsi_streaming_pipe') as pipe_status;

-- Check warehouse utilization
SHOW WAREHOUSES LIKE 'STREAMING_WH_%';

-- =====================================================
-- 10. CLEANUP OPERATIONS FOR STREAMING
-- =====================================================

-- Clean streaming data (preserves historical)
-- DELETE FROM TRANSACTIONS_TABLE WHERE data_source = 'STREAMING';

-- Clean specific streaming batch
-- DELETE FROM TRANSACTIONS_TABLE 
-- WHERE data_source = 'STREAMING' AND batch_id = 'specific_batch_id';

-- Reset streaming infrastructure (use with caution)
-- DROP PIPE IF EXISTS fsi_streaming_pipe;
-- DROP STAGE IF EXISTS streaming_files_stage;
-- DROP WAREHOUSE IF EXISTS STREAMING_WH_XS;

-- =====================================================
-- NOTES FOR IMPLEMENTATION:
-- =====================================================
-- 1. Business Critical+ edition required for true streaming channels
-- 2. File-based Snowpipe alternative provided for Standard edition
-- 3. Dedicated warehouse optimizes streaming performance
-- 4. Monitoring views track streaming vs direct insert performance
-- 5. Configuration table allows runtime parameter tuning
-- 6. Clean separation between streaming approaches maintained
-- =====================================================
