-- =====================================================
-- CDC STREAMING SETUP - Complete Snowpipe Streaming Demo
-- =====================================================
-- Purpose: Setup CDC_STREAMING_TABLE, parsing views, and mapping to TRANSACTIONS_TABLE format
-- Usage: Java CDCSimulatorApp for true Snowpipe Streaming demonstrations

USE ROLE data_engineer_role;
USE WAREHOUSE INGESTION_WH_XS;
USE DATABASE FSI_DEMO;
USE SCHEMA RAW_DATA;

-- =====================================================
-- 1. CREATE CDC STREAMING TABLE
-- =====================================================

-- Create table optimized for Snowpipe Streaming (single VARIANT column)
DROP TABLE IF EXISTS CDC_STREAMING_TABLE;
CREATE TABLE CDC_STREAMING_TABLE (
    RECORD_CONTENT VARIANT
) COMMENT = 'FSI CDC Streaming Table - optimized for Snowpipe Streaming demos';

-- Grant permissions for CDC simulator
GRANT INSERT ON TABLE CDC_STREAMING_TABLE TO ROLE data_engineer_role;
GRANT SELECT ON TABLE CDC_STREAMING_TABLE TO ROLE data_engineer_role;

-- =====================================================
-- 2. JSON PARSING VIEWS - Extract Structured Data
-- =====================================================

-- Comprehensive view with all CDC fields parsed
CREATE OR REPLACE VIEW CDC_PARSED_COMPREHENSIVE AS
SELECT 
    -- CDC metadata
    RECORD_CONTENT:object::STRING as cdc_object_type,
    RECORD_CONTENT:transaction:transaction_id::NUMBER as transaction_id,
    RECORD_CONTENT:transaction:action::STRING as cdc_action,
    RECORD_CONTENT:transaction:schema::STRING as source_schema,
    RECORD_CONTENT:transaction:table::STRING as source_table,
    RECORD_CONTENT:transaction:dbuser::STRING as db_user,
    TO_TIMESTAMP(RECORD_CONTENT:transaction:committed_at::NUMBER / 1000) as committed_timestamp,
    
    -- FSI transaction data
    RECORD_CONTENT:transaction:record_after:customer_id::NUMBER as customer_id,
    RECORD_CONTENT:transaction:record_after:transaction_type::STRING as transaction_type,
    RECORD_CONTENT:transaction:record_after:transaction_amount::NUMBER as transaction_amount,
    RECORD_CONTENT:transaction:record_after:transaction_date::DATE as transaction_date,
    RECORD_CONTENT:transaction:record_after:data_source::STRING as data_source,
    
    -- Security fields
    RECORD_CONTENT:transaction:primaryKey_tokenized::STRING as primary_key_tokenized,
    RECORD_CONTENT:transaction:record_after:transaction_id_encrypted::STRING as transaction_id_encrypted,
    
    -- Raw JSON for reference
    RECORD_CONTENT as raw_json
FROM CDC_STREAMING_TABLE;

-- Simplified view for easy demo consumption
CREATE OR REPLACE VIEW CDC_FSI_TRANSACTIONS AS
SELECT 
    RECORD_CONTENT:transaction:transaction_id::NUMBER as transaction_id,
    RECORD_CONTENT:transaction:action::STRING as cdc_action,
    RECORD_CONTENT:transaction:record_after:customer_id::NUMBER as customer_id,
    RECORD_CONTENT:transaction:record_after:transaction_type::STRING as transaction_type,
    RECORD_CONTENT:transaction:record_after:transaction_amount::NUMBER as transaction_amount,
    RECORD_CONTENT:transaction:record_after:transaction_date::DATE as transaction_date,
    RECORD_CONTENT:transaction:dbuser::STRING as db_user,
    TO_TIMESTAMP(RECORD_CONTENT:transaction:committed_at::NUMBER / 1000) as committed_timestamp
FROM CDC_STREAMING_TABLE 
WHERE RECORD_CONTENT:transaction:action::STRING IS NOT NULL;

-- =====================================================
-- 3. TRANSACTIONS_TABLE FORMAT MAPPING
-- =====================================================

-- View that matches TRANSACTIONS_TABLE structure exactly (for easy comparison)
CREATE OR REPLACE VIEW CDC_AS_TRANSACTIONS_TABLE AS
SELECT 
    RECORD_CONTENT:transaction:transaction_id::NUMBER(38,0) as TRANSACTION_ID,
    RECORD_CONTENT:transaction:record_after:customer_id::NUMBER(38,0) as CUSTOMER_ID,
    RECORD_CONTENT:transaction:record_after:transaction_date::DATE as TRANSACTION_DATE,
    RECORD_CONTENT:transaction:record_after:transaction_amount::NUMBER(10,2) as TRANSACTION_AMOUNT,
    RECORD_CONTENT:transaction:record_after:transaction_type::VARCHAR(255) as TRANSACTION_TYPE,
    RECORD_CONTENT:transaction:record_after:data_source::VARCHAR(50) as DATA_SOURCE,
    TO_TIMESTAMP(RECORD_CONTENT:transaction:committed_at::NUMBER / 1000) as INGESTION_TIMESTAMP,
    TO_TIMESTAMP(RECORD_CONTENT:transaction:committed_at::NUMBER / 1000) as CREATED_AT
FROM CDC_STREAMING_TABLE
WHERE RECORD_CONTENT:transaction:action::STRING = 'INSERT';  -- Only new transactions

-- =====================================================
-- 4. ANALYTICS VIEWS - CDC Insights
-- =====================================================

-- CDC action distribution summary
CREATE OR REPLACE VIEW CDC_ACTION_SUMMARY AS
SELECT 
    RECORD_CONTENT:transaction:action::STRING as cdc_action,
    COUNT(*) as transaction_count,
    COUNT(DISTINCT RECORD_CONTENT:transaction:record_after:customer_id::NUMBER) as unique_customers,
    ROUND(AVG(RECORD_CONTENT:transaction:record_after:transaction_amount::NUMBER), 2) as avg_amount,
    ROUND(SUM(RECORD_CONTENT:transaction:record_after:transaction_amount::NUMBER), 2) as total_amount
FROM CDC_STREAMING_TABLE 
WHERE RECORD_CONTENT:transaction:action::STRING IS NOT NULL
GROUP BY RECORD_CONTENT:transaction:action::STRING
ORDER BY transaction_count DESC;

-- Transaction type analysis (for INSERT actions)
CREATE OR REPLACE VIEW CDC_TRANSACTION_TYPES AS
SELECT 
    RECORD_CONTENT:transaction:record_after:transaction_type::STRING as transaction_type,
    RECORD_CONTENT:transaction:action::STRING as cdc_action,
    COUNT(*) as count,
    ROUND(AVG(RECORD_CONTENT:transaction:record_after:transaction_amount::NUMBER), 2) as avg_amount,
    MIN(RECORD_CONTENT:transaction:record_after:transaction_amount::NUMBER) as min_amount,
    MAX(RECORD_CONTENT:transaction:record_after:transaction_amount::NUMBER) as max_amount
FROM CDC_STREAMING_TABLE 
WHERE RECORD_CONTENT:transaction:action::STRING = 'INSERT'
GROUP BY 1, 2
ORDER BY count DESC;

-- =====================================================
-- 5. MONITORING QUERIES - Real-time Status
-- =====================================================

-- Quick CDC table status
SELECT COUNT(*) as total_cdc_records FROM CDC_STREAMING_TABLE;

-- Recent transactions (simplified view)
SELECT * FROM CDC_FSI_TRANSACTIONS ORDER BY transaction_id DESC LIMIT 10;

-- CDC data in TRANSACTIONS_TABLE format
SELECT * FROM CDC_AS_TRANSACTIONS_TABLE ORDER BY TRANSACTION_ID DESC LIMIT 10;

-- Action distribution
SELECT * FROM CDC_ACTION_SUMMARY;

-- Transaction type breakdown
SELECT * FROM CDC_TRANSACTION_TYPES LIMIT 5;

-- =====================================================
-- 6. COMPARISON QUERIES - CDC vs Direct Streaming
-- =====================================================

-- Compare data sources between CDC and direct methods
SELECT 'CDC_STREAM' as method, COUNT(*) as count, DATA_SOURCE 
FROM CDC_AS_TRANSACTIONS_TABLE GROUP BY DATA_SOURCE
UNION ALL
SELECT 'DIRECT_INSERT' as method, COUNT(*) as count, DATA_SOURCE 
FROM TRANSACTIONS_TABLE GROUP BY DATA_SOURCE
ORDER BY method, DATA_SOURCE;

-- Real-time activity monitoring (last hour)
SELECT 
    cdc_action,
    COUNT(*) as recent_transactions,
    ROUND(AVG(transaction_amount), 2) as avg_amount
FROM CDC_FSI_TRANSACTIONS 
WHERE committed_timestamp >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
GROUP BY cdc_action
ORDER BY recent_transactions DESC;

-- Anomaly detection (high-value transactions)
SELECT 
    transaction_id,
    customer_id,
    transaction_type,
    transaction_amount,
    committed_timestamp,
    'HIGH_VALUE' as anomaly_type
FROM CDC_FSI_TRANSACTIONS 
WHERE transaction_amount > 10000
ORDER BY transaction_amount DESC;

-- =====================================================
-- 7. DEMO MANAGEMENT - Cleanup & Reset
-- =====================================================

-- Cleanup CDC data (run between demos)
-- TRUNCATE TABLE CDC_STREAMING_TABLE;

-- Verify main TRANSACTIONS_TABLE is intact
SELECT 
    data_source,
    COUNT(*) as record_count,
    COUNT(DISTINCT customer_id) as unique_customers,
    MIN(transaction_date) as earliest_date,
    MAX(transaction_date) as latest_date
FROM TRANSACTIONS_TABLE 
GROUP BY data_source;

-- =====================================================
-- USAGE NOTES
-- =====================================================

/*
💡 KEY VIEWS FOR DEMOS:
- CDC_FSI_TRANSACTIONS: Easy-to-read FSI transaction data
- CDC_AS_TRANSACTIONS_TABLE: Exact TRANSACTIONS_TABLE format
- CDC_ACTION_SUMMARY: CDC action distribution (INSERT/UPDATE/DELETE)
- CDC_TRANSACTION_TYPES: Transaction type analytics

🚀 DEMO COMMANDS:
1. Start Java simulator: java -jar CDCSimulatorClient.jar SLOOW
2. Monitor: SELECT * FROM CDC_FSI_TRANSACTIONS ORDER BY transaction_id DESC LIMIT 10;
3. Analytics: SELECT * FROM CDC_ACTION_SUMMARY;
4. Cleanup: TRUNCATE TABLE CDC_STREAMING_TABLE;

📊 COLUMN MAPPING (CDC → TRANSACTIONS_TABLE):
- transaction_id → TRANSACTION_ID
- record_after.customer_id → CUSTOMER_ID  
- record_after.transaction_date → TRANSACTION_DATE
- record_after.transaction_amount → TRANSACTION_AMOUNT
- record_after.transaction_type → TRANSACTION_TYPE
- record_after.data_source → DATA_SOURCE
- committed_at → INGESTION_TIMESTAMP & CREATED_AT
*/
