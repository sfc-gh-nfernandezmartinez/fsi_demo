-- =====================================================
-- CDC JSON Parsing Queries for FSI Demo
-- =====================================================

USE ROLE data_engineer_role;
USE DATABASE FSI_DEMO;
USE SCHEMA RAW_DATA;

-- =====================================================
-- 1. COMPREHENSIVE VIEW - All JSON Fields Parsed
-- =====================================================

CREATE OR REPLACE VIEW CDC_PARSED_COMPREHENSIVE AS
SELECT 
    -- Top-level CDC metadata
    RECORD_CONTENT:object::STRING as cdc_object_type,
    
    -- Transaction metadata  
    RECORD_CONTENT:transaction:transaction_id::NUMBER as transaction_id,
    RECORD_CONTENT:transaction:action::STRING as cdc_action,
    RECORD_CONTENT:transaction:schema::STRING as source_schema,
    RECORD_CONTENT:transaction:table::STRING as source_table,
    RECORD_CONTENT:transaction:dbuser::STRING as db_user,
    TO_TIMESTAMP(RECORD_CONTENT:transaction:committed_at::NUMBER / 1000) as committed_timestamp,
    
    -- FSI Transaction data (from record_after)
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

-- =====================================================
-- 2. FOCUSED FSI VIEW - Easy to Read for Demos
-- =====================================================

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
-- 3. SUMMARY ANALYTICS - CDC Action Distribution
-- =====================================================

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

-- =====================================================
-- 4. TRANSACTION TYPE ANALYTICS
-- =====================================================

CREATE OR REPLACE VIEW CDC_TRANSACTION_TYPES AS
SELECT 
    RECORD_CONTENT:transaction:record_after:transaction_type::STRING as transaction_type,
    RECORD_CONTENT:transaction:action::STRING as cdc_action,
    COUNT(*) as count,
    ROUND(AVG(RECORD_CONTENT:transaction:record_after:transaction_amount::NUMBER), 2) as avg_amount,
    MIN(RECORD_CONTENT:transaction:record_after:transaction_amount::NUMBER) as min_amount,
    MAX(RECORD_CONTENT:transaction:record_after:transaction_amount::NUMBER) as max_amount
FROM CDC_STREAMING_TABLE 
WHERE RECORD_CONTENT:transaction:action::STRING = 'INSERT'  -- Focus on new transactions
GROUP BY 1, 2
ORDER BY count DESC;

-- =====================================================
-- 5. SAMPLE QUERIES FOR DEMONSTRATIONS
-- =====================================================

-- Quick view of recent transactions
SELECT * FROM CDC_FSI_TRANSACTIONS ORDER BY transaction_id DESC LIMIT 10;

-- Action distribution
SELECT * FROM CDC_ACTION_SUMMARY;

-- Transaction type analysis
SELECT * FROM CDC_TRANSACTION_TYPES;

-- Real-time monitoring (streaming transactions in last hour)
SELECT 
    cdc_action,
    COUNT(*) as recent_transactions,
    ROUND(AVG(transaction_amount), 2) as avg_amount
FROM CDC_FSI_TRANSACTIONS 
WHERE committed_timestamp >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
GROUP BY cdc_action
ORDER BY recent_transactions DESC;

-- Customer activity summary
SELECT 
    customer_id,
    COUNT(*) as total_transactions,
    COUNT(DISTINCT cdc_action) as action_types,
    ROUND(SUM(CASE WHEN cdc_action = 'INSERT' THEN transaction_amount ELSE 0 END), 2) as total_new_amount,
    MAX(committed_timestamp) as last_activity
FROM CDC_FSI_TRANSACTIONS 
GROUP BY customer_id
ORDER BY total_transactions DESC
LIMIT 10;

-- Anomaly detection (high-value transactions)
SELECT 
    transaction_id,
    customer_id,
    transaction_type,
    transaction_amount,
    committed_timestamp,
    'HIGH_VALUE' as anomaly_type
FROM CDC_FSI_TRANSACTIONS 
WHERE transaction_amount > 10000  -- Anomaly threshold
ORDER BY transaction_amount DESC;
