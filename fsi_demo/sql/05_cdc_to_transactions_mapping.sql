-- =====================================================
-- CDC_STREAMING_TABLE to TRANSACTIONS_TABLE Mapping
-- =====================================================

USE ROLE data_engineer_role;
USE DATABASE FSI_DEMO;
USE SCHEMA RAW_DATA;

-- =====================================================
-- EXACT QUERY TO MATCH TRANSACTIONS_TABLE COLUMNS
-- =====================================================

/*
This query extracts data from CDC_STREAMING_TABLE.RECORD_CONTENT 
to match the exact structure of TRANSACTIONS_TABLE:

TRANSACTIONS_TABLE columns:
1. TRANSACTION_ID (NUMBER(38,0)) - Primary key
2. CUSTOMER_ID (NUMBER(38,0))
3. TRANSACTION_DATE (DATE)
4. TRANSACTION_AMOUNT (NUMBER(10,2))
5. TRANSACTION_TYPE (VARCHAR(255))
6. DATA_SOURCE (VARCHAR(50))
7. INGESTION_TIMESTAMP (TIMESTAMP_LTZ(9))
8. CREATED_AT (TIMESTAMP_LTZ(9))
*/

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
WHERE RECORD_CONTENT:transaction:action::STRING = 'INSERT'  -- Only INSERT actions (new transactions)
ORDER BY TRANSACTION_ID DESC;

-- =====================================================
-- VIEW FOR EASY ACCESS
-- =====================================================

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
WHERE RECORD_CONTENT:transaction:action::STRING = 'INSERT';

-- =====================================================
-- USAGE EXAMPLES
-- =====================================================

-- View CDC data in TRANSACTIONS_TABLE format
SELECT * FROM CDC_AS_TRANSACTIONS_TABLE ORDER BY TRANSACTION_ID DESC LIMIT 10;

-- Compare with actual TRANSACTIONS_TABLE
SELECT 'CDC_DATA' as source, COUNT(*) as count, DATA_SOURCE 
FROM CDC_AS_TRANSACTIONS_TABLE GROUP BY DATA_SOURCE
UNION ALL
SELECT 'DIRECT_DATA' as source, COUNT(*) as count, DATA_SOURCE 
FROM TRANSACTIONS_TABLE GROUP BY DATA_SOURCE
ORDER BY source, DATA_SOURCE;

-- Unified view (combine both tables)
SELECT * FROM (
    SELECT *, 'CDC_STREAM' as ingestion_method FROM CDC_AS_TRANSACTIONS_TABLE
    UNION ALL
    SELECT *, 'DIRECT_INSERT' as ingestion_method FROM TRANSACTIONS_TABLE WHERE DATA_SOURCE = 'STREAMING'
) 
ORDER BY TRANSACTION_ID DESC;

-- =====================================================
-- NOTES ON MAPPING
-- =====================================================

/*
Key Mappings:
- TRANSACTION_ID: From CDC transaction.transaction_id
- CUSTOMER_ID: From CDC record_after.customer_id  
- TRANSACTION_DATE: From CDC record_after.transaction_date
- TRANSACTION_AMOUNT: From CDC record_after.transaction_amount
- TRANSACTION_TYPE: From CDC record_after.transaction_type
- DATA_SOURCE: From CDC record_after.data_source
- INGESTION_TIMESTAMP: From CDC transaction.committed_at (converted from epoch)
- CREATED_AT: From CDC transaction.committed_at (same as ingestion_timestamp)

Filtering:
- Only 'INSERT' actions are included to match new transaction records
- 'UPDATE' and 'DELETE' actions are excluded as they don't represent new transactions

Usage:
- Use CDC_AS_TRANSACTIONS_TABLE view for easy access
- Perfect for joining with TRANSACTIONS_TABLE or replacing it in queries
- Maintains exact column compatibility for analytics and reporting
*/
