-- =====================================================
-- Load Historical Data into TRANSACTIONS_TABLE
-- =====================================================
-- Run this SQL to load the historical 365-day dataset

USE ROLE data_engineer_role;
USE WAREHOUSE INGESTION_WH_XS;
USE DATABASE FSI_DEMO;
USE SCHEMA RAW_DATA;

-- Upload historical data file to existing external stage
PUT file:///Users/nfernandezmartinez/Desktop/hands-on/cursor/fsi_demo/Cursor_Tests/historical_365_days.json @FSI_DEMO.RAW_DATA.STG_EXT_AWS;

-- Load historical data with proper data_source marking (TESTED AND WORKING)
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

-- Verify load success
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

-- Show sample historical records
SELECT 
    transaction_id,
    customer_id,
    transaction_date,
    transaction_amount,
    transaction_type,
    data_source
FROM TRANSACTIONS_TABLE 
WHERE data_source = 'HISTORICAL'
ORDER BY transaction_date ASC
LIMIT 5;
