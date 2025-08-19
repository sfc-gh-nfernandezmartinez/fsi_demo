-- =====================================================
-- Load Customer Data into Iceberg Table  
-- =====================================================
-- Purpose: Load generated customer data into CUSTOMER_TABLE (Iceberg format)
-- Prerequisites: 
--   1. CUSTOMER_TABLE created via 02_ingestion_setup.sql
--   2. customer_data.json uploaded to AWS S3 bucket

USE ROLE data_engineer_role;
USE WAREHOUSE INGESTION_WH_XS;
USE DATABASE FSI_DEMO;
USE SCHEMA RAW_DATA;

-- =====================================================
-- 1. UPLOAD CUSTOMER DATA TO S3 STAGE
-- =====================================================

-- Upload customer data file to existing external stage
-- Run this from local machine: PUT file:///path/to/customer_data.json @FSI_DEMO.RAW_DATA.STG_EXT_AWS;
PUT file:///Users/nfernandezmartinez/Desktop/hands-on/cursor/fsi_demo/Cursor_Tests/customer_data.json @FSI_DEMO.RAW_DATA.STG_EXT_AWS;

-- =====================================================
-- 2. LOAD CUSTOMER DATA INTO ICEBERG TABLE
-- =====================================================

-- Load customer data from JSON into Iceberg table
INSERT INTO FSI_DEMO.RAW_DATA.CUSTOMER_TABLE (
    customer_id, loan_id, first_name, last_name, phone_number
)
SELECT 
    $1:customer_id::NUMBER(38,0),
    $1:loan_id::VARCHAR(255),
    $1:first_name::VARCHAR(255),
    $1:last_name::VARCHAR(255),
    $1:phone_number::VARCHAR(255)
FROM @FSI_DEMO.RAW_DATA.STG_EXT_AWS/customer_data.json
(FILE_FORMAT => (TYPE = 'JSON'));

-- =====================================================
-- 3. VERIFY CUSTOMER DATA LOADING
-- =====================================================

-- Check customer data load success
SELECT 
    COUNT(*) as total_customers,
    COUNT(DISTINCT loan_id) as unique_loans,
    COUNT(DISTINCT customer_id) as unique_customers,
    MIN(customer_id) as min_customer_id,
    MAX(customer_id) as max_customer_id
FROM CUSTOMER_TABLE;

-- Sample customer records
SELECT 
    customer_id,
    loan_id,
    first_name,
    last_name,
    phone_number,
    created_at
FROM CUSTOMER_TABLE
ORDER BY customer_id
LIMIT 10;

-- =====================================================
-- 4. VALIDATE DATA RELATIONSHIPS
-- =====================================================

-- Check if customer loan_ids have corresponding mortgage records
-- (This will show zero matches until mortgage data is loaded)
SELECT 
    'CUSTOMERS_WITH_MORTGAGES' as metric,
    COUNT(DISTINCT c.customer_id) as count
FROM CUSTOMER_TABLE c
INNER JOIN MORTGAGE_TABLE m ON c.loan_id = m.loan_id;

-- Check customer_ids that will link to transaction data
SELECT 
    'CUSTOMER_IDS_RANGE' as metric,
    CONCAT(MIN(customer_id), ' - ', MAX(customer_id)) as customer_id_range,
    COUNT(*) as total_customers
FROM CUSTOMER_TABLE;

-- =====================================================
-- 5. PERFORMANCE CHECK
-- =====================================================

-- Verify Iceberg table format and location
DESCRIBE TABLE CUSTOMER_TABLE;

-- Check Iceberg metadata (if needed)
-- SHOW ICEBERG TABLES LIKE 'CUSTOMER_TABLE';

-- =====================================================
-- NOTES
-- =====================================================

/*
🏦 Customer Data Overview:
- Customer IDs: 1001-1100 (matches transaction generator range)
- Loan IDs: 361100-361374 (should align with mortgage data)
- PII Fields: last_name, phone_number (for governance testing)
- Format: Iceberg table stored in AWS S3 'fsi-demo-nfm' bucket

📊 Data Relationships:
- customer_id → Used in TRANSACTIONS_TABLE
- loan_id → Links to MORTGAGE_TABLE  
- PII fields → Target for masking policies

🔧 Iceberg Benefits:
- Schema evolution support
- Time travel capabilities  
- Efficient metadata management
- Advanced governance features
- Better performance for analytics

⚠️  Important:
- Iceberg tables require external volume configuration
- Data physically stored in S3 but managed by Snowflake
- Supports advanced features like branching and tagging
*/
