-- =====================================================
-- FSI Demo - Data Ingestion Setup SQL Worksheet
-- =====================================================
-- Purpose: Complete ingestion workflow for historical and real-time data
-- Historical: Batch load from AWS S3 (JSON format) OR Python generators
-- Real-time: Snowpipe Streaming via Python/Java simulators
-- 
-- STAGE REQUIREMENTS (Simplified):
-- • STG_EXT_AWS: External stage pointing to AWS S3 fsi-demo-nfm bucket (if using S3 data)
-- • fsi_demo_stage: Auto-created by Streamlit deployment (SnowCLI managed)
-- • No manual internal stages needed - Python generators insert directly
--
-- ALTERNATIVE APPROACH: Use Python generators instead of S3 stages
-- • python stream_demo.py customers    (generates 5,000 customers)
-- • python stream_demo.py historical   (generates 200k transactions)

USE ROLE data_engineer_role;
USE WAREHOUSE INGESTION_WH_XS;
USE DATABASE FSI_DEMO;
USE SCHEMA RAW_DATA;

-- =====================================================
-- 1. MORTGAGE TABLE - FOUNDATIONAL LOAN DATA
-- =====================================================

DROP TABLE IF EXISTS MORTGAGE_TABLE;

CREATE TABLE MORTGAGE_TABLE (
    loan_record_id NUMBER(38,0) PRIMARY KEY,
    loan_id VARCHAR(255),
    week_start_date DATE NOT NULL,
    week INT NOT NULL,
    ts VARCHAR(50),
    loan_type_name VARCHAR(255) NOT NULL,
    loan_purpose_name VARCHAR(255) NOT NULL,
    applicant_income_000s NUMBER(10,2) NOT NULL,
    loan_amount_000s NUMBER(10,2) NOT NULL,
    county_name VARCHAR(255) NOT NULL,
    mortgageresponse INT NOT NULL,
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
) COMMENT = 'Mortgage data with loan_record_id as primary key';


-- =====================================================
-- 2. CUSTOMER TABLE - REGULAR TABLE
-- =====================================================

DROP TABLE IF EXISTS CUSTOMER_TABLE;

CREATE TABLE CUSTOMER_TABLE (
    customer_id NUMBER(38,0) PRIMARY KEY,
    loan_record_id NUMBER(38,0),
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL,
    phone_number VARCHAR(255) NOT NULL,
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
) COMMENT = 'Customer data with 1:1 loan_record_id mapping';


-- =====================================================
-- 3. TRANSACTIONS TABLE
-- =====================================================

DROP TABLE IF EXISTS TRANSACTIONS_TABLE;

CREATE TABLE TRANSACTIONS_TABLE (
    transaction_id NUMBER(38,0) PRIMARY KEY,
    customer_id NUMBER(38,0) NOT NULL,
    transaction_date DATE NOT NULL,
    transaction_amount NUMBER(10,2) NOT NULL,
    transaction_type VARCHAR(255) NOT NULL,
    data_source VARCHAR(50) NOT NULL DEFAULT 'HISTORICAL',
    ingestion_timestamp TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
) COMMENT = 'Transaction data with source tracking';


-- =====================================================
-- 4. ICEBERG TABLE (DATA LAKEHOUSE)
-- =====================================================

CREATE OR REPLACE ICEBERG TABLE customer_table_iceberg (
    customer_id NUMBER(38,0),
    loan_record_id NUMBER(38,0),
    first_name STRING,
    last_name STRING,
    phone_number STRING,
    created_at TIMESTAMP_LTZ
)
EXTERNAL_VOLUME = 'vol_fsi_demo'
CATALOG = 'SNOWFLAKE'
BASE_LOCATION = 'customer_data/'
COMMENT = 'Customer data in Iceberg format';

-- =====================================================
-- 5. DATA INGESTION - MORTGAGE DATA
-- =====================================================

-- Prerequisites: STG_EXT_AWS external stage must exist pointing to AWS S3 fsi-demo-nfm bucket
-- Load mortgage data from AWS S3
COPY INTO MORTGAGE_TABLE (
    week_start_date, week, loan_id, ts, loan_type_name, 
    loan_purpose_name, applicant_income_000s, loan_amount_000s, 
    county_name, mortgageresponse
) FROM (
    SELECT $1::DATE, $2::INT, $3::VARCHAR, $4::VARCHAR, $5::VARCHAR,
           $6::VARCHAR, $7::NUMBER(10,2), $8::NUMBER(10,2), $9::VARCHAR, $10::INT
    FROM @FSI_DEMO.RAW_DATA.STG_EXT_AWS/Mortgage_Data.csv
) FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1);

-- Add loan_record_id as sequential numbers
CREATE OR REPLACE TABLE MORTGAGE_TABLE_NEW AS
SELECT ROW_NUMBER() OVER (ORDER BY loan_id, week_start_date) as loan_record_id,
       loan_id, week_start_date, week, ts, loan_type_name, loan_purpose_name,
       applicant_income_000s, loan_amount_000s, county_name, mortgageresponse, created_at
FROM MORTGAGE_TABLE;
DROP TABLE MORTGAGE_TABLE;
ALTER TABLE MORTGAGE_TABLE_NEW RENAME TO MORTGAGE_TABLE;


-- =====================================================
-- 6. DATA INGESTION - HISTORICAL TRANSACTIONS  
-- =====================================================

-- Prerequisites: STG_EXT_AWS external stage must exist pointing to AWS S3 fsi-demo-nfm bucket
-- Alternative: Generate historical data using Python generators:
-- Run: python stream_demo.py historical

-- Load historical transactions from AWS S3 (if using external stage)
COPY INTO TRANSACTIONS_TABLE (
    transaction_id, customer_id, transaction_date,
    transaction_amount, transaction_type, data_source
) FROM (
    SELECT $1:transaction_id::NUMBER, $1:customer_id::NUMBER, $1:transaction_date::DATE,
           $1:transaction_amount::NUMBER(10,2), $1:transaction_type::VARCHAR, $1:data_source::VARCHAR
    FROM @FSI_DEMO.RAW_DATA.STG_EXT_AWS/historical_365_days.json
) FILE_FORMAT = (TYPE = 'JSON');


-- =====================================================
-- 7. DATA INGESTION - CUSTOMER DATA
-- =====================================================

-- Customer data is loaded via Python generators (no stages needed)
-- Run: python stream_demo.py customers
-- This generates 5,000 customers with 1:1 loan_record_id mapping
-- Direct INSERT into tables using snowflake-connector-python

-- Note: customer_table_iceberg requires external volume permissions
-- Data is inserted programmatically via streaming/customer_generator.py


-- =====================================================
-- 8. DATA VERIFICATION
-- =====================================================

-- Verify data load counts
SELECT 'MORTGAGE_TABLE' as table_name, COUNT(*) as record_count FROM MORTGAGE_TABLE
UNION ALL
SELECT 'CUSTOMER_TABLE' as table_name, COUNT(*) as record_count FROM CUSTOMER_TABLE  
UNION ALL
SELECT 'TRANSACTIONS_TABLE' as table_name, COUNT(*) as record_count FROM TRANSACTIONS_TABLE
UNION ALL
SELECT 'CUSTOMER_TABLE_ICEBERG' as table_name, COUNT(*) as record_count FROM customer_table_iceberg;

-- Verify data relationships (should show 1:1 mapping for customers 1001-5800)
SELECT 
    'Customer-Mortgage Relationship' as validation,
    COUNT(DISTINCT c.customer_id) as total_customers,
    COUNT(DISTINCT c.loan_record_id) as customers_with_mortgages,
    COUNT(DISTINCT m.loan_record_id) as total_mortgage_records
FROM CUSTOMER_TABLE c
LEFT JOIN MORTGAGE_TABLE m ON c.loan_record_id = m.loan_record_id;
