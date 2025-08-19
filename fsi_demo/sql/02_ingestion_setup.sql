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
-- 4. ICEBERG TABLE (OPTIONAL)
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
-- 5. DATA INGESTION
-- =====================================================
-- Load mortgage data
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

-- Load historical transactions
COPY INTO TRANSACTIONS_TABLE (
    transaction_id, customer_id, transaction_date,
    transaction_amount, transaction_type, data_source
) FROM (
    SELECT $1:transaction_id::NUMBER, $1:customer_id::NUMBER, $1:transaction_date::DATE,
           $1:transaction_amount::NUMBER(10,2), $1:transaction_type::VARCHAR, $1:data_source::VARCHAR
    FROM @FSI_DEMO.RAW_DATA.STG_EXT_AWS/historical_365_days.json
) FILE_FORMAT = (TYPE = 'JSON');

-- Load customer data (manual steps)
-- 1. PUT file:///path/to/customer_data.json @customer_stage;
-- 2. Run customer data generator to create file
-- 3. Load using: INSERT INTO CUSTOMER_TABLE SELECT ... FROM @customer_stage/customer_data.json
