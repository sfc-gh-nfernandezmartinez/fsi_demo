# 🔄 FSI Demo - Data Pipeline

## 🎯 Overview
Complete data pipeline for FSI demo: historical batch loads, real-time streaming, data governance, and monitoring.

## 📥 Data Ingestion Strategy

### Historical Data (Batch)
```sql
-- Mortgage data from AWS S3 CSV
COPY INTO MORTGAGE_TABLE FROM @STG_EXT_AWS/Mortgage_Data.csv

-- Transaction history (200k+ records) from JSON
COPY INTO TRANSACTIONS_TABLE FROM @STG_EXT_AWS/historical_365_days.json

-- Customer data (5k customers) via internal stage
INSERT INTO CUSTOMER_TABLE FROM @customer_stage/customer_data.json
```

### Real-time Streaming (2 Options)

#### Option 1: Python Direct INSERT ✅ **RECOMMENDED FOR DEMOS**
```bash
# Start demo streaming (2 TPS for 30 seconds)
python stream_demo.py start --rate 2 --duration 30

# Monitor in real-time
snow sql --query "SELECT COUNT(*) FROM TRANSACTIONS_TABLE WHERE data_source = 'STREAMING'"

# Cleanup when done  
python stream_demo.py cleanup
```

**Benefits**: Immediate visibility, simple setup, cost-effective

#### Option 2: Java Snowpipe Streaming ✅ **PRODUCTION SCALE**
```bash
cd fsi_demo/java_streaming

# Test connection
java -jar CDCSimulatorClient.jar TEST

# High-throughput streaming (100 TPS)
java -jar CDCSimulatorClient.jar SLOOW

# Monitor CDC records
snow sql --query "SELECT COUNT(*) FROM CDC_STREAMING_TABLE"
```

**Benefits**: True production streaming, high throughput, CDC patterns

## 🛡️ Data Governance

### Source Tracking
- **data_source** column distinguishes 'HISTORICAL' vs 'STREAMING'
- **ingestion_timestamp** for lineage tracking
- **Easy cleanup** of streaming data without affecting historical

### PII Masking Policies (Strict)
```sql
CREATE OR REPLACE MASKING POLICY mask_last_name AS (val STRING) RETURNS STRING ->
  CASE WHEN UPPER(CURRENT_ROLE()) = 'DATA_STEWARD' THEN val ELSE '***' END;

CREATE OR REPLACE MASKING POLICY mask_phone_partial AS (val STRING) RETURNS STRING ->
  CASE WHEN UPPER(CURRENT_ROLE()) = 'DATA_STEWARD' THEN val ELSE CONCAT('***-***-', RIGHT(val, 4)) END;
```

### Access Control
- **data_analyst_role**: Masked PII for analytics
- **data_steward**: Only role with unmasked PII
- **data_engineer_role**: Pipeline operations

## ✅ Optional: Data Metric Functions (DMFs) & Audit

These features are optional for demos. If your account has Data Quality Monitoring enabled, you can illustrate built-in metrics and access history.

### Data Metric Functions (system DMFs)
```sql
-- Grants (serverless compute + system DMFs access)
GRANT EXECUTE DATA METRIC FUNCTION ON ACCOUNT TO ROLE SYSADMIN;
GRANT DATABASE ROLE SNOWFLAKE.DATA_METRIC_USER TO ROLE SYSADMIN;

-- Assign a completeness metric on PHONE_NUMBER
ALTER TABLE FSI_DEMO.RAW_DATA.CUSTOMER_TABLE
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_RATIO ON (PHONE_NUMBER);

-- Assign a uniqueness metric on CUSTOMER_ID
ALTER TABLE FSI_DEMO.RAW_DATA.CUSTOMER_TABLE
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.UNIQUENESS_RATIO ON (CUSTOMER_ID);

-- Set a single schedule for all DMFs on this table
ALTER TABLE FSI_DEMO.RAW_DATA.CUSTOMER_TABLE
  SET DATA METRIC SCHEDULE = 'USING CRON 0 2 * * * UTC';
```

Notes:
- DMF names and availability can vary; see docs for the latest system DMFs.
- Alternatively call DMFs ad hoc in SELECT statements without scheduling.

Reference: Snowflake Data Quality and DMFs (`https://docs.snowflake.com/en/user-guide/data-quality-intro`).

### Audit (Access/Query History)
```sql
-- ACCESS_HISTORY (requires appropriate privileges)
-- SELECT ah.EVENT_TIME, ah.USER_NAME, ah.ROLE_NAME, ah.CLIENT_TYPE,
--        obj.value:objectName::string AS OBJECT_NAME, obj.value:columns::string AS COLUMNS_ACCESSED
-- FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah,
--      LATERAL FLATTEN(input => ah.BASE_OBJECTS_ACCESSED) obj
-- WHERE ah.EVENT_TIME >= DATEADD('day', -1, CURRENT_TIMESTAMP())
--   AND obj.value:objectName::string IN ('FSI_DEMO.RAW_DATA.CUSTOMER_TABLE', 'FSI_DEMO.ANALYTICS.CUSTOMER_360')
-- ORDER BY ah.EVENT_TIME DESC;

-- QUERY_HISTORY (fallback)
-- SELECT qh.START_TIME AS EVENT_TIME, qh.USER_NAME, qh.ROLE_NAME, qh.WAREHOUSE_NAME,
--        LEFT(qh.QUERY_TEXT, 200) AS QUERY_SNIPPET
-- FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh
-- WHERE qh.START_TIME >= DATEADD('day', -1, CURRENT_TIMESTAMP())
--   AND (REGEXP_LIKE(UPPER(qh.QUERY_TEXT), '\\bFSI_DEMO\\.RAW_DATA\\.CUSTOMER_TABLE\\b')
--     OR REGEXP_LIKE(UPPER(qh.QUERY_TEXT), '\\bFSI_DEMO\\.ANALYTICS\\.CUSTOMER_360\\b'))
--   AND qh.QUERY_TYPE IN ('SELECT')
-- ORDER BY qh.START_TIME DESC;
```

## 📊 Demo Flow Examples

### Quick Demo (5 minutes)
```bash
# 1. Start streaming
python stream_demo.py start --rate 3 --duration 60

# 2. Monitor live data
snow sql --query "
SELECT data_source, COUNT(*), AVG(transaction_amount) 
FROM TRANSACTIONS_TABLE 
GROUP BY data_source"

# 3. Show customer segmentation
snow sql --query "
SELECT customer_tier, COUNT(*) 
FROM ANALYTICS.customer_360 
GROUP BY customer_tier"

# 4. Cleanup
python stream_demo.py cleanup
```

### Production Demo (15 minutes)
```bash
# 1. Java high-throughput streaming
cd fsi_demo/java_streaming
java -jar CDCSimulatorClient.jar SLOOW

# 2. Monitor CDC processing
snow sql --query "
SELECT action, COUNT(*) 
FROM CDC_STREAMING_TABLE 
GROUP BY RECORD_CONTENT:action"

# 3. Show real-time analytics
snow sql --query "
SELECT * FROM ANALYTICS.transaction_summary 
WHERE transaction_month = DATE_TRUNC('month', CURRENT_DATE())"

# 4. Cleanup
snow sql --query "TRUNCATE TABLE CDC_STREAMING_TABLE"
```

## 🔍 Monitoring Queries

### Data Health Checks
```sql
-- Verify data relationships
SELECT 
    'Total Customers' as metric, COUNT(*) as count FROM CUSTOMER_TABLE
UNION ALL
SELECT 
    'Customers with Mortgages', COUNT(*) FROM CUSTOMER_TABLE WHERE loan_record_id IS NOT NULL
UNION ALL
SELECT 
    'Total Transactions', COUNT(*) FROM TRANSACTIONS_TABLE
UNION ALL
SELECT 
    'Streaming Transactions Today', COUNT(*) 
    FROM TRANSACTIONS_TABLE 
    WHERE data_source = 'STREAMING' AND DATE(ingestion_timestamp) = CURRENT_DATE();
```

### Performance Monitoring
```sql
-- Streaming performance
SELECT 
    DATE_TRUNC('minute', ingestion_timestamp) as minute,
    COUNT(*) as transactions_per_minute,
    AVG(transaction_amount) as avg_amount
FROM TRANSACTIONS_TABLE 
WHERE data_source = 'STREAMING'
GROUP BY 1 
ORDER BY 1 DESC 
LIMIT 10;
```

## 🎯 Key Capabilities Demonstrated

1. **Real-time Streaming**: Python and Java options for different use cases
2. **Batch Processing**: Historical data loads from AWS S3
3. **Data Governance**: PII masking with role-based access
4. **Source Lineage**: Clear tracking of data origins
5. **Monitoring**: Built-in queries for pipeline health
6. **Cleanup**: Easy removal of demo data

This pipeline showcases enterprise-grade data ingestion patterns with governance and monitoring built-in from day one.
