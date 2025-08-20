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

### PII Masking Policies
```sql
-- Full masking for last_name
CREATE MASKING POLICY mask_last_name AS (val STRING) ->
  CASE WHEN CURRENT_ROLE() IN ('SYSADMIN', 'ACCOUNTADMIN') 
       THEN val ELSE '***' END;

-- Partial masking for phone_number  
CREATE MASKING POLICY mask_phone_partial AS (val STRING) ->
  CASE WHEN CURRENT_ROLE() IN ('SYSADMIN', 'ACCOUNTADMIN') 
       THEN val ELSE CONCAT('***-***-', RIGHT(val, 4)) END;
```

### Access Control
- **data_analyst**: Sees masked PII, can analyze patterns
- **data_steward (SYSADMIN)**: Full PII access for governance
- **data_engineer**: Pipeline management, no PII access needed

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
