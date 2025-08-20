# 01_Ingestion

## Scope
End-to-end ingestion used in this project: batch from S3, generated customers, and CDC-style streaming.

## Snowflake features used
- Stages, file formats, COPY INTO. See Data loading overview: https://docs.snowflake.com/en/user-guide/data-load-overview
- Apache Iceberg tables managed by Snowflake. See Apache Iceberg tables: https://docs.snowflake.com/en/user-guide/tables-iceberg
- Snowpipe Streaming (Java SDK) for low-latency ingest. See Snowpipe Streaming: https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-overview
- Python Connector for simple inserts. See Python Connector: https://docs.snowflake.com/en/developer-guide/python-connector/python-connector

## What is implemented in this repo
- `sql/02_ingestion_setup.sql` creates and loads:
  - `RAW_DATA.MORTGAGE_TABLE` from S3 (`References/Mortgage_Data.csv`).
  - `RAW_DATA.CUSTOMER_TABLE` and optional `CUSTOMER_TABLE_ICEBERG` (STRING columns for compatibility).
  - `RAW_DATA.TRANSACTIONS_TABLE` for historical and streaming.
- `streaming/` Python generators to create 5,000 customers and ~200k transactions; `stream_demo.py` is the CLI.
- `java_streaming/` CDC simulator pushing JSON CDC events (INSERT/UPDATE/DELETE).

## How to run (demo)
1) Foundation and tables
```sql
@sql/01_foundation_setup.sql;
@sql/02_ingestion_setup.sql;
```
2) Generate and/or stream
```bash
python stream_demo.py customers
python stream_demo.py historical
python stream_demo.py start      # small real-time demo
```
3) Optional production-like streaming
```bash
cd java_streaming
java -jar CDCSimulatorClient.jar SLOOW
```

## Benefits
- Clear schemas, no SELECT *, reduced data transfer.
- Flexible: demo-grade streaming (Python) and production-grade streaming (Snowpipe Streaming).
- Open table format with Iceberg.

## Considerations
- Prefer X-Small warehouses for dev/test. Warehouses: https://docs.snowflake.com/en/user-guide/warehouses-overview
- Iceberg DDL: use `STRING` types; avoid unsupported defaults.
- Ensure external stage/IAM permissions when reading from S3.
