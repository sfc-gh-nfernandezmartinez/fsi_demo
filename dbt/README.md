# FSI Demo dbt Project

Simple dbt transformations for FSI demo data.

## Project Structure
```
staging/     → TRANSFORMED schema (views)
marts/       → ANALYTICS schema (tables)
```

## Usage in Snowflake Workspaces

1. Create dbt project from this folder
2. Run in Snowflake:
```sql
-- Deploy project
EXECUTE DBT PROJECT FSI_DEMO.TRANSFORMED.fsi_demo_project

-- Run staging models (views)
EXECUTE DBT PROJECT FSI_DEMO.TRANSFORMED.fsi_demo_project --models staging.*

-- Run mart models (tables)  
EXECUTE DBT PROJECT FSI_DEMO.TRANSFORMED.fsi_demo_project --models marts.*
```

## Models

**Staging (Views in TRANSFORMED):**
- `stg_customers` - Clean customer data
- `stg_transactions` - Clean transaction data  
- `stg_mortgages` - Clean mortgage data

**Marts (Tables in ANALYTICS):**
- `customer_360` - Complete customer profile
- `transaction_summary` - Monthly transaction aggregations
- `customer_segments` - Business segmentation
