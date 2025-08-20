# 📊 FSI Demo - Analytics & Transformations

## 🎯 Overview
dbt-powered transformation pipeline that converts raw FSI data into business-ready analytics using Snowflake native dbt.

## 🏗️ Transformation Architecture
```
RAW_DATA schema (source tables)
    ↓
TRANSFORMED schema (staging views - clean data)
    ↓  
ANALYTICS schema (mart tables - business insights)
```

## 📋 dbt Models

### Staging Models (Views in TRANSFORMED)
- **`stg_customers`** - Clean names, standardize phones, add mortgage flag
- **`stg_transactions`** - Add categories, month dimensions, amount classification
- **`stg_mortgages`** - Convert amounts, calculate debt-to-income ratios

### Mart Models (Tables in ANALYTICS)
- **`customer_360`** - Complete customer profile with transactions + mortgage data
- **`transaction_summary`** - Monthly aggregations by customer/type/source
- **`customer_segments`** - 6 business segments for targeted marketing

## 💼 Business Value

### Customer 360 View
```sql
SELECT 
    customer_id,
    first_name || ' ' || last_name as full_name,
    customer_tier,           -- Premium/Standard/Basic
    has_mortgage,
    total_spent,
    total_transactions,
    last_transaction_date,
    loan_amount,
    debt_to_income_ratio
FROM ANALYTICS.customer_360
LIMIT 10;
```

### Customer Segmentation Strategy
| Segment | Criteria | Use Case |
|---------|----------|----------|
| **High Value Mortgage** | Mortgage + >$30k spent | Premium banking products |
| **Standard Mortgage** | Mortgage + >$10k spent | Mortgage refinancing offers |
| **Basic Mortgage** | Mortgage + <$10k spent | Basic banking services |
| **High Value Non-Mortgage** | No mortgage + >$20k spent | Mortgage acquisition campaigns |
| **Standard Non-Mortgage** | No mortgage + >$5k spent | Credit card offers |
| **Basic Non-Mortgage** | No mortgage + <$5k spent | Basic account services |

### Monthly Transaction Analytics
```sql
SELECT 
    transaction_month,
    transaction_type,
    COUNT(*) as transaction_count,
    SUM(total_amount) as monthly_volume,
    AVG(avg_amount) as avg_transaction_size
FROM ANALYTICS.transaction_summary
WHERE transaction_month >= DATE_TRUNC('month', DATEADD('month', -3, CURRENT_DATE()))
ORDER BY transaction_month DESC, monthly_volume DESC;
```

## 🚀 Deployment in Snowflake Workspaces

### Setup Process
1. **Create workspace** from GitHub repository: `sfc-gh-nfernandezmartinez/fsi_demo`
2. **Navigate** to `fsi_demo/dbt/` folder in workspace
3. **Create dbt project** in Snowflake

### Deployment Commands
```sql
-- Create project from repository
CREATE OR REPLACE DBT PROJECT FSI_DEMO.TRANSFORMED.fsi_demo_project
FROM REPOSITORY 'sfc-gh-nfernandezmartinez/fsi_demo' 
PATH 'fsi_demo/dbt/';

-- Run staging models (creates views in TRANSFORMED)
EXECUTE DBT PROJECT FSI_DEMO.TRANSFORMED.fsi_demo_project --models staging.*;

-- Run mart models (creates tables in ANALYTICS)
EXECUTE DBT PROJECT FSI_DEMO.TRANSFORMED.fsi_demo_project --models marts.*;

-- Run entire project
EXECUTE DBT PROJECT FSI_DEMO.TRANSFORMED.fsi_demo_project;
```

### Validation Queries
```sql
-- Verify model creation
SHOW VIEWS IN SCHEMA FSI_DEMO.TRANSFORMED;
SHOW TABLES IN SCHEMA FSI_DEMO.ANALYTICS;

-- Check data quality
SELECT 'Customer 360' as model, COUNT(*) as records FROM ANALYTICS.customer_360
UNION ALL
SELECT 'Transaction Summary', COUNT(*) FROM ANALYTICS.transaction_summary
UNION ALL  
SELECT 'Customer Segments', COUNT(*) FROM ANALYTICS.customer_segments;
```

## 📈 Demo Analytics Queries

### Top Customer Segments
```sql
SELECT 
    customer_segment,
    customer_count,
    avg_total_spent,
    mortgage_percentage
FROM ANALYTICS.customer_segments
ORDER BY avg_total_spent DESC;
```

### Recent Transaction Trends
```sql
SELECT 
    transaction_type,
    SUM(transaction_count) as total_transactions,
    SUM(total_amount) as total_volume
FROM ANALYTICS.transaction_summary
WHERE transaction_month >= DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE()))
GROUP BY transaction_type
ORDER BY total_volume DESC;
```

### Risk Analysis
```sql
SELECT 
    CASE 
        WHEN debt_to_income_ratio > 4 THEN 'High Risk'
        WHEN debt_to_income_ratio > 2.5 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END as risk_category,
    COUNT(*) as customer_count,
    AVG(total_spent) as avg_spending,
    AVG(debt_to_income_ratio) as avg_debt_ratio
FROM ANALYTICS.customer_360
WHERE has_mortgage = TRUE
GROUP BY 1
ORDER BY avg_debt_ratio DESC;
```

## 🔄 Why dbt Native in Snowflake?

### Benefits
- **No CLI setup** - runs directly in Snowflake Workspaces
- **Git integration** - deploys automatically from repository
- **Warehouse optimization** - uses TRANSFORMATION_WH_S efficiently  
- **Simple maintenance** - managed entirely by Snowflake
- **Performance** - leverages Snowflake's compute optimization

### Model Strategy
- **Views for staging** - Always fresh, lightweight transformations
- **Tables for marts** - Pre-computed for fast analytics queries
- **Incremental potential** - Ready for incremental models as data grows

This transformation layer demonstrates how to build production-ready analytics with clear business value while maintaining simplicity and performance.
