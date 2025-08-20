# 02_Transform

## Scope
Transform raw data into analytics-ready models with dbt running natively in Snowflake Workspaces.

## Snowflake features used
- dbt Projects on Snowflake (native). Docs: https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake
- Warehouses for transformations. Docs: https://docs.snowflake.com/en/user-guide/warehouses-overview

## What is implemented in this repo
- `dbt/dbt_project.yml` configures:
  - Staging models materialize as views in `TRANSFORMED`.
  - Mart models materialize as tables in `ANALYTICS` using a custom schema macro.
- Staging models:
  - `stg_customers.sql`, `stg_transactions.sql`, `stg_mortgages.sql`.
- Marts:
  - `customer_360.sql`, `transaction_summary.sql`, `customer_segments.sql`.
- Macro `macros/get_custom_schema.sql` to route marts to `ANALYTICS` when base schema is `TRANSFORMED`.

## How to run (native dbt)
```sql
-- Create/refresh dbt project in Workspaces and run
EXECUTE DBT PROJECT ... --models staging.*;
EXECUTE DBT PROJECT ... --models marts.*;
```

## Benefits
- Separation of concerns: `TRANSFORMED` (views) vs `ANALYTICS` (tables).
- Governance-friendly: no role switching needed; runs inside Snowflake.
- Performance: precomputed marts for fast BI queries.

## Considerations
- Keep `profiles.yml` minimal for native execution (account/user often empty in Workspaces context).
- Avoid creating new schemas inadvertently; control with schema macros.
- Incremental models can be introduced later if needed. Docs: https://docs.getdbt.com/docs/build/incremental-models
