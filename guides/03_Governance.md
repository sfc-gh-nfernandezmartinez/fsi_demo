# 03_Governance

## Scope
Implement strict PII masking and least-privilege access for analytics.

## Snowflake features used
- Dynamic Data Masking. Docs: https://docs.snowflake.com/en/user-guide/security-column-intro
- Tag-based masking (optional). Docs: https://docs.snowflake.com/en/user-guide/tag-based-masking
- Access history (optional). Docs: https://docs.snowflake.com/en/user-guide/access-history
- RBAC. Docs: https://docs.snowflake.com/en/user-guide/security-access-control-overview

## What is implemented in this repo
- `sql/04_governance.sql`:
  - Roles: `data_steward` (only role with unmasked PII), `data_analyst_role` (masked PII).
  - Masking policies:
    - `mask_last_name`: full mask for non-stewards (`***`).
    - `mask_phone_partial`: partial mask for non-stewards (`***-***-XXXX`).
  - Applied to `RAW_DATA.CUSTOMER_TABLE` (and Iceberg variant when supported).

## How to run
```sql
@sql/04_governance.sql;
```
Validate with role switches:
```sql
USE ROLE data_steward;   SELECT * FROM RAW_DATA.CUSTOMER_TABLE;
USE ROLE data_analyst_role; SELECT * FROM RAW_DATA.CUSTOMER_TABLE;
```

## Benefits
- Compliance-by-default: strong PII controls enforced at query time.
- Simplicity: a single steward role with unmasked privileges.
- Works with views and marts transparently.

## Considerations
- Owner role determines masking visibility; Streamlit app should be owned by a role that must see masked data.
- For Iceberg column tagging/policies, use `ALTER ICEBERG TABLE` where supported.
- Optional: Data Metric Functions (DMFs) require additional privileges. Docs: https://docs.snowflake.com/en/user-guide/data-quality-intro
