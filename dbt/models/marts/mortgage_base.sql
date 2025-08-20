-- Materialization: table in ANALYTICS schema via dbt_project.yml schema strategy
-- Purpose: Provide a clean, analytics-ready base table from RAW_DATA.MORTGAGE_TABLE

SELECT
    loan_record_id,
    loan_id,
    week_start_date,
    week,
    ts,
    loan_type_name,
    loan_purpose_name,
    applicant_income_000s,
    loan_amount_000s,
    county_name,
    mortgageresponse,
    created_at
FROM {{ source('raw_data', 'mortgage_table') }}
-- Explicit columns only; no SELECT *

