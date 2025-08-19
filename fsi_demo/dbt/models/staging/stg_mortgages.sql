SELECT 
    loan_record_id,
    loan_id,
    week_start_date,
    week,
    UPPER(TRIM(loan_type_name)) as loan_type,
    UPPER(TRIM(loan_purpose_name)) as loan_purpose,
    applicant_income_000s * 1000 as applicant_income,
    loan_amount_000s * 1000 as loan_amount,
    UPPER(TRIM(county_name)) as county,
    mortgageresponse as mortgage_approved,
    ROUND(loan_amount_000s / NULLIF(applicant_income_000s, 0), 2) as debt_to_income_ratio,
    created_at
FROM {{ source('raw_data', 'mortgage_table') }}
