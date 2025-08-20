SELECT 
    customer_id,
    UPPER(TRIM(first_name)) as first_name,
    UPPER(TRIM(last_name)) as last_name,
    REGEXP_REPLACE(phone_number, '[^0-9]', '') as phone_clean,
    loan_record_id,
    CASE WHEN loan_record_id IS NOT NULL THEN TRUE ELSE FALSE END as has_mortgage,
    created_at
FROM {{ source('raw_data', 'customer_table') }}
