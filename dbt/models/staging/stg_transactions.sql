SELECT 
    transaction_id,
    customer_id,
    transaction_date,
    transaction_amount,
    UPPER(TRIM(transaction_type)) as transaction_type,
    data_source,
    DATE_TRUNC('month', transaction_date) as transaction_month,
    CASE 
        WHEN transaction_amount > 5000 THEN 'High'
        WHEN transaction_amount > 1000 THEN 'Medium' 
        ELSE 'Low' 
    END as amount_category,
    ingestion_timestamp,
    created_at
FROM {{ source('raw_data', 'transactions_table') }}
