SELECT 
    customer_id,
    transaction_month,
    transaction_type,
    data_source,
    COUNT(*) as transaction_count,
    SUM(transaction_amount) as total_amount,
    AVG(transaction_amount) as avg_amount,
    MIN(transaction_amount) as min_amount,
    MAX(transaction_amount) as max_amount,
    COUNT(CASE WHEN amount_category = 'High' THEN 1 END) as high_value_count,
    COUNT(CASE WHEN amount_category = 'Medium' THEN 1 END) as medium_value_count,
    COUNT(CASE WHEN amount_category = 'Low' THEN 1 END) as low_value_count
FROM {{ ref('stg_transactions') }}
GROUP BY 1,2,3,4
