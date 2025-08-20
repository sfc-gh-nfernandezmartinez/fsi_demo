WITH customer_metrics AS (
    SELECT 
        customer_id,
        total_spent,
        total_transactions,
        avg_transaction,
        has_mortgage,
        customer_tier,
        CASE 
            WHEN has_mortgage AND total_spent > 30000 THEN 'High Value Mortgage'
            WHEN has_mortgage AND total_spent > 10000 THEN 'Standard Mortgage'
            WHEN has_mortgage THEN 'Basic Mortgage'
            WHEN total_spent > 20000 THEN 'High Value Non-Mortgage'
            WHEN total_spent > 5000 THEN 'Standard Non-Mortgage'
            ELSE 'Basic Non-Mortgage'
        END as customer_segment
    FROM {{ ref('customer_360') }}
)

SELECT 
    customer_segment,
    COUNT(*) as customer_count,
    AVG(total_spent) as avg_total_spent,
    AVG(total_transactions) as avg_total_transactions,
    AVG(avg_transaction) as avg_transaction_size,
    COUNT(CASE WHEN has_mortgage THEN 1 END) as mortgage_holders,
    ROUND(100.0 * COUNT(CASE WHEN has_mortgage THEN 1 END) / COUNT(*), 1) as mortgage_percentage
FROM customer_metrics
GROUP BY 1
ORDER BY avg_total_spent DESC
