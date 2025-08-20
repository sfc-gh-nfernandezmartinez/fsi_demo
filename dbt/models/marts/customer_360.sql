SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    c.phone_clean,
    c.has_mortgage,
    m.loan_amount,
    m.applicant_income,
    m.debt_to_income_ratio,
    m.loan_type,
    m.county,
    COUNT(t.transaction_id) as total_transactions,
    SUM(t.transaction_amount) as total_spent,
    AVG(t.transaction_amount) as avg_transaction,
    MAX(t.transaction_date) as last_transaction_date,
    COUNT(DISTINCT t.transaction_month) as active_months,
    CASE 
        WHEN SUM(t.transaction_amount) > 50000 THEN 'Premium'
        WHEN SUM(t.transaction_amount) > 15000 THEN 'Standard' 
        ELSE 'Basic' 
    END as customer_tier
FROM {{ ref('stg_customers') }} c
LEFT JOIN {{ ref('stg_mortgages') }} m ON c.loan_record_id = m.loan_record_id
LEFT JOIN {{ ref('stg_transactions') }} t ON c.customer_id = t.customer_id
GROUP BY 1,2,3,4,5,6,7,8,9,10
