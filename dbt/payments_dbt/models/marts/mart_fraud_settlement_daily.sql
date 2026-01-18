select
    f.date,
    f.merchant_id,
    sum(f.amount) as total_amount,
    count(distinct f.transaction_id) as transaction_count,
    sum(case when f.is_fraud then 1 else 0 end) as fraud_count,
    sum(case when f.is_fraud then f.amount else 0 end) as fraud_amount
from {{ ref('fact_transactions') }} f
join {{ ref('dim_merchants') }} m
  on f.merchant_id = m.merchant_id
group by f.date, f.merchant_id
