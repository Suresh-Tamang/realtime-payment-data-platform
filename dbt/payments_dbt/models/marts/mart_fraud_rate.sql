
select
    f.date,
    f.merchant_id,
    count(*) as total_transactions,
    sum(case when f.is_fraud then 1 else 0 end) as fraud_transactions,
    round(100.0 * sum(case when f.is_fraud then 1 else 0 end) / count(*), 2) as fraud_rate_pct
from {{ ref('fact_fraud_signals') }} f
join {{ ref('dim_merchants') }} m
  on f.merchant_id = m.merchant_id
group by f.date, f.merchant_id
