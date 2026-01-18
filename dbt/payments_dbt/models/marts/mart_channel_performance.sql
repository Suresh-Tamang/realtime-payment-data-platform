select
    f.date,
    d.channel,
    count(distinct f.transaction_id) as transaction_count,
    sum(f.amount) as total_amount,
    sum(case when f.is_fraud then 1 else 0 end) as fraud_count
from {{ ref('fact_transactions') }} f
join {{ ref('dim_merchants') }} d
  on f.merchant_id = d.merchant_id
group by f.date, d.channel
