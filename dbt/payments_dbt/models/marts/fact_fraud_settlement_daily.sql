select
    merchant_id,
    date(event_ts) as fraud_date,
    currency,

    count(*) as fraud_txn_count,
    sum(amount) as fraud_amount,

    -- Useful risk metrics
    round(avg(amount), 2) as avg_fraud_amount,
    max(amount) as max_fraud_amount

from {{ ref('stg_transactions') }}
where is_fraud = true
group by 1, 2, 3
