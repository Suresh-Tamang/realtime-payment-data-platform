select 
    merchant_id,
    date(event_ts) as settlement_date,
    currency,
    count(*) as total_txns,
    sum(amount) as gross_amount,
    round(sum(amount) * 0.02, 2) as fees,
    round(sum(amount) * 0.98,2) as net_settlement
from {{ ref('stg_transactions')}}
where 
    auth_result = 'APPROVED'
    and is_fraud = false
group by 1,2,3