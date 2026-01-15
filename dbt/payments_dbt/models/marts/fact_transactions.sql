select 
    transaction_id,
    merchant_id,
    card_hash,
    amount,
    currency,
    auth_result,
    event_ts
from {{ ref('stg_transactions')}}
where auth_result = 'APPROVED'
and is_fraud = false