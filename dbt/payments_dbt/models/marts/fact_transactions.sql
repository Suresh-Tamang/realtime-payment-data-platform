select 
    transaction_id,
    merchant_id,
    card_hash,
    amount,
    currency,
    auth_result,
    event_ts,
    is_fraud,
    fraud_reason
from {{ ref('stg_fact_tra')}}