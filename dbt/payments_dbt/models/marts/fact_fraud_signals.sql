select
    transaction_id,
    merchant_id,
    card_hash,
    event_ts,
    fraud_reason
from {{ ref('stg_transactions') }}
where is_fraud = true
