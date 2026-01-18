select
    transaction_id,
    merchant_id,
    amount,
    currency,
    event_ts,
    card_hash,
    auth_result,
    is_fraud,
    fraud_reason,
    rule_high_amount,
    rule_blacklist,
    processed_at,
    date
from {{ ref('stg_transactions') }}
