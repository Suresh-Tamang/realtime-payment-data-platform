select
    date,
    transaction_id,
    merchant_id,
    is_fraud,
    fraud_reason,
    rule_high_amount,
    rule_blacklist
from {{ ref('stg_transactions') }}
