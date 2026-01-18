select
    merchant_id ,
    transaction_id,
    ts_event,
    card_hash,
    amount,
    currency,
    mcc,
    channel,
    auth_result,
    location,
    event_ts,
    rule_high_amount,
    rule_blacklist,
    is_fraud,
    fraud_reason,
    processed_at,
    date
from public.transactions
