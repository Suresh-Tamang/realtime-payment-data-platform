select
    transaction_id,
    merchant_id,
    card_hash,
    amount,
    currency,
    mcc,
    channel,
    auth_result,
    location,
    ts_event::timestamp as event_ts,

    -- fraud flags
    rule_high_amount,
    rule_blacklist,
    is_fraud,
    fraud_reason,
    processed_at

from public.transactions
