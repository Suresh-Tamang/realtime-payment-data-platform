select
    transaction_id,
    merchant_id,
    card_hash,
    amount,
    mcc,
    channel,
    auth_result,
    location,
    ts_event::timestamp as event_ts
from public.fact_transactions