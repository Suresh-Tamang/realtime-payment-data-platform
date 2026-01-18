select distinct
    merchant_id,
    location,
    mcc,
    channel
from {{ ref('stg_transactions') }}
