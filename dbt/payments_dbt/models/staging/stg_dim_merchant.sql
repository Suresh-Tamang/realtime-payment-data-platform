select distinct
    merchant_id,
    mcc,
    location
from {{ ref('stg_fact_transactions') }}