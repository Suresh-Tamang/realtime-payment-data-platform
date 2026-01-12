select distinct
    card_hash
from {{ ref('stg_fact_transactions') }}