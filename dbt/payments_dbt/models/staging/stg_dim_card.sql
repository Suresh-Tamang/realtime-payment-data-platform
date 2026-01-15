select distinct
    card_hash
from {{ ref('stg_transactions') }}