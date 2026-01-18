select distinct
    currency as currency_code
from {{ ref('stg_transactions') }}
