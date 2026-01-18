select distinct
    auth_result
from {{ ref('stg_transactions') }}
