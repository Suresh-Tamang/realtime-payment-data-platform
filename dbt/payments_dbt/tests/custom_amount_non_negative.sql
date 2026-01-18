select *
from {{ ref('fact_transactions') }}
where amount < 0
