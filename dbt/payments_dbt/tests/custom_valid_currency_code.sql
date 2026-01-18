select *
from {{ ref('fact_transactions') }} t
left join {{ ref('dim_currency') }} c
  on t.currency = c.currency_code
where c.currency_code is null
