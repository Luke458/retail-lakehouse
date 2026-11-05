select
  week_ending,
  store_nbr,
  sum(transactions) as transactions,
  count(*) as trading_days
from {{ ref("stg_transactions") }}
where {{ week_filter("week_ending") }}
group by all
