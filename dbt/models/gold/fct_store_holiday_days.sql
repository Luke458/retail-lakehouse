-- Every trading store-day with the holidays that applied to that store: national ones,
-- its state's regional ones, and its city's local ones. Only observed days count.
with days as (
  select distinct date, week_ending, store_nbr
  from {{ ref("stg_transactions") }}
  where {{ week_filter("week_ending") }}
),
applied as (
  select d.date, d.store_nbr, h.description, h.scope
  from days d
  join {{ ref("stg_stores") }} st using (store_nbr)
  join {{ ref("stg_holidays") }} h
    on h.date = d.date and h.is_observed
   and (h.scope = 'national'
        or (h.scope = 'regional' and h.locale_name = st.state)
        or (h.scope = 'local' and h.locale_name = st.city))
)
select
  d.week_ending,
  d.date,
  d.store_nbr,
  count(a.description) > 0 as is_holiday,
  {{ any_true("a.scope = 'national'") }} as is_national_holiday,
  {{ ordered_string_agg('a.description', '; ', 'a.description') }} as holidays
from days d
left join applied a using (date, store_nbr)
group by all
