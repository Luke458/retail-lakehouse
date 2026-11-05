select
  f.week_ending,
  st.state,
  f.family,
  sum(f.units) as units,
  count(distinct f.store_nbr) as stores
from {{ ref("fct_sales_store_family_week") }} f
join {{ ref("stg_stores") }} st using (store_nbr)
where {{ week_filter("f.week_ending") }}
group by all
