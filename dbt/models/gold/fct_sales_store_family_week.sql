-- Weekly units by store and family (the department). The grain verity's gate and the marts use.
select
  s.week_ending,
  s.store_nbr,
  i.family,
  sum(s.unit_sales) as units,
  sum(case when s.on_promotion then s.unit_sales else 0 end) as units_on_promotion,
  sum(case when s.is_return then -s.unit_sales else 0 end) as units_returned,
  count(distinct s.item_nbr) as items_sold,
  count(*) as item_days
from {{ ref("stg_sales") }} s
join {{ ref("stg_items") }} i using (item_nbr)
where {{ week_filter("s.week_ending") }}
group by all
