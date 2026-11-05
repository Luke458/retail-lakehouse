-- Does promotion move units, by family? Average units per item-day on and off promotion.
-- Only rows where promotion status is known (null before 2014-04, kept null in silver).
select
  s.week_ending,
  i.family,
  avg(case when s.on_promotion then s.unit_sales end) as units_per_item_day_promo,
  avg(case when not s.on_promotion then s.unit_sales end) as units_per_item_day_regular,
  count(case when s.on_promotion then 1 end) as promo_item_days,
  count(case when not s.on_promotion then 1 end) as regular_item_days,
  units_per_item_day_promo / nullif(units_per_item_day_regular, 0) as uplift_ratio
from {{ ref("stg_sales") }} s
join {{ ref("stg_items") }} i using (item_nbr)
where s.on_promotion is not null and {{ week_filter("s.week_ending") }}
group by all
