-- Every week served in gold carries exactly silver's units for that week:
-- nothing lost in a join, nothing double counted. Weeks the gate has not
-- approved are absent from gold and not compared.
with gold as (
  select week_ending, sum(units) as units
  from {{ ref("fct_sales_store_family_week") }}
  group by 1
),
silver as (
  select week_ending, sum(unit_sales) as units
  from {{ ref("stg_sales") }}
  where week_ending in (select week_ending from gold)
  group by 1
)
select week_ending, gold.units as gold_units, silver.units as silver_units
from gold
full join silver using (week_ending)
where gold.units is null or silver.units is null or abs(gold.units - silver.units) > 0.001
