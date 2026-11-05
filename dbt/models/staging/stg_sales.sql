select date, week_ending, store_nbr, item_nbr, unit_sales, on_promotion, is_return
from {{ source("silver", "sales") }}
