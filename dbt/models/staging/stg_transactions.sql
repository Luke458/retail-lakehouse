select date, week_ending, store_nbr, transactions
from {{ source("silver", "transactions") }}
