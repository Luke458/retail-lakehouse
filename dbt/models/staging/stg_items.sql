select item_nbr, family, class as item_class, perishable = 1 as is_perishable
from {{ source("silver", "items") }}
