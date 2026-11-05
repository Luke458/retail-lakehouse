select store_nbr, city, state, type as store_type, cluster
from {{ source("silver", "stores") }}
