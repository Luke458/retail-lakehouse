-- Favorita's calendar: a transferred holiday is not a day off on its original date;
-- its 'Transfer' row marks the day it was actually observed. 'Work Day' makes up a bridge.
select
  date,
  type as holiday_type,
  lower(locale) as scope,
  locale_name,
  description,
  transferred,
  type in ('Holiday', 'Transfer', 'Additional', 'Bridge', 'Event') and not transferred as is_observed
from {{ source("silver", "holidays_events") }}
