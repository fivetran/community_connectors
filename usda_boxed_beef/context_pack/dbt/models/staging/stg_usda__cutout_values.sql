-- The Choice and Select cutout for 600 to 900 pound carcasses, report 2453, one row per report date, in U.S. dollars
-- per 100 pounds.
select
  slug_id,
  report_date,
  choice_cutout as choice_cutout_usd_cwt,
  select_cutout as select_cutout_usd_cwt,
  choice_change as choice_change_usd_cwt,
  select_change as select_change_usd_cwt,
  published_at
from ({{ live_rows(source('usda', 'cutout_values')) }})
