-- The Choice minus Select cutout spread, one row per report date, in U.S. dollars per 100 pounds of carcass.
select
  report_date,
  choice_cutout_usd_cwt,
  select_cutout_usd_cwt,
  choice_cutout_usd_cwt - select_cutout_usd_cwt as spread_usd_cwt,
  published_at
from {{ ref('stg_usda__cutout_values') }}
