-- The Choice minus Select cutout spread, one row per report date, in U.S. dollars per 100 pounds of carcass, with
-- the report's correction flag.
with cutout as (
  select * from {{ ref('stg_usda__cutout_values') }}
),

reports as (
  select slug_id, report_date, is_correction from {{ ref('stg_usda__reports') }}
)

select
  cutout.report_date,
  cutout.choice_cutout_usd_cwt,
  cutout.select_cutout_usd_cwt,
  cutout.choice_cutout_usd_cwt - cutout.select_cutout_usd_cwt as spread_usd_cwt,
  cutout.published_at,
  coalesce(reports.is_correction, false) as is_correction
from cutout
left join reports
  on cutout.slug_id = reports.slug_id
  and cutout.report_date = reports.report_date
