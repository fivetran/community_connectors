-- The estimated boxed beef cutout value, one row per report date and grade (Choice, Select). USDA bases it "on
-- negotiated prices and volume of boxed beef cuts delivered within 0-21 days and on average industry cutting yields"
-- (LM_XB403, https://www.ams.usda.gov/mnreports/ams_2453.pdf). Units: U.S. dollars per 100 pounds of carcass.
with cutout as (
  select * from {{ ref('stg_usda__cutout_values') }}
),

reports as (
  select slug_id, report_date, is_correction from {{ ref('stg_usda__reports') }}
),

long as (
  select report_date, slug_id, 'Choice' as grade, choice_cutout_usd_cwt as cutout_usd_cwt,
    choice_change_usd_cwt as change_usd_cwt, published_at
  from cutout
  union all
  select report_date, slug_id, 'Select' as grade, select_cutout_usd_cwt, select_change_usd_cwt, published_at
  from cutout
)

select
  long.report_date,
  long.grade,
  long.cutout_usd_cwt,
  long.change_usd_cwt,
  long.published_at,
  coalesce(reports.is_correction, false) as is_correction
from long
left join reports
  on long.slug_id = reports.slug_id
  and long.report_date = reports.report_date
