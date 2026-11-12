-- Composite primal values, one row per report date, primal and grade (Choice, Select), in U.S. dollars per 100
-- pounds, with the report's correction flag.
with primals as (
  select * from {{ ref('stg_usda__primal_values') }}
),

reports as (
  select slug_id, report_date, is_correction from {{ ref('stg_usda__reports') }}
),

long as (
  select slug_id, report_date, primal, 'Choice' as grade, choice_value_usd_cwt as value_usd_cwt, published_at
  from primals
  union all
  select slug_id, report_date, primal, 'Select' as grade, select_value_usd_cwt as value_usd_cwt, published_at
  from primals
)

select
  long.report_date,
  long.primal,
  long.grade,
  long.value_usd_cwt,
  long.published_at,
  coalesce(reports.is_correction, false) as is_correction
from long
left join reports
  on long.slug_id = reports.slug_id
  and long.report_date = reports.report_date
