-- One row per report and report date. cadence separates the daily report (2453) from the weekly Prime report (2460).
with source as (
  select *
  from {{ source('usda', 'reports') }}
  where not coalesce(_fivetran_deleted, false)
)

select
  slug_id,
  report_date,
  case slug_id when '2460' then 'weekly' else 'daily' end as cadence,
  report_title,
  published_at,
  coalesce(is_correction, false) as is_correction,
  narrative
from source
