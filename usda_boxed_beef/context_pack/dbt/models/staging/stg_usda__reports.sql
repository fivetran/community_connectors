-- One row per report and report date. cadence separates the daily report (2453) from the weekly Prime report (2460).
with source as (
  select *
  from ({{ live_rows(source('usda', 'reports')) }})
)

select
  slug_id,
  report_date,
  {{ cadence('slug_id') }} as cadence,
  report_title,
  published_at,
  coalesce(is_correction, false) as is_correction,
  narrative
from source
