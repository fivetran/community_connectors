-- One row per report: the report dates loaded and the latest published time. Every answer names the window it
-- covers from this view.
select
  slug_id,
  max(cadence) as cadence,
  max(report_title) as report_title,
  min(report_date) as first_report_date,
  max(report_date) as last_report_date,
  count(*) as report_dates_loaded,
  count(*) filter (where is_correction) as corrected_reports,
  max(published_at) as last_published_at
from {{ ref('stg_usda__reports') }}
group by slug_id
