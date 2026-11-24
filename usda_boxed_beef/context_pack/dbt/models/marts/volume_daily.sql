-- Loads and pounds traded per report date and volume class, with the report's correction flag. Report 2453 reports
-- choice, select, trimmings and coarse (ground beef); report 2460 reports one weekly Prime total.
with volume as (
  select * from {{ ref('stg_usda__volume') }}
),

reports as (
  select slug_id, report_date, is_correction from {{ ref('stg_usda__reports') }}
)

select
  volume.slug_id,
  volume.report_date,
  volume.cadence,
  volume.volume_class,
  volume.loads,
  volume.pounds,
  coalesce(reports.is_correction, false) as is_correction
from volume
left join reports
  on volume.slug_id = reports.slug_id
  and volume.report_date = reports.report_date
