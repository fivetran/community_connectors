-- Loads and pounds traded per report date and volume class. Report 2453 reports choice, select, trimmings and
-- coarse (ground beef); report 2460 reports one weekly Prime total.
select
  slug_id,
  report_date,
  case slug_id when '2460' then 'weekly' else 'daily' end as cadence,
  volume_class,
  loads,
  pounds
from {{ ref('stg_usda__volume') }}
