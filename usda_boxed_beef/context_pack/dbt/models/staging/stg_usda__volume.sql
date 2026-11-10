-- Loads and pounds traded per report date and volume class. One load is 40,000 pounds (LM_XB403).
select
  slug_id,
  report_date,
  volume_class,
  loads,
  pounds,
  published_at
from {{ source('usda', 'volume') }}
where not coalesce(_fivetran_deleted, false)
