-- Loads and pounds traded per report date and volume class. One load is 40,000 pounds (LM_XB403).
select
  slug_id,
  report_date,
  {{ cadence('slug_id') }} as cadence,
  volume_class,
  loads,
  pounds,
  published_at
from ({{ live_rows(source('usda', 'volume')) }})
