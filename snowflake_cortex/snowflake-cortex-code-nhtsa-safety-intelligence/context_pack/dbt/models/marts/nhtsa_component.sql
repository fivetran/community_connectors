-- Campaigns per component group per make. A campaign that covers two makes counts once under each, so add
-- campaign_count across makes only when the question is per make; for a total, count nhtsa_campaign instead.

select
  component_group,
  make,
  cast(count(distinct nhtsa_campaign_number) as integer) as campaign_count,
  cast(count(*) as integer) as vehicle_rows
from {{ ref('nhtsa_recall') }}
group by
  component_group,
  make
