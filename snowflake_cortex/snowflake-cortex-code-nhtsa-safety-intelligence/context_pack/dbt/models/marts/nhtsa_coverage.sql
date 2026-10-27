-- One row per configured make x model x model year, from the connector's configured_vehicle table.
-- campaign_count is 0 when the API returned no campaign in the window for that vehicle, so "no recalls" is a
-- value here, not a missing row. A vehicle that was never configured has no row: its recall history is unknown.
-- campaigns_returned is the connector's count of the campaign rows it kept for the same vehicle (inside the report-date
-- window, after its own filtering, plus any tracked campaign kept because its result was skipped), not what the API
-- returned. It is kept for reconciliation.

with configured as (
  select * from {{ ref('stg_nhtsa__configured_vehicle') }}
),

vehicle_campaigns as (
  select
    make,
    model,
    model_year,
    count(distinct nhtsa_campaign_number) as campaign_count
  from {{ ref('nhtsa_recall') }}
  group by
    make,
    model,
    model_year
)

select
  c.make,
  c.model,
  c.model_year,
  c.nhtsa_model,
  coalesce(v.campaign_count, 0) as campaign_count,
  coalesce(v.campaign_count, 0) > 0 as has_campaigns,
  c.campaigns_returned,
  c.last_synced_at
from configured as c
left join vehicle_campaigns as v
  on c.make = v.make
  and c.model = v.model
  and c.model_year = v.model_year
