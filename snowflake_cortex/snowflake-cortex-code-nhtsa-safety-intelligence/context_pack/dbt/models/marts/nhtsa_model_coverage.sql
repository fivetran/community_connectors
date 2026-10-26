-- One row per configured make x model, across every model year configured for it. campaign_count is the distinct
-- campaigns that cover any of those years, so a campaign spanning three model years counts once. has_campaigns is
-- false when the API returned no campaign for any configured year: "no recalls in the window" as a value, never a
-- missing row. Answers "which configured models had no recalls" without summing across model years.

with configured_models as (
  select
    make,
    model,
    count(*) as configured_model_years,
    min(model_year) as model_year_min,
    max(model_year) as model_year_max,
    max(last_synced_at) as last_synced_at
  from {{ ref('stg_nhtsa__configured_vehicle') }}
  group by
    make,
    model
),

model_campaigns as (
  select
    make,
    model,
    count(distinct nhtsa_campaign_number) as campaign_count
  from {{ ref('nhtsa_recall') }}
  group by
    make,
    model
)

select
  c.make || ' ' || c.model as make_model,
  c.make,
  c.model,
  c.configured_model_years,
  c.model_year_min,
  c.model_year_max,
  coalesce(m.campaign_count, 0) as campaign_count,
  coalesce(m.campaign_count, 0) > 0 as has_campaigns,
  c.last_synced_at,
  cast(c.last_synced_at as date) as last_synced_date
from configured_models as c
left join model_campaigns as m
  on c.make = m.make
  and c.model = m.model
