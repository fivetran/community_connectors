-- One row: the report dates the answers come from and the sync that produced them. Every answer names this window.
-- first_report_date and last_report_date are the dates observed in the data, which sit inside the connector's
-- configured report_date_from and report_date_to and are usually narrower. The exception is a tracked campaign the
-- connector kept because NHTSA's record for it could not be read: its last readable date may fall outside a window
-- that has since changed. An absence answer ("no campaigns") holds
-- for the configured window, which this model does not carry (README, Limitations).

with campaign_window as (
  select
    min(report_received_date) as first_report_date,
    max(report_received_date) as last_report_date,
    count(*) as campaigns
  from {{ ref('nhtsa_campaign') }}
),

configured_window as (
  select
    count(*) as configured_vehicles,
    count(distinct make || ' ' || model) as configured_models,
    max(last_synced_at) as last_synced_at
  from {{ ref('nhtsa_coverage') }}
)

select
  w.first_report_date,
  w.last_report_date,
  w.campaigns,
  c.configured_vehicles,
  c.configured_models,
  c.last_synced_at
from campaign_window as w
cross join configured_window as c
