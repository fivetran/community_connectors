-- One row per campaign. A campaign that covers several configured vehicles appears once, with the makes and
-- models it covers listed. The three flags are true when any covered vehicle row is true, false when every row is
-- false, and null when NHTSA sent no usable value on any row: null means unknown, never false.
-- Every per-campaign value is the same on every vehicle row of that campaign, so min() picks it deterministically.
-- No any_value(): Cortex Search refreshes incrementally only over deterministic queries.

with campaign_makes as (
  select distinct
    nhtsa_campaign_number,
    make
  from {{ ref('nhtsa_recall') }}
),

campaign_models as (
  select distinct
    nhtsa_campaign_number,
    make || ' ' || model as make_model
  from {{ ref('nhtsa_recall') }}
),

make_lists as (
  select
    nhtsa_campaign_number,
    {{ list_agg('make') }} as makes
  from campaign_makes
  group by nhtsa_campaign_number
),

model_lists as (
  select
    nhtsa_campaign_number,
    cast(count(*) as integer) as configured_models,
    {{ list_agg('make_model') }} as models
  from campaign_models
  group by nhtsa_campaign_number
),

campaign_rows as (
  select
    nhtsa_campaign_number,
    min(manufacturer) as manufacturer,
    min(component) as component,
    min(component_group) as component_group,
    min(report_received_date) as report_received_date,
    min(report_month) as report_month,
    min(model_year) as model_year_min,
    max(model_year) as model_year_max,
    cast(count(*) as integer) as vehicle_rows,
    max(case when park_it then 1 when not park_it then 0 end) = 1 as park_it,
    max(case when park_outside then 1 when not park_outside then 0 end) = 1 as park_outside,
    max(case when over_the_air_update then 1 when not over_the_air_update then 0 end) = 1 as over_the_air_update,
    min(summary) as summary,
    min(consequence) as consequence,
    min(remedy) as remedy,
    min(notes) as notes
  from {{ ref('nhtsa_recall') }}
  group by nhtsa_campaign_number
)

select
  r.nhtsa_campaign_number,
  r.manufacturer,
  mk.makes,
  ml.models,
  ml.configured_models,
  r.model_year_min,
  r.model_year_max,
  r.vehicle_rows,
  r.component,
  r.component_group,
  r.report_received_date,
  r.report_month,
  r.park_it,
  r.park_outside,
  r.over_the_air_update,
  r.summary,
  r.consequence,
  r.remedy,
  r.notes
from campaign_rows as r
inner join make_lists as mk
  on r.nhtsa_campaign_number = mk.nhtsa_campaign_number
inner join model_lists as ml
  on r.nhtsa_campaign_number = ml.nhtsa_campaign_number
