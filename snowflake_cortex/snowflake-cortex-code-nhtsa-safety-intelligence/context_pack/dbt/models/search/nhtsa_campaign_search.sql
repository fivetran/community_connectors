-- One document per campaign for Cortex Search. search_text holds the words people search for (component, summary,
-- consequence, remedy), each coalesced so a recall NHTSA sent with a field missing still gets a document. makes and
-- models are sorted arrays, so a search filters on one make with @contains without splitting the campaign into one
-- document per make. Deterministic, so the search service refreshes incrementally.

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

make_arrays as (
  select
    nhtsa_campaign_number,
    {{ array_agg_sorted('make') }} as makes
  from campaign_makes
  group by nhtsa_campaign_number
),

model_arrays as (
  select
    nhtsa_campaign_number,
    {{ array_agg_sorted('make_model') }} as models
  from campaign_models
  group by nhtsa_campaign_number
)

select
  c.nhtsa_campaign_number,
  coalesce(c.component, '')
    || ' | ' || coalesce(c.summary, '')
    || ' | ' || coalesce(c.consequence, '')
    || ' | ' || coalesce(c.remedy, '') as search_text,
  mk.makes,
  ml.models,
  c.manufacturer,
  c.component_group,
  c.model_year_min,
  c.model_year_max,
  c.report_received_date,
  c.park_it,
  c.park_outside,
  c.over_the_air_update,
  c.summary,
  c.consequence,
  c.remedy
from {{ ref('nhtsa_campaign') }} as c
inner join make_arrays as mk
  on c.nhtsa_campaign_number = mk.nhtsa_campaign_number
inner join model_arrays as ml
  on c.nhtsa_campaign_number = ml.nhtsa_campaign_number
