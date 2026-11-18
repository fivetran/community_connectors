-- One row per campaign x configured vehicle, with the component group and report month.
-- component_group is the text before the first colon of component: NHTSA writes components as a path
-- (BACK OVER PREVENTION:SOFTWARE), so grouping on the full string splits one system into many.
-- recall_row_key is the row's grain (campaign, make, model, model year) as one column, for the Semantic Layer.

select
  nhtsa_campaign_number || '|' || make || '|' || model || '|' || cast(model_year as varchar) as recall_row_key,
  nhtsa_campaign_number,
  make,
  model,
  model_year,
  nhtsa_make,
  nhtsa_model,
  manufacturer,
  component,
  trim(split_part(component, ':', 1)) as component_group,
  summary,
  consequence,
  remedy,
  notes,
  report_received_date,
  cast(date_trunc('month', report_received_date) as date) as report_month,
  park_it,
  park_outside,
  over_the_air_update
from {{ ref('stg_nhtsa__recalls') }}
