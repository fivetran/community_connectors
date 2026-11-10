-- Composite primal values, report 2453, one row per report date and primal, in U.S. dollars per 100 pounds.
select
  slug_id,
  report_date,
  primal,
  choice_value as choice_value_usd_cwt,
  select_value as select_value_usd_cwt,
  published_at
from {{ source('usda', 'primal_values') }}
where not coalesce(_fivetran_deleted, false)
