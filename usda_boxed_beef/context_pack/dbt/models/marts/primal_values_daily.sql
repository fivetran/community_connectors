-- Composite primal values, one row per report date, primal and grade (Choice, Select), in U.S. dollars per 100
-- pounds.
with primals as (
  select * from {{ ref('stg_usda__primal_values') }}
)

select report_date, primal, 'Choice' as grade, choice_value_usd_cwt as value_usd_cwt, published_at
from primals
union all
select report_date, primal, 'Select' as grade, select_value_usd_cwt as value_usd_cwt, published_at
from primals
