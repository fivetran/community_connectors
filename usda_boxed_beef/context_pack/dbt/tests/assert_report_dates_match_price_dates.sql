-- Every report date has a reports row and cut prices, and every price, primal, volume and cutout row belongs to a
-- report date in reports. A date on only one side is a sync that stopped partway, and an answer would then name a
-- latest report date that report_coverage does not. It runs on the staging views, so a failure keeps the marts at
-- their last passing build.
with reported as (
  select slug_id, report_date from {{ ref('stg_usda__reports') }}
),

priced as (
  select slug_id, report_date, 'cut_prices' as source_table from {{ ref('stg_usda__cut_prices') }}
  union
  select slug_id, report_date, 'grind_prices' from {{ ref('stg_usda__grind_prices') }}
  union
  select slug_id, report_date, 'primal_values' from {{ ref('stg_usda__primal_values') }}
  union
  select slug_id, report_date, 'volume' from {{ ref('stg_usda__volume') }}
  union
  select slug_id, report_date, 'cutout_values' from {{ ref('stg_usda__cutout_values') }}
)

select p.slug_id, p.report_date, p.source_table || ' row without a reports row' as problem
from priced as p
left join reported as r
  on p.slug_id = r.slug_id
  and p.report_date = r.report_date
where r.report_date is null
union all
select r.slug_id, r.report_date, 'reports row without cut prices' as problem
from reported as r
where not exists (
  select 1 from {{ ref('stg_usda__cut_prices') }} as c
  where c.slug_id = r.slug_id and c.report_date = r.report_date
)
