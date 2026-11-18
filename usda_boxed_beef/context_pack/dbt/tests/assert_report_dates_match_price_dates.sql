-- Every price, primal, volume and cutout row belongs to a report date in reports; every report date carries its
-- report's core cut prices (Choice for 2453, Prime for 2460); and each report's latest date carries every section
-- its report publishes. A date short of these is a sync that stopped partway, and report_coverage would then name
-- report dates the prices do not cover. Only the latest date needs every section, so older history from before a
-- section existed cannot hold the marts back for good. It runs on the staging views, so a failure keeps the marts
-- at their last passing build.
with reported as (
  select slug_id, report_date from {{ ref('stg_usda__reports') }}
),

-- Every row, labelled with the section it came from
loaded as (
  select slug_id, report_date, 'cuts: ' || grade as part from {{ ref('stg_usda__cut_prices') }}
  union
  select slug_id, report_date, 'grind: ' || section from {{ ref('stg_usda__grind_prices') }}
  union
  select slug_id, report_date, 'primal values' from {{ ref('stg_usda__primal_values') }}
  union
  select slug_id, report_date, 'volume' from {{ ref('stg_usda__volume') }}
  union
  select slug_id, report_date, 'cutout values' from {{ ref('stg_usda__cutout_values') }}
),

-- The sections each report publishes, as connector/tables.py REPORTS maps them; core marks the ones every report
-- date must carry. Every daily and weekly report date from 2024-01-02 to 2026-11-09 carries all of them.
expected (slug_id, part, core) as (
  values
    ('2453', 'cuts: Choice', true),
    ('2453', 'cuts: Select', false),
    ('2453', 'cuts: Choice, Select and Ungraded', false),
    ('2453', 'grind: Ground Beef', false),
    ('2453', 'grind: Blended Ground Beef', false),
    ('2453', 'grind: Beef Trimmings', false),
    ('2453', 'primal values', false),
    ('2453', 'volume', false),
    ('2453', 'cutout values', false),
    ('2460', 'cuts: Prime', true),
    ('2460', 'volume', false)
),

latest as (
  select slug_id, max(report_date) as report_date from reported group by slug_id
)

select l.slug_id, l.report_date, l.part || ' row without a reports row' as problem
from loaded as l
left join reported as r
  on l.slug_id = r.slug_id
  and l.report_date = r.report_date
where r.report_date is null
union all
select r.slug_id, r.report_date, 'report date without ' || e.part as problem
from reported as r
inner join expected as e
  on e.slug_id = r.slug_id
left join loaded as l
  on l.slug_id = r.slug_id
  and l.report_date = r.report_date
  and l.part = e.part
left join latest
  on latest.slug_id = r.slug_id
  and latest.report_date = r.report_date
where l.part is null
  and (e.core or latest.report_date is not null)
