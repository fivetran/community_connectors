-- Every price, primal, volume and cutout row belongs to a report date in reports, and every report date carries
-- every section its report publishes. A date missing a section is a sync that stopped partway, and report_coverage
-- would then name report dates the prices do not cover. It runs on the staging views, so a failure keeps the marts
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

-- The sections each report publishes on every report date, as connector/tables.py REPORTS maps them. All 729 daily
-- and 150 weekly report dates from 2024-01-02 to 2026-11-09 carry every one.
expected (slug_id, part) as (
  values
    ('2453', 'cuts: Choice'),
    ('2453', 'cuts: Select'),
    ('2453', 'cuts: Choice, Select and Ungraded'),
    ('2453', 'grind: Ground Beef'),
    ('2453', 'grind: Blended Ground Beef'),
    ('2453', 'grind: Beef Trimmings'),
    ('2453', 'primal values'),
    ('2453', 'volume'),
    ('2453', 'cutout values'),
    ('2460', 'cuts: Prime'),
    ('2460', 'volume')
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
where l.part is null
