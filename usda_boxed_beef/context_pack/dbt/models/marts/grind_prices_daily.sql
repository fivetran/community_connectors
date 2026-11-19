-- Ground beef, blended ground beef and 50% lean trimmings prices, one row per report date and item, report 2453,
-- with the report's correction flag.
with grind as (
  select * from {{ ref('stg_usda__grind_prices') }}
),

reports as (
  select slug_id, report_date, is_correction from {{ ref('stg_usda__reports') }}
)

select
  grind.price_key,
  grind.report_date,
  grind.section,
  grind.item_description,
  grind.has_trades,
  grind.number_trades,
  grind.total_pounds,
  grind.weighted_average_usd_cwt,
  grind.price_per_lb,
  grind.published_at,
  coalesce(reports.is_correction, false) as is_correction
from grind
left join reports
  on grind.slug_id = reports.slug_id
  and grind.report_date = reports.report_date
