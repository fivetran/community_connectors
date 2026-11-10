-- Ground beef, blended ground beef and 50% lean trimmings prices, one row per report date and item, report 2453.
select
  price_key,
  report_date,
  section,
  item_description,
  has_trades,
  number_trades,
  total_pounds,
  weighted_average_usd_cwt,
  price_per_lb,
  published_at
from {{ ref('stg_usda__grind_prices') }}
