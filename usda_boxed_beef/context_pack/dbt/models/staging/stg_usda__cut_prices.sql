-- One row per report, report date, grade and cut. USDA reports prices in U.S. dollars per 100 pounds (cwt); the
-- _per_lb columns divide by 100. A cut with no trades that day keeps its row with null prices and has_trades false.
-- Source of the unit: LM_XB403 and LM_XB456, "Values reflect U.S. dollars per 100 pounds"
-- (https://www.ams.usda.gov/mnreports/ams_2453.pdf, https://www.ams.usda.gov/mnreports/ams_2460.pdf).
with source as (
  select *
  from ({{ live_rows(source('usda', 'cut_prices')) }})
)

select
  slug_id || '|' || cast(report_date as varchar) || '|' || grade || '|' || item_description as price_key,
  slug_id,
  report_date,
  {{ cadence('slug_id') }} as cadence,
  grade,
  item_description,
  imps_code,
  fat_limitation,
  cut_name,
  trim(split_part(cut_name, ',', 1)) as cut_family,
  coalesce(number_trades, 0) > 0 as has_trades,
  number_trades,
  total_pounds,
  price_range_low as price_low_usd_cwt,
  price_range_high as price_high_usd_cwt,
  weighted_average as weighted_average_usd_cwt,
  price_range_low / 100 as price_low_per_lb,
  price_range_high / 100 as price_high_per_lb,
  weighted_average / 100 as price_per_lb,
  published_at
from source
