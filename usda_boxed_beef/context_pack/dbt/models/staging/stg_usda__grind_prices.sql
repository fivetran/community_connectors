-- Ground beef, blended ground beef and 50% lean trimmings, report 2453, one row per report date and item. Prices are
-- U.S. dollars per 100 pounds as reported, with _per_lb columns beside them.
with ground_beef as (
  select slug_id, report_date, section, item_description, number_trades, total_pounds,
    price_range_low, price_range_high, weighted_average, published_at
  from {{ source('usda', 'ground_beef_prices') }}
  where not coalesce(_fivetran_deleted, false)
),

trimmings as (
  select slug_id, report_date, 'Beef Trimmings' as section, item_description, number_trades, total_pounds,
    price_range_low, price_range_high, weighted_average, published_at
  from {{ source('usda', 'trimmings_prices') }}
  where not coalesce(_fivetran_deleted, false)
),

unioned as (
  select * from ground_beef
  union all
  select * from trimmings
)

select
  slug_id || '|' || cast(report_date as varchar) || '|' || item_description as price_key,
  slug_id,
  report_date,
  section,
  item_description,
  coalesce(number_trades, 0) > 0 as has_trades,
  number_trades,
  total_pounds,
  price_range_low as price_low_usd_cwt,
  price_range_high as price_high_usd_cwt,
  weighted_average as weighted_average_usd_cwt,
  weighted_average / 100 as price_per_lb,
  published_at
from unioned
