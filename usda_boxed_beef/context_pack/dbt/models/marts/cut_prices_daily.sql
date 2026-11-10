-- Every cut price row with its report's correction flag. Prime comes from the weekly report 2460, Choice and Select
-- from the daily report 2453: cadence says which. A row with has_trades false is a cut USDA listed with no trades
-- that day; its prices are null, never zero.
with cuts as (
  select * from {{ ref('stg_usda__cut_prices') }}
),

reports as (
  select slug_id, report_date, is_correction from {{ ref('stg_usda__reports') }}
)

select
  cuts.price_key,
  cuts.slug_id,
  cuts.report_date,
  cuts.cadence,
  cuts.grade,
  cuts.item_description,
  cuts.imps_code,
  cuts.fat_limitation,
  cuts.cut_name,
  cuts.cut_family,
  cuts.has_trades,
  cuts.number_trades,
  cuts.total_pounds,
  cuts.price_low_usd_cwt,
  cuts.price_high_usd_cwt,
  cuts.weighted_average_usd_cwt,
  cuts.price_low_per_lb,
  cuts.price_high_per_lb,
  cuts.price_per_lb,
  cuts.published_at,
  coalesce(reports.is_correction, false) as is_correction
from cuts
left join reports
  on cuts.slug_id = reports.slug_id
  and cuts.report_date = reports.report_date
