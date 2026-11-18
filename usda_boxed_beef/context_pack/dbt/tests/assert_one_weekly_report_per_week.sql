-- The weekly Prime report publishes once per report week (Monday to Sunday; a Monday holiday moves it to Tuesday).
-- cut_price_trend windows Prime by that week, so a second report date in one week, in reports or in the Prime cut
-- prices, would put two reports in a 7-day average. It runs on the staging views, so a failure keeps the marts at
-- their last passing build.
with weekly_dates as (
  select report_date from {{ ref('stg_usda__reports') }} where cadence = 'weekly'
  union
  select report_date from {{ ref('stg_usda__cut_prices') }} where cadence = 'weekly'
)

select cast(date_trunc('week', report_date) as date) as report_week, count(*) as report_dates
from weekly_dates
group by 1
having count(*) > 1
