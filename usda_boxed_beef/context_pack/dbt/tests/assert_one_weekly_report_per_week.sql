-- The weekly Prime report publishes once per report week (Monday to Sunday; a Monday holiday moves it to Tuesday).
-- cut_price_trend windows Prime by that week, so a second report in one week would put two reports in a 7-day
-- average. It runs on the staging views, so a failure keeps the marts at their last passing build.
select cast(date_trunc('week', report_date) as date) as report_week, count(*) as reports
from {{ ref('stg_usda__reports') }}
where cadence = 'weekly'
group by 1
having count(*) > 1
