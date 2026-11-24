-- Every trend row is a report date the cut traded, and every average holds at least that date. No day without a
-- report or without trades is counted. A weekly Prime average holds one report over 7 days and at most four over 28.
-- It runs on the marts, so it guards their SQL; assert_one_weekly_report_per_week checks the synced data.
with traded as (
  select grade, item_description, report_date
  from {{ ref('cut_prices_daily') }}
  where has_trades and price_per_lb is not null
)

select t.trend_key
from {{ ref('cut_price_trend') }} as t
left join traded as d
  on t.grade = d.grade
  and t.item_description = d.item_description
  and t.report_date = d.report_date
where d.report_date is null
   or t.n_reports_7d < 1
   or t.n_reports_28d < t.n_reports_7d
   or (t.cadence = 'weekly' and (t.n_reports_7d <> 1 or t.n_reports_28d > 4))
