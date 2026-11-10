-- Price per pound of each cut and grade on each report date it traded, with its average over the 7 and 28 days
-- ending that date. The averages use only report dates with trades: weekends, federal holidays and no-trade days are
-- absent, never counted as zero. n_reports_7d and n_reports_28d say how many report dates each average holds.
-- The weekly Prime report is windowed by report week (Monday to Sunday), so a report moved to Tuesday by a Monday
-- holiday stays in its own week: its 7-day average holds that one report and its 28-day average up to four.
with traded as (
  select
    report_date,
    -- The date each window is ordered on: the report date, or for the weekly report the Monday of its week
    case cadence when 'weekly' then cast(date_trunc('week', report_date) as date) else report_date end
      as window_date,
    cadence,
    grade,
    item_description,
    cut_name,
    cut_family,
    price_per_lb,
    is_correction
  from {{ ref('cut_prices_daily') }}
  where has_trades
    and price_per_lb is not null
),

windowed as (
  select
    *,
    avg(price_per_lb) over w7 as avg_price_per_lb_7d,
    count(*) over w7 as n_reports_7d,
    avg(price_per_lb) over w28 as avg_price_per_lb_28d,
    count(*) over w28 as n_reports_28d
  from traded
  window
    w7 as (
      partition by grade, item_description
      order by window_date
      range between interval 6 days preceding and current row
    ),
    w28 as (
      partition by grade, item_description
      order by window_date
      range between interval 27 days preceding and current row
    )
)

select
  grade || '|' || item_description || '|' || cast(report_date as varchar) as trend_key,
  report_date,
  cadence,
  grade,
  item_description,
  cut_name,
  cut_family,
  price_per_lb,
  avg_price_per_lb_7d,
  n_reports_7d,
  avg_price_per_lb_28d,
  n_reports_28d,
  round(100 * (price_per_lb / nullif(avg_price_per_lb_28d, 0) - 1), 2) as pct_vs_28d_avg,
  is_correction
from windowed
