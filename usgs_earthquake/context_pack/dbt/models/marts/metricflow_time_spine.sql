-- MetricFlow's day spine: the Monday of start_date's week through one year past today. A range() view, so the lake
-- renderer can replay it without writing a table.

select cast(range as date) as date_day
from range(
  date_trunc('week', date '{{ var("start_date") }}'),
  timezone('UTC', now())::date + interval 1 year,
  interval 1 day
)
