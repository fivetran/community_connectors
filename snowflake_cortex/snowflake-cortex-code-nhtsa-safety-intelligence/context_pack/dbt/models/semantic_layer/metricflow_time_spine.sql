-- MetricFlow's day spine, 2015-01-01 through 2030-12-31. The connector does not bound report_date_from, so a window that
-- starts before 2015 or ends after 2030 needs the bounds here widened, or its time-based metric queries miss those days.

{{ day_spine('2015-01-01', '2030-12-31') }}
