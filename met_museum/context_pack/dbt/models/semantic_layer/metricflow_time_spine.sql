-- MetricFlow's day spine, 2015-01-01 through 2030-12-31. The Semantic Layer's time dimension is metadata_day, the
-- date the Met last updated an object's record. The earliest in European Paintings (department 11) on 2026-10-30 was
-- 2020-09-16; a department with older records needs the start moved back, or its time-based metric queries miss them.

{{ day_spine('2015-01-01', '2030-12-31') }}
