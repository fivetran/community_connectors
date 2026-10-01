-- One row per region per day through today, zero-filled, so every region-week exists before any metric reads it.
--
-- MetricFlow's fill_nulls_with fills gaps along the time spine but not across region-by-week combinations: at event
-- grain, a region with no rows in any of the four prior weeks got a null 4-week average instead of 0, and the answer
-- dropped it. Counting from here makes those weeks real zeros. The rule itself stays in fct_seismic_events.

with days as (
  select date_day
  from {{ ref('metricflow_time_spine') }}
  where date_day <= timezone('UTC', now())::date
),

regions as (
  select region_id
  from {{ ref('dim_region') }}
),

daily_significant as (
  select
    region_id,
    event_day,
    count(*) as significant_events
  from {{ ref('fct_seismic_events') }}
  where is_significant
  group by region_id, event_day
),

final as (
  select
    regions.region_id || '|' || strftime(days.date_day, '%Y-%m-%d') as region_day_id,
    regions.region_id,
    days.date_day as activity_day,
    cast(coalesce(daily_significant.significant_events, 0) as bigint) as significant_events
  from regions
  cross join days
  left join daily_significant
    on regions.region_id = daily_significant.region_id
    and days.date_day = daily_significant.event_day
)

select * from final
