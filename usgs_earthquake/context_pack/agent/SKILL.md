# USGS earthquakes: questions, queries and gotchas

Every query goes through the DuckDB query tool, in a session that has the lake catalog attached as `<lake_catalog_alias>`. The raw table is `<lake_catalog_alias>.<lake_schema>.earthquake`. Never attach or detach the lake yourself.

## Canonical question: regions above their prior 4-week average

Question: which regions had more M4.5+ earthquakes last week than their prior 4-week average?

Use it for rephrasings too: which regions are busier than usual, which regions are above their recent average, where is M4.5+ activity up.

Pass the string below to the query tool, whole and unchanged, in one call. Copy it character for character, including the `-- render:` comment on its first line. Do not reformat, shorten or split it. If the call errors, report the error. Leave the row cap at its default: the result has at most one row per region (the seed has 20), so it always fits.

```sql
-- render: target=lake week=last-complete start_date=2026-08-03 updated_at_cutoff=none single_row=false seed_sha256=e5da23b9eaad589c7562b6c08a75949634e409af8c6d377852065bef659f687e semantic_sha256=6ac42c322011a92ad2b62c89826d28a0fb667c5dd91f0dc3ddd2d74f589891b3 models_sha256=1692df0e720ce69114930935997ce1daca8c9091d700cb6ed2f8969b9c80c576 renderer_sha256=b6055a47c3b3b2fe80854560afbe722c1423dbb1b1aa63c9afbbe3e377f79ec0
attach if not exists ':memory:' as usgs_local;

create schema if not exists "usgs_local"."usgs";

create or replace table "usgs_local"."usgs"."region_bounds" as
select
  cast(region_id as varchar) as region_id,
  cast(name as varchar) as name,
  cast(lat_min as double) as lat_min,
  cast(lat_max as double) as lat_max,
  cast(lon_min as double) as lon_min,
  cast(lon_max as double) as lon_max,
  cast(priority as integer) as priority
from (values
  ('yellowstone', 'Yellowstone', '44.0', '45.2', '-111.2', '-109.8', '50'),
  ('hawaii', 'Hawaii', '18.0', '23.0', '-161.0', '-154.0', '40'),
  ('pacific_northwest', 'Pacific Northwest', '42.0', '50.0', '-128.0', '-116.0', '30'),
  ('california_nevada', 'California and Nevada', '32.0', '42.0', '-126.0', '-114.0', '30'),
  ('new_zealand', 'New Zealand', '-48.0', '-34.0', '165.0', '179.0', '25'),
  ('melanesia', 'Papua New Guinea, Solomon Islands, Vanuatu and New Caledonia', '-25.0', '0.0', '142.0', '172.0', '25'),
  ('mariana_islands', 'Mariana Islands and Guam', '11.0', '23.0', '142.0', '150.0', '25'),
  ('alaska_aleutians', 'Alaska and the Aleutians', '50.0', '72.0', '170.0', '-129.0', '20'),
  ('intermountain_west', 'Intermountain West', '31.0', '49.0', '-116.0', '-102.0', '20'),
  ('caribbean', 'Caribbean', '10.0', '24.0', '-86.0', '-59.0', '20'),
  ('japan_kuril_kamchatka', 'Japan, Kuril Islands and Kamchatka', '24.0', '60.0', '122.0', '165.0', '20'),
  ('indonesia_philippines', 'Indonesia and the Philippines', '-11.0', '21.0', '94.0', '142.0', '20'),
  ('tonga_fiji_kermadec', 'Tonga, Fiji and Kermadec', '-40.0', '-10.0', '172.0', '-170.0', '20'),
  ('south_sandwich_scotia', 'South Sandwich Islands and Scotia Sea', '-62.0', '-53.0', '-50.0', '-20.0', '20'),
  ('mexico_central_america', 'Mexico and Central America', '7.0', '32.0', '-118.0', '-77.0', '15'),
  ('south_america', 'South America', '-56.0', '13.0', '-82.0', '-34.0', '15'),
  ('mediterranean_middle_east', 'Mediterranean and Middle East', '11.0', '46.0', '-10.0', '60.0', '15'),
  ('central_south_asia', 'Central and South Asia', '20.0', '50.0', '60.0', '122.0', '15'),
  ('central_eastern_us', 'Central and Eastern US', '24.0', '50.0', '-102.0', '-65.0', '10'),
  ('other', 'Other / open ocean', null, null, null, null, '0')
) t(region_id, name, lat_min, lat_max, lon_min, lon_max, priority);

create or replace view "usgs_local"."usgs"."dim_region" as (
-- One row per region in the human-owned seed, bounds included, so fct_seismic_events assigns events from here.

select
  region_id,
  name,
  lat_min,
  lat_max,
  lon_min,
  lon_max,
  priority
from "usgs_local"."usgs"."region_bounds"
);

create or replace view "usgs_local"."usgs"."stg_usgs__earthquakes" as (
-- One row per live USGS event, with times in UTC.
--
-- Deleted events (_fivetran_deleted = true) are dropped here, so every model downstream sees live events only.
--
-- event_time and updated_at land as timestamptz. timezone('UTC', ...) turns each one into a UTC timestamp without a
-- time zone. A cast would read the session's time zone instead: the local DuckDB session runs in the machine's time
-- zone, and a bare cast to date would move an event at 02:00 UTC on a Monday into the previous week.
--
-- The updated_at_cutoff block is only for comparing two copies of the table (the local warehouse and the lake). It
-- keeps the rows each copy last saw updated at or before one instant. A delete or preferred-id change that one copy
-- synced and the other did not keeps the row's old updated_at, so the cutoff does not hide that difference. Pass the
-- var as ISO 8601 with an explicit offset (Z or +00:00), because a string cast to timestamptz without an offset is
-- read in the session's time zone.
--
-- Every event type and every magnitude is kept, including null and negative magnitudes. The significant-event rule
-- lives in fct_seismic_events.is_significant, not here.

with source as (
  select *
  from "<lake_catalog_alias>"."<lake_schema>"."earthquake"
  where not coalesce(_fivetran_deleted, false)
  
),

renamed as (
  select
    id as event_id,
    timezone('UTC', event_time) as event_time_utc,
    timezone('UTC', updated_at) as updated_at_utc,
    mag,
    mag_type,
    event_type,
    review_status,
    latitude,
    longitude,
    depth_km,
    place,
    network,
    alert,
    tsunami,
    sig,
    felt,
    ids
  from source
)

select * from renamed
);

create or replace view "usgs_local"."usgs"."fct_seismic_events" as (
-- One row per live event, each assigned to exactly one region.
--
-- An event matches every box that contains it and keeps the one with the highest priority, so overlapping boxes are
-- deterministic. A box with lon_min > lon_max crosses the antimeridian. The "other" row has null bounds and matches
-- every event, at priority 0, so no event goes without a region.
--
-- is_significant is the one place the M4.5+ earthquake rule lives. fct_region_days sums it.

with events as (
  select
    event_id,
    event_time_utc,
    mag,
    event_type,
    latitude,
    longitude
  from "usgs_local"."usgs"."stg_usgs__earthquakes"
),

regions as (
  select
    region_id,
    lat_min,
    lat_max,
    lon_min,
    lon_max,
    priority
  from "usgs_local"."usgs"."dim_region"
),

candidate_regions as (
  select
    events.event_id,
    regions.region_id,
    regions.priority
  from events
  inner join regions
    on regions.lat_min is null
    or (
      events.latitude between regions.lat_min and regions.lat_max
      and case
        when regions.lon_min <= regions.lon_max
          then events.longitude between regions.lon_min and regions.lon_max
        else events.longitude >= regions.lon_min or events.longitude <= regions.lon_max
      end
    )
),

assigned_region as (
  select
    event_id,
    region_id
  from candidate_regions
  qualify row_number() over (partition by event_id order by priority desc, region_id) = 1
),

final as (
  select
    events.event_id,
    assigned_region.region_id,
    events.event_time_utc as event_time,
    cast(events.event_time_utc as date) as event_day,
    events.mag,
    events.event_type,
    coalesce(events.mag >= 4.5 and events.event_type = 'earthquake', false) as is_significant
  from events
  inner join assigned_region
    on events.event_id = assigned_region.event_id
)

select * from final
);

create or replace view "usgs_local"."usgs"."metricflow_time_spine" as (
-- MetricFlow's day spine: the Monday of start_date's week through one year past today. A range() view, so the lake
-- renderer can replay it without writing a table.

select cast(range as date) as date_day
from range(
  date_trunc('week', date '2026-08-03'),
  timezone('UTC', now())::date + interval 1 year,
  interval 1 day
)
);

create or replace view "usgs_local"."usgs"."fct_region_days" as (
-- One row per region per day through today, zero-filled, so every region-week exists before any metric reads it.
--
-- MetricFlow's fill_nulls_with fills gaps along the time spine but not across region-by-week combinations: at event
-- grain, a region with no rows in any of the four prior weeks got a null 4-week average instead of 0, and the answer
-- dropped it. Counting from here makes those weeks real zeros. The rule itself stays in fct_seismic_events.

with days as (
  select date_day
  from "usgs_local"."usgs"."metricflow_time_spine"
  where date_day <= timezone('UTC', now())::date
),

regions as (
  select region_id
  from "usgs_local"."usgs"."dim_region"
),

daily_significant as (
  select
    region_id,
    event_day,
    count(*) as significant_events
  from "usgs_local"."usgs"."fct_seismic_events"
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
);

select region__name, significant_event_count, significant_events_4wk_avg
from (
-- Combine Aggregated Outputs
-- Write to DataTable
WITH sma_10001_cte AS (
  -- Read Elements From Semantic Model 'region_days'
  -- Metric Time Dimension 'activity_day'
  SELECT
    DATE_TRUNC('week', activity_day) AS metric_time__week
    , region_id AS region
    , significant_events AS __significant_event_count
  FROM "usgs_local"."usgs"."fct_region_days" region_days_src_10000
)

, rss_10002_cte AS (
  -- Read Elements From Semantic Model 'regions'
  SELECT
    name
    , region_id AS region
  FROM "usgs_local"."usgs"."dim_region" regions_src_10000
)

, rss_10000_cte AS (
  -- Read From Time Spine '"metricflow_time_spine"'
  SELECT
    DATE_TRUNC('week', date_day) AS date_day__week
  FROM "usgs_local"."usgs"."metricflow_time_spine" time_spine_src_10000
)

, cm_7_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    metric_time__week
    , region__name
    , COALESCE(__significant_event_count, 0) AS significant_event_count
  FROM (
    -- Join to Time Spine Dataset
    SELECT
      subq_13.metric_time__week AS metric_time__week
      , subq_8.region__name AS region__name
      , subq_8.__significant_event_count AS __significant_event_count
    FROM (
      -- Constrain Output with WHERE
      -- Select: ['metric_time__week']
      SELECT
        metric_time__week
      FROM (
        -- Read From CTE For node_id=rss_10000
        -- Change Column Aliases
        -- Select: ['metric_time__week']
        SELECT
          date_day__week AS metric_time__week
        FROM rss_10000_cte
      ) subq_11
      WHERE metric_time__week = date_trunc('week', timezone('UTC', now())) - interval 7 day
      GROUP BY
        metric_time__week
    ) subq_13
    LEFT OUTER JOIN (
      -- Constrain Output with WHERE
      -- Select: ['__significant_event_count', 'region__name', 'metric_time__week']
      -- Aggregate Inputs for Simple Metrics
      SELECT
        metric_time__week
        , region__name
        , SUM(significant_event_count) AS __significant_event_count
      FROM (
        -- Join Standard Outputs
        -- Select: ['__significant_event_count', 'region__name', 'metric_time__week']
        SELECT
          sma_10001_cte.metric_time__week AS metric_time__week
          , rss_10002_cte.name AS region__name
          , sma_10001_cte.__significant_event_count AS significant_event_count
        FROM sma_10001_cte
        LEFT OUTER JOIN
          rss_10002_cte
        ON
          sma_10001_cte.region = rss_10002_cte.region
      ) subq_5
      WHERE metric_time__week = date_trunc('week', timezone('UTC', now())) - interval 7 day
      GROUP BY
        metric_time__week
        , region__name
    ) subq_8
    ON
      subq_13.metric_time__week = subq_8.metric_time__week
  ) subq_14
)

, cm_12_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    metric_time__week
    , region__name
    , (w1 + w2 + w3 + w4) / 4.0 AS significant_events_4wk_avg
  FROM (
    -- Combine Aggregated Outputs
    SELECT
      COALESCE(subq_29.metric_time__week, subq_43.metric_time__week, subq_57.metric_time__week, subq_71.metric_time__week) AS metric_time__week
      , COALESCE(subq_29.region__name, subq_43.region__name, subq_57.region__name, subq_71.region__name) AS region__name
      , COALESCE(MAX(subq_29.w1), 0) AS w1
      , COALESCE(MAX(subq_43.w2), 0) AS w2
      , COALESCE(MAX(subq_57.w3), 0) AS w3
      , COALESCE(MAX(subq_71.w4), 0) AS w4
    FROM (
      -- Compute Metrics via Expressions
      SELECT
        metric_time__week
        , region__name
        , COALESCE(__significant_event_count, 0) AS w1
      FROM (
        -- Join to Time Spine Dataset
        SELECT
          subq_27.metric_time__week AS metric_time__week
          , subq_22.region__name AS region__name
          , subq_22.__significant_event_count AS __significant_event_count
        FROM (
          -- Constrain Output with WHERE
          -- Select: ['metric_time__week']
          SELECT
            metric_time__week
          FROM (
            -- Read From CTE For node_id=rss_10000
            -- Change Column Aliases
            -- Select: ['metric_time__week']
            SELECT
              date_day__week AS metric_time__week
            FROM rss_10000_cte
          ) subq_25
          WHERE metric_time__week = date_trunc('week', timezone('UTC', now())) - interval 7 day
          GROUP BY
            metric_time__week
        ) subq_27
        LEFT OUTER JOIN (
          -- Join Standard Outputs
          -- Select: ['__significant_event_count', 'region__name', 'metric_time__week']
          -- Select: ['__significant_event_count', 'region__name', 'metric_time__week']
          -- Aggregate Inputs for Simple Metrics
          SELECT
            sma_10001_cte.metric_time__week AS metric_time__week
            , rss_10002_cte.name AS region__name
            , SUM(sma_10001_cte.__significant_event_count) AS __significant_event_count
          FROM sma_10001_cte
          LEFT OUTER JOIN
            rss_10002_cte
          ON
            sma_10001_cte.region = rss_10002_cte.region
          GROUP BY
            sma_10001_cte.metric_time__week
            , rss_10002_cte.name
        ) subq_22
        ON
          subq_27.metric_time__week - INTERVAL 1 week = subq_22.metric_time__week
      ) subq_28
    ) subq_29
    FULL OUTER JOIN (
      -- Compute Metrics via Expressions
      SELECT
        metric_time__week
        , region__name
        , COALESCE(__significant_event_count, 0) AS w2
      FROM (
        -- Join to Time Spine Dataset
        SELECT
          subq_41.metric_time__week AS metric_time__week
          , subq_36.region__name AS region__name
          , subq_36.__significant_event_count AS __significant_event_count
        FROM (
          -- Constrain Output with WHERE
          -- Select: ['metric_time__week']
          SELECT
            metric_time__week
          FROM (
            -- Read From CTE For node_id=rss_10000
            -- Change Column Aliases
            -- Select: ['metric_time__week']
            SELECT
              date_day__week AS metric_time__week
            FROM rss_10000_cte
          ) subq_39
          WHERE metric_time__week = date_trunc('week', timezone('UTC', now())) - interval 7 day
          GROUP BY
            metric_time__week
        ) subq_41
        LEFT OUTER JOIN (
          -- Join Standard Outputs
          -- Select: ['__significant_event_count', 'region__name', 'metric_time__week']
          -- Select: ['__significant_event_count', 'region__name', 'metric_time__week']
          -- Aggregate Inputs for Simple Metrics
          SELECT
            sma_10001_cte.metric_time__week AS metric_time__week
            , rss_10002_cte.name AS region__name
            , SUM(sma_10001_cte.__significant_event_count) AS __significant_event_count
          FROM sma_10001_cte
          LEFT OUTER JOIN
            rss_10002_cte
          ON
            sma_10001_cte.region = rss_10002_cte.region
          GROUP BY
            sma_10001_cte.metric_time__week
            , rss_10002_cte.name
        ) subq_36
        ON
          subq_41.metric_time__week - INTERVAL 2 week = subq_36.metric_time__week
      ) subq_42
    ) subq_43
    ON
      (
        subq_29.region__name = subq_43.region__name
      ) AND (
        subq_29.metric_time__week = subq_43.metric_time__week
      )
    FULL OUTER JOIN (
      -- Compute Metrics via Expressions
      SELECT
        metric_time__week
        , region__name
        , COALESCE(__significant_event_count, 0) AS w3
      FROM (
        -- Join to Time Spine Dataset
        SELECT
          subq_55.metric_time__week AS metric_time__week
          , subq_50.region__name AS region__name
          , subq_50.__significant_event_count AS __significant_event_count
        FROM (
          -- Constrain Output with WHERE
          -- Select: ['metric_time__week']
          SELECT
            metric_time__week
          FROM (
            -- Read From CTE For node_id=rss_10000
            -- Change Column Aliases
            -- Select: ['metric_time__week']
            SELECT
              date_day__week AS metric_time__week
            FROM rss_10000_cte
          ) subq_53
          WHERE metric_time__week = date_trunc('week', timezone('UTC', now())) - interval 7 day
          GROUP BY
            metric_time__week
        ) subq_55
        LEFT OUTER JOIN (
          -- Join Standard Outputs
          -- Select: ['__significant_event_count', 'region__name', 'metric_time__week']
          -- Select: ['__significant_event_count', 'region__name', 'metric_time__week']
          -- Aggregate Inputs for Simple Metrics
          SELECT
            sma_10001_cte.metric_time__week AS metric_time__week
            , rss_10002_cte.name AS region__name
            , SUM(sma_10001_cte.__significant_event_count) AS __significant_event_count
          FROM sma_10001_cte
          LEFT OUTER JOIN
            rss_10002_cte
          ON
            sma_10001_cte.region = rss_10002_cte.region
          GROUP BY
            sma_10001_cte.metric_time__week
            , rss_10002_cte.name
        ) subq_50
        ON
          subq_55.metric_time__week - INTERVAL 3 week = subq_50.metric_time__week
      ) subq_56
    ) subq_57
    ON
      (
        COALESCE(subq_29.region__name, subq_43.region__name) = subq_57.region__name
      ) AND (
        COALESCE(subq_29.metric_time__week, subq_43.metric_time__week) = subq_57.metric_time__week
      )
    FULL OUTER JOIN (
      -- Compute Metrics via Expressions
      SELECT
        metric_time__week
        , region__name
        , COALESCE(__significant_event_count, 0) AS w4
      FROM (
        -- Join to Time Spine Dataset
        SELECT
          subq_69.metric_time__week AS metric_time__week
          , subq_64.region__name AS region__name
          , subq_64.__significant_event_count AS __significant_event_count
        FROM (
          -- Constrain Output with WHERE
          -- Select: ['metric_time__week']
          SELECT
            metric_time__week
          FROM (
            -- Read From CTE For node_id=rss_10000
            -- Change Column Aliases
            -- Select: ['metric_time__week']
            SELECT
              date_day__week AS metric_time__week
            FROM rss_10000_cte
          ) subq_67
          WHERE metric_time__week = date_trunc('week', timezone('UTC', now())) - interval 7 day
          GROUP BY
            metric_time__week
        ) subq_69
        LEFT OUTER JOIN (
          -- Join Standard Outputs
          -- Select: ['__significant_event_count', 'region__name', 'metric_time__week']
          -- Select: ['__significant_event_count', 'region__name', 'metric_time__week']
          -- Aggregate Inputs for Simple Metrics
          SELECT
            sma_10001_cte.metric_time__week AS metric_time__week
            , rss_10002_cte.name AS region__name
            , SUM(sma_10001_cte.__significant_event_count) AS __significant_event_count
          FROM sma_10001_cte
          LEFT OUTER JOIN
            rss_10002_cte
          ON
            sma_10001_cte.region = rss_10002_cte.region
          GROUP BY
            sma_10001_cte.metric_time__week
            , rss_10002_cte.name
        ) subq_64
        ON
          subq_69.metric_time__week - INTERVAL 4 week = subq_64.metric_time__week
      ) subq_70
    ) subq_71
    ON
      (
        COALESCE(subq_29.region__name, subq_43.region__name, subq_57.region__name) = subq_71.region__name
      ) AND (
        COALESCE(subq_29.metric_time__week, subq_43.metric_time__week, subq_57.metric_time__week) = subq_71.metric_time__week
      )
    GROUP BY
      COALESCE(subq_29.metric_time__week, subq_43.metric_time__week, subq_57.metric_time__week, subq_71.metric_time__week)
      , COALESCE(subq_29.region__name, subq_43.region__name, subq_57.region__name, subq_71.region__name)
  ) subq_72
)

SELECT
  COALESCE(subq_15.metric_time__week, subq_73.metric_time__week, subq_77.metric_time__week) AS metric_time__week
  , COALESCE(subq_15.region__name, subq_73.region__name, subq_77.region__name) AS region__name
  , COALESCE(MAX(subq_15.significant_event_count), 0) AS significant_event_count
  , MAX(subq_73.significant_events_4wk_avg) AS significant_events_4wk_avg
  , MAX(subq_77.significant_events_vs_4wk_avg) AS significant_events_vs_4wk_avg
FROM (
  -- Read From CTE For node_id=cm_7
  SELECT
    metric_time__week
    , region__name
    , significant_event_count
  FROM cm_7_cte
) subq_15
FULL OUTER JOIN (
  -- Read From CTE For node_id=cm_12
  SELECT
    metric_time__week
    , region__name
    , significant_events_4wk_avg
  FROM cm_12_cte
) subq_73
ON
  (
    subq_15.region__name = subq_73.region__name
  ) AND (
    subq_15.metric_time__week = subq_73.metric_time__week
  )
FULL OUTER JOIN (
  -- Compute Metrics via Expressions
  SELECT
    metric_time__week
    , region__name
    , significant_event_count - significant_events_4wk_avg AS significant_events_vs_4wk_avg
  FROM (
    -- Combine Aggregated Outputs
    SELECT
      COALESCE(subq_74.metric_time__week, subq_75.metric_time__week) AS metric_time__week
      , COALESCE(subq_74.region__name, subq_75.region__name) AS region__name
      , COALESCE(MAX(subq_74.significant_event_count), 0) AS significant_event_count
      , MAX(subq_75.significant_events_4wk_avg) AS significant_events_4wk_avg
    FROM (
      -- Read From CTE For node_id=cm_7
      SELECT
        metric_time__week
        , region__name
        , significant_event_count
      FROM cm_7_cte
    ) subq_74
    FULL OUTER JOIN (
      -- Read From CTE For node_id=cm_12
      SELECT
        metric_time__week
        , region__name
        , significant_events_4wk_avg
      FROM cm_12_cte
    ) subq_75
    ON
      (
        subq_74.region__name = subq_75.region__name
      ) AND (
        subq_74.metric_time__week = subq_75.metric_time__week
      )
    GROUP BY
      COALESCE(subq_74.metric_time__week, subq_75.metric_time__week)
      , COALESCE(subq_74.region__name, subq_75.region__name)
  ) subq_76
) subq_77
ON
  (
    COALESCE(subq_15.region__name, subq_73.region__name) = subq_77.region__name
  ) AND (
    COALESCE(subq_15.metric_time__week, subq_73.metric_time__week) = subq_77.metric_time__week
  )
GROUP BY
  COALESCE(subq_15.metric_time__week, subq_73.metric_time__week, subq_77.metric_time__week)
  , COALESCE(subq_15.region__name, subq_73.region__name, subq_77.region__name)
) q
where region__name is not null
  and significant_events_vs_4wk_avg > 0
order by significant_events_vs_4wk_avg desc, region__name;
```

The string runs in three parts:

1. A preamble recreates the `region_bounds` seed and the model views in the session.
2. The MetricFlow render of the saved query `weekly_exposure_by_region` follows, constrained to one week on `metric_time` week and grouped by region name.
3. An outer wrapper keeps the named regions whose count is above their prior 4-week average and returns three columns.

The string returns exactly these columns, in this order:

- `region__name`: `dim_region.name`. Report it as written.
- `significant_event_count`: the region's M4.5+ earthquakes (`fct_seismic_events.is_significant`) in the answered Monday-to-Sunday UTC week. A whole number.
- `significant_events_4wk_avg`: the mean of the region's weekly counts over the four weeks before, `(w1 + w2 + w3 + w4) / 4.0`. It can be fractional, for example 2.25.

The wrapper keeps rows where `region__name is not null and significant_events_vs_4wk_avg > 0`, and orders them by that margin (count minus average), largest first, then by name. The margin is not returned. If asked for it, compute it as count minus average.

What the rows mean:

- Each row is a region whose count last week was above its prior 4-week average.
- A region that is absent was at or below its average. It is not a region with zero events.
- An empty result means no region was above its average. That is a valid answer; say it plainly.
- A region with an average of 0 appears as soon as it has one M4.5+ earthquake.
- `Other / open ocean` is a real region, the catch-all for events outside every box.

Which week the answer covers:

- Report the Monday and Sunday UTC dates of the week.
- The string's first line, `-- render: ... week=<value> ...`, names the week it answers. `last-complete` means the last complete UTC week at run time.
- If it names a Monday instead, that pinned week is the one answered. Compare it with `last_complete_week_start_utc` from drill-down 1. If they differ, say that the canonical answer is for the pinned week, not the latest complete week.
- If the same line shows an `updated_at_cutoff` other than `none`, the string counts only events USGS last updated at or before that instant. Say so, since drill-down counts for the same week can then be slightly higher.

In the answer, give each region with its count and its average, the line `Source: canonical query weekly_exposure_by_region`, and the credit line.

Follow-ups about a region, such as a region's count in another week or the list of events behind a region's count, have no query of their own. Region assignment exists only inside the canonical string, and you never edit it. Say that the canonical query does not break a region's count down. Offer drill-down 3 (last week's largest M4.5+ earthquakes, with `place`), labelled ad hoc, and do not attach region names to its rows.

## Drill-down questions

Each query reads the raw table, drops deleted events and converts times with `timezone('UTC', ...)`. Change literals as the question needs, but keep both rules and a `LIMIT`. Label these answers `Source: ad hoc query over the raw earthquake table`. They apply no update cutoff, so for a week the canonical string answers with a cutoff, their counts can be slightly higher.

### 1. What period does the table cover, and which week is the last complete one?

```sql
select
  count(*) as live_events,
  min(timezone('UTC', event_time)) as earliest_event_utc,
  max(timezone('UTC', event_time)) as latest_event_utc,
  max(timezone('UTC', updated_at)) as latest_update_utc,
  max(timezone('UTC', _fivetran_synced)) as latest_sync_utc,
  cast(date_trunc('week', min(timezone('UTC', event_time)) + interval 6 day) + interval 28 day as date) as earliest_week_with_4wk_baseline,
  cast(date_trunc('week', timezone('UTC', now())) - interval 7 day as date) as last_complete_week_start_utc,
  cast(date_trunc('week', timezone('UTC', now())) - interval 1 day as date) as last_complete_week_end_utc
from <lake_catalog_alias>.<lake_schema>.earthquake
where not coalesce(_fivetran_deleted, false);
```

### 2. How many events, earthquakes and M4.5+ earthquakes landed in each of the last eight complete weeks?

Weeks with no events show 0, not a missing row. Weeks before the earliest event are zero because they were never synced, not because nothing happened.

```sql
with weeks as (
  select range as week_start
  from range(
    date_trunc('week', timezone('UTC', now())) - interval 56 day,
    date_trunc('week', timezone('UTC', now())),
    interval 7 day
  )
),

live as (
  select
    date_trunc('week', timezone('UTC', event_time)) as week_start,
    event_type,
    mag
  from <lake_catalog_alias>.<lake_schema>.earthquake
  where not coalesce(_fivetran_deleted, false)
    and timezone('UTC', event_time) >= date_trunc('week', timezone('UTC', now())) - interval 56 day
    and timezone('UTC', event_time) < date_trunc('week', timezone('UTC', now()))
)

select
  cast(weeks.week_start as date) as week_start_utc,
  count(live.week_start) as live_events,
  count(live.week_start) filter (where live.event_type = 'earthquake') as earthquakes,
  count(live.week_start) filter (where live.event_type = 'earthquake' and live.mag >= 4.5) as m45_earthquakes,
  count(live.week_start) filter (where live.event_type <> 'earthquake') as other_event_types,
  count(live.week_start) filter (where live.mag is null) as null_magnitude
from weeks
left join live
  on live.week_start = weeks.week_start
group by weeks.week_start
order by weeks.week_start;
```

### 3. What were the 50 largest M4.5+ earthquakes last week?

A busy week has more than 50. For the count, use drill-down 2.

```sql
with bounds as (
  select
    date_trunc('week', timezone('UTC', now())) - interval 7 day as week_start,
    date_trunc('week', timezone('UTC', now())) as week_end
)

select
  e.id as event_id,
  timezone('UTC', e.event_time) as event_time_utc,
  e.mag,
  e.mag_type,
  e.depth_km,
  e.place,
  e.network,
  e.review_status,
  e.alert,
  e.tsunami,
  e.url
from <lake_catalog_alias>.<lake_schema>.earthquake as e
cross join bounds
where not coalesce(e._fivetran_deleted, false)
  and e.event_type = 'earthquake'
  and e.mag >= 4.5
  and timezone('UTC', e.event_time) >= bounds.week_start
  and timezone('UTC', e.event_time) < bounds.week_end
order by e.mag desc, event_time_utc, event_id
limit 50;
```

### 4. Which places had the most events last week?

The query groups on the text after ` of ` in `place`, so `16 km NE of Milford, Utah` and `15 km NE of Milford, Utah` count together.

```sql
with bounds as (
  select
    date_trunc('week', timezone('UTC', now())) - interval 7 day as week_start,
    date_trunc('week', timezone('UTC', now())) as week_end
)

select
  coalesce(nullif(regexp_extract(e.place, ' of (.+)$', 1), ''), e.place) as place_area,
  count(*) as live_events,
  count(*) filter (where e.event_type = 'earthquake') as earthquakes,
  count(*) filter (where e.event_type = 'earthquake' and e.mag >= 4.5) as m45_earthquakes,
  max(e.mag) as max_mag
from <lake_catalog_alias>.<lake_schema>.earthquake as e
cross join bounds
where not coalesce(e._fivetran_deleted, false)
  and timezone('UTC', e.event_time) >= bounds.week_start
  and timezone('UTC', e.event_time) < bounds.week_end
group by place_area
order by live_events desc, place_area
limit 25;
```

### 5. Which networks and magnitude scales reported last week's events?

```sql
with bounds as (
  select
    date_trunc('week', timezone('UTC', now())) - interval 7 day as week_start,
    date_trunc('week', timezone('UTC', now())) as week_end
)

select
  e.network,
  e.mag_type,
  count(*) as live_events,
  count(*) filter (where e.review_status = 'automatic') as automatic,
  min(e.mag) as min_mag,
  max(e.mag) as max_mag
from <lake_catalog_alias>.<lake_schema>.earthquake as e
cross join bounds
where not coalesce(e._fivetran_deleted, false)
  and timezone('UTC', e.event_time) >= bounds.week_start
  and timezone('UTC', e.event_time) < bounds.week_end
group by e.network, e.mag_type
order by e.network, live_events desc, e.mag_type
limit 150;
```

### 6. Which M2.5+ events happened within 50 km of a point in the last 30 days?

The distance is a great-circle (haversine) distance on a 6371 km sphere. Replace the point and radius with the ones asked about.

```sql
with target_point as (
  -- Near Ridgecrest, CA.
  select 35.77 as lat, -117.60 as lon, 50.0 as radius_km
),

nearby as (
  select
    e.id as event_id,
    timezone('UTC', e.event_time) as event_time_utc,
    e.event_type,
    e.mag,
    e.mag_type,
    e.depth_km,
    e.place,
    2 * 6371.0 * asin(least(1.0, sqrt(
      pow(sin(radians(e.latitude - target_point.lat) / 2), 2)
      + cos(radians(target_point.lat)) * cos(radians(e.latitude))
        * pow(sin(radians(e.longitude - target_point.lon) / 2), 2)
    ))) as distance_km,
    target_point.radius_km
  from <lake_catalog_alias>.<lake_schema>.earthquake as e
  cross join target_point
  where not coalesce(e._fivetran_deleted, false)
    and timezone('UTC', e.event_time) >= timezone('UTC', now()) - interval 30 day
    and e.mag >= 2.5
)

select
  event_id,
  event_time_utc,
  event_type,
  mag,
  mag_type,
  depth_km,
  place,
  round(distance_km, 1) as distance_km
from nearby
where distance_km <= radius_km
order by event_time_utc desc, event_id
limit 100;
```

## Gotchas

### What the rows are

Counts below come from the profiled sample (about 22,000 live events over eight weeks of the local copy). They drift with every sync and differ on the lake; use them for scale, not as current values.

- **Not every row is an earthquake.** In the profiled sample, 474 of 21,949 live rows are other types: quarry blast (304), explosion (141), ice quake (19), landslide (7), mining explosion (2) and experimental explosion (1). USGS can add types. Say earthquakes only when you filtered on `event_type = 'earthquake'`.
- **Deleted events.** The connector keeps rows USGS deleted, marked `_fivetran_deleted = true`. Apply the filter on every query; deleted rows keep their last values.
- **Ids merged under a preferred id.** `id` is the preferred id, and USGS can change it. The row under the old id is then deleted, so an old id appears only inside `ids`, a comma-separated list with no wrapping commas. To look up an id, match `id = '<id>' or list_contains(string_split(ids, ','), '<id>')`. `sources` lists the contributing networks, for example `ak,us`.
- **Revisions.** About 13% of rows in the profiled sample are `review_status = 'automatic'`. They can be re-sized, merged or deleted later, so recent counts can move between syncs.
- **Coverage.** Nothing before the connector's `start_date` exists in the table. Counts for those weeks read as 0, and a 4-week average needs four full weeks of data before the week in question.

### Magnitudes

- **Null magnitudes.** One row in the profiled sample has a null `mag`, and the same row has a null `mag_type`. A filter such as `mag >= 4.5` or `mag < 2` drops it, and `avg(mag)` ignores it. The significant-event rule treats it as not significant.
- **Negative magnitudes are real.** The profiled sample's minimum is -1.25: tiny events on local scales. Do not filter `mag > 0` as a validity check.
- **Mixed magnitude scales.** `mag` is on the scale in `mag_type`: ml (13,857 rows), md (5,687), mb (2,104), mww (201), mwr, mh, mb_lg, mw, ms_vx, mun and mwb (1 row). Local networks report small events in ml or md, and large events are mostly mb or mww. The M4.5 threshold applies to `mag` as reported, whatever the scale. Name the scale when you compare magnitudes across networks or regions.
- **Do not parse magnitude from `title`.** `title` rounds it (`M 1.1 - ...`). Use `mag`.

### Flags and scores

- **The tsunami flag is not a warning.** `tsunami = 1` (11 rows in the profiled sample) means the event was large and oceanic, so USGS links tsunami information. It does not mean a tsunami occurred or that a warning was issued. It is an integer: compare with `= 1`.
- **`alert` is mostly null.** In the profiled sample, 131 rows have a PAGER level: 126 green, 2 yellow, 2 orange, 1 red. Null means PAGER did not assess the event, not green.
- **`sig` is not capped at 1000.** The profiled sample's maximum is 2910. It is a USGS score, unrelated to the M4.5+ significant-event rule.
- **`felt`, `cdi` and `mmi` are sparse.** In the profiled sample, `felt` and `cdi` are null in 21,015 rows and `mmi` in 21,503. Null means no reports or no ShakeMap, not zero; 0 also occurs.

### Location and place

- **Negative depth.** `depth_km` goes down to -3.48 in the profiled sample: events located above the reference surface, not errors. The sample's maximum is 665 km.
- **Place text is inconsistent.** Some US states are spelled out and others abbreviated (`Milford, Utah`, `Coso Junction, CA`). Use `ilike` and try both forms. `place` is not a region; regions come from coordinates.
- **Small-event clusters dominate raw counts.** Places near Milford, Utah, The Geysers, CA and Coso Junction, CA have hundreds of small events each. A high event count there is not M4.5+ activity.
- **Longitude runs -180 to 180.** An area across the antimeridian needs `longitude >= a or longitude <= b`, not `between`.

### Time

- **timestamptz columns.** `event_time`, `updated_at` and `_fivetran_synced` carry a time zone, and the session time zone is not guaranteed to be UTC. Always `timezone('UTC', col)` before `date_trunc`, extracting a day or hour, or comparing. Never cast to `date`. `current_date` and `now()::date` follow the session time zone; use `timezone('UTC', now())`.
- **Weeks.** `date_trunc('week', ...)` starts weeks on Monday, which matches the canonical query. A plain `group by` week loses weeks with no events; build the weeks with `range()` as drill-down 2 does.

### Results

- **Row cap.** The tool returns at most its row cap (200 by default). Aggregate and `LIMIT` in SQL. A result with exactly as many rows as the cap may be cut off; say so.
- **Region numbers.** Only the canonical string assigns regions. Do not rebuild region boxes from memory or infer regions from `place`.
