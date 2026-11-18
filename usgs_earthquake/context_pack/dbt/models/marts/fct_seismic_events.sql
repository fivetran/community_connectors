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
  from {{ ref('stg_usgs__earthquakes') }}
),

regions as (
  select
    region_id,
    lat_min,
    lat_max,
    lon_min,
    lon_max,
    priority
  from {{ ref('dim_region') }}
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
