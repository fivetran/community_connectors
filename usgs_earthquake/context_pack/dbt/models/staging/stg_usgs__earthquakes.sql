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
  from {{ source('usgs', 'earthquake') }}
  where not coalesce(_fivetran_deleted, false)
  {% if var('updated_at_cutoff') %}
    and updated_at <= cast('{{ var("updated_at_cutoff") }}' as timestamptz)
  {% endif %}
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
