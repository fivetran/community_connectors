-- One row per region in the human-owned seed, bounds included, so fct_seismic_events assigns events from here.

select
  region_id,
  name,
  lat_min,
  lat_max,
  lon_min,
  lon_max,
  priority
from {{ ref('region_bounds') }}
