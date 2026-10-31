-- The constituents credited on a live object, current rows only. The API returns only an object's current
-- constituents, so the connector stamps each row with the object's metadata_date (object_metadata_date). A row whose
-- stamp differs from the object's metadata_date was dropped by the Met at a later update and is left out here.

select
  cast(c.{{ lake_col('object_id') }} as integer) as object_id,
  cast(c.{{ lake_col('constituent_id') }} as integer) as constituent_id,
  cast(c.{{ lake_col('role') }} as varchar) as role,
  cast(c.{{ lake_col('name') }} as varchar) as name,
  cast(c.{{ lake_col('gender') }} as varchar) as gender,
  cast(c.{{ lake_col('constituent_ulan_url') }} as varchar) as constituent_ulan_url,
  cast(c.{{ lake_col('constituent_wikidata_url') }} as varchar) as constituent_wikidata_url
from {{ source('met', 'object_constituent') }} as c
inner join {{ ref('stg_met__object') }} as o
  on cast(c.{{ lake_col('object_id') }} as integer) = o.object_id
  and c.{{ lake_col('object_metadata_date') }} = o.metadata_date
where not coalesce(c.{{ lake_col('_fivetran_deleted') }}, false)
