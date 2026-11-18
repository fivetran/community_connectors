-- The elements the model saw in a live object's image, current rows only. Each row carries the enrichment_hash of
-- the enrichment that wrote it; a re-enrichment that no longer sees an element leaves the old row behind with the old
-- hash, so a row is current only when its hash equals the object's current enrichment_hash.

select
  cast(d.{{ lake_col('object_id') }} as integer) as object_id,
  cast(d.{{ lake_col('element') }} as varchar) as element
from {{ source('met', 'object_depicted_element') }} as d
inner join {{ ref('stg_met__object_visual_attribute') }} as v
  on cast(d.{{ lake_col('object_id') }} as integer) = v.object_id
  and cast(d.{{ lake_col('enrichment_hash') }} as varchar) = v.enrichment_hash
where {{ live_rows('object_depicted_element', 'd') }}
