-- One row per element the model saw in a live object's image, current enrichment only. Filter here to find objects
-- by what they show (element like '%cypress%'), then join fct_objects_enriched on object_id.

select
  cast(object_id as varchar) || '|' || element as object_element_key,
  object_id,
  element
from {{ ref('stg_met__object_depicted_element') }}
