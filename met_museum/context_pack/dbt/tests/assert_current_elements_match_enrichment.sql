-- The hash join keeps exactly the depicted elements the current enrichment wrote. The connector records how many it
-- wrote (written_element_count); a mismatch means stale rows passed the join or current rows were dropped.

with current_elements as (
  select object_id, count(*) as element_count
  from {{ ref('stg_met__object_depicted_element') }}
  group by object_id
)

select
  v.object_id,
  v.written_element_count,
  coalesce(e.element_count, 0) as element_count
from {{ ref('stg_met__object_visual_attribute') }} as v
left join current_elements as e on v.object_id = e.object_id
where coalesce(e.element_count, 0) <> coalesce(v.written_element_count, 0)
