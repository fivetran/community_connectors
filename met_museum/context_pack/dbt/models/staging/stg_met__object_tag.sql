-- The curator tags on a live object, current rows only, by the same stamp rule as stg_met__object_constituent: a tag
-- row is current only when its object_metadata_date equals the object's metadata_date.

select
  cast(t.{{ lake_col('object_id') }} as integer) as object_id,
  cast(t.{{ lake_col('term') }} as varchar) as term,
  cast(t.{{ lake_col('aat_url') }} as varchar) as aat_url,
  cast(t.{{ lake_col('wikidata_url') }} as varchar) as wikidata_url
from {{ source('met', 'object_tag') }} as t
inner join {{ ref('stg_met__object') }} as o
  on cast(t.{{ lake_col('object_id') }} as integer) = o.object_id
  and t.{{ lake_col('object_metadata_date') }} = o.metadata_date
where {{ live_rows('object_tag', 't') }}
