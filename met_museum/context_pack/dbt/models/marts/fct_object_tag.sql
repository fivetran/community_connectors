-- One row per current curator tag on a live object. The curators' own subject terms, for comparison with what the
-- model saw (fct_object_depicted_element).

select
  cast(object_id as varchar) || '|' || term as object_tag_key,
  object_id,
  term,
  aat_url
from {{ ref('stg_met__object_tag') }}
