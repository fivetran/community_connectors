-- One row per live public-domain object: the Met's record, its current curator tags, and the Cortex vision
-- enrichment when the connector ran with enable_cortex. Objects without an enrichment keep null AI columns and
-- is_enriched = false, so counts over this model cover the whole collection the connector synced.

with tags as (
  select
    object_id,
    cast(count(*) as integer) as curator_tag_count,
    {{ list_agg('term') }} as curator_tags
  from {{ ref('stg_met__object_tag') }}
  group by object_id
),

elements as (
  select
    object_id,
    cast(count(*) as integer) as depicted_element_count,
    {{ list_agg('element') }} as depicted_elements
  from {{ ref('stg_met__object_depicted_element') }}
  group by object_id
)

select
  o.object_id,
  o.department_id,
  d.department_name,
  o.title,
  o.object_name,
  o.classification,
  o.artist_display_name,
  o.artist_nationality,
  o.culture,
  o.object_date,
  o.object_begin_date,
  o.object_end_date,
  cast(floor(o.object_begin_date / 10.0) * 10 as integer) as object_decade,
  o.medium,
  o.credit_line,
  o.is_highlight,
  o.is_timeline_work,
  o.accession_number,
  o.gallery_number,
  o.object_url,
  o.primary_image_small,
  {{ utc_date('o.metadata_date') }} as metadata_day,
  cast(coalesce(t.curator_tag_count, 0) as integer) as curator_tag_count,
  t.curator_tags,
  cast(v.object_id is not null as boolean) as is_enriched,
  v.subject_category,
  v.scene_type,
  v.style_period,
  v.dominant_colors,
  v.materials_visible,
  v.visual_description,
  cast(coalesce(e.depicted_element_count, 0) as integer) as depicted_element_count,
  e.depicted_elements,
  v.curator_tag_agreement,
  v.curator_tags_matched,
  v.curator_tags_missed,
  v.enrichment_model,
  v.enriched_date
from {{ ref('stg_met__object') }} as o
left join {{ ref('stg_met__department') }} as d on o.department_id = d.department_id
left join tags as t on o.object_id = t.object_id
left join {{ ref('stg_met__object_visual_attribute') }} as v on o.object_id = v.object_id
left join elements as e on o.object_id = e.object_id
