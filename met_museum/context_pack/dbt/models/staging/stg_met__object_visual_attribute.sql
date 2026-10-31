-- The Cortex vision enrichment of a live object, one row per enriched object.
-- Live rows come from the inner join to stg_met__object, not from _fivetran_deleted: the connector deletes this row
-- with its object, and on the Managed Data Lake the enrichment tables have no _fivetran_deleted column until the
-- first delete lands, so filtering on it would fail on a lake that has never deleted an enrichment.
-- dominant_colors and materials_visible are JSON arrays; they are kept as JSON text, which both engines read
-- byte-identical from the same Iceberg file.

select
  cast(v.{{ lake_col('object_id') }} as integer) as object_id,
  cast(v.{{ lake_col('subject_category') }} as varchar) as subject_category,
  cast(v.{{ lake_col('scene_type') }} as varchar) as scene_type,
  cast(v.{{ lake_col('style_period') }} as varchar) as style_period,
  cast(v.{{ lake_col('dominant_colors') }} as varchar) as dominant_colors,
  cast(v.{{ lake_col('materials_visible') }} as varchar) as materials_visible,
  cast(v.{{ lake_col('visual_description') }} as varchar) as visual_description,
  cast(v.{{ lake_col('depicted_element_count') }} as integer) as written_element_count,
  cast(v.{{ lake_col('curator_tag_agreement') }} as double) as curator_tag_agreement,
  cast(v.{{ lake_col('curator_tags_matched') }} as integer) as curator_tags_matched,
  cast(v.{{ lake_col('curator_tags_missed') }} as integer) as curator_tags_missed,
  cast(v.{{ lake_col('enrichment_model') }} as varchar) as enrichment_model,
  cast(v.{{ lake_col('enrichment_hash') }} as varchar) as enrichment_hash,
  {{ utc_date('v.' ~ lake_col('enriched_at')) }} as enriched_date
from {{ source('met', 'object_visual_attribute') }} as v
inner join {{ ref('stg_met__object') }} as o
  on cast(v.{{ lake_col('object_id') }} as integer) = o.object_id
