-- curator_tags_matched + curator_tags_missed is the number of curator tags the connector scored, and it scores the
-- tags it writes in the same sync. The enrichment hash covers the tags, so a tag change re-enriches the object. A
-- mismatch warns, not fails: max_enrichments_per_sync can defer that re-enrichment to a later sync, and a term the
-- API repeats is scored twice but written once.
{{ config(severity='warn') }}

select
  object_id,
  curator_tag_count,
  curator_tags_matched + curator_tags_missed as scored_tags
from {{ ref('fct_objects_enriched') }}
where is_enriched
  and curator_tags_matched + curator_tags_missed <> curator_tag_count
