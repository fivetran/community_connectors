-- One row per live public-domain object, every column cast once and named in lowercase on every target.
-- metadata_date keeps the source's timestamp type: the child models compare it with their own stamp column, which
-- the connector wrote from the same value, so the comparison happens inside one engine and never across two.

{%- set columns = [
  ('object_id', 'integer'), ('department_id', 'integer'), ('department', 'varchar'),
  ('is_highlight', 'boolean'), ('is_timeline_work', 'boolean'), ('accession_number', 'varchar'),
  ('accession_year', 'varchar'), ('primary_image', 'varchar'), ('primary_image_small', 'varchar'),
  ('additional_image_count', 'integer'), ('object_name', 'varchar'), ('title', 'varchar'), ('culture', 'varchar'),
  ('period', 'varchar'), ('dynasty', 'varchar'), ('reign', 'varchar'), ('portfolio', 'varchar'),
  ('artist_role', 'varchar'), ('artist_prefix', 'varchar'), ('artist_display_name', 'varchar'),
  ('artist_display_bio', 'varchar'), ('artist_suffix', 'varchar'), ('artist_alpha_sort', 'varchar'),
  ('artist_nationality', 'varchar'), ('artist_begin_date', 'varchar'), ('artist_end_date', 'varchar'),
  ('artist_gender', 'varchar'), ('artist_wikidata_url', 'varchar'), ('artist_ulan_url', 'varchar'),
  ('object_date', 'varchar'), ('object_begin_date', 'integer'), ('object_end_date', 'integer'),
  ('medium', 'varchar'), ('dimensions', 'varchar'), ('credit_line', 'varchar'), ('geography_type', 'varchar'),
  ('city', 'varchar'), ('state_province', 'varchar'), ('county', 'varchar'), ('country', 'varchar'),
  ('region', 'varchar'), ('subregion', 'varchar'), ('locale', 'varchar'), ('locus', 'varchar'),
  ('excavation', 'varchar'), ('river', 'varchar'), ('classification', 'varchar'),
  ('rights_and_reproduction', 'varchar'), ('link_resource', 'varchar'), ('repository', 'varchar'),
  ('object_url', 'varchar'), ('object_wikidata_url', 'varchar'), ('gallery_number', 'varchar')
] %}

select
  {%- for column, data_type in columns %}
  cast({{ lake_col(column) }} as {{ data_type }}) as {{ column }},
  {%- endfor %}
  {{ lake_col('metadata_date') }} as metadata_date
from {{ source('met', 'object') }}
where {{ live_rows('object') }}
