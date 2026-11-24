-- One row per live configured make x model x model year, columns named in lowercase on every target.
-- last_synced_at keeps the connector's timestamp type, which differs between engines; everything else is cast here.

{%- set columns = [
  ('make', 'varchar'), ('model', 'varchar'), ('model_year', 'integer'), ('nhtsa_model', 'varchar'),
  ('campaigns_returned', 'integer')
] %}

select
  {%- for column, data_type in columns %}
  cast({{ lake_col(column) }} as {{ data_type }}) as {{ column }},
  {%- endfor %}
  {{ lake_col('last_synced_at') }} as last_synced_at
from {{ source('nhtsa', 'configured_vehicle') }}
where not coalesce({{ lake_col('_fivetran_deleted') }}, false)
