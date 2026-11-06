-- One row per live campaign x configured vehicle, columns named in lowercase on every target.
-- Rows the connector marked deleted are dropped here, so every model downstream sees live rows only.
-- Every column is cast once here, so the contracts on the public marts hold on Snowflake and DuckDB alike.

{%- set columns = [
  ('nhtsa_campaign_number', 'varchar'), ('make', 'varchar'), ('model', 'varchar'), ('model_year', 'integer'),
  ('nhtsa_make', 'varchar'), ('nhtsa_model', 'varchar'), ('manufacturer', 'varchar'), ('component', 'varchar'),
  ('summary', 'varchar'), ('consequence', 'varchar'), ('remedy', 'varchar'), ('notes', 'varchar'),
  ('report_received_date', 'date'), ('park_it', 'boolean'), ('park_outside', 'boolean'),
  ('over_the_air_update', 'boolean')
] %}

select
  {%- for column, data_type in columns %}
  cast({{ lake_col(column) }} as {{ data_type }}) as {{ column }}{{ "," if not loop.last }}
  {%- endfor %}
from {{ source('nhtsa', 'recalls') }}
where not coalesce({{ lake_col('_fivetran_deleted') }}, false)
