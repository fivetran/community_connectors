{#- The rows of a connector table that Fivetran has not marked deleted. A Fivetran destination adds
    _fivetran_deleted to every table, but fivetran debug adds it only once a table has received a row, so the filter
    is applied only when the column exists. -#}
{% macro live_rows(source_relation) -%}
  {%- set columns = adapter.get_columns_in_relation(source_relation) | map(attribute='name') | map('lower') | list -%}
  select * from {{ source_relation }}
  {%- if '_fivetran_deleted' in columns %}
  where not coalesce(_fivetran_deleted, false)
  {%- endif %}
{%- endmacro %}
