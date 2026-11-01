{#- Every SQL difference between Snowflake and DuckDB sits behind one of these dispatched macros, so a model has one
    call site per difference and never an `if target.type` block. -#}

{#- A column of the connector's source tables. Double-quoted when the source keeps the lake's lowercase names
    (var quote_lake_identifiers), bare otherwise. Aliased to the bare lowercase name at the call site. -#}
{% macro lake_col(name) -%}
  {%- if var('quote_lake_identifiers') -%}"{{ name }}"{%- else -%}{{ name }}{%- endif -%}
{%- endmacro %}

{#- A sorted, ' | '-separated list of a column's distinct values. DuckDB's string_agg and Snowflake's listagg render
    byte-identical lists, so the parity check can compare them as strings. The separator is a pipe, because a
    depicted element or a curator tag can itself hold a comma. -#}
{% macro list_agg(column) -%}{{ return(adapter.dispatch('list_agg')(column)) }}{%- endmacro %}
{% macro default__list_agg(column) -%}
  listagg(distinct {{ column }}, ' | ') within group (order by {{ column }})
{%- endmacro %}
{% macro duckdb__list_agg(column) -%}
  string_agg(distinct {{ column }}, ' | ' order by {{ column }})
{%- endmacro %}

{#- A timestamp with time zone as a UTC calendar date. Snowflake reads the lake's timestamps as TIMESTAMP_LTZ, whose
    date depends on the session time zone; DuckDB reads TIMESTAMP WITH TIME ZONE. Converting to UTC first gives both
    engines the same date. -#}
{% macro utc_date(column) -%}{{ return(adapter.dispatch('utc_date')(column)) }}{%- endmacro %}
{% macro default__utc_date(column) -%}
  cast(convert_timezone('UTC', {{ column }}) as date)
{%- endmacro %}
{% macro duckdb__utc_date(column) -%}
  cast(timezone('UTC', {{ column }}) as date)
{%- endmacro %}

{#- One row per day from start to end inclusive (ISO date literals), as date_day. The Snowflake generator caps the
    spine at 20,000 days (about 54 years). -#}
{% macro day_spine(start, end) -%}{{ return(adapter.dispatch('day_spine')(start, end)) }}{%- endmacro %}
{% macro default__day_spine(start, end) -%}
select dateadd(day, day_offset, to_date('{{ start }}')) as date_day
from (
  select row_number() over (order by seq4()) - 1 as day_offset
  from table(generator(rowcount => 20000))
)
where day_offset <= datediff(day, to_date('{{ start }}'), to_date('{{ end }}'))
{%- endmacro %}
{% macro duckdb__day_spine(start, end) -%}
select cast(range as date) as date_day
from range(date '{{ start }}', date '{{ end }}' + interval 1 day, interval 1 day)
{%- endmacro %}

{#- The filter that keeps a source table's live rows. A connector table gets its _fivetran_deleted column only when a
    delete first lands, so the column can be missing on a new Managed Data Lake table or on a table of the local debug
    warehouse that has no rows yet. The filter is written only when the column exists; without it, every row is live.
    var('fivetran_deleted_tables') lists the tables that have the column, for a compile with no live connection
    (parity.py passes the list it read from the lake). Otherwise the source table is described. -#}
{% macro live_rows(table_name, alias=none) -%}
  {%- set prefix = alias ~ '.' if alias else '' -%}
  {%- if has_fivetran_deleted(table_name) -%}
  not coalesce({{ prefix }}{{ lake_col('_fivetran_deleted') }}, false)
  {%- else -%}
  1 = 1
  {%- endif -%}
{%- endmacro %}

{% macro has_fivetran_deleted(table_name) -%}
  {%- set known = var('fivetran_deleted_tables', none) -%}
  {%- if known is not none -%}
    {{ return(table_name in known) }}
  {%- endif -%}
  {%- if not execute -%}
    {{ return(true) }}
  {%- endif -%}
  {{ return(adapter.dispatch('has_fivetran_deleted')(source('met', table_name))) }}
{%- endmacro %}
{% macro default__has_fivetran_deleted(relation) -%}
  {%- set names = adapter.get_columns_in_relation(relation) | map(attribute='name') | map('lower') | list -%}
  {{ return('_fivetran_deleted' in names) }}
{%- endmacro %}
{#- DuckDB's information_schema lists only one column per table of an attached Iceberg catalog, so the table is
    described instead. -#}
{% macro duckdb__has_fivetran_deleted(relation) -%}
  {%- set described = run_query('describe ' ~ relation) -%}
  {{ return('_fivetran_deleted' in (described.columns[0].values() | map('lower') | list)) }}
{%- endmacro %}
