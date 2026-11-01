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

{#- The filter that keeps a source table's live rows. In the Managed Data Lake used for this pack, all six tables of
    the connector carry _fivetran_deleted, including the three it never deletes from. The filter is written only when
    the column exists, and without it every row is live. dbt decides this when it builds the view, so a view built
    before its table had the column stays unfiltered until the next build.
    var('fivetran_deleted_tables') is for parity.py only, which compiles with no live connection and passes the list it
    read from the lake. It must be a list, and names compare in lowercase. Never set it on the dbt platform, where it
    would pin the filters instead of describing the source table. -#}
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
    {%- if known is string or known is mapping or known is not iterable -%}
      {{ exceptions.raise_compiler_error("var fivetran_deleted_tables must be a list of table names, got " ~ known) }}
    {%- endif -%}
    {{ return(table_name | lower in (known | map('lower') | list)) }}
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
