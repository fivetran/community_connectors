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
