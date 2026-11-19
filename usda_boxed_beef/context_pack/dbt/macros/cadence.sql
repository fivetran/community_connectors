{#- The report's cadence: 2460 (LM_XB456, Prime cuts) is weekly, 2453 (LM_XB403) is daily. One definition for
    every model that labels a row, so the labels cannot disagree. -#}
{% macro cadence(slug_id) -%}
  case {{ slug_id }} when '2460' then 'weekly' else 'daily' end
{%- endmacro %}
