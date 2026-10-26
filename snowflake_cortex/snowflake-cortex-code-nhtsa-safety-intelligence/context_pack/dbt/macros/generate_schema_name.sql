{#-
  Production (target name 'prod', set by the dbt platform Production job): every model in the target schema, so the
  agent, the search service and the DDL in ../sql read one schema. The pack has its own database, so nothing else
  can collide there. No model sets a custom schema today.
  Everything else (a CI job's dbt_cloud_pr_* schema, a developer's dbt_<user>, a local run): the target schema, then
  any custom schema after it, so a CI or development run can never overwrite a Production view.
-#}
{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- if custom_schema_name is none or target.name == 'prod' -%}
    {{ target.schema | trim }}
  {%- else -%}
    {{ target.schema | trim }}_{{ custom_schema_name | trim }}
  {%- endif -%}
{%- endmacro %}
