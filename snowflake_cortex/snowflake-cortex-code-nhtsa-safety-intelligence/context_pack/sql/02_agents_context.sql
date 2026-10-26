-- 02_agents_context.sql: a read-only lookup over Agents Schema, so the agent can say what a model, column or metric
-- source means and where it comes from.
--
-- Placeholders, filled by render_sql.py: {{pack}}, {{agents}}.
--
-- Run after the dbt Production job and the Agents Schema publish, which writes the run's manifest into
-- {{agents}} (DBT_MODEL, DBT_COLUMN, DBT_DEPENDENCY), the run's dbt Semantic Layer as OSI (OSI_MODEL, OSI_DATASET,
-- OSI_FIELD, OSI_METRIC, OSI_RELATIONSHIP) and the pack's AGENTS.md as the ROOT row skill/nhtsa-recalls-agent.
-- The publish uses the dbt-labs/agents_schema CLI:
--   agents-schema dbt --project-dir <dir holding target/manifest.json from the Production run>
--   agents-schema osi --osi-dir <dir holding the *.osi.yaml converted from target/semantic_manifest.json>
--   agents-schema skills --skills-dir <dir holding nhtsa-recalls-agent.md, a copy of agent/AGENTS.md>
--
-- A Cortex Agent cannot run SQL of its own, so 04_agent.sql exposes this procedure as the agents_context tool. It
-- reads only the rows for models in {{pack}} and returns JSON:
--   models   name, materialization, description, upstream and downstream models (dbt lineage)
--   columns  model, column, data type and description
--   metrics  name, description, the aggregation it computes, the SQL the dbt Semantic Layer compiles for it, its
--            dbt definition (label, semantic model, saved queries that use it), synonyms and usage instructions
--   skills   the AGENTS.ROOT skill keys published for this schema
-- object_name is a model name, a column name, model.column, or a metric name or synonym. An empty string or *
-- lists every model and metric. Counts never come from here: the dbt Semantic Layer gives every count.
--
-- Needs: CREATE PROCEDURE on the pack schema; whoever runs the agent needs USAGE on the procedure and SELECT on
-- the Agents Schema tables (the procedure runs as caller).
create or replace procedure {{pack}}.agents_context(object_name varchar)
  returns varchar
  language sql
  comment = 'NHTSA Recalls context pack: dbt model, column, lineage and metric definitions from Agents Schema'
  execute as caller
as
$$
declare
  result varchar;
  name varchar default lower(trim(coalesce(object_name, '')));
  model_part varchar;
  column_part varchar;
  list_all boolean;
begin
  list_all := (name in ('', '*'));
  model_part := iff(contains(name, '.'), split_part(name, '.', 1), name);
  column_part := iff(contains(name, '.'), split_part(name, '.', 2), name);

  select payload into :result from (
    with pack_models as (
      select unique_id, name as model_name, materialization, description
      from {{agents}}.DBT_MODEL
      where database_name = split_part('{{pack}}', '.', 1)
        and schema_name = split_part('{{pack}}', '.', 2)
    ),

    upstream as (
      select downstream_id as unique_id,
        array_agg(split_part(upstream_id, '.', -1)) within group (order by upstream_id) as models
      from {{agents}}.DBT_DEPENDENCY
      group by downstream_id
    ),

    downstream as (
      select upstream_id as unique_id,
        array_agg(split_part(downstream_id, '.', -1)) within group (order by downstream_id) as models
      from {{agents}}.DBT_DEPENDENCY
      group by upstream_id
    ),

    matched_models as (
      select
        m.model_name,
        iff(:list_all,
          object_construct('name', m.model_name, 'description', m.description),
          object_construct(
            'name', m.model_name,
            'materialization', m.materialization,
            'description', m.description,
            'upstream', coalesce(u.models, array_construct()),
            'downstream', coalesce(d.models, array_construct())
          )
        ) as model_object
      from pack_models as m
      left join upstream as u on u.unique_id = m.unique_id
      left join downstream as d on d.unique_id = m.unique_id
      where :list_all or lower(m.model_name) = :model_part
    ),

    matched_columns as (
      select
        m.model_name,
        c.column_name,
        object_construct(
          'model', m.model_name,
          'column', c.column_name,
          'data_type', c.data_type,
          'description', c.description
        ) as column_object
      from {{agents}}.DBT_COLUMN as c
      inner join pack_models as m on c.model_id = m.unique_id
      where not :list_all
        and (
          (contains(:name, '.') and lower(m.model_name) = :model_part and lower(c.column_name) = :column_part)
          or (not contains(:name, '.') and (lower(c.column_name) = :name or lower(m.model_name) = :name))
        )
    ),

    -- OSI models with a dataset in the pack schema; their metrics are the pack's dbt Semantic Layer metrics.
    pack_osi_models as (
      select distinct model_name
      from {{agents}}.OSI_DATASET
      where upper(source) like upper('{{pack}}.%')
    ),

    matched_metrics as (
      select
        m.name as metric_name,
        parse_json(filter(m.custom_extensions, e -> e:vendor_name::varchar = 'DBT')[0]:data::varchar) as dbt,
        iff(:list_all,
          object_construct('name', m.name, 'label', dbt:label, 'description', m.description),
          object_construct(
            'name', m.name,
            'label', dbt:label,
            'description', m.description,
            'expression', m.expressions[0]:expression,
            'compiled_sql', dbt:compiled_sql,
            'semantic_model', dbt:semantic_model,
            'aggregation', dbt:agg,
            'saved_queries', dbt:saved_queries,
            'synonyms', m.synonyms,
            'instructions', m.ai_context:instructions
          )
        ) as metric_object
      from {{agents}}.OSI_METRIC as m
      inner join pack_osi_models as p on m.model_name = p.model_name
      where :list_all
        or lower(m.name) = :name
        or array_contains(:name::variant, transform(m.synonyms, s -> lower(s::varchar)::variant))
    ),

    skills as (
      select skill_key
      from {{agents}}.SKILL_USE
      where use_kind = 'schema'
        and upper(object_ref) = upper(split_part('{{pack}}', '.', 2))
    )

    select to_varchar(object_construct(
      'object_name', :object_name,
      'models', (select coalesce(array_agg(model_object) within group (order by model_name), array_construct())
                 from matched_models),
      'columns', (select coalesce(array_agg(column_object) within group (order by model_name, column_name),
                                  array_construct())
                  from matched_columns),
      'metrics', (select coalesce(array_agg(metric_object) within group (order by metric_name), array_construct())
                  from matched_metrics),
      'skills', (select coalesce(array_agg(skill_key) within group (order by skill_key), array_construct())
                 from skills)
    )) as payload
  );

  return result;
end;
$$;
