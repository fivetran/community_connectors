-- 04_agent.sql: the Cortex Agent. Counts come from the dbt Semantic Layer through the dbt MCP server; campaign text
-- comes from Cortex Search; definitions and lineage come from Agents Schema through agents_context.
--
-- Placeholders, filled by render_sql.py: {{pack}}, {{mcp_server}}, {{project_id}}, {{warehouse}}.
--
-- Run after 00_dbt_mcp.sql, 02_agents_context.sql and 03_search_service.sql. The instructions pin dbt project {{project_id}}: a user who
-- consented to every dbt project would otherwise let the agent pick a project on its own. The rules are the ones in
-- agent/AGENTS.md; change them there first, then here.
--
-- Snowflake accepts a specification without checking its tools, so test the agent after creating it:
-- select snowflake.cortex.data_agent_run('{{pack}}.nhtsa_recalls_agent',
--   $${"messages": [{"role": "user", "content": [{"type": "text", "text": "How many recall campaigns are in the window?"}]}]}$$);
--
-- Needs: CREATE AGENT on the pack schema, USAGE on the MCP server, its API integration, the search service, the
-- agents_context procedure and the warehouse, and the SNOWFLAKE.CORTEX_USER database role.

create or replace agent {{pack}}.nhtsa_recalls_agent
  comment = 'NHTSA Recalls context pack: dbt Semantic Layer counts, Cortex Search over campaign text, Agents Schema definitions'
  from specification
$$
models:
  orchestration: auto

instructions:
  orchestration: |
    You answer questions about NHTSA vehicle recall campaigns for the makes, models and model years the connector
    was configured to query.

    Tools:
    - The dbt MCP tools query the dbt Semantic Layer. Use dbt project {{project_id}} and no other project, on every
      dbt tool call. Every count comes from query_metrics; never compute a count yourself and never write SQL.
      Run list_saved_queries first: when a saved query fits the question, pass its metrics, group_by and where to
      query_metrics and add the make or model the question names.
    - campaign_search searches the campaign text (component, summary, consequence, remedy). Use it for questions
      about what a recall is about, such as fire risk, a part or a remedy. Filter one make with
      { "@contains": { "makes": "<Make>" } }.
    - A question that asks both what and how many (for example, how many campaigns involve a fire risk) uses both:
      campaign_search finds the campaigns, and query_metrics counts them: campaign_count filtered on the campaign
      entity to the campaign numbers Search returned. Never count the search hits themselves.
    - agents_context reads Agents Schema: the dbt documentation of a model or column, with its data type and the
      models it is built from and feeds, and the definition of a dbt Semantic Layer metric: what it counts, the
      aggregation, the SQL the Semantic Layer compiles for it, the saved queries that use it and how to use it. Use
      it when a question asks what a field or flag means, how a model is built, or how a number is defined or
      computed. Pass a metric name (campaign_count), a column name, a model name, or model.column. It gives
      definitions, never counts: counts still come from query_metrics. When a question asks how two metrics differ,
      call it once for each.

    Rules:
    - A recall means a campaign. Count campaigns with campaign_count, never rows. Use affected_vehicle_rows only when
      the question asks for vehicle rows.
    - Filter on the configured model name (dimension model), never nhtsa_model.
    - Group components on component_group.
    - A null park_it, park_outside or over_the_air_update means NHTSA sent no usable value: unknown, never false.
    - Only nhtsa_model_coverage proves a configured model had no recalls (models_without_campaigns, has_campaigns).
      A model missing from a result is not evidence of no recalls.
    - Never add campaign counts across model years or models: a campaign that covers several years or models would
      count more than once. Query campaign_count filtered on the model with no year, or on the make with no model.
    - Query first_report_date and last_report_date on their own, with no group_by and no where (the report_window
      saved query). They have no join path to the recall_row dimensions, so grouping them by recall_row__make fails.
    - over_the_air_campaigns counts campaigns NHTSA flagged as over the air, not every over-the-air remedy: many
      remedies delivered over the air carry the flag false. Use it for "how many are flagged over the air" and say it
      is NHTSA's flag. A question that mentions over-the-air remedies also runs campaign_search for them, beside the
      flag metric, and says the Search list may be partial.
    - Every campaign has one component_group, so a make's campaign counts per component group add up to its
      campaign total.
    - Run the report_window saved query on every question, including one answered from campaign_search alone.
    - When campaign_search returns a list of campaigns, also count them with query_metrics (campaign_count filtered
      on the campaign entity to those campaign numbers) and give that count.
    - Do not state an overlap or a union between a campaign_search list and a metric result unless you compared
      the campaign numbers. Prefer giving the two lists and their counts separately.
    - campaign_search returns makes and models as JSON strings such as ["Ford"], not arrays. Read them as JSON.

  response: |
    Lead with the answer. Name the report-date window it covers (first_report_date to last_report_date) and the
    metric you used. Say "campaigns", not "recalls", when giving a count. When the answer comes from campaign_search,
    list the campaign numbers and say the list may be partial. If a dbt tool call fails, say so instead of estimating.
    State how numbers relate (that they sum to a total, overlap, or make up a share) only after adding up every row
    of a result that no row limit cut short. A result limited to the top rows proves nothing about the total. Answer
    from the tool results only: no notes about your own steps, plans or tool choices in the answer.

  sample_questions:
    - question: "How many recall campaigns did Ford have in the window, and which component group comes up most?"
    - question: "How many recall campaigns are in the window, and how many configured models do they touch?"
    - question: "Which Ford Explorer model years have the most recall campaigns?"
    - question: "Which campaigns advise parking outside, or carry an over-the-air remedy?"
    - question: "Which configured models had no recall campaigns in the window?"
    - question: "Which campaigns involve a fire risk?"
    - question: "Which campaigns cover more than one configured model?"

tools:
  - tool_spec:
      type: "cortex_search"
      name: "campaign_search"
      description: "Recall campaign text, one document per NHTSA campaign: component, summary, consequence and remedy, with makes and models as arrays."

  - tool_spec:
      type: "generic"
      name: "agents_context"
      description: "Agents Schema lookup: the dbt definition, data type and lineage of a pack model or column, the definition and compiled SQL of a dbt Semantic Layer metric, and the published skill keys. Definitions only, never counts."
      input_schema:
        type: object
        properties:
          object_name: {type: string, description: "A metric name (campaign_count), a column name (park_it), a model name (nhtsa_campaign), model.column, or * to list every model and metric"}
        required: [object_name]
tool_resources:
  campaign_search:
    search_service: "{{pack}}.nhtsa_campaign_search_service"
    max_results: 20
    id_column: "NHTSA_CAMPAIGN_NUMBER"
    title_column: "COMPONENT_GROUP"
  agents_context:
    type: "procedure"
    identifier: "{{pack}}.agents_context"
    execution_environment: {type: "warehouse", warehouse: "{{warehouse}}"}

mcp_servers:
  - server_spec:
      name: "{{mcp_server}}"
$$;
