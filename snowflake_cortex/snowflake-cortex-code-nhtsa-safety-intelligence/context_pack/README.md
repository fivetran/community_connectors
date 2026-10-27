# NHTSA Recalls Context Pack

In list mode, the connector syncs NHTSA vehicle recall campaigns for a configured list of makes, models and model years. This pack is the context an AI agent needs to answer questions from that data correctly. It has four parts:

- `dbt/`: a dbt project that builds views over the connector's two tables, and defines every count once as a metric in the [dbt Semantic Layer](https://docs.getdbt.com/docs/use-dbt-semantic-layer/dbt-sl) (MetricFlow).
- `publish_agents_schema.py`: publishes what the dbt project says about itself into an `AGENTS` schema in Snowflake, with [Agents Schema](https://github.com/dbt-labs/agents_schema), a dbt Labs project that defines warehouse tables an agent can query to learn what a model, column or metric means. You run it after each dbt Production run. It writes the models, columns and lineage from the run's manifest, the Semantic Layer metrics converted to OSI (Open Semantic Interchange), and `agent/AGENTS.md` as a skill. Any tool that runs SQL on the warehouse can read those tables.
- `sql/` and `render_sql.py`: Snowflake DDL templates for what dbt cannot create. They create a connection to the [remote dbt MCP server](https://docs.getdbt.com/docs/dbt-ai/about-mcp), a [Cortex Search](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-overview) service over the campaign text, `agents_context`, a read-only procedure over the `AGENTS` tables, and a [Cortex Agent](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents) that uses all three. There is also a teardown script. `render_sql.py` fills in your database, schema, warehouse, dbt host and dbt project ID.
- `agent/`: `AGENTS.md` holds the agent's instructions. They cover the rules every answer follows, the metrics and saved queries, seven checked questions with their metric queries, SQL and answers, and the answer format. `sql/04_agent.sql` embeds the same rules. `osi_context.yaml` holds the synonyms and usage rules the publish adds to each metric.

The pack sits beside the `connector/` directory, not inside it, so `fivetran debug` and `fivetran deploy` never package it. The connector does not use the pack.

The connector has two modes. Seed mode calls Cortex during the sync, discovers related vehicles from one seed vehicle, and writes the model's analysis into its own tables. List mode, set by the `models` configuration key, makes no AI call. This pack reads the two list-mode tables, and the agent queries the dbt models and metrics built over them at query time.

Data: National Highway Traffic Safety Administration, Recalls API (public).

## Why an agent needs it

The connector writes one row per campaign per configured make, model and model year, because that is how a dealer or a lender asks the question: "does this vehicle, of this make, model and model year, have a recall campaign?" A recall campaign often covers many vehicles, so it appears on many rows, and the table holds only campaigns NHTSA returned. Facts the agent needs to answer correctly, such as which vehicles were configured, which report dates the sync covers and how NHTSA groups components, are not in it.

To measure the difference, the same questions went to two Snowflake Cortex Agents on 2026-10-22, over the sync of 2026-10-21 (36 configured models, report dates 2025-01-07 to 2026-09-24):

- The first agent had Cortex Analyst over a semantic view of the raw `recalls` table: every column a dimension, one row-count metric, and no descriptions, Search, dbt or instructions.
- The second agent was this pack's agent.

The pack's agent answered all seven checked questions correctly, and so did the same agent in an earlier run. The raw-table agent counted distinct campaigns on its own. Counting rows would have given 477 recalls instead of 163 campaigns, and 158 for Ford instead of 66. Its mistakes came on questions whose answer needs a fact the raw table does not hold:

| What the raw table does not hold | Question | Raw-table agent | Pack agent |
|---|---|---|---|
| The configured models with no recalls | Which configured models had no recall campaigns? | "I can't answer this from the recalls data alone" | 4: Audi A4, BMW 3 Series, Chevrolet Traverse and Mazda CX-5, from `models_without_campaigns` |
| The list of configured models | How many configured models do the campaigns touch? | "173 configured models": it counted distinct make, model and model year combinations in the recall rows | 32 of the 36 configured models |
| The report-date window | How many campaigns did Ford have in the window? | 66, "from January 17, 2025 to September 9, 2026. That's the full date range in the data". Those are the first and last dates of the Ford rows it had filtered | 66, from 2025-01-07 to 2026-09-24. Every answer names the window |
| NHTSA's component groups | Which component comes up most for Ford? | `BACK OVER PREVENTION:SOFTWARE`, 5 campaigns: it ranked the raw colon-delimited `component` strings | POWER TRAIN, 13 campaigns, grouped on `component_group` |

The raw-table agent named no window in four of its seven answers. In its answer to the zero-recall question, it said why: "There's also no list of configured or tracked models, and no defined reporting window."

The connector's `configured_vehicle` table answers the first two questions. It has one row per configured make, model and model year, including the vehicles with no campaign. The dbt model `nhtsa_model_coverage` is built on it, and it is the only model that shows a configured model had no campaigns.

## What dbt gives the agent

The agent gets two things from the dbt project: definitions in the warehouse, which say what each model, column and metric means, and numbers through the Semantic Layer, which computes every count.

After each Production run, `publish_agents_schema.py` writes the project's definitions into `<pack_database>.AGENTS`: every model and column with its description and data type, the lineage between the models, and every metric with its aggregation, its filter, the saved queries that use it and the SQL the dbt Semantic Layer compiles for it. `agent/osi_context.yaml` adds the pack's synonyms and rules to the metrics, for example `recalls` as a synonym of `campaign_count`, and the note that `affected_vehicle_rows` is not a recall count. The agent's `agents_context` tool reads those tables when a question asks what a field means or how a number is defined.

Every count the agent gives comes from `query_metrics` on the remote dbt MCP server, and its instructions tell it never to compute a count itself. What a campaign is about comes from Cortex Search. When the agent explains how a count is defined, it reads the definition of the metric the Semantic Layer ran, as long as you published the Production run that built it.

On 2026-10-25, with both in place, the agent answered all seven checked questions correctly in each of two runs. None of the seven called `agents_context`, since each one asks for a count or a list, but two smoke questions did. Asked what `park_it` means and which model it is built from, the agent returned both published `park_it` definitions and the lineage from `nhtsa_recall` to `nhtsa_campaign`. Asked how `campaign_count` is defined and why it differs from `affected_vehicle_rows`, it looked up each metric once and answered from their definitions, `COUNT(DISTINCT nhtsa_campaign_number)` against a count of rows, and it gave no count from the data.

## What the pack builds

### The dbt models

The dbt project `nhtsa_recalls_context` builds every model as a view in the pack schema. A view always reads the latest sync.

| Model | One row per | What it adds |
|---|---|---|
| `stg_nhtsa__recalls`, `stg_nhtsa__configured_vehicle` | Source row | Renames, types and drops rows with `_fivetran_deleted = true` |
| `nhtsa_recall` | Campaign x configured vehicle | `component_group`, the first part of NHTSA's colon-delimited `component`. The grain every recall count is taken at |
| `nhtsa_campaign` | Campaign | The makes and models it covers, its model-year range, its vehicle-row count, one copy of its text, and flags that are true when any covered row is true and null when unknown |
| `nhtsa_component` | Component group x make | Campaign counts per component group and make |
| `nhtsa_coverage` | Configured make x model x model year | `campaign_count`, 0 when the connector holds no campaign for the vehicle |
| `nhtsa_model_coverage` | Configured make x model | `has_campaigns`. A model that was configured and had none is `false`. A model that was never configured has no row |
| `nhtsa_sync_window` | (one row) | The first and last report dates and the sync that produced them |
| `nhtsa_campaign_search` | Campaign | The document Cortex Search indexes: `search_text`, plus `makes` and `models` as sorted arrays |
| `metricflow_time_spine` | Day | The daily spine MetricFlow needs |

The semantic models, metrics and dimensions are declared on the models themselves (`semantic_model:`, `dimension:` and `metrics:` in `_marts.yml`). The remote dbt MCP server was tested with this style. The USGS Earthquake pack declares them in a separate `semantic_models:` block.

The project has 29 data tests. Two reconcile the models against each other: every campaign's rows add up, and `nhtsa_coverage` matches `campaigns_returned`, the connector's count of the campaign rows it kept for each vehicle (not what the API returned) and has one vehicle with campaigns for each vehicle `affected_vehicles` counts. Another checks that every campaign has exactly one component group.

### The metrics

| Metric | What it counts |
|---|---|
| `campaign_count` | Distinct campaigns. The answer to every "how many recalls" question |
| `affected_vehicle_rows` | Campaign x configured vehicle rows. Not a recall count, and not a vehicle count: a vehicle covered by two campaigns counts twice |
| `affected_vehicles` | Distinct configured vehicles (make, model, model year) that at least one campaign covers |
| `park_it_campaigns`, `park_outside_campaigns`, `over_the_air_campaigns` | Campaigns NHTSA flagged: do not drive, park outside, remedy delivered over the air |
| `multi_model_campaigns` | Campaigns that cover more than one configured model |
| `first_report_date`, `last_report_date` | The report-date window |
| `configured_models`, `models_with_campaigns`, `models_without_campaigns` | Configured make and model pairs, with and without a campaign |

Six saved queries cover the common question shapes: `report_window`, `campaigns_by_make_component`, `campaigns_by_model_year`, `flagged_campaigns`, `models_without_campaigns` and `multi_model_campaigns`. The dbt MCP server lists them, and the agent passes their metrics, group-by and filter to `query_metrics`.

### The Snowflake objects

Render each template with `render_sql.py`, then run it. The order matters:

| File | Creates | Run it |
|---|---|---|
| `sql/00_dbt_mcp.sql` | An API integration (`EXTERNAL_MCP`, OAuth) and an external MCP server pointing at `https://<dbt_host>/api/ai/v1/mcp` | First. Then each Snowflake user who will run the agent connects once: Snowflake Intelligence > Settings > MCP Connectors > the server > Connect |
| `sql/02_agents_context.sql` | `agents_context`, a read-only procedure over Agents Schema (`<pack_database>.AGENTS`) that returns a model's or column's dbt description, data type and lineage, or a metric's definition, looked up by its name or a synonym | After the dbt Production job and the Agents Schema publish (below) |
| `sql/03_search_service.sql` | `nhtsa_campaign_search_service`, one document per campaign, filterable on `makes`, `models` and `component_group` | After the dbt Production job has built `nhtsa_campaign_search` |
| `sql/04_agent.sql` | `nhtsa_recalls_agent`, with the dbt MCP server, `campaign_search` and `agents_context` as its tools, the rules from `AGENTS.md` and the seven checked questions as sample questions | After `02` and `03` |
| `sql/05_reader_grants.sql` | Grants a reader role what asking the agent needs: `USAGE` on the pack database and schema, the agent, the search service, `agents_context`, the warehouse, the MCP server and its integration, the `SNOWFLAKE.CORTEX_AGENT_USER` database role, and `SELECT` on the current and future `AGENTS` tables, so the table grants survive each publish, which recreates the tables | When a role that owns none of the pack's objects asks the agent, after `04`. Again after re-running `02` or `03`, which drop the grants on the procedure and the search service. `04` keeps the agent's grants (`COPY GRANTS`) |
| `sql/98_reader_revoke.sql` | Revokes every grant `05` made | Before `99`, when `05` ran. Leave out the grants the role needs for other work, such as the warehouse |
| `sql/99_teardown.sql` | Drops the agent, the search service, `agents_context`, the MCP server and the dbt views. The `AGENTS` tables stay, because other agents may read them, and so does the API integration, which is account-level and other MCP servers can share. Drop `<pack_database>.AGENTS`, and the integration (the statement is in the file's header), yourself when nothing else uses them | To remove the pack. Dropping the MCP server or the integration removes every user's consent |

From this directory:

```bash
python render_sql.py --pack-database <pack_database> --pack-schema <pack_schema> --warehouse <warehouse> \
    --dbt-host <dbt_host> --project-id <dbt_project_id> --output rendered/pack.sql \
    sql/00_dbt_mcp.sql sql/02_agents_context.sql sql/03_search_service.sql sql/04_agent.sql

# Only when a role that owns none of the pack's objects asks the agent; run it with MANAGE GRANTS.
# The same flags render sql/98_reader_revoke.sql, which undoes it.
python render_sql.py --pack-database <pack_database> --pack-schema <pack_schema> --warehouse <warehouse> \
    --reader-role <reader_role> --output rendered/reader_grants.sql sql/05_reader_grants.sql
```

`rendered/` is in the pack's `.gitignore`, because the rendered SQL holds your real names.

**Cost.** The search service refreshes on your warehouse at most once per `--target-lag` (default 1 day) and has a serving charge while it exists. Each agent question is billed for its Cortex tokens. In two test runs of the seven checked questions on 2026-10-22, each question used between 178,000 and 273,000 input tokens, most of them read from the prompt cache, and between 722 and 2,638 output tokens, and each answer took 14 to 37 seconds over three runs. On 2026-10-25, in six runs of the agent with `agents_context`, each answer took 14.8 to 34.2 seconds. The dbt Semantic Layer queries run on the warehouse of your dbt Semantic Layer configuration. `99_teardown.sql` removes every Snowflake object that can be billed.

### The Agents Schema publish

`publish_agents_schema.py` runs the [`agents-schema`](https://pypi.org/project/agents-schema/) CLI (version 0.0.11). Around it, the script downloads the manifest and the semantic manifest from a dbt platform Production run, so the published relations are the Snowflake ones and not a local build, converts the Semantic Layer to OSI, and copies `agent/AGENTS.md` into a skill file whose frontmatter ties it to the pack schema, so the shipped file is not edited.

Agents Schema 0.0.11 has no dbt metric table: semantic-layer metrics go into its `OSI_*` tables, in OSI form. The `osi` step converts the run's `semantic_manifest.json` with MetricFlow's own converter (`metricflow.converters.msi_to_osi`, in MetricFlow 0.213.0), which dbt v2 does not run yet. The converter writes OSI 0.1.1 and the CLI validates against OSI 0.2.0.dev0, so the script moves the document to 0.2.0.dev0 (it drops the document-level `dialects` and renames each relationship's `from_dataset` to `from`) and validates it against the CLI's own schema before anything is written. It then adds each metric's dbt definition, the SQL the Semantic Layer compiles for it through the `compileSql` GraphQL mutation, which runs no query, and the synonyms and rules in `agent/osi_context.yaml`. A metric or dataset name in that file that the semantic manifest does not have stops the step.

From this directory, after a successful Production run:

```bash
pip install 'agents-schema==0.0.11' 'metricflow==0.213.0'

python publish_agents_schema.py fetch --run-id <dbt_run_id> --work-dir agents_schema_work
python publish_agents_schema.py osi --work-dir agents_schema_work --model-name nhtsa_recalls_context \
    --context agent/osi_context.yaml
python publish_agents_schema.py publish --work-dir agents_schema_work \
    --skill nhtsa-recalls-agent=agent/AGENTS.md --uses-schema <pack_schema>
```

| Step | What it does | Settings it needs |
|---|---|---|
| `fetch` | Downloads `manifest.json` and `semantic_manifest.json` of the run through the dbt Admin API. Refuses a run that did not succeed, a run outside the Production environment, whose schemas the agent does not read, and a manifest with no models or a semantic manifest with no metrics, which would empty the published tables | `DBT_HOST`, `DBT_ACCOUNT_ID`, `DBT_PROD_ENV_ID`, and a `DBT_TOKEN` that can read the run's artifacts |
| `osi` | Writes `agents_schema_work/osi/nhtsa_recalls_context.osi.yaml`. Writes nothing remote | `DBT_HOST`, `DBT_PROD_ENV_ID` and a Semantic Layer `DBT_TOKEN`, for the compiled SQL. `--no-compiled-sql` skips it |
| `publish` | Runs `agents-schema dbt`, `agents-schema osi` and `agents-schema skills`, and refuses to start without OSI files converted from the fetched run, so the `DBT_*` and `OSI_*` tables never come from different runs. The CLI creates `AGENTS` in the database of the credentials if it does not exist, merges the skill rows into `ROOT`, and replaces the `DBT_*`, `OSI_*` and `SKILL_USE` tables. Replacing a table drops the rows another project published in it and every grant on it | `WAREHOUSE_CREDENTIALS`: the CLI's Snowflake credentials as JSON, with `type` `snowflake`, `account`, `user`, `warehouse`, `database` (the pack database), an optional `role`, and `private_key_path`, `private_key_pem` or `password` |

Each step reads its settings from the environment, or from a file of `KEY=VALUE` lines passed with `--env-file`, so no token goes on the command line, and the script prints none of the values it reads. `agents_schema_work/` is in the pack's `.gitignore`.

A publish from a dbt Production run on 2026-10-25 wrote these rows:

| Table | Rows | From |
|---|---|---|
| `DBT_MODEL` | 10 | `manifest.json` |
| `DBT_COLUMN` | 71 | `manifest.json`. Every column has a description |
| `DBT_DEPENDENCY` | 13 | `manifest.json` |
| `OSI_MODEL`, `OSI_DATASET`, `OSI_FIELD`, `OSI_METRIC`, `OSI_RELATIONSHIP` | 1, 3, 30, 11, 1 | `semantic_manifest.json`. Every metric carries its compiled SQL |
| `ROOT` | 15 | The CLI's pointer rows for each source (4 for dbt, 6 for OSI, 3 for skills), its own `skill/agents-schema-analyst`, and `skill/nhtsa-recalls-agent` from `AGENTS.md` |
| `SKILL_USE` | 1 | `skill/nhtsa-recalls-agent` reads the pack schema |

## Requirements

- A sync of the connector into Snowflake: a [Fivetran Managed Data Lake](https://fivetran.com/docs/destinations/managed-data-lake-service) read in place through a catalog-linked database, or a standard Fivetran Snowflake destination.
- A dbt platform account on the Starter or Enterprise plan, on a static subdomain (for example `abc123.us1.dbt.com`), with the remote dbt MCP server's OAuth connection enabled.
- A dbt platform project for `context_pack/dbt/` on a Snowflake connection, with:
  - a Production environment and job that run `dbt build`;
  - the Semantic Layer configured on that environment;
  - an analytics credential on the connection selected in the project's Analytics configuration (set in the dbt platform UI).
- A Snowflake role with:
  - `CREATE INTEGRATION` on the account;
  - `CREATE EXTERNAL MCP SERVER`, `CREATE CORTEX SEARCH SERVICE`, `CREATE PROCEDURE` and `CREATE AGENT` on the pack schema;
  - `USAGE` on the warehouse;
  - `SELECT` on the dbt views and on the `AGENTS` tables (`05_reader_grants.sql` grants a reader role everything it needs to ask the agent, but not to build the pack);
  - the `SNOWFLAKE.CORTEX_USER` database role.

  Each template lists what it needs in its header. The test runs used an administrator role, so this list comes from the templates, not from a least-privilege run.
- For the Agents Schema publish:
  - Python with `agents-schema` 0.0.11 and `metricflow` 0.213.0 (tested on Python 3.12);
  - a dbt platform token that can read the Production run's artifacts, and, optionally, a Semantic Layer token for the compiled SQL;
  - for the credentials in `WAREHOUSE_CREDENTIALS`, a role that can create a schema in the pack database (the CLI creates `AGENTS` on its first run) and create and replace tables in it;
  - a pack database whose `AGENTS` schema no other dbt project publishes to. The publish replaces the `DBT_*`, `OSI_*` and `SKILL_USE` tables, so a second project publishing into the same schema would replace the pack's model, column, lineage and metric rows, and the pack's publish would replace that project's.
- For local testing only: the dbt CLI with DuckDB, and a local sync of the connector, so that `../connector/files/warehouse.db` exists. The project uses dbt v2 settings (`info_schema` in `dbt_project.yml`) that dbt-core 1.x rejects, so the pack has no `requirements.txt`: use the dbt v2 CLI, which ran the local build (dbt 2.0.9), or the dbt platform. `render_sql.py` needs only the Python standard library. `publish_agents_schema.py` needs only the two packages above.

## Connect your AI to this data

### A Snowflake Cortex Agent

This is the path the pack was built and tested on.

1. Sync the connector.
2. Set `lake_database` and `lake_schema` in `dbt/dbt_project.yml` to the database and schema that hold the connector's `recalls` and `configured_vehicle` tables, or pass both with `--vars` on the job command.
3. Run the Production job.
4. Publish the run to Agents Schema with `publish_agents_schema.py` ([The Agents Schema publish](#the-agents-schema-publish)).
5. Render and run `00_dbt_mcp.sql`, then connect once in Snowflake Intelligence.
6. Render and run `02_agents_context.sql`, `03_search_service.sql` and `04_agent.sql`. If a role that owns none of the pack's objects will ask the agent, also render `05_reader_grants.sql` with `--reader-role <role>` and run it with `MANAGE GRANTS`. Its future grant on the `AGENTS` tables is schema-level, and Snowflake then ignores database-level future grants on tables for that schema: repeat any you rely on at schema level (the file's header has the statement). Each reader still connects to the MCP server once in Snowflake Intelligence.
7. Ask the agent in Snowflake Intelligence, or from SQL:

```sql
select snowflake.cortex.data_agent_run('<pack_database>.<pack_schema>.nhtsa_recalls_agent',
  $${"messages": [{"role": "user", "content": [{"type": "text", "text": "Which configured models had no recall campaigns in the window?"}]}]}$$);
```

The instructions pin the agent to your dbt project ID. Without the pin, a user who consented to every dbt project in the account lets the agent choose a project, and in testing it chose a different one.

### Any MCP client on the dbt Semantic Layer

Claude Code, Cursor and other MCP clients can query the same metrics through a dbt MCP server for your project. Load `agent/AGENTS.md` as the client's instructions. On 2026-10-22, Claude Code queried this project's Semantic Layer through a local dbt MCP server and returned `campaign_count` = 66 for Ford and `models_without_campaigns` = 4 of 36 `configured_models`, the same as the Cortex Agent. Questions about what a campaign is about, such as fire risk, need the Cortex Search service or a SQL tool over the campaign text.

### An agent with a SQL tool

Every checked question in `AGENTS.md` carries the SQL behind its metric query, written against the dbt models. An agent with a SQL tool and `AGENTS.md` as its instructions can answer from the pack schema with no MCP server. It can also read the definitions from `<pack_database>.AGENTS`, the same tables `agents_context` reads, starting from the `ROOT` table, which lists what the others hold.

The role the SQL tool runs as needs `SELECT` on the views in the pack schema, which `05_reader_grants.sql` does not grant: the Cortex Agent never reads them directly. dbt recreates every view on each Production run, so a grant on one view lasts only until the next run. Grant on the schema's current and future views instead, once: `grant select on all views in schema <pack_database>.<pack_schema> to role <role>;` and `grant select on future views in schema <pack_database>.<pack_schema> to role <role>;`. The schema-level future grant has the same effect on database-level future grants as the one `05` makes on the `AGENTS` tables (see the `05` header).

### A Managed Data Lake or a standard Snowflake destination

The live path and every test run read the Managed Data Lake through a catalog-linked database, where the schema, table and column names stay lowercase and must be double-quoted. `quote_lake_identifiers: true` in `dbt_project.yml` does that. For a standard Fivetran Snowflake destination:

1. Set `quote_lake_identifiers` to `false`, because its names are uppercase and unquoted.
2. Turn on change tracking on the `recalls` table so the search service can refresh incrementally: `alter table <recalls table> set change_tracking = true`.

These two settings were not tested.

## Checked questions

`agent/AGENTS.md` holds seven questions, each with its metric query, its SQL and the answer it returned on the sync of 2026-10-21:

1. How many recall campaigns did Ford have in the window, and which component group comes up most? (66; POWER TRAIN, 13)
2. How many recall campaigns are in the window, and how many configured models do they touch? (163; 32 of 36)
3. Which Ford Explorer model years have the most recall campaigns? (2025, 2021, 2020)
4. Which campaigns advise parking outside, or carry an over-the-air remedy? (4 park outside, 1 flagged over the air)
5. Which configured models had no recall campaigns in the window? (4)
6. Which campaigns involve a fire risk? (15 name fire in their consequence)
7. Which campaigns cover more than one configured model? (26)

The answers carry the report-date window they came from. When NHTSA publishes newer campaigns and the connector syncs them, the queries still run, but the numbers change. The agent reports its own result, not the checked one.

## Limitations

- The data is NHTSA's campaign records for the configured vehicles. It holds no VINs, owner complaints or investigations, and nothing about a make or model the connector was not configured to query.
- Cortex Search returns a ranked list, not a complete one. A `fire risk` search with 15 results returned 11 of the 15 campaigns whose consequence names fire, and NHTSA's own wording returned 12. The agent counts what Search returns and says the list may be partial.
- NHTSA's over-the-air flag is narrower than the campaign text. On the sync of 2026-10-21, one campaign carried the flag, while 37 mentioned an over-the-air update in their text. `over_the_air_campaigns` counts the flag.
- The report-date window every answer names (`first_report_date` to `last_report_date`) is the earliest and latest report date among the campaigns synced, not the window the connector queried (`report_date_from` to `report_date_to`). The two differ whenever no campaign was reported on the first or last day of the configured window, and a campaign the connector kept because its record could not be read can carry a date outside a configured window that has since changed. An answer that a model had no campaigns holds for the configured window, which is usually wider than the dates the answer names. The pack does not carry the configured window, so quote it from the connector's configuration when an absence answer needs it.
- When NHTSA answers no recalls for a whole model year it listed campaigns for before, the connector keeps those campaigns, not deletes them, as long as they were tracked with the same `nhtsa_model` in any letter case and under a report window the current one contains, because that is more likely an NHTSA fault than a withdrawal. A single campaign dropped from a year that still lists others is deleted. Nothing in the views marks it: the connector sends a sync warning naming the model years, and a full re-sync clears it.
- The views read what the connector has checkpointed. A sync writes one model at a time, so an answer read while it runs can mix states: after a first sync or a re-sync that truncates both tables it covers only the models finished so far, a model added to the configuration has no rows until the sync reaches it, the models not reached yet still hold what the last sync wrote, and a model removed from the configuration keeps its rows until the pass ends. Nothing in the views marks a pass as complete. The agent gives `configured_models` with an answer of zero campaigns so a short count shows; compare it with the number of models configured.
- Snowflake supports only OAuth for external MCP servers, so each Snowflake user who runs the agent connects to the dbt MCP server once. Recreating the integration or the MCP server means connecting again.
- At the time of writing, the remote dbt MCP server's OAuth connection was in beta, and it needs a Starter or Enterprise plan and a static subdomain.
- The agent sometimes writes a short working note between tool calls, apart from its final answer. This happened on one question in each of three test runs, and the instructions do not stop it. An application that displays the answer should display only the text after the last tool result.
- The Agents Schema publish is a step you run after a Production run, not part of the dbt job. Until you run it again, the `AGENTS` tables describe the last run you published, even if a later run changed a model, column or metric.
- `agents-schema` 0.0.11 replaces the `DBT_*`, `OSI_*` and `SKILL_USE` tables on every publish instead of merging rows into them. The pack therefore needs an `AGENTS` schema of its own, and a grant on one of those tables lasts only until the next publish, which is why `05_reader_grants.sql` grants on future tables.
- The `osi` step was tested with one pair of versions: MetricFlow 0.213.0, whose converter writes OSI 0.1.1, and `agents-schema` 0.0.11, which validates OSI 0.2.0.dev0. If a later CLI expects a different OSI version, the step stops and prints the schema errors instead of writing.
- Keep the pack beside the connector, not inside it. `fivetran deploy` packages every file in the connector directory.

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
