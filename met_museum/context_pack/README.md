# Met Museum Context Pack

The `met_museum` connector syncs public-domain objects from The Metropolitan Museum of Art's collection, with their curator tags and, when enrichment is on, what a Claude model on Snowflake Cortex saw in each object's image. This pack is the context an AI agent needs to answer questions from that data correctly:

- `dbt/`: a dbt v2 project that builds three models over the connector's six tables, with the metrics and saved queries an agent counts with.
- `agent/AGENTS.md`: instructions for the agent. They say which models to use, the rules each query follows, five checked questions with their SQL and answers, and the answer format.
- `parity.py`: runs the models and the checked questions on Snowflake and on DuckDB over the same Managed Data Lake tables, and compares the results.
- `sql/05_reader_grants.sql`: the grants a role that owns none of the pack's objects needs to read the views and the `AGENTS` tables.

The pack sits beside the connector directory, `met_museum/connector/`, not inside it, so `fivetran debug` and `fivetran deploy` never package it. The connector does not use it.

Data: The Metropolitan Museum of Art Collection API (public domain, CC0).

## Why an agent needs it

The connector writes what the API and the model return, one table per kind of record. That keeps every record the connector read and makes each table easy to check against the API. An agent that queries those six tables directly can still return a wrong answer with no error to warn it.

Ask which paintings show cypress trees, and the answer depends on which source the agent reads. The curators tag 4 objects in European Paintings `Cypresses`. The model names a cypress in the images of 16, and 3 objects are in both, so each source finds objects the other one misses. `AGENTS.md` tells the agent to give both results and say which source each came from. For the 1880s question in the checked list, both sources return the same two Van Gogh paintings from 1889 (436535 *Wheat Field with Cypresses* and 437980 *Cypresses*), so that answer doesn't depend on the enrichment at all.

The element table exists because of searches like oak. `fct_objects_enriched` keeps the model's elements as one ' | '-separated string for reading, and a `like '%oak%'` on that string matches 383 objects, most of them because the model saw a cloak (376 objects have a `cloak` element). Filtering `fct_object_depicted_element` on the oak elements themselves, `oak` and `oak tree`, finds 2.

Curator agreement is the other place a query on the raw tables goes wrong with no error. Dividing matched tags by scored tags across every object gives 2,682 of 7,416, or 0.362. Averaging each object's own agreement gives 0.331 over the 2,341 objects that have a scored tag, because that weighs an object with one tag the same as an object with ten. The pack's `curator_tag_agreement` metric uses the first.

## What the pack builds

### The dbt models

| Model | One row per | What it adds to the raw tables |
|---|---|---|
| `fct_objects_enriched` | live public-domain object | The catalog record, the current curator tags as a sorted list, the decade, the AI columns, and the curator agreement for the object. The AI columns are null when the object was not enriched |
| `fct_object_depicted_element` | thing the model saw in a live object's image | Only the current enrichment's elements, so an element an earlier enrichment named and the latest one did not never reaches an answer |
| `fct_object_tag` | current curator tag on a live object | Only the tags the Met reports today |

The staging models under `dbt/models/staging/` cast every column to a type Snowflake and DuckDB name the same way, and apply the connector's stamp rule. The API returns only an object's current tags and constituents, so a removed one stays in the destination with an older stamp. A tag or constituent row is current only when its `object_metadata_date` equals the live object's `metadata_date`, and a depicted element only when its `enrichment_hash` equals the object's current enrichment. Staging keeps the current rows and drops the rest. Every model is a view and reads the latest sync. dbt decides the filter on `_fivetran_deleted` when it builds a view. A connector table gets that column with its first delete, and a view built before then has no filter. Run the Production job after the sync that first deletes from a table, or set the `fivetran_deleted_tables` var to the tables that have the column. The marts are public with enforced contracts, so a column type change fails the build before an agent sees it.

### The metrics

| Metric | What it counts |
|---|---|
| `public_domain_object_count` | Objects synced. Every row is public domain |
| `enriched_object_count` | Objects with an enrichment |
| `curator_tags_matched_total`, `curator_tags_scored_total` | Curator tags that match a depicted element, and curator tags compared |
| `curator_tag_agreement` | Matched tags over scored tags, so each tag weighs the same |

Three saved queries cover the common shapes: `objects_by_subject`, `curator_agreement_by_subject` and `objects_by_department_decade`.

### The parity check

`parity.py` has two lanes.

- `compute` compiles the dbt project for Snowflake and for DuckDB, inlines every model as a CTE ahead of each checked query, and runs the queries on both engines over the same Iceberg tables. Nothing is created in Snowflake or in the lake. Every result is normalized and hashed, and a check passes only when both engines return the same rows in the same order. On October 31, 2026, 11 of 11 checks returned identical results, including all 2,365 rows of `fct_objects_enriched`. That run read the lake after the connection's third sync, which updated 15 objects and deleted 1 the Met no longer marks public domain.
- `ai` runs Snowflake Cortex `AI_FILTER`, `AI_CLASSIFY`, `AI_COMPLETE` and `AI_AGG` over `visual_description`, against DuckDB functions with the same names that call the Anthropic API. Two models never answer the same way, so this lane reports an agreement rate per function instead of pass or fail. On October 31, 2026, after the retry sync, with claude-sonnet-5 on both sides and a sample of 25 objects taken in turn from each subject category, `AI_FILTER` agreed on 25 of 25 (6 true answers on each side), `AI_CLASSIFY` on 16 and `AI_COMPLETE` on 22. `AI_AGG`'s answers for the 12 subject groups had a mean term overlap (Jaccard) of 0.38, against 0.47 in the run before the retry sync.

### The Agents Schema publish

[Agents Schema](https://pypi.org/project/agents-schema/) publishes a dbt project's models, its Semantic Layer metrics and an agent's instructions as tables in an `AGENTS` schema in Snowflake, so an agent can read that context with SQL. `publish_agents_schema.py` runs the `agents-schema` CLI (version 0.0.11). Around it, the script downloads the manifest and the semantic manifest from a dbt platform Production run, so the published relations are the Snowflake ones and not a local build. It converts the Semantic Layer to OSI, and copies `agent/AGENTS.md` into a skill file whose frontmatter ties it to the pack schema, so the shipped file is not edited.

Agents Schema 0.0.11 has no dbt metric table: semantic-layer metrics go into its `OSI_*` tables, in OSI form. The `osi` step converts the run's `semantic_manifest.json` with MetricFlow's own converter (`metricflow.converters.msi_to_osi`, in MetricFlow 0.213.0), which dbt v2 does not run yet. The converter writes OSI 0.1.1 and the CLI validates against OSI 0.2.0.dev0, so the script moves the document to 0.2.0.dev0 and validates it against the CLI's own schema before anything is written. It then adds each metric's dbt definition and the SQL the Semantic Layer compiles for it through the `compileSql` GraphQL mutation, which runs no query. Last, it adds the synonyms and rules in `agent/osi_context.yaml`, such as `artworks` and `paintings` for `public_domain_object_count`, and the rule that `curator_tag_agreement` divides matched tags by scored tags and is never an average of the per-object column. A metric or dataset name in that file that the semantic manifest does not have stops the step.

From this directory, after a successful Production run:

```bash
pip install 'agents-schema==0.0.11' 'metricflow==0.213.0'

python publish_agents_schema.py fetch --run-id <dbt_run_id> --work-dir agents_schema_work --env-file <admin_env_file>
python publish_agents_schema.py osi --work-dir agents_schema_work --model-name met_museum_context \
    --context agent/osi_context.yaml --env-file <semantic_layer_env_file>
python publish_agents_schema.py publish --work-dir agents_schema_work \
    --skill met-museum-agent=agent/AGENTS.md --uses-schema <pack_schema>
```

| Step | What it does | Settings it needs |
|---|---|---|
| `fetch` | Downloads `manifest.json` and `semantic_manifest.json` of the run through the dbt Admin API. Refuses a run that did not succeed, a run outside the Production environment, whose schemas the agent does not read, and a manifest with no models or a semantic manifest with no metrics, which would empty the published tables | `DBT_HOST`, `DBT_ACCOUNT_ID`, `DBT_PROD_ENV_ID`, and a `DBT_TOKEN` that can read the run's artifacts |
| `osi` | Writes `agents_schema_work/osi/met_museum_context.osi.yaml`. Writes nothing remote | `DBT_HOST`, `DBT_PROD_ENV_ID` and a Semantic Layer `DBT_TOKEN`, for the compiled SQL. `--no-compiled-sql` skips it |
| `publish` | Runs `agents-schema dbt`, `agents-schema osi` and `agents-schema skills`, and refuses to start without OSI files converted from the fetched run. The CLI writes each step's tables in place. When `osi` or `skills` fails after `dbt` succeeded, the `DBT_*` tables are from the new run and the `OSI_*` tables are from an earlier one. Run `publish` again. When every step succeeds, both sets of tables are from the same run. The CLI creates `AGENTS` in the database of the credentials if it does not exist, merges the skill rows into `ROOT`, and replaces the `DBT_*`, `OSI_*` and `SKILL_USE` tables. Replacing a table drops the rows another project published in it and every grant on it, so give each pack its own database, and keep reader access with `sql/05_reader_grants.sql`, whose future grant applies to each table as the CLI recreates it | `WAREHOUSE_CREDENTIALS`: the CLI's Snowflake credentials as JSON, with `type` `snowflake`, `account`, `user`, `warehouse`, `database` (the pack database), an optional `role`, and `private_key_path`, `private_key_pem` or `password` |

Each step reads its settings from the environment, or from a file of `KEY=VALUE` or `export KEY=VALUE` lines passed with `--env-file`, so no token goes on the command line, and the script prints none of the values it reads. `agents_schema_work/` is in the pack's `.gitignore`.

A publish from a dbt platform Production run on October 31, 2026 wrote these rows:

| Table | Rows | From |
|---|---|---|
| `DBT_MODEL` | 10 | `manifest.json`: the three marts, six staging models and the MetricFlow time spine |
| `DBT_COLUMN` | 57 | `manifest.json`. Every mart column has a description. 11 staging columns have none, most of them keys |
| `DBT_DEPENDENCY` | 17 | `manifest.json` |
| `OSI_MODEL`, `OSI_DATASET`, `OSI_FIELD`, `OSI_METRIC`, `OSI_RELATIONSHIP` | 1, 1, 14, 5, 0 | `semantic_manifest.json`. The one dataset is `fct_objects_enriched`, so there is no relationship to publish. Every metric carries its compiled SQL |
| `ROOT` | 15 | The CLI's pointer rows for each source (4 for dbt, 6 for OSI, 3 for skills), its own `skill/agents-schema-analyst`, and `skill/met-museum-agent` from `AGENTS.md` |
| `SKILL_USE` | 1 | `skill/met-museum-agent` reads the pack schema |

## Requirements

- A sync of the connector with enrichment on, into a [Fivetran Managed Data Lake](https://fivetran.com/docs/destinations/managed-data-lake-service) or a standard Fivetran Snowflake destination. Without enrichment the models build, and every AI column is null.
- The dbt v2 CLI or the dbt platform. The project uses dbt v2 settings (`info_schema` in `dbt_project.yml`) that dbt-core 1.x rejects, so the pack has no `requirements.txt` for dbt. The local build ran on dbt 2.0.9.
- For the Snowflake target: a role with `USAGE` on the warehouse and on the database that holds the connector's tables, `SELECT` on those tables, and `CREATE VIEW` on the pack schema. `profiles.yml` reads every connection value from the environment and uses key-pair authentication.
- For `parity.py`: Python 3.12 with `duckdb` and `snowflake-connector-python`, plus `anthropic` and `numpy` for the ai lane (DuckDB 1.5.6 needs numpy to register the Python functions that stand in for Cortex). A Snowflake connection in `~/.snowflake/connections.toml`, and for the lake, `lake.json` (copy `lake.example.json`) with the Polaris client secret in `secrets/polaris_client_secret`.
- For local testing only: a local `fivetran debug` sync of the connector, so that `../connector/files/warehouse.db` exists.

## Connect your AI to this data

### An agent with a SQL tool on Snowflake

1. Sync the connector with enrichment on.
2. Set `lake_database` and `lake_schema` in `dbt/dbt_project.yml` to the database and schema that hold the connector's tables, or pass both with `--vars`. For a standard Fivetran Snowflake destination, also set `quote_lake_identifiers` to `false`.
3. From `dbt/`, run `dbt build --profiles-dir . --target snowflake`.
4. Give the agent `agent/AGENTS.md` as its instructions and a tool that runs SQL as a role with `SELECT` on the pack schema's views. Then ask a question, for example "Show me public-domain landscapes with cypress trees from the 1880s." The agent should answer with the checked SQL from `AGENTS.md`.

### An agent with DuckDB on the lake

DuckDB reads the same Iceberg tables in place through the Managed Data Lake's Polaris catalog. Attach the catalog with vended credentials, and set `DEFAULT_REGION` to the lake's region, because the catalog vends credentials without one. Then compile the project for the `lake` target (`dbt compile --profiles-dir . --target lake`) and create its compiled models as views in the session, or let the agent run the checked SQL with `parity.py`'s CTE inlining. Never put the catalog credentials in a tracked file.

### Any MCP client on the dbt Semantic Layer

Claude Code, Cursor and other MCP clients can query the pack's metrics through a [dbt MCP server](https://github.com/dbt-labs/dbt-mcp) for your dbt platform project, with a Semantic Layer token. Load `agent/AGENTS.md` as the client's instructions. On October 31, 2026, Claude Code queried this project's Semantic Layer through a local dbt MCP server and returned `public_domain_object_count` = 2,365, `enriched_object_count` = 2,365 and `curator_tag_agreement` = 0.362, the same numbers `AGENTS.md` gives for that sync. The Semantic Layer covers `fct_objects_enriched` only. A question about what the images show or how the curators tag them, such as which paintings show cypress trees, needs a SQL tool over `fct_object_depicted_element` and `fct_object_tag`.

## Checked questions

`agent/AGENTS.md` holds five questions, each with SQL and the answer it returned on October 31, 2026:

1. Which public-domain landscapes from the 1880s show cypress trees?
2. Which 1880s objects do the curators tag as both a landscape and cypresses?
3. What subjects do the objects show, and how many are classified?
4. How often do the model's depicted elements match the curators' tags?
5. How many objects are there per decade?

The answers carry the sync they came from. After a later sync the SQL still runs; the numbers can change.

## Limitations

- The AI columns are a model's reading of a web-size image, not the Met's catalog. A different model, or the same model on a later run, can classify the same image differently. The connector enriches an object again only when its image, catalog text, tags or model change.
- Curator agreement counts matching words after ignoring case and regular plurals. It does not match synonyms, so a tag such as `Women` and an element such as `figure` count as a miss.
- The data covers the departments the connector syncs. The checked answers come from European Paintings alone.
- An enrichment sync is slow: one Cortex vision call per object, one at a time. On a Managed Data Lake the rows appear only when the sync finishes, so the AI columns stay null until then. The connection's second sync, on October 30, 2026, ran 3 hours 52 minutes and left 2,355 of the 2,365 objects enriched.
- 10 objects with images were not enriched in that sync: 435669, 436208, 436570, 436799, 436838, 436836, 437800, 435671, 436569 and 438820. The connector keeps the id of each object whose image download or enrichment fails, and every incremental sync retries those objects. The sync on October 31, 2026 enriched all 10, so all 2,365 objects now have AI columns. An object that fails in a later sync has null AI columns until a retry succeeds.
- The stamp rule hasn't dropped a row yet, because no tag, constituent or element in this data has gone out of date. The parity check's `stamp_rule_rows_dropped` compares source and current rows per table, so the first drop shows up there.
- In each of the three ai lane runs, DuckDB's `AI_AGG` came back empty for at least one subject group: `portrait` in the first run, the null group in the second, and the null group and `marine` in the third, after the retry sync. The cause isn't known yet.
- The ai lane runs on 25 objects, so its rates can move from one run to the next. `AI_FILTER`'s 25 of 25 rests on 6 true answers per side.
- Keep the pack beside the connector, not inside it. `fivetran deploy` packages every file in the connector directory.

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
