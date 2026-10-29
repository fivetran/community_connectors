# CMS Hospital Compare Context Pack

The `cms_hospital_compare` connector lands CMS Care Compare data: star ratings, emergency department times and readmission measures for every Medicare-certified hospital. This pack is the context an AI agent needs to answer questions from that data correctly. It has two files and no code:

- `cms_views.sql`: DuckDB views that read only the latest CMS release the connector recorded, turn CMS's text values into numbers and dates, and set every "Not Available" to NULL.
- `agent/AGENTS.md`: instructions for the agent. They say which views to use, the rules each query follows, four checked questions with their SQL and answers, and the answer format.

The pack sits beside the connector directory, `cms_hospital_compare/connector/`, not inside it, so `fivetran debug` and `fivetran deploy` never package it. The connector does not use it. It makes no LLM call and needs no API key. Any agent that can run DuckDB SQL can use it.

Data: Centers for Medicare & Medicaid Services, Provider Data Catalog (public).

## Why an agent needs it

The connector writes every value as text, exactly as CMS sends it, and keeps every release it loads. That is the right choice for the connector: nothing is lost and every release can be compared. But an agent that queries those tables directly can return a wrong number with no error to warn it.

The table below answers one question four ways over the same sync, run on 2026-10-12 against the CMS releases of 2026-08-13 and 2026-09-30: how many Connecticut hospitals are worse than the nation on heart failure readmissions? The first three queries are plausible first attempts written from the raw column names, and the last one uses the pack.

| Query | Hospitals | What went wrong |
|---|---|---|
| Raw table, `compared_to_national = 'Worse Than the National Rate'` | 0 | CMS left that field `Not Available` for every `READM_30_*` row in this release |
| Raw table, `score > '21.3'` | 15 | A text comparison: 2 of the 15 have the score `Not Available`, which sorts after `21.3` |
| Raw `hrrp` table, `excess_readmission_ratio > '1'` | 14 | A different measure, for an earlier period, and 2 of the 14 have the ratio `N/A` |
| `cms_views.sql` and the checked SQL in `AGENTS.md` | 13 | 13 of the 26 hospitals that reported a rate are above the national 21.3% |

The same text problem breaks other questions. The longest median ED time (`OP_18b`) in the raw table is `max(score) = 'Not Available'`; in the view it is 413 minutes. An average star rating over the raw table fails with `Could not convert string 'Not Available' to INT32`; over `cms_hospital` it is 3.21, across the 3,174 of 5,419 hospitals that have a rating.

Two more mistakes have no number yet, because this sync holds one release per dataset:

- After the next CMS release, the raw tables hold both releases. A count over them counts most hospitals twice. The views read the latest release only.
- While a release is loading, its rows are in the table before it is complete. If CMS replaces the file during the load, those rows stay incomplete. The connector writes a release's `dataset_release` row only after its last page, and only if CMS has not replaced it. The views read only releases that have that row. When CMS reports more records than it returns on two syncs in a row, the connector records the release with the records CMS returned, and `row_count` is below `api_row_count` in `cms_release`; `agent/AGENTS.md` has the agent say so. A release that writes no rows, because CMS returned none or none of its records could be loaded (no natural key, or not an object), is never recorded, so a table can have no row in `cms_release` until its first release loads. A release that loses some records is recorded, and `rows_written` below `row_count` in `cms_release` shows how many were loaded. If CMS shrinks a file during the load so that its count falls below the rows already read, the release is not recorded until the connector re-reads it from the start as a new load attempt. Rows that only the abandoned attempt read stay in the raw table but out of the views, which read only the attempt `load_attempt` in `cms_release` names. If that release later loses its connector state and loads again, it reloads as attempt 1, and those rows are back in the views until CMS publishes the dataset's next release (the connector README's Load attempts bullet has the detail).

## What the views do

`cms_views.sql` creates one view per table, plus a `_history` view for each that keeps every recorded release:

| View | Reads | What changes |
|---|---|---|
| `cms_release`, `cms_release_history` | `dataset_release` | `cms_released` becomes a DATE; `is_latest_release` marks the release the other views read; `row_count` is the records paged, including records skipped (no natural key, or not an object) and repeated keys; `rows_written` the records written to the table, repeats included; `api_row_count` the count CMS reported; `load_attempt` the load attempt whose rows the views read |
| `cms_hospital` | `hospital` | `hospital_overall_rating` and the measure-group counts become integers; `emergency_services` becomes `has_emergency_services` |
| `cms_timely_effective_care` | `timely_effective_care` | `score` becomes a number and `score_text` keeps what CMS sent (the `EDV` band) |
| `cms_unplanned_hospital_visit` | `unplanned_hospital_visit` | Scores, estimates and counts become numbers; `comparison` folds the labels of `compared_to_national` into five values (on `OP_36`, CMS compares with the expected rate, not the national rate) |
| `cms_unplanned_hospital_visit_national` | `unplanned_hospital_visit_national` | `national_rate` and the hospital counts become numbers |
| `cms_hrrp` | `hrrp` | Ratios, rates and counts become numbers; `condition` is the short code (`HF`) |

Every view drops rows with `_fivetran_deleted = true` and turns `start_date` and `end_date` from `MM/DD/YYYY` into DATE. Column names match the connector's tables, so a query moves from one to the other without a rename map.

The views name the tables without a schema. The session puts the schema that holds them on the DuckDB search path, so the same file runs over the local warehouse or a deployed copy. The names are resolved each time a view is queried, so create nothing in the session's own database named like a raw table (`hospital`, `hrrp` and the others): it would hide the warehouse table from every view.

## Requirements

- DuckDB, as the CLI or a library. The SQL was run on DuckDB 1.3.2 (CLI) and 1.5.5 (Python).
- A sync of the connector. `fivetran debug` writes `../connector/files/warehouse.db`, with the tables in the schema `tester`.

## Connect your AI to this data

Every path below starts a session the same way: attach the connector's tables read-only, put their schema on the search path, check that every table has loaded a release, and run `cms_views.sql`. The views are created in the session's in-memory database, so the warehouse is never written.

Create the views only once every table has a row in `dataset_release`. `fivetran debug` creates each table at the start of the first sync with only its key and release columns, and adds the other columns with the first rows, so `cms_views.sql` fails on a table that has no rows yet. The check query below returns 5 when every table has a row in `dataset_release`; with a lower number, wait for a sync that loads the rest. Before the first release is recorded it fails instead, with `Referenced column "_fivetran_deleted" not found`, because that column is added with the first row; treat the error as 0 and wait.

### The DuckDB CLI

From this directory:

```bash
duckdb
```

```sql
ATTACH '../connector/files/warehouse.db' AS cms_wh (READ_ONLY);
SET search_path = 'memory.main,cms_wh.tester';
select count(distinct table_name) as tables_loaded from dataset_release where not coalesce(_fivetran_deleted, false);
.read cms_views.sql
select table_name, cms_released from cms_release order by table_name;
```

### An agent with a DuckDB or SQL tool

Give Claude Code, Cursor or another coding assistant `agent/AGENTS.md` as its instructions, and a tool that runs DuckDB SQL. Have the agent run the first three statements above as its first call, and the fourth only when `tables_loaded` is 5; otherwise the agent says the data has not loaded. The fifth is a check you can skip. In a tool that does not support the CLI's `.read` command, pass the contents of `cms_views.sql` instead. Then ask a question, for example "Which states have the longest emergency department visits?". The agent should answer with the checked SQL from `AGENTS.md`.

### An MCP DuckDB server

Point a DuckDB MCP server at an in-memory database (`:memory:`), not at `warehouse.db`. With a scratch DuckDB file instead, the session's own catalog is named after the file, and `memory.main` does not exist: replace `memory.main` in the search path with `<file name>.main` (for `scratch.duckdb`, `scratch.main`). The views stay in that file and look their tables up by name when they are queried, so every new connection attaches the warehouse and sets the search path again before it queries them. Run the setup statements through the server's query tool, then load `agent/AGENTS.md` into the client as the system prompt or project instructions.

### A Fivetran Managed Data Lake

The same views run over a deployed connection read in place: attach the lake's Iceberg catalog in DuckDB, then set the search path to `memory.main,<lake_catalog_alias>.<connection_schema>`. Attach after the first sync finishes: a session that attached the catalog earlier does not see the new schema, so attach again. Never put the catalog credentials in a tracked file.

## Checked questions

`agent/AGENTS.md` holds four questions, each with SQL and the answer it returned on this sync:

1. Which hospitals in a state have a heart failure readmission rate above the national rate?
2. Which hospitals have the highest HRRP excess readmission ratio for heart failure?
3. Which states have the longest median ED time (`OP_18b`)?
4. How do star ratings differ by hospital ownership?

The answers carry the CMS release they came from. After a newer release syncs, the SQL still runs; the numbers change.

## Limitations

- The data is published benchmarks, not patient records. The pack answers questions about hospitals and states, not about patients or costs.
- CMS chooses what to publish in each release. In the release synced here it gave no "compared to the national rate" category for the `READM_30_*` measures, so the pack compares rates. A rate above the national rate is not the same as CMS's statistical finding that a hospital is worse.
- Each measure has its own period, and it can be years before the release date. The answer format puts the period next to the number.
- The hospital and national views each read their own latest release, so after a partial sync they can cover different periods. The checked comparison joins the national rate on the measurement period and reports the benchmark as unavailable when no period matches.
- A state's ED time is the median of its hospitals' medians, not a median over patients.
- Keep the pack beside the connector, not inside it. `fivetran deploy` packages every file in the connector directory.

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
