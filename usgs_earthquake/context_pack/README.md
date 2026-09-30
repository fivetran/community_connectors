# USGS earthquake context pack

This directory gives an AI agent governed context for the `earthquake` table that the `usgs_earthquake` connector syncs. It sits beside the connector directory, `usgs_earthquake/connector/`, not inside it, so `fivetran debug` and `fivetran deploy` of the connector never scan or package it. The connector does not use it.

The pack answers one question the same way on every read path: which regions had more M4.5+ earthquakes last week than their prior 4-week average? It has three parts:

- A Claude-drafted context layer. `context_pack.py` profiles the synced table and makes one Claude call that drafts the staging model, its column docs and tests, and the agent's instruction files. A person reviews the draft before any of it is used.
- A dbt and MetricFlow semantic layer. A region seed, the models `stg_usgs__earthquakes`, `dim_region`, `fct_seismic_events` and `fct_region_days`, a time spine, the metrics and the saved query `weekly_exposure_by_region`.
- The canonical agent query. `render_lake_sql.py` renders the saved query into one self-contained DuckDB string. An agent runs it against the same table, read in place: the connector's local DuckDB warehouse, or a Fivetran Managed Data Lake.

Data: U.S. Geological Survey (public domain).

## Models

Lineage:

- `region_bounds` (seed) feeds `dim_region`, which feeds `fct_seismic_events` and `fct_region_days`.
- `earthquake` (source) feeds `stg_usgs__earthquakes`, then `fct_seismic_events`, then `fct_region_days`.
- `metricflow_time_spine` feeds `fct_region_days`.

What each model does:

- `stg_usgs__earthquakes` drops deleted events and converts both timestamps to UTC. The source columns are `timestamptz`, and a bare cast to `date` in a non-UTC session moves an event at 02:00 UTC on a Monday into the previous week. When the var `updated_at_cutoff` is set, rows updated after it are also dropped, so two copies of the table synced at different times can be compared by `updated_at`. A delete or preferred-id change that only one copy has synced keeps the row's old `updated_at`, so the cutoff does not hide that difference.
- `dim_region` holds one row per region in the seed: 19 latitude and longitude boxes with a priority, plus an `Other / open ocean` catch-all. A box whose `lon_min` is greater than its `lon_max` crosses the antimeridian.
- `fct_seismic_events` holds one row per live event, assigned to the matching box with the highest priority. `is_significant` (an earthquake of magnitude 4.5 or more, false for a null magnitude) is the only place the M4.5+ rule lives.
- `fct_region_days` holds one row per region per UTC day, zero-filled. The significant-event metrics count here.
- `metricflow_time_spine` is a `range()` view from the Monday of `start_date`'s week to one year past today.

The metrics are `significant_event_count`, `significant_events_4wk_avg` (the mean of the four weeks before the week, not including it) and `significant_events_vs_4wk_avg`, plus `event_count` and `max_magnitude` for drill-down. The saved query `weekly_exposure_by_region` groups all three significant-event metrics by region name and week.

Why a region-day fact: MetricFlow's `fill_nulls_with: 0` fills gaps along the time spine, not across region-by-week combinations. At event grain, a region with no events in any of its four prior weeks gets a null 4-week average instead of 0, the margin is then null too, and the answer drops the region. `fct_region_days` makes every region-week exist before any metric reads it.

The source switches on `target.name`:

- `local` reads `usgs_wh.tester.earthquake`, the connector's `../connector/files/warehouse.db` attached read-only. `dbt build` and `mf query` run here.
- `lake` reads `<lake_catalog_alias>.<lake_schema>.earthquake`, from the vars `lake_catalog` and `lake_schema` in `dbt/dbt_project.yml`. It is compile-only: the agent's DuckDB session attaches the lake catalog itself, and `dbt/profiles.yml` holds no credentials.

## Requirements

- Python 3.12 and the pinned packages in `requirements.txt`: the Anthropic Python SDK, DuckDB, PyYAML, dbt-core, dbt-duckdb, MetricFlow and dbt-metricflow. These are the pack's dependencies, not the connector's.
- A local sync of the connector, so that `../connector/files/warehouse.db` exists.
- For `generate` only: an Anthropic API key.
- For the lake path only: a DuckDB session, or an agent tool that runs DuckDB SQL, with the Managed Data Lake catalog attached.

## Setup

Create a virtual environment and install the pack's dependencies, from the repository root:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r usgs_earthquake/context_pack/requirements.txt
```

Sync the connector into its local DuckDB warehouse, from `usgs_earthquake/connector/`:

```bash
cd usgs_earthquake/connector
fivetran debug --configuration configuration.json
```

Set the Anthropic key in the environment before you run `generate`:

```bash
export ANTHROPIC_API_KEY=<your_anthropic_api_key>
```

Never put the key in `configuration.json`, `dbt/profiles.yml` or any other tracked file in `usgs_earthquake/`. If you keep it in a file, use `usgs_earthquake/context_pack/.env`, which the pack's `.gitignore` covers, or a path outside the repository, and pass it with `--env-file <path>`. The script never prints the key.

Set `start_date` in `dbt/dbt_project.yml` to the connector's `start_date`. To use the lake path, set `lake_catalog` to the alias your DuckDB session attaches the lake catalog under, and `lake_schema` to the connection's destination schema, or pass both with `--var`.

## Commands

Run these from `usgs_earthquake/context_pack/` unless a block says otherwise.

Draft the context with one Claude call:

```bash
# Profile the table and build the prompt, with no API call
python context_pack.py generate --dry-run

# Profile the table read-only, call Claude once, write context_pack_draft/ and append to _ai_calls.jsonl
python context_pack.py generate
```

By default `generate` reads `../connector/files/warehouse.db`, schema `tester`, table `earthquake`. Pass `--duckdb`, `--schema` or `--table` to profile and check another copy, and `--connector-dir` if the connector is somewhere other than `../connector/`. Review `context_pack_draft/REVIEW.md` before you promote anything. A reviewed staging proposal reaches `dbt/models/staging/` by hand, followed by `dbt build`.

Build and test the dbt project against the local warehouse:

```bash
cd dbt
dbt build --profiles-dir . --target local
```

Query the saved query through MetricFlow, for the last complete UTC week:

```bash
cd dbt
DBT_PROFILES_DIR=. mf query --saved-query weekly_exposure_by_region \
  --where "{{ TimeDimension('metric_time', 'week') }} = date_trunc('week', timezone('UTC', now())) - interval 7 day"
```

`mf query` returns every region with its count, average and margin. The canonical string adds a wrapper that keeps only the regions above their average.

Render the canonical string:

```bash
# Production: the last complete UTC week, read from the lake
python render_lake_sql.py --target lake --week last-complete \
  --var lake_catalog=<lake_catalog_alias> --var lake_schema=<lake_schema>

# The same string over the local warehouse
python render_lake_sql.py --target local --week last-complete

# Comparing two copies: a pinned Monday and an updated_at cutoff, returned as one row with an md5
python render_lake_sql.py --target local --week <YYYY-MM-DD> --var updated_at_cutoff=<ISO 8601 UTC> --single-row
python render_lake_sql.py --target lake --week <YYYY-MM-DD> --var updated_at_cutoff=<ISO 8601 UTC> --single-row \
  --var lake_catalog=<lake_catalog_alias> --var lake_schema=<lake_schema>
```

Each command writes one file to `lake_sql/`, or prints it with `--stdout`. Run the local render in any DuckDB session that has the warehouse attached as `usgs_wh`:

```bash
python - <<'EOF'
import duckdb
from pathlib import Path

con = duckdb.connect()
con.execute("attach '../connector/files/warehouse.db' as usgs_wh (read_only)")
sql = Path("lake_sql/weekly_exposure_by_region.local.last_complete_week.sql").read_text()
print(con.execute(sql).fetchall())
EOF
```

Paste a reviewed lake render and your lake names into the reviewed drafts, and write `agent/`:

```bash
python context_pack.py assemble --sql lake_sql/weekly_exposure_by_region.lake.last_complete_week.sql \
  --lake-catalog <lake_catalog_alias> --lake-schema <lake_schema>
```

`assemble` reads `context_pack_draft/AGENTS.md` and `context_pack_draft/SKILL.md`, which `generate` writes. It also accepts the pair `<lake_catalog_alias>` and `<lake_schema>`, which is how the published `agent/` files were written. It takes only a current lake render: the render's first line must name the lake target, not the single-row form, and the `start_date`, seed, `_semantic.yml`, models and renderer the project has now.

The `agent/` files in this directory already hold the canonical string for the last complete UTC week, rendered from this dbt project, with `<lake_catalog_alias>` and `<lake_schema>` in place of the lake names. To use them without a new draft, replace both placeholders, for example:

```bash
sed -i.bak -e 's/<lake_catalog_alias>/my_lake/g' -e 's/<lake_schema>/usgs_earthquake/g' agent/AGENTS.md agent/SKILL.md \
  && rm agent/AGENTS.md.bak agent/SKILL.md.bak
```

Re-render and re-assemble after any change to the seed, the models, `_semantic.yml`, `start_date` or `render_lake_sql.py`.

## The canonical string

The string runs as one multi-statement call and needs nothing set up beforehand:

1. A `-- render:` comment line names the target, the week, `start_date` and the cutoff, and the hashes of the seed, `_semantic.yml`, the models and the renderer. `assemble` uses it to refuse a stale render.
2. `attach if not exists ':memory:' as usgs_local`, then the schemas, the seed as a table and every model as a view, parents first.
3. The saved query as MetricFlow renders it for DuckDB, constrained to one week.
4. An outer wrapper that keeps the named regions whose count is above their prior 4-week average and returns `region__name`, `significant_event_count` and `significant_events_4wk_avg`.

The production week filter is `date_trunc('week', timezone('UTC', now())) - interval 7 day`. It avoids `current_date`, which follows the session time zone, so the week turns over at Monday 00:00 UTC in every session. The string never attaches or detaches the lake catalog: the session that runs it owns that attachment.

`--single-row` returns one row, the answer as JSON plus its md5, so two sessions can be compared by hash. The renderer compiles into `dbt/target_<target>/` with `--no-populate-cache`, so the lake target needs no connection.

## Layout

- `README.md` – This file.
- `requirements.txt` – The pack's pinned dependencies.
- `context_pack.py` – Profiles the table, makes the one Claude call, and writes the draft and its checks (`generate`). Fills the reviewed drafts into `agent/` (`assemble`).
- `render_lake_sql.py` – Renders the canonical string for a target and a week.
- `agent/AGENTS.md` – The DuckDB agent's orientation: the data, the grain, the join path, the rules every query follows and how to answer the canonical question.
- `agent/SKILL.md` – The canonical string, how to read its result, six drill-down queries and the known gotchas.
- `dbt/dbt_project.yml` – The project and its vars: `start_date`, `updated_at_cutoff`, `lake_catalog` and `lake_schema`.
- `dbt/profiles.yml` – DuckDB only, with no credentials: the `local` and `lake` targets.
- `dbt/seeds/` – The region boxes and their docs.
- `dbt/models/staging/` – The source and the staging model.
- `dbt/models/marts/` – The region dimension, the two facts, the time spine and `_semantic.yml`.

The scripts also write `context_pack_draft/`, `_ai_calls.jsonl`, `lake_sql/`, `dbt/target*/` and DuckDB files here. The draft and the call ledger are meant to be reviewed and committed. `lake_sql/`, `dbt/target*/` and the DuckDB files are gitignored.

## Human-owned and AI-drafted files

`generate` drafts, a person reviews, and only then does anything reach `dbt/` or `agent/`.

- Human-owned: the region seed, `_semantic.yml`, the marts models, `dbt_project.yml`, both scripts and the canonical string. `generate` never writes them. It records the hashes of the seed, `_semantic.yml` and the renderer before and after the run in `REVIEW.md`, and refuses an `--out` inside `dbt/` or `lake_sql/`.
- AI-drafted, then reviewed: the staging model and its YAML (column docs, units, PII flags and tests), `agent/AGENTS.md`, and `agent/SKILL.md` apart from the canonical string. The model writes `{{CANONICAL_SQL}}`, `{{LAKE_CATALOG}}` and `{{LAKE_SCHEMA}}` placeholders, and `assemble` fills them in. The shipped files came from one Claude draft. Claude Code edited it and the connector's author approved the edits before it was committed.

`generate` gives Claude the profile (row count and, per column, nulls, distinct values, min and max, and top values where values repeat, plus five sample rows, read in a UTC session) and the human-owned files as context: the connector README, this README, the seed, every model and YAML file, `dbt_project.yml` and `render_lake_sql.py`. The script renders the column YAML itself from structured output, so it is always valid dbt. `REVIEW.md` records these checks:

- The staging proposal keeps the `not coalesce(_fivetran_deleted, false)` filter and the `updated_at_cutoff` block, converts both timestamps with `timezone('UTC', ...)`, casts no timestamp to `date` and keeps the columns the marts read.
- The proposal's output columns and types, described against the local warehouse, match its YAML, so the enforced contract cannot fail later at `dbt build`.
- `SKILL.md` holds the canonical placeholder exactly once.
- Every drill-down SQL block is one read-only `select`. Each runs against the local warehouse under both UTC and `Pacific/Kiritimati` and must return the same rows in both.
- The human-owned files are unchanged, and both staging proposals are shown as diffs against `dbt/`.

## Cost

`generate` makes one streamed `claude-opus-5-5` request at effort `high`, with a JSON-schema structured output. A recorded run used 22 uncached input tokens, 26,186 cache-write input tokens and 41,705 output tokens. It cost $0.97 at the list prices pinned in the script and took about 7 minutes. `_ai_calls.jsonl` records each call's model, prompt hash, tokens, cost and latency. The line is written as soon as the response arrives, before the refusal, `max_tokens` and JSON checks, so a paid call is recorded even when the run then fails. Prices change, so check current Anthropic pricing before you rely on the recorded cost.

`--dry-run`, `dbt build`, `mf query`, `render_lake_sql.py` and `assemble` make no API calls.

## Limitations

- Coverage: the table holds events from the connector's `start_date` onward, and weeks before it read as zero, not as missing. A 4-week average is only a real baseline for a week with four full weeks of data before it. The renderer refuses a pinned week less than four full weeks after `start_date`. The production filter moves with the calendar and is not checked, so the agent instructions tell the agent to say when last week's baseline is understated.
- Region boxes are approximate latitude and longitude rectangles, not tectonic or political boundaries. Overlaps are settled by priority, and every event outside all boxes goes to `Other / open ocean`.
- Magnitudes are on mixed scales (`mag_type`), and the M4.5 threshold applies to `mag` as reported.
- USGS revises the catalog after the fact, so a count for a past week can change slightly between syncs.
- Keep the pack beside the connector, not inside it. `fivetran debug` and `fivetran deploy` scan the connector directory for imports and package every file in it, so a copy inside `usgs_earthquake/connector/` would add the pack's dependencies to the connector's requirements and upload the pack with it.

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
