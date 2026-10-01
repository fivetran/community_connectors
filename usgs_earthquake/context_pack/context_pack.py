"""Draft the agent context for the usgs_earthquake table: one Claude call, reviewed by a person before anything builds.

AI proposes and deterministic code executes. This script profiles the synced table, makes one claude-opus-5-5 call with
structured outputs, and writes a draft for review. It never writes into dbt/ or lake_sql/, and it never rewrites the
human-owned files: the region seed, _semantic.yml and the canonical SQL rendered by render_lake_sql.py.

Two commands, run from this directory in a venv with this directory's requirements.txt installed:

    # 1. Profile the connector's debug warehouse, call Claude once, write context_pack_draft/ and append to
    #    _ai_calls.jsonl
    python context_pack.py generate

    # 2. After review: fill the placeholders in the reviewed drafts and write agent/AGENTS.md and agent/SKILL.md
    python context_pack.py assemble --sql lake_sql/weekly_exposure_by_region.lake.last_complete_week.sql \\
        --lake-catalog <lake_catalog_alias> --lake-schema <lake_schema>

`generate --dry-run` profiles and builds the prompt without calling the API. assemble takes only a current lake
render: its stamp must name the lake target, not the single-row form, and today's start_date, seed and _semantic.yml.

The draft contains:
- stg_usgs__earthquakes.sql and _stg_usgs__models.yml: proposals, shown in REVIEW.md as a diff against dbt/
- AGENTS.md and SKILL.md: the DuckDB agent's instruction files, with {{CANONICAL_SQL}}, {{LAKE_CATALOG}} and
  {{LAKE_SCHEMA}} placeholders
- profile.json, response.json and REVIEW.md: the input, the raw output and the deterministic checks

The key is read from the ANTHROPIC_API_KEY environment variable, or from a KEY=VALUE file passed with --env-file. It
is never printed.
"""

from __future__ import annotations

# For command-line arguments
import argparse

# For the staging proposal diffs in REVIEW.md
import difflib

# For the prompt, profile and file hashes
import hashlib

# For the profile, the response and the ledger
import json

# For reading ANTHROPIC_API_KEY
import os

# For the SQL guards and the markdown fences
import re

# For the exit code and stderr
import sys

# For the time limit on each checked SQL block
import threading

# For the call's latency
import time

# For type hints
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

# For profiling the warehouse read-only and running the drill-down checks
import duckdb

# For rendering and reading dbt YAML
import yaml

# For the render stamp and the current input hashes
import render_lake_sql

HERE = Path(__file__).resolve().parent
DBT_DIR = HERE / "dbt"
LAKE_SQL_DIR = HERE / "lake_sql"
DRAFT_DIR = HERE / "context_pack_draft"
AGENT_DIR = HERE / "agent"
LEDGER = HERE / "_ai_calls.jsonl"
# The connector directory holds connector.py.
CONNECTOR_NAME = "usgs_earthquake"
# The pack sits beside the connector (usgs_earthquake/context_pack/ next to usgs_earthquake/connector/). Pass
# --connector-dir to read the connector from anywhere else.
CONNECTOR_DIR_CANDIDATES = (HERE.parent / "connector",)

MODEL = "claude-opus-5-5"
EFFORT = "high"
MAX_TOKENS = 64000
# Server-side refusal fallback, routed by refusal category. A fallback-served response is priced separately (below).
FALLBACK_BETA = "server-side-fallback-2026-07-01"

# USD per million tokens, Anthropic first-party API list prices, pinned on 2026-09-25. Check current pricing before
# relying on a recorded cost. Cache writes use the 5-minute TTL, 1.25x input.
PRICING = {
    "claude-opus-5-5": {
        "input": 4.00,
        "output": 20.00,
        "cache_write_5m": 5.00,
        "cache_read": 0.20,
    },
}
PRICING_SOURCE = "Anthropic first-party API list prices, pinned 2026-09-25"

CANONICAL_PLACEHOLDER = "{{CANONICAL_SQL}}"
LAKE_CATALOG_PLACEHOLDER = "{{LAKE_CATALOG}}"
LAKE_SCHEMA_PLACEHOLDER = "{{LAKE_SCHEMA}}"
LAKE_TABLE = f"{LAKE_CATALOG_PLACEHOLDER}.{LAKE_SCHEMA_PLACEHOLDER}.earthquake"
# The alias the local debug warehouse is attached under, here and in dbt/profiles.yml.
LOCAL_ALIAS = "usgs_wh"
# A catalog alias or schema name that assemble accepts: a plain SQL identifier.
PLAIN_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
# The placeholders the published agent/ files carry instead of real lake names. assemble accepts them as a pair.
LAKE_CATALOG_TEMPLATE = "<lake_catalog_alias>"
LAKE_SCHEMA_TEMPLATE = "<lake_schema>"
# Run in every check session after the warehouse is attached: no file or network access beyond it, and no setting a
# drill-down could change back.
LOCKDOWN_STATEMENTS = ("set enable_external_access = false", "set lock_configuration = true")
# Resource bounds for every check session, set before the lockdown locks them. A generated block that cross-joins or
# ranges without end fails the check with an error instead of exhausting the machine's memory or CPU.
CHECK_MEMORY_LIMIT = "1GB"
CHECK_THREADS = 2
CHECK_TIMEOUT_SECONDS = 30
# The agent's query tool returns at most this many rows (AGENTS.md and SKILL.md document the default). A block that
# returns more fails, and the check never fetches more than one row past the cap.
CHECK_ROW_CAP = 200
# Every drill-down block runs under both zones and must return the same rows: the UTC rule, checked, not trusted.
CHECK_TIME_ZONES = ("UTC", "Pacific/Kiritimati")
# A drill-down is one read-only select. Anything that changes the session or writes is refused.
FORBIDDEN_STATEMENT = re.compile(
    r"\b(attach|detach|use|set|reset|create|drop|alter|insert|update|delete|copy|pragma|install|load|export|import)\b",
    re.IGNORECASE,
)
NOT_NULL_SEVERITIES = ("none", "warn", "error")

# Human-owned or already-reviewed files Claude reads as context, after the connector's README. Paths are relative
# to this directory.
CONNECTOR_README_LABEL = f"{CONNECTOR_NAME}/connector/README.md"
CONTEXT_FILES = (
    "README.md",
    "dbt/seeds/region_bounds.csv",
    "dbt/seeds/_seeds.yml",
    "dbt/models/staging/_usgs__sources.yml",
    "dbt/models/staging/stg_usgs__earthquakes.sql",
    "dbt/models/staging/_stg_usgs__models.yml",
    "dbt/models/marts/dim_region.sql",
    "dbt/models/marts/fct_seismic_events.sql",
    "dbt/models/marts/fct_region_days.sql",
    "dbt/models/marts/metricflow_time_spine.sql",
    "dbt/models/marts/_marts__models.yml",
    "dbt/models/marts/_semantic.yml",
    "dbt/dbt_project.yml",
    "render_lake_sql.py",
)
# The files this script must leave byte-for-byte unchanged. REVIEW.md records their hashes before and after.
HUMAN_OWNED = (
    "dbt/seeds/region_bounds.csv",
    "dbt/models/marts/_semantic.yml",
    "render_lake_sql.py",
)
CURRENT_STAGING_SQL = DBT_DIR / "models/staging/stg_usgs__earthquakes.sql"
CURRENT_STAGING_YML = DBT_DIR / "models/staging/_stg_usgs__models.yml"
# Columns the marts read from staging. A proposal that drops one breaks the build.
DOWNSTREAM_COLUMNS = ("event_id", "event_time_utc", "mag", "event_type", "latitude", "longitude")
DUCKDB_TYPES = {
    "varchar",
    "double",
    "integer",
    "bigint",
    "boolean",
    "timestamp",
    "timestamptz",
    "date",
}

SYSTEM_PROMPT = """\
You are drafting the agent context for a governed DuckDB dataset of USGS earthquake events. A person reviews and edits \
your draft before dbt builds anything, so say plainly where you are unsure rather than guessing.

## The setup

A Fivetran Connector SDK connector (its README is below) syncs the USGS FDSN event service into a table named \
earthquake: one row per event, keyed by the USGS preferred id, with _fivetran_deleted = true on events USGS deleted. \
The same table is read in two places: a local DuckDB debug warehouse, and a Fivetran Managed Data Lake (Iceberg in a \
Polaris catalog) that DuckDB reads in place.

A dbt project on top of it already exists; every model and YAML file is below. The question it answers is: "Which \
regions had more M4.5+ earthquakes last week than their prior 4-week average?" That answer comes from one pinned SQL \
string rendered by render_lake_sql.py from the saved query weekly_exposure_by_region: a preamble that recreates the \
seed and the model views in the session, then the MetricFlow-rendered saved query inside an outer wrapper.

The consumer is a DuckDB agent. DuckDB has no native agent, so AGENTS.md and SKILL.md are the agent: Claude runs with \
them as system context and queries the lake through one DuckDB query tool. One call runs a whole multi-statement \
string and returns the last statement's rows, up to the tool's row cap (200 by default). The session already has the \
lake catalog attached under the alias {{LAKE_CATALOG}}, so SQL must never attach or detach it. The lake table is \
{{LAKE_CATALOG}}.{{LAKE_SCHEMA}}.earthquake; {{LAKE_CATALOG}} and {{LAKE_SCHEMA}} are placeholders filled in after \
deployment, so write them exactly like that.

## Human-owned: reference these, never redefine them

- dbt/seeds/region_bounds.csv, the region boxes and priorities.
- dbt/models/marts/_semantic.yml, the measures, metrics and the saved query.
- The canonical SQL string. You never write it. SKILL.md holds the literal placeholder {{CANONICAL_SQL}} where it goes, \
and a deterministic step pastes the reviewed render in.
- The significant-event rule, fct_seismic_events.is_significant: event_type = 'earthquake' and mag >= 4.5, false for a \
null magnitude. Describe it in those words; do not restate it as a different rule.

## What to return

1. staging_sql: your proposal for dbt/models/staging/stg_usgs__earthquakes.sql. Start from the current file. Keep what \
the downstream models and the copy comparison rely on: drop rows where coalesce(_fivetran_deleted, false) is true; keep \
the var('updated_at_cutoff') block exactly as it is; convert every timestamptz with timezone('UTC', ...), never with a \
cast, because in a non-UTC session a cast moves an event at 02:00 UTC on a Monday into the previous week; rename id to \
event_id; keep the columns the marts read (event_id, event_time_utc, mag, event_type, latitude, longitude). You may \
add or drop other source columns where the profile justifies it. The model's contract is enforced, so every output \
column needs an entry in columns, with the exact DuckDB type it produces, and nothing else may be in columns.
2. model_description and columns: docs for the staging model and each of its columns, with the unit where there is one, \
a PII flag (USGS data is public and describes events, not people, so expect none), and tests grounded in the profile: \
unique only for keys; not_null only where the profile shows no nulls, at severity error only for fields the pipeline \
itself guarantees (the key and the two timestamps) and warn elsewhere, because the connector writes some nulls on \
purpose; accepted_values only for low-cardinality columns, listing every value USGS documents (never fewer than the \
current file lists, since a value the profile has not seen yet is still valid), with severity warn where USGS can add \
values later. accepted_values on an integer column needs quote set to false.
3. agents_md: the agent's orientation. Cover what the data is and where it comes from; the grain; the join path from \
the source through staging to fct_seismic_events, fct_region_days and dim_region; the rename map from source columns to \
staging columns; the deleted-event rule; the UTC rule; how events are assigned to regions; the significant-event rule; \
what "last week" means (the last complete week, Monday to Sunday, UTC); the data coverage limit (weeks before the \
connector's start_date read as zero, so a 4-week average needs four full weeks after it); how to call the query tool; \
the rule that the demo question is answered only by running the canonical string from SKILL.md, never by the agent's \
own aggregate over raw rows, and that the answer says which it used; and the credit line "Data: U.S. Geological Survey \
(public domain)."
4. skill_md: example questions and gotchas. It must contain one section with the canonical question, the placeholder \
{{CANONICAL_SQL}} alone inside a ```sql fence (exactly once in the whole file), and how to read its three result \
columns. Then four to six drill-down questions, each with a ```sql block against \
{{LAKE_CATALOG}}.{{LAKE_SCHEMA}}.earthquake that applies the deleted-event rule and the UTC rule itself, returns \
fewer than 200 rows, and runs on DuckDB 1.5. Region assignment exists only inside the canonical string, which the \
agent never edits: a region follow-up the canonical answer does not cover is answered by saying so, never by the \
agent assigning regions itself from coordinates or place. Drill-downs work on the raw table by time, magnitude, place, \
network or coordinates, without region names. Then the gotchas the profile shows: event types other than earthquake, null magnitudes, mixed magnitude \
scales, ids merged under a preferred id, the tsunami flag not being a warning, the row cap, timestamptz columns, and \
anything else you find in the profile. Counts from the profile drift with every sync and differ on the lake, so \
label any you quote as from the profiled sample.
5. review_notes: what you changed from the current files and why, what you are unsure of, and anything in the profile \
that contradicts the current docs.

Write plainly and specifically for a technical reader. No marketing language. Do not mention people, project dates, \
tickets or pull requests.
"""

TASK_PROMPT = "Draft the context pack from the files and the profile above."

_NULLABLE_STRING = {"anyOf": [{"type": "string"}, {"type": "null"}]}
OUTPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "staging_sql",
        "model_description",
        "columns",
        "agents_md",
        "skill_md",
        "review_notes",
    ],
    "properties": {
        "staging_sql": {"type": "string"},
        "model_description": {"type": "string"},
        "columns": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["name", "data_type", "description", "unit", "pii", "tests"],
                "properties": {
                    "name": {"type": "string"},
                    "data_type": {"type": "string"},
                    "description": {"type": "string"},
                    "unit": _NULLABLE_STRING,
                    "pii": {"type": "boolean"},
                    "tests": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["unique", "not_null", "accepted_values"],
                        "properties": {
                            "unique": {"type": "boolean"},
                            "not_null": {"type": "string", "enum": list(NOT_NULL_SEVERITIES)},
                            "accepted_values": {
                                "anyOf": [
                                    {"type": "null"},
                                    {
                                        "type": "object",
                                        "additionalProperties": False,
                                        "required": ["values", "quote", "severity"],
                                        "properties": {
                                            "values": {
                                                "type": "array",
                                                "items": {"type": "string"},
                                            },
                                            "quote": {"type": "boolean"},
                                            "severity": {
                                                "type": "string",
                                                "enum": ["warn", "error"],
                                            },
                                        },
                                    },
                                ]
                            },
                        },
                    },
                },
            },
        },
        "agents_md": {"type": "string"},
        "skill_md": {"type": "string"},
        "review_notes": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["file", "note"],
                "properties": {"file": {"type": "string"}, "note": {"type": "string"}},
            },
        },
    },
}


class ContextPackError(RuntimeError):
    """The pack could not be generated or assembled: a bad path, a missing key, a refused or truncated response."""


# ---------------------------------------------------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------------------------------------------------


def load_env(path: Path) -> None:
    """Load KEY=VALUE lines from path into os.environ, without overriding what the shell already set.

    A minimal reader, so the pack does not depend on python-dotenv. Values are never printed or logged.

    Args:
        path: the file passed with --env-file. Keep it out of git: a dotfile or a gitignored path.

    Raises:
        ContextPackError: when the file does not exist.
    """
    if not path.is_file():
        raise ContextPackError(f"--env-file {path} does not exist")
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.removeprefix("export ").partition("=")
        key, value = key.strip(), value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
            value = value[1:-1]
        if key and key not in os.environ:
            os.environ[key] = value


def require_api_key(env_file: Path | None = None) -> None:
    """Fail before any work if the key is missing, naming the variable but never the value.

    Args:
        env_file: an optional KEY=VALUE file to load first. The environment wins over the file.
    """
    if env_file is not None:
        load_env(env_file)
    if not os.environ.get("ANTHROPIC_API_KEY", "").strip():
        raise ContextPackError(
            "ANTHROPIC_API_KEY is not set; export it in the shell or pass --env-file"
        )


# ---------------------------------------------------------------------------------------------------------------------
# Step 1: profile the table, deterministically
# ---------------------------------------------------------------------------------------------------------------------


def _jsonable(value: Any) -> Any:
    """A DuckDB value as JSON: timestamps in ISO 8601 (UTC when zoned), dates as ISO dates, decimals as floats."""
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat() if value.tzinfo else value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def _ident(name: str) -> str:
    """A double-quoted SQL identifier."""
    return '"' + name.replace('"', '""') + '"'


def _as_value(expression: str, data_type: str) -> str:
    """timestamptz as text, rendered by the UTC session, so the profile needs no Python time zone library."""
    return (
        f"cast({expression} as varchar)" if data_type == "TIMESTAMP WITH TIME ZONE" else expression
    )


def profile_table(
    db_path: Path, schema: str, table: str, sample_rows: int = 5, top_k: int = 10
) -> dict[str, Any]:
    """Row count and, per column, type, nulls, distinct values, min/max and top values. No writes, UTC session.

    Every list is ordered, so the same table always gives the same profile and the same prompt hash.
    """
    if not db_path.is_file():
        raise ContextPackError(f"{db_path} does not exist")
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        # Session setting only; the file is read-only. Renders timestamptz in UTC whatever the machine's zone.
        con.execute("set TimeZone = 'UTC'")
        relation = f"{_ident(schema)}.{_ident(table)}"
        columns = con.execute(
            "select column_name, data_type from information_schema.columns "
            "where table_schema = ? and table_name = ? order by ordinal_position",
            [schema, table],
        ).fetchall()
        if not columns:
            raise ContextPackError(f"{schema}.{table} not found in {db_path}")
        row_count = con.execute(f"select count(*) from {relation}").fetchone()[0]
        profiled = []
        for name, data_type in columns:
            col = _ident(name)
            nulls, distinct = con.execute(
                f"select count(*) filter (where {col} is null), count(distinct {col}) from {relation}"
            ).fetchone()
            entry: dict[str, Any] = {
                "name": name,
                "type": data_type,
                "nulls": nulls,
                "distinct": distinct,
            }
            if data_type == "VARCHAR":
                min_len, max_len = con.execute(
                    f"select min(length({col})), max(length({col})) from {relation}"
                ).fetchone()
                entry["length"] = {"min": min_len, "max": max_len}
            else:
                low, high = con.execute(
                    f"select {_as_value(f'min({col})', data_type)}, {_as_value(f'max({col})', data_type)} "
                    f"from {relation}"
                ).fetchone()
                entry["min"], entry["max"] = _jsonable(low), _jsonable(high)
            # Top values only where values repeat: low-cardinality columns and text that is not near-unique.
            if distinct < 0.9 * row_count and (distinct <= 50 or data_type == "VARCHAR"):
                top = con.execute(
                    f"select {_as_value(col, data_type)} as v, count(*) as n from {relation} "
                    "group by v order by n desc, v nulls last limit ?",
                    [top_k],
                ).fetchall()
                entry["top_values"] = [{"value": _jsonable(v), "count": n} for v, n in top]
            profiled.append(entry)
        select_list = ", ".join(
            f"{_as_value(_ident(name), data_type)} as {_ident(name)}"
            for name, data_type in columns
        )
        sample_cursor = con.execute(
            f"select {select_list} from {relation} order by {_ident(columns[0][0])} limit ?",
            [sample_rows],
        )
        sample_names = [d[0] for d in sample_cursor.description]
        sample = [
            {k: _jsonable(v) for k, v in zip(sample_names, row)}
            for row in sample_cursor.fetchall()
        ]
    finally:
        con.close()
    return {
        "table": f"{schema}.{table}",
        "row_count": row_count,
        "columns": profiled,
        "sample_rows": sample,
    }


# ---------------------------------------------------------------------------------------------------------------------
# Step 2: one Claude call
# ---------------------------------------------------------------------------------------------------------------------


def find_connector_dir(candidates: Sequence[Path] = CONNECTOR_DIR_CANDIDATES) -> Path:
    """The connector directory: the first candidate that holds connector.py."""
    for candidate in candidates:
        if (candidate / "connector.py").is_file():
            return candidate.resolve()
    shown = ", ".join(str(c) for c in candidates)
    raise ContextPackError(
        f"no {CONNECTOR_NAME} connector.py found in {shown}; pass --connector-dir"
    )


def read_context_files(
    paths: Sequence[str] = CONTEXT_FILES, connector_dir: Path | None = None
) -> str:
    """The context files as tagged blocks, in a fixed order, so the cached prefix is byte-stable between runs.

    The connector's README comes first, labelled usgs_earthquake/connector/README.md wherever the pack sits.
    """
    readme = (connector_dir or find_connector_dir()) / "README.md"
    if not readme.is_file():
        raise ContextPackError(f"the connector README {readme} does not exist")
    blocks = [f'<file path="{CONNECTOR_README_LABEL}">\n{readme.read_text()}\n</file>']
    for rel in paths:
        path = (HERE / rel).resolve()
        if not path.is_file():
            raise ContextPackError(f"context file {rel} does not exist")
        blocks.append(f'<file path="{rel}">\n{path.read_text()}\n</file>')
    return "\n\n".join(blocks)


def build_request(profile: Mapping[str, Any], context: str) -> dict[str, Any]:
    """The full request body. The cache breakpoint sits on the profile, the last stable block."""
    profile_text = json.dumps(profile, indent=1, sort_keys=True)
    return {
        "model": MODEL,
        "max_tokens": MAX_TOKENS,
        "thinking": {"type": "adaptive"},
        "output_config": {
            "effort": EFFORT,
            "format": {"type": "json_schema", "schema": OUTPUT_SCHEMA},
        },
        "system": [
            {"type": "text", "text": SYSTEM_PROMPT},
            {"type": "text", "text": f"<context_files>\n{context}\n</context_files>"},
            {
                "type": "text",
                "text": f"<profile>\n{profile_text}\n</profile>",
                "cache_control": {"type": "ephemeral"},
            },
        ],
        "messages": [{"role": "user", "content": TASK_PROMPT}],
    }


def prompt_hash(request: Mapping[str, Any]) -> str:
    """The SHA-256 of the request body with sorted keys, recorded in the ledger."""
    return hashlib.sha256(json.dumps(request, sort_keys=True).encode()).hexdigest()


def call_claude(
    request: Mapping[str, Any], ledger_fields: Mapping[str, Any], ledger: Path = LEDGER
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Stream the one call and return (parsed output, call record). Raises on a refusal or a truncated response.

    The ledger line is written as soon as the response is in, before any check that can raise, so a paid call is
    always recorded. ledger_fields (the prompt and profile hashes) go into both the record and the ledger line.
    """
    import anthropic

    client = anthropic.Anthropic()
    started = time.monotonic()
    try:
        with client.beta.messages.stream(
            **request, betas=[FALLBACK_BETA], fallbacks="default"
        ) as stream:
            message = stream.get_final_message()
            # The accumulated message never gets _request_id; the stream carries it from the response headers.
            request_id = stream.request_id
    except anthropic.BadRequestError as exc:
        raise ContextPackError(f"the API rejected the request: {exc.message}") from exc
    except anthropic.AuthenticationError as exc:
        raise ContextPackError("the API rejected ANTHROPIC_API_KEY") from exc
    except anthropic.RateLimitError as exc:
        raise ContextPackError("rate limited; retry later") from exc
    except anthropic.APIStatusError as exc:
        raise ContextPackError(
            f"API error {exc.status_code} (request {exc.request_id}): {exc.message}"
        ) from exc
    except anthropic.APIConnectionError as exc:
        raise ContextPackError("could not reach the API") from exc
    latency = time.monotonic() - started

    record = {**call_record(message, latency, request_id), **ledger_fields}
    append_ledger(ledger_entry(record), ledger)
    if message.stop_reason == "refusal":
        details = getattr(message, "stop_details", None)
        raise ContextPackError(
            f"the model declined (category {getattr(details, 'category', None)})"
        )
    if message.stop_reason == "max_tokens":
        raise ContextPackError(
            f"the response hit max_tokens ({MAX_TOKENS}); the JSON is truncated"
        )
    text = next((block.text for block in message.content if block.type == "text"), None)
    if text is None:
        raise ContextPackError("the response has no text block")
    try:
        return json.loads(text), record
    except json.JSONDecodeError as exc:
        raise ContextPackError(
            f"the response is not valid JSON ({exc.msg} at char {exc.pos})"
        ) from exc


def ledger_entry(record: Mapping[str, Any]) -> dict[str, Any]:
    """One _ai_calls.jsonl line: the call record plus what was asked for and the rates it was priced at."""
    return {
        "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "task": "usgs_earthquake context pack",
        "model_requested": MODEL,
        "effort": EFFORT,
        "pricing_usd_per_mtok": PRICING.get(record["model_served"]),
        "pricing_source": PRICING_SOURCE,
        **record,
    }


def call_record(message: Any, latency_s: float, request_id: str | None) -> dict[str, Any]:
    """Tokens, cost and latency for the ledger, from the response's own usage block."""
    usage = message.usage
    tokens = {
        "input": usage.input_tokens or 0,
        "cache_write": getattr(usage, "cache_creation_input_tokens", None) or 0,
        "cache_read": getattr(usage, "cache_read_input_tokens", None) or 0,
        "output": usage.output_tokens or 0,
    }
    iterations = getattr(usage, "iterations", None) or []
    fallback_ran = any(getattr(item, "type", None) == "fallback_message" for item in iterations)
    return {
        "request_id": request_id,
        "model_served": message.model,
        "fallback_ran": fallback_ran,
        "stop_reason": message.stop_reason,
        "tokens": tokens,
        "cost_usd": cost_usd(message.model, tokens) if not fallback_ran else None,
        "latency_s": round(latency_s, 2),
    }


def cost_usd(model: str, tokens: Mapping[str, int]) -> float | None:
    """Cost from the pinned price table, or None for a model the table does not price."""
    rates = PRICING.get(model)
    if rates is None:
        return None
    # Each token class and the rate it is priced at.
    priced = (
        (tokens["input"], rates["input"]),
        (tokens["cache_write"], rates["cache_write_5m"]),
        (tokens["cache_read"], rates["cache_read"]),
        (tokens["output"], rates["output"]),
    )
    total = sum(count * rate for count, rate in priced) / 1_000_000
    return round(total, 6)


# ---------------------------------------------------------------------------------------------------------------------
# Step 3: write the draft, check it, and record the call
# ---------------------------------------------------------------------------------------------------------------------


class _IndentedDumper(yaml.SafeDumper):
    """Indents list items under their key, the style of the hand-written YAML in dbt/."""

    def increase_indent(self, flow: bool = False, indentless: bool = False) -> None:
        """Never indentless, so a list sits two spaces under its key."""
        return super().increase_indent(flow, False)


def not_null_severity(value: Any) -> str | None:
    """The not_null test's severity, or None for no test. True means error and False means no test."""
    if value is True:
        return "error"
    if value in (False, None, "none"):
        return None
    if value in ("warn", "error"):
        return value
    raise ContextPackError(
        f"not_null must be one of {', '.join(NOT_NULL_SEVERITIES)}, not {value!r}"
    )


def render_staging_yml(description: str, columns: Sequence[Mapping[str, Any]]) -> str:
    """The staging model's YAML, built here from the structured columns so it is always valid dbt YAML."""
    rendered = []
    for column in columns:
        entry: dict[str, Any] = {
            "name": column["name"],
            "description": column["description"],
            "data_type": column["data_type"],
        }
        meta = {"pii": column["pii"]}
        if column.get("unit"):
            meta["unit"] = column["unit"]
        entry["config"] = {"meta": meta}
        tests: list[Any] = []
        if column["tests"]["unique"]:
            tests.append("unique")
        severity = not_null_severity(column["tests"]["not_null"])
        if severity == "error":
            tests.append("not_null")
        elif severity == "warn":
            tests.append({"not_null": {"config": {"severity": "warn"}}})
        accepted = column["tests"]["accepted_values"]
        if accepted:
            arguments: dict[str, Any] = {"values": list(accepted["values"])}
            if not accepted["quote"]:
                arguments["quote"] = False
            test: dict[str, Any] = {"arguments": arguments}
            if accepted["severity"] == "warn":
                test["config"] = {"severity": "warn"}
            tests.append({"accepted_values": test})
        if tests:
            entry["data_tests"] = tests
        rendered.append(entry)
    document = {
        "version": 2,
        "models": [
            {
                "name": "stg_usgs__earthquakes",
                "description": description,
                "config": {"contract": {"enforced": True}},
                "columns": rendered,
            }
        ],
    }
    return yaml.dump(
        document, Dumper=_IndentedDumper, sort_keys=False, width=118, allow_unicode=True
    )


def sql_blocks(markdown: str) -> list[str]:
    """Every ```sql fenced block in a markdown document, in order."""
    return [
        block.strip() for block in re.findall(r"```sql\s*\n(.*?)```", markdown, flags=re.DOTALL)
    ]


def check_staging_sql(
    sql: str, columns: Sequence[Mapping[str, Any]]
) -> list[tuple[str, bool, str]]:
    """Guards on the proposed staging SQL: the rules the marts and the copy comparison depend on."""
    names = {column["name"] for column in columns}
    converts_both_timestamps = all(
        re.search(rf"timezone\('UTC',\s*{column}\)", sql)
        for column in ("event_time", "updated_at")
    )
    missing = [name for name in DOWNSTREAM_COLUMNS if name not in names]
    bad_types = sorted(
        f"{column['name']}: {column['data_type']}"
        for column in columns
        if column["data_type"].lower() not in DUCKDB_TYPES
    )
    return [
        (
            "drops deleted events",
            bool(
                re.search(
                    r"not\s+coalesce\(\s*_fivetran_deleted\s*,\s*false\s*\)", sql, re.IGNORECASE
                )
            ),
            "not coalesce(_fivetran_deleted, false)",
        ),
        ("keeps the updated_at_cutoff block", "var('updated_at_cutoff')" in sql, "comparison var"),
        (
            "converts both timestamps with timezone('UTC', ...)",
            converts_both_timestamps,
            "event_time and updated_at",
        ),
        (
            "casts no timestamp to date",
            not re.search(r"::\s*date|as\s+date\b", sql, re.IGNORECASE),
            "UTC rule",
        ),
        ("keeps the columns the marts read", not missing, ", ".join(missing) or "all present"),
        ("uses known DuckDB types", not bad_types, ", ".join(bad_types) or "all known"),
    ]


def local_relation(schema: str, table: str) -> str:
    """The profiled table in a check session, where the warehouse is attached as LOCAL_ALIAS."""
    return f"{LOCAL_ALIAS}.{_ident(schema)}.{_ident(table)}"


def open_check_session(db_path: Path, time_zone: str | None = None) -> duckdb.DuckDBPyConnection:
    """An in-memory session with bounded memory and threads, the warehouse attached read-only, then locked down.

    After the attach, external access is switched off and the configuration locked, so a checked block can read the
    attached warehouse and nothing else: no read_text('/etc/hosts'), no httpfs, no settings changed back, including
    CHECK_MEMORY_LIMIT and CHECK_THREADS.

    Args:
        db_path: the warehouse file. A path holding a single quote is refused rather than escaped.
        time_zone: the session time zone, or None for DuckDB's default.
    """
    if "'" in str(db_path):
        raise ContextPackError(
            f"the warehouse path {db_path} holds a single quote; move or rename it"
        )
    con = duckdb.connect()
    try:
        if time_zone:
            con.execute(f"set TimeZone = '{time_zone}'")
        con.execute(f"set memory_limit = '{CHECK_MEMORY_LIMIT}'")
        con.execute(f"set threads = {CHECK_THREADS}")
        con.execute(f"attach '{db_path}' as {LOCAL_ALIAS} (read_only)")
        for statement in LOCKDOWN_STATEMENTS:
            con.execute(statement)
    except duckdb.Error:
        con.close()
        raise
    return con


def staging_select(sql: str, schema: str = "tester", table: str = "earthquake") -> str:
    """The staging proposal as plain SQL over the local warehouse: source() swapped, the updated_at_cutoff block dropped."""
    plain = re.sub(
        r"\{\{\s*source\('usgs',\s*'earthquake'\)\s*\}\}",
        local_relation(schema, table),
        sql,
    )
    plain = re.sub(
        r"\{%\s*if var\('updated_at_cutoff'\)\s*%\}.*?\{%\s*endif\s*%\}",
        "",
        plain,
        flags=re.DOTALL,
    )
    if "{{" in plain or "{%" in plain:
        raise ContextPackError(
            "the staging proposal has Jinja other than source() and the updated_at_cutoff block"
        )
    return plain


def _duckdb_type(data_type: str) -> str:
    """The type name DuckDB's describe reports for a YAML data_type."""
    return {"timestamptz": "TIMESTAMP WITH TIME ZONE"}.get(data_type.lower(), data_type.upper())


def check_staging_columns(
    sql: str,
    columns: Sequence[Mapping[str, Any]],
    db_path: Path,
    schema: str = "tester",
    table: str = "earthquake",
) -> tuple[str, bool, str]:
    """The proposal's output columns and types against its YAML, as the enforced contract will check at dbt build."""
    name = "output columns and types match the YAML"
    con = None
    try:
        con = open_check_session(db_path)
        plain = staging_select(sql, schema, table)
        described = con.execute(f"describe select * from (\n{plain}\n) q").fetchall()
    except (duckdb.Error, ContextPackError) as exc:
        return name, False, f"could not describe the proposal: {str(exc).splitlines()[0]}"
    finally:
        if con is not None:
            con.close()
    produced = [(row[0], row[1]) for row in described]
    declared = [(column["name"], _duckdb_type(column["data_type"])) for column in columns]
    if produced == declared:
        return name, True, f"{len(produced)} columns"
    mismatches = [f"{p} vs {d}" for p, d in zip(produced, declared) if p != d]
    if len(produced) != len(declared):
        mismatches.append(f"{len(produced)} produced vs {len(declared)} declared")
    return name, False, "; ".join(mismatches[:5])


def _strip_comments_and_strings(sql: str) -> str:
    """The SQL with line comments removed and string literals emptied, so keywords inside them do not count."""
    return re.sub(r"'(?:[^']|'')*'", "''", re.sub(r"--[^\n]*", "", sql))


def statement_problem(sql: str) -> str | None:
    """Why a drill-down block is not one read-only select, or None when it is."""
    code = _strip_comments_and_strings(sql).strip()
    if not re.match(r"(select|with)\b", code, re.IGNORECASE):
        return "does not start with select or with"
    if ";" in code:
        return "holds more than one statement"
    found = FORBIDDEN_STATEMENT.search(code)
    if found:
        return f"uses {found.group(1).lower()}"
    return None


def _interrupt_quietly(con: duckdb.DuckDBPyConnection) -> None:
    """Interrupt a check session from the timer thread. A session closed in the meantime has nothing to interrupt."""
    try:
        con.interrupt()
    except duckdb.Error:
        pass


def run_capped(con: duckdb.DuckDBPyConnection, sql: str) -> list[tuple]:
    """Run one checked block under CHECK_TIMEOUT_SECONDS and return at most CHECK_ROW_CAP + 1 rows.

    fetchmany streams, so a block with a huge result is never materialized. A block still running at the time limit is
    interrupted, which raises duckdb.InterruptException like any other DuckDB error.
    """
    timer = threading.Timer(CHECK_TIMEOUT_SECONDS, _interrupt_quietly, args=(con,))
    timer.start()
    try:
        return con.execute(sql).fetchmany(CHECK_ROW_CAP + 1)
    finally:
        timer.cancel()


def check_sql_blocks(
    markdown: str, db_path: Path, schema: str = "tester", table: str = "earthquake"
) -> list[dict[str, Any]]:
    """Run each drill-down block against the local warehouse, with the lake table swapped for the local one.

    Every block must be one read-only select, must return the same rows under each of CHECK_TIME_ZONES, and must fit
    the agent tool's CHECK_ROW_CAP. Each session is bounded in memory and threads and locked down after the attach
    (open_check_session), and each run of a block has a time limit (run_capped). The canonical placeholder block is
    skipped: the render is built and checked by render_lake_sql.py.
    """
    results = []
    sessions: dict[str, duckdb.DuckDBPyConnection] = {}
    relation = local_relation(schema, table)
    try:
        for zone in CHECK_TIME_ZONES:
            sessions[zone] = open_check_session(db_path, zone)
        for index, block in enumerate(sql_blocks(markdown), start=1):
            if block == CANONICAL_PLACEHOLDER:
                results.append(
                    {"block": index, "status": "skipped: canonical placeholder", "rows": None}
                )
                continue
            local = block.replace(LAKE_TABLE, relation).rstrip().rstrip(";")
            if LAKE_CATALOG_PLACEHOLDER in local or LAKE_SCHEMA_PLACEHOLDER in local:
                results.append(
                    {
                        "block": index,
                        "status": "references a lake table other than earthquake",
                        "rows": None,
                    }
                )
                continue
            problem = statement_problem(local)
            if problem:
                results.append(
                    {"block": index, "status": f"not a read-only select: {problem}", "rows": None}
                )
                continue
            try:
                rows = {zone: run_capped(con, local) for zone, con in sessions.items()}
            except duckdb.Error as exc:
                results.append(
                    {"block": index, "status": f"error: {str(exc).splitlines()[0]}", "rows": None}
                )
                continue
            first = rows[CHECK_TIME_ZONES[0]]
            if len(first) > CHECK_ROW_CAP:
                results.append(
                    {
                        "block": index,
                        "status": f"returns more than the {CHECK_ROW_CAP}-row cap",
                        "rows": None,
                    }
                )
                continue
            if any(other != first for other in rows.values()):
                results.append(
                    {
                        "block": index,
                        "status": f"rows differ across {', '.join(CHECK_TIME_ZONES)}",
                        "rows": None,
                    }
                )
                continue
            results.append({"block": index, "status": "ok", "rows": len(first)})
    finally:
        for con in sessions.values():
            con.close()
    return results


def block_verdict(status: str) -> str:
    """pass, skipped or FAIL, for REVIEW.md and the exit code."""
    if status == "ok":
        return "pass"
    return "skipped" if status.startswith("skipped") else "FAIL"


def file_hashes(paths: Sequence[str] = HUMAN_OWNED) -> dict[str, str]:
    """The SHA-256 of each human-owned file, recorded before and after a run."""
    return {rel: hashlib.sha256((HERE / rel).read_bytes()).hexdigest() for rel in paths}


def _diff(current: Path, proposed: str, label: str) -> str:
    """A unified diff of a proposal against the current file in dbt/, or "(no change)"."""
    lines = difflib.unified_diff(
        current.read_text().splitlines(keepends=True),
        proposed.splitlines(keepends=True),
        fromfile=f"dbt/{current.relative_to(DBT_DIR)}",
        tofile=f"context_pack_draft/{label}",
    )
    return "".join(lines) or "(no change)\n"


def review_markdown(
    output: Mapping[str, Any],
    staging_yml: str,
    staging_checks: Sequence[tuple[str, bool, str]],
    block_checks: Mapping[str, Sequence[Mapping[str, Any]]],
    placeholder_count: int,
    hashes_before: Mapping[str, str],
    hashes_after: Mapping[str, str],
    record: Mapping[str, Any],
) -> str:
    """REVIEW.md: what the reviewer checks before promoting anything."""
    out = ["# Context pack review", ""]
    out += ["## The call", ""]
    tokens = record["tokens"]
    out.append(
        f"- Model served: `{record['model_served']}` (fallback ran: {record['fallback_ran']})"
    )
    out.append(
        f"- Tokens: input {tokens['input']:,}, cache write {tokens['cache_write']:,}, "
        f"cache read {tokens['cache_read']:,}, output {tokens['output']:,}"
    )
    cost = record["cost_usd"]
    out.append(
        f"- Cost: {'$' + format(cost, '.4f') if cost is not None else 'not priced'} ({PRICING_SOURCE})"
    )
    out.append(f"- Latency: {record['latency_s']} s")
    out.append(f"- Prompt sha256: `{record['prompt_sha256']}`")
    out += ["", "## Deterministic checks", "", "| Check | Result | Detail |", "|---|---|---|"]
    for name, passed, detail in staging_checks:
        out.append(f"| staging: {name} | {'pass' if passed else 'FAIL'} | {detail} |")
    out.append(
        f"| SKILL.md holds {CANONICAL_PLACEHOLDER} exactly once | "
        f"{'pass' if placeholder_count == 1 else 'FAIL'} | found {placeholder_count} |"
    )
    unchanged = hashes_before == hashes_after
    out.append(
        f"| human-owned files unchanged | {'pass' if unchanged else 'FAIL'} | {', '.join(HUMAN_OWNED)} |"
    )
    for doc, results in block_checks.items():
        for result in results:
            rows = "" if result["rows"] is None else f", {result['rows']} rows"
            out.append(
                f"| {doc} sql block {result['block']} runs locally in {' and '.join(CHECK_TIME_ZONES)} | "
                f"{block_verdict(result['status'])} | {result['status']}{rows} |"
            )
    out += ["", "## Claude's review notes", ""]
    out += [f"- **{note['file']}**: {note['note']}" for note in output["review_notes"]] or [
        "(none)"
    ]
    out += ["", "## Proposed staging SQL, against the current model", "", "```diff"]
    out.append(
        _diff(CURRENT_STAGING_SQL, output["staging_sql"], "stg_usgs__earthquakes.sql").rstrip()
    )
    out += ["```", "", "## Proposed staging YAML, against the current file", "", "```diff"]
    out.append(_diff(CURRENT_STAGING_YML, staging_yml, "_stg_usgs__models.yml").rstrip())
    out += ["```", ""]
    return "\n".join(out)


def _shown(path: Path) -> str:
    """A path relative to this directory when it is inside it, for messages."""
    return str(path.relative_to(HERE)) if path.is_relative_to(HERE) else str(path)


def append_ledger(entry: Mapping[str, Any], path: Path = LEDGER) -> None:
    """Append one JSON line to the call ledger."""
    with path.open("a") as handle:
        handle.write(json.dumps(entry, sort_keys=True) + "\n")


def _check_out_dir(out_dir: Path) -> Path:
    """Refuse any output directory inside dbt/ or lake_sql/, where the human-owned and rendered files live."""
    resolved = out_dir.resolve()
    for protected in (DBT_DIR, LAKE_SQL_DIR):
        if resolved == protected or protected in resolved.parents:
            raise ContextPackError(
                f"--out {out_dir} is inside {protected.relative_to(HERE)}/, which the pack never writes"
            )
    return resolved


def generate(
    db_path: Path,
    schema: str,
    table: str,
    out_dir: Path,
    dry_run: bool,
    env_file: Path | None = None,
    connector_dir: Path | None = None,
) -> int:
    """Profile the table, make the one Claude call, write the draft and REVIEW.md, and run the checks.

    Args:
        db_path: the DuckDB warehouse, opened read-only.
        schema: the connector's schema in the warehouse.
        table: the synced table.
        out_dir: where the draft goes. Never inside dbt/ or lake_sql/.
        dry_run: profile and build the prompt only; no API call.
        env_file: an optional KEY=VALUE file holding ANTHROPIC_API_KEY.
        connector_dir: the connector directory, when it cannot be found from this file's location.

    Returns:
        0 when every check passes, 2 when a check fails.
    """
    out_dir = _check_out_dir(out_dir)
    if not dry_run:
        require_api_key(env_file)
    hashes_before = file_hashes()
    profile = profile_table(db_path, schema, table)
    request = build_request(profile, read_context_files(connector_dir=connector_dir))
    digest = prompt_hash(request)
    profile_text = json.dumps(profile, indent=1, sort_keys=True) + "\n"
    out_dir.mkdir(parents=True, exist_ok=True)
    if dry_run:
        # A separate file, so a dry run never leaves a new profile beside an older run's response.json.
        (out_dir / "profile.dry_run.json").write_text(profile_text)
        system_chars = sum(len(block["text"]) for block in request["system"])
        print(
            f"dry run: profiled {profile['row_count']:,} rows, {len(profile['columns'])} columns"
        )
        print(f"prompt: {system_chars:,} system characters, sha256 {digest}")
        print(f"wrote {_shown(out_dir)}/profile.dry_run.json; no API call made")
        return 0

    (out_dir / "profile.json").write_text(profile_text)
    print(
        f"profiled {profile['row_count']:,} rows; calling {MODEL} (effort {EFFORT}) ...",
        flush=True,
    )
    ledger_fields = {
        "prompt_sha256": digest,
        "profile_sha256": hashlib.sha256(json.dumps(profile, sort_keys=True).encode()).hexdigest(),
    }
    output, record = call_claude(request, ledger_fields)

    staging_yml = render_staging_yml(output["model_description"], output["columns"])
    (out_dir / "response.json").write_text(json.dumps(output, indent=1, sort_keys=True) + "\n")
    (out_dir / "stg_usgs__earthquakes.sql").write_text(output["staging_sql"].rstrip() + "\n")
    (out_dir / "_stg_usgs__models.yml").write_text(staging_yml)
    (out_dir / "AGENTS.md").write_text(output["agents_md"].rstrip() + "\n")
    (out_dir / "SKILL.md").write_text(output["skill_md"].rstrip() + "\n")

    staging_checks = check_staging_sql(output["staging_sql"], output["columns"])
    staging_checks.append(
        check_staging_columns(output["staging_sql"], output["columns"], db_path, schema, table)
    )
    block_checks = {
        "SKILL.md": check_sql_blocks(output["skill_md"], db_path, schema, table),
        "AGENTS.md": check_sql_blocks(output["agents_md"], db_path, schema, table),
    }
    placeholder_count = output["skill_md"].count(CANONICAL_PLACEHOLDER)
    hashes_after = file_hashes()
    review = review_markdown(
        output,
        staging_yml,
        staging_checks,
        block_checks,
        placeholder_count,
        hashes_before,
        hashes_after,
        record,
    )
    (out_dir / "REVIEW.md").write_text(review)

    failures = [name for name, passed, _ in staging_checks if not passed]
    failures += [
        f"{doc} block {r['block']}"
        for doc, rs in block_checks.items()
        for r in rs
        if block_verdict(r["status"]) == "FAIL"
    ]
    if placeholder_count != 1:
        failures.append("canonical placeholder count")
    if hashes_before != hashes_after:
        failures.append("human-owned files changed")
    cost = record["cost_usd"]
    print(f"wrote {_shown(out_dir)}/ (review REVIEW.md before promoting anything)")
    print(
        f"tokens: {record['tokens']}; cost {'$' + format(cost, '.4f') if cost is not None else 'not priced'}; "
        f"{record['latency_s']} s; ledger {LEDGER.name}"
    )
    print(f"checks: {'all pass' if not failures else 'FAIL: ' + '; '.join(failures)}")
    return 0 if not failures else 2


# ---------------------------------------------------------------------------------------------------------------------
# assemble: the reviewed drafts plus the reviewed render, no AI involved
# ---------------------------------------------------------------------------------------------------------------------


def check_render(canonical: str, lake_catalog: str, lake_schema: str) -> dict[str, str]:
    """Refuse a render that is not the current lake render for this catalog and schema. Returns its stamp."""
    stamp = render_lake_sql.parse_stamp(canonical)
    if stamp is None:
        raise ContextPackError(
            "the render has no stamp line; re-render it with render_lake_sql.py"
        )
    if stamp.get("target") != render_lake_sql.LAKE_TARGET:
        raise ContextPackError(f"the render is for target {stamp.get('target')}, not lake")
    if stamp.get("single_row") != "false":
        raise ContextPackError(
            "the render is the single-row form; SKILL.md documents the three answer columns"
        )
    current = render_lake_sql.current_inputs()
    if stamp.get("start_date") != current["start_date"]:
        raise ContextPackError(
            f"the render's start_date {stamp.get('start_date')} is not {current['start_date']}, the start_date in "
            "dbt/dbt_project.yml; set start_date in dbt/dbt_project.yml and re-render without --var start_date"
        )
    stale = [key for key, value in current.items() if stamp.get(key) != value]
    if stale:
        raise ContextPackError(
            f"the render is stale ({', '.join(stale)} changed since); re-render it"
        )
    relation = f'"{lake_catalog}"."{lake_schema}"."earthquake"'
    if relation not in canonical:
        raise ContextPackError(
            f"the render does not read {relation}; render it with "
            f"--var lake_catalog={lake_catalog} --var lake_schema={lake_schema}"
        )
    for marker in (f'"{LOCAL_ALIAS}"', "rows_md5"):
        if marker in canonical:
            raise ContextPackError(
                f"the render holds {marker}, so it is not the lake answer string"
            )
    return stamp


def assemble(
    draft_dir: Path, sql_path: Path, lake_catalog: str, lake_schema: str, out_dir: Path
) -> int:
    """Paste the reviewed render and the lake names into the reviewed drafts, and write agent/. No AI involved.

    Args:
        draft_dir: the reviewed draft holding AGENTS.md and SKILL.md with their placeholders.
        sql_path: the lake render from render_lake_sql.py.
        lake_catalog: the alias the agent's DuckDB session attaches the lake catalog under.
        lake_schema: the deployed connection's schema in that catalog.
        out_dir: where AGENTS.md and SKILL.md go. Never inside dbt/ or lake_sql/.

    Returns:
        0 on success.
    """
    out_dir = _check_out_dir(out_dir)
    # The published agent/ files keep both placeholders; anything else must be a real, plain identifier.
    is_template = (lake_catalog, lake_schema) == (LAKE_CATALOG_TEMPLATE, LAKE_SCHEMA_TEMPLATE)
    for flag, value in (("--lake-catalog", lake_catalog), ("--lake-schema", lake_schema)):
        if not is_template and not PLAIN_IDENTIFIER.fullmatch(value):
            raise ContextPackError(f"{flag} {value!r} is not a plain SQL identifier")
    canonical = sql_path.read_text().strip()
    try:
        stamp = check_render(canonical, lake_catalog, lake_schema)
    except ContextPackError as exc:
        raise ContextPackError(f"{sql_path.name}: {exc}") from exc
    skill = (draft_dir / "SKILL.md").read_text()
    if skill.count(CANONICAL_PLACEHOLDER) != 1:
        raise ContextPackError(
            f"{draft_dir.name}/SKILL.md must hold {CANONICAL_PLACEHOLDER} exactly once"
        )
    documents = {
        "AGENTS.md": (draft_dir / "AGENTS.md").read_text(),
        "SKILL.md": skill.replace(CANONICAL_PLACEHOLDER, canonical),
    }
    out_dir.mkdir(parents=True, exist_ok=True)
    for name, text in documents.items():
        text = text.replace(LAKE_CATALOG_PLACEHOLDER, lake_catalog).replace(
            LAKE_SCHEMA_PLACEHOLDER, lake_schema
        )
        leftover = sorted(set(re.findall(r"\{\{[A-Z_]+\}\}", text)))
        if leftover:
            raise ContextPackError(f"{name} still holds placeholders: {', '.join(leftover)}")
        (out_dir / name).write_text(text)
        print(f"wrote {_shown(out_dir / name)}")
    print(
        f"canonical string: week {stamp['week']}, updated_at_cutoff {stamp['updated_at_cutoff']}"
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    """Parse the command line and run generate or assemble. Returns the process exit code."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    commands = parser.add_subparsers(dest="command", required=True)

    gen = commands.add_parser(
        "generate", help="profile the table, call Claude once, write the draft"
    )
    gen.add_argument(
        "--duckdb",
        type=Path,
        help="the DuckDB warehouse, opened read-only (default: the connector's files/warehouse.db)",
    )
    gen.add_argument("--schema", default="tester", help="the connector's schema in the warehouse")
    gen.add_argument("--table", default="earthquake")
    gen.add_argument("--out", type=Path, default=DRAFT_DIR)
    gen.add_argument(
        "--dry-run", action="store_true", help="profile and build the prompt; no API call"
    )
    gen.add_argument("--env-file", type=Path, help="a KEY=VALUE file holding ANTHROPIC_API_KEY")
    gen.add_argument("--connector-dir", type=Path, help="the usgs_earthquake connector directory")

    asm = commands.add_parser(
        "assemble", help="fill the reviewed drafts' placeholders into agent/"
    )
    asm.add_argument(
        "--sql", type=Path, required=True, help="the reviewed render from render_lake_sql.py"
    )
    asm.add_argument(
        "--lake-catalog",
        required=True,
        help="the alias the agent's session attaches the lake catalog under",
    )
    asm.add_argument(
        "--lake-schema", required=True, help="the deployed connection's schema in that catalog"
    )
    asm.add_argument("--draft", type=Path, default=DRAFT_DIR)
    asm.add_argument("--out", type=Path, default=AGENT_DIR)

    args = parser.parse_args(argv)
    try:
        if args.command == "generate":
            connector_dir = args.connector_dir.resolve() if args.connector_dir else None
            db_path = (
                args.duckdb or (connector_dir or find_connector_dir()) / "files" / "warehouse.db"
            )
            return generate(
                db_path.resolve(),
                args.schema,
                args.table,
                args.out,
                args.dry_run,
                env_file=args.env_file,
                connector_dir=connector_dir,
            )
        return assemble(args.draft, args.sql, args.lake_catalog, args.lake_schema, args.out)
    except ContextPackError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
