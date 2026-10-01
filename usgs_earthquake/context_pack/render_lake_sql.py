"""Render the canonical DuckDB SQL for the saved query weekly_exposure_by_region, as one self-contained string.

The string answers "which regions had more M4.5+ earthquakes last week than their prior 4-week average?" in any
DuckDB session that can already read the source table: the connector's local debug warehouse attached as usgs_wh, or
a Managed Data Lake catalog that the session has attached under the alias set by the dbt var lake_catalog. It
carries its own preamble, so it survives a recycled session without a separate setup step:

0. a comment line stamping what the string was rendered from (target, week, start_date, cutoff, and the hashes of the
   seed, _semantic.yml, the models and this file), so context_pack.py assemble can refuse a stale render
1. attach if not exists ':memory:' as usgs_local
2. create schema if not exists, for each schema the build uses
3. the region seed, as create or replace table ... from (values ...)
4. create or replace view for each model, parents first, the time spine included
5. the saved query, rendered by MetricFlow with the week filter, inside the outer wrapper

The string never attaches or detaches the lake catalog. The session that runs it owns that attachment.

Usage, from this directory, in a venv with this directory's requirements.txt installed:
    python render_lake_sql.py --target lake --week last-complete --var lake_catalog=<lake_catalog_alias>
    python render_lake_sql.py --target local --week 2026-09-21 --var updated_at_cutoff=2026-10-05T00:00:00Z
"""

from __future__ import annotations

# For command-line arguments
import argparse

# For reading the region seed CSV
import csv

# For the stamp's content hashes
import hashlib

# For reading dbt's manifest.json and passing --vars
import json

# For parsing the stamp line
import re

# For running dbt compile
import subprocess

# For the exit code, stderr and the venv's dbt executable
import sys

# For type hints
from collections.abc import Mapping
from datetime import date, timedelta
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
DBT_DIR = HERE / "dbt"
OUT_DIR = HERE / "lake_sql"
# The target the rendered string reads the lake from. Every other profile target reads the local warehouse.
LAKE_TARGET = "lake"

SAVED_QUERY = "weekly_exposure_by_region"
# The database every target builds its models into, and the in-memory database the preamble attaches on the lake.
LOCAL_ALIAS = "usgs_local"
# The last complete week: the Monday before this week's Monday.
LAST_COMPLETE_WEEK = "date_trunc('week', timezone('UTC', now())) - interval 7 day"
WEEK_DIMENSION = "{{ TimeDimension('metric_time', 'week') }}"
# The human-owned inputs a render depends on besides the models. Their hashes go into the stamp.
SEED_CSV = DBT_DIR / "seeds" / "region_bounds.csv"
SEMANTIC_YML = DBT_DIR / "models" / "marts" / "_semantic.yml"
# The first line of every render: what it was rendered from, so context_pack.py assemble can refuse a stale one.
STAMP_PREFIX = "-- render:"
STAMP_FIELD = re.compile(r"(\w+)=(\S+)")

# The comparison and the null-region filter live here, never in MetricFlow's where: a region filter inside MetricFlow
# applies before the offset windows are computed and blanks the 4-week average. join_to_timespine also adds
# null-region rows and future weeks, which this drops.
ANSWER_COLUMNS = ("region__name", "significant_event_count", "significant_events_4wk_avg")
ANSWER_WRAPPER = """select region__name, significant_event_count, significant_events_4wk_avg
from (
{sql}
) q
where region__name is not null
  and significant_events_vs_4wk_avg > 0
order by significant_events_vs_4wk_avg desc, region__name"""


class RenderError(RuntimeError):
    """The SQL could not be rendered: a missing manifest, a non-view model, an untyped seed column or a bad week."""


# ---------------------------------------------------------------------------------------------------------------------
# Rendering helpers: MetricFlow without a connection, the model replay, and the single-row comparison form.
# ---------------------------------------------------------------------------------------------------------------------


def _quote(value: str) -> str:
    """A SQL string literal, with embedded single quotes doubled."""
    return "'" + value.replace("'", "''") + "'"


class _ExplainOnlyClient:
    """The SqlClient MetricFlow needs to render DuckDB SQL. It never executes: the caller runs the SQL."""

    def __init__(self) -> None:
        """Set the DuckDB engine type and renderer MetricFlow reads from the client."""
        from metricflow.protocols.sql_client import SqlEngine
        from metricflow.sql.render.duckdb_renderer import DuckDbSqlPlanRenderer

        self.sql_engine_type = SqlEngine.DUCKDB
        self.sql_plan_renderer = DuckDbSqlPlanRenderer()

    def query(self, *_: Any, **__: Any) -> Any:
        """Refuse to execute: this client only renders."""
        raise RenderError("MetricFlow tried to execute through the explain-only client")

    execute = dry_run = query

    def close(self) -> None:
        """Nothing to close: the client holds no connection."""

    def render_bind_parameter_key(self, key: str) -> str:
        """DuckDB's bind parameter syntax. A render that needs one is refused in render_saved_query."""
        return f"${key}"


def model_views(manifest: Mapping[str, Any], project_dir: Path) -> list[dict]:
    """`create schema`, seed `create or replace table` and `create or replace view` statements, parents first.

    Every model is a view, so replaying these in a session that can read the source reproduces the build without
    writing anything outside the attached in-memory database. Seeds become tables built from their CSV
    (seed_table_sql), placed in the graph so a model that refs one comes after its table.
    """
    models = {uid: n for uid, n in manifest["nodes"].items() if n["resource_type"] == "model"}
    for uid, n in models.items():
        if n["config"].get("materialized") != "view":
            raise RenderError(
                f"{uid} is {n['config'].get('materialized')}, not a view; the preamble replays views"
            )
        if not n.get("compiled_code"):
            raise RenderError(f"{uid} has no compiled_code; compile the target first")
    seeds = {uid: n for uid, n in manifest["nodes"].items() if n["resource_type"] == "seed"}
    nodes = {**models, **seeds}

    ordered: list[str] = []
    state: dict[str, str] = {}

    def visit(uid: str) -> None:
        if state.get(uid) == "done":
            return
        if state.get(uid) == "visiting":
            raise RenderError(f"model dependency cycle through {uid}")
        state[uid] = "visiting"
        for parent in sorted(nodes[uid].get("depends_on", {}).get("nodes", [])):
            if parent in nodes:
                visit(parent)
        state[uid] = "done"
        ordered.append(uid)

    for uid in sorted(nodes):
        visit(uid)

    schemas = sorted({(nodes[u]["database"], nodes[u]["schema"]) for u in ordered})
    out = [
        {"relation": f'"{db}"."{schema}"', "sql": f'create schema if not exists "{db}"."{schema}"'}
        for db, schema in schemas
    ]
    for uid in ordered:
        n = nodes[uid]
        if uid in seeds:
            out.append({"relation": n["relation_name"], "sql": seed_table_sql(n, project_dir)})
            continue
        body = n["compiled_code"].rstrip().rstrip(";")
        out.append(
            {
                "relation": n["relation_name"],
                "sql": f"create or replace view {n['relation_name']} as (\n{body}\n)",
            }
        )
    return out


def wrap_single_row(key: str, sql: str, order_by: str) -> str:
    """One row per query, whatever its row count: the key, the result as JSON text (a list of row objects) and the
    text's md5, so a result copied out of a session by hand cannot drift.

    The rows are listed in `order_by` order, so the md5 is stable across sessions and DuckDB versions. An aggregate
    over an ordered subquery does not keep its order.
    """
    return (
        f"select k, rows, md5(rows) as rows_md5 from (\n"
        f"select {_quote(key)} as k, cast(to_json(list(t order by {order_by})) as varchar) as rows from (\n{sql}\n) t\n"
        f") w"
    )


def seed_table_sql(node: Mapping[str, Any], project_dir: Path) -> str:
    """`create or replace table <seed> as select <casts> from (values ...)`, read from the seed's own CSV.

    The CSV stays the source of truth: nobody copies it into SQL by hand. Every column must declare its type in
    column_types, so nothing here guesses one. An empty cell becomes null.
    """
    column_types = node["config"].get("column_types") or {}
    path = project_dir / node["original_file_path"]
    if not path.is_file():
        raise RenderError(f"seed file {path} does not exist")
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        header = next(reader)
        rows = [row for row in reader if row]
    untyped = [c for c in header if c not in column_types]
    if untyped:
        raise RenderError(f"seed {node['name']} has no column_types for {', '.join(untyped)}")
    if not rows:
        raise RenderError(f"seed {node['name']} has no rows")

    def literal(cell: str) -> str:
        return "null" if cell == "" else _quote(cell)

    values = ",\n".join("  (" + ", ".join(literal(cell) for cell in row) + ")" for row in rows)
    casts = ",\n".join(f"  cast({c} as {column_types[c]}) as {c}" for c in header)
    return (
        f"create or replace table {node['relation_name']} as\n"
        f"select\n{casts}\nfrom (values\n{values}\n) t({', '.join(header)})"
    )


def week_expression(week: str) -> str:
    """The SQL the week filter compares metric_time__week to: the last complete week, or one pinned Monday."""
    if week == "last-complete":
        return LAST_COMPLETE_WEEK
    try:
        monday = date.fromisoformat(week)
    except ValueError as exc:
        raise RenderError(
            f"--week must be last-complete or a YYYY-MM-DD Monday, not {week!r}"
        ) from exc
    if monday.weekday() != 0:
        raise RenderError(f"--week {week} is a {monday.strftime('%A')}; weeks start on Monday")
    return f"date '{monday.isoformat()}'"


def resolve_start_date(dbt_vars: Mapping[str, str], project_dir: Path = DBT_DIR) -> date:
    """start_date as the build sees it: the --var value, else dbt_project.yml."""
    import yaml  # dbt-core depends on PyYAML

    start = dbt_vars.get("start_date")
    if not start:
        project = yaml.safe_load((project_dir / "dbt_project.yml").read_text())
        start = project.get("vars", {}).get("start_date")
    if not start:
        raise RenderError("start_date is not set; pass --var start_date=YYYY-MM-DD")
    try:
        return date.fromisoformat(str(start))
    except ValueError as exc:
        raise RenderError(f"start_date must be a YYYY-MM-DD date, not {start!r}") from exc


def _sha256(path: Path) -> str:
    """The hex SHA-256 of a file's bytes."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def models_sha256(project_dir: Path = DBT_DIR) -> str:
    """One hash over every model file, path and content, so any model edit makes an older render stale."""
    digest = hashlib.sha256()
    for path in sorted((project_dir / "models").rglob("*")):
        if path.is_file() and path.suffix in (".sql", ".yml"):
            digest.update(
                str(path.relative_to(project_dir)).encode() + b"\0" + path.read_bytes() + b"\0"
            )
    return digest.hexdigest()


def current_inputs(project_dir: Path = DBT_DIR) -> dict[str, str]:
    """The stamp fields that must still match the project for a render to be current."""
    return {
        "start_date": resolve_start_date({}, project_dir).isoformat(),
        "seed_sha256": _sha256(SEED_CSV),
        "semantic_sha256": _sha256(SEMANTIC_YML),
        "models_sha256": models_sha256(project_dir),
        "renderer_sha256": _sha256(Path(__file__).resolve()),
    }


def render_stamp(
    target: str,
    week: str,
    dbt_vars: Mapping[str, str],
    single_row: bool,
    project_dir: Path = DBT_DIR,
) -> dict[str, str]:
    """The fields of a render's first line. A render is stale when any of them no longer matches the project."""
    return {
        "target": target,
        "week": week,
        "start_date": resolve_start_date(dbt_vars, project_dir).isoformat(),
        "updated_at_cutoff": dbt_vars.get("updated_at_cutoff") or "none",
        "single_row": "true" if single_row else "false",
        **{k: v for k, v in current_inputs(project_dir).items() if k != "start_date"},
    }


def format_stamp(stamp: Mapping[str, str]) -> str:
    """The stamp as the render's first line: a SQL comment of key=value fields."""
    return STAMP_PREFIX + " " + " ".join(f"{k}={v}" for k, v in stamp.items())


def parse_stamp(sql: str) -> dict[str, str] | None:
    """The stamp fields from a render's first line, or None when the first line is not a stamp."""
    first = sql.split("\n", 1)[0]
    if not first.startswith(STAMP_PREFIX):
        return None
    return dict(STAMP_FIELD.findall(first[len(STAMP_PREFIX) :]))


def check_week_coverage(
    week: str, dbt_vars: Mapping[str, str], project_dir: Path = DBT_DIR
) -> None:
    """Refuse a pinned week whose four prior weeks start before the data does.

    Weeks before start_date read as zero in fct_region_days, so a week too close to it has an understated 4-week
    average and no error to show it. The production filter (last-complete) moves with the calendar and is not checked.
    """
    if week == "last-complete":
        return
    start_date = resolve_start_date(dbt_vars, project_dir)
    first_full_week = start_date + timedelta(days=(7 - start_date.weekday()) % 7)
    earliest = first_full_week + timedelta(weeks=4)
    if date.fromisoformat(week) < earliest:
        raise RenderError(
            f"--week {week} needs four full prior weeks of data; with start_date {start_date} the earliest pinned "
            f"week is {earliest}"
        )


def compile_project(target: str, dbt_vars: Mapping[str, str], project_dir: Path = DBT_DIR) -> Path:
    """Run dbt compile for one target into its own target path, and return that path.

    --no-populate-cache keeps dbt-core 1.11 from connecting during compile; the lake target has nothing to connect
    to. dbt comes from the same venv as this interpreter.
    """
    target_path = project_dir / f"target_{target}"
    (project_dir / ".duckdb" / target).mkdir(parents=True, exist_ok=True)
    dbt = Path(sys.executable).parent / "dbt"
    command = [
        str(dbt),
        "compile",
        "--target",
        target,
        "--target-path",
        str(target_path),
        "--profiles-dir",
        ".",
        "--no-populate-cache",
        "--quiet",
        "--vars",
        json.dumps(dict(dbt_vars)),
    ]
    result = subprocess.run(command, cwd=project_dir, capture_output=True, text=True)
    if result.returncode != 0:
        raise RenderError(
            f"dbt compile --target {target} failed:\n{result.stdout}\n{result.stderr}"
        )
    return target_path


def render_saved_query(semantic_manifest: Path, week: str) -> str:
    """The saved query's DuckDB SQL for one week, rendered by MetricFlow without a connection."""
    from metricflow.engine.metricflow_engine import MetricFlowEngine, MetricFlowQueryRequest
    from metricflow_semantics.model.dbt_manifest_parser import (
        parse_manifest_from_dbt_generated_manifest,
    )
    from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup

    if not semantic_manifest.is_file():
        raise RenderError(f"{semantic_manifest} does not exist; compile the target first")
    parsed = parse_manifest_from_dbt_generated_manifest(
        manifest_json_string=semantic_manifest.read_text()
    )
    engine = MetricFlowEngine(SemanticManifestLookup(parsed), _ExplainOnlyClient())
    request = MetricFlowQueryRequest.create(
        saved_query_name=SAVED_QUERY,
        where_constraints=[f"{WEEK_DIMENSION} = {week_expression(week)}"],
    )
    statement = engine.explain(request).sql_statement
    # A bind parameter would need a value the query tool cannot pass, so the string must be complete as rendered.
    if list(statement.bind_parameter_set.param_items):
        raise RenderError(
            f"the rendered SQL has bind parameters: {list(statement.bind_parameter_set.param_items)}"
        )
    return statement.sql.rstrip().rstrip(";")


def preamble(manifest: Mapping[str, Any], project_dir: Path = DBT_DIR) -> list[str]:
    """Steps 1 to 4: attach the in-memory database, then the schemas, the seed table and the views."""
    return [f"attach if not exists ':memory:' as {LOCAL_ALIAS}"] + [
        s["sql"] for s in model_views(manifest, project_dir)
    ]


def join_statements(statements: list[str]) -> str:
    """The statements as one string, each ending in exactly one semicolon."""
    return ";\n\n".join(s.rstrip().rstrip(";") for s in statements) + ";\n"


def render(
    target: str,
    week: str,
    dbt_vars: Mapping[str, str],
    single_row: bool = False,
    compile_first: bool = True,
) -> str:
    """The full canonical string for one target and week: the stamp, the preamble and the wrapped saved query."""
    week_expression(week)
    check_week_coverage(week, dbt_vars)
    stamp = format_stamp(render_stamp(target, week, dbt_vars, single_row))
    target_path = (
        compile_project(target, dbt_vars) if compile_first else DBT_DIR / f"target_{target}"
    )
    manifest_path = target_path / "manifest.json"
    if not manifest_path.is_file():
        raise RenderError(f"{manifest_path} does not exist; compile the target first")
    manifest = json.loads(manifest_path.read_text())
    answer = ANSWER_WRAPPER.format(
        sql=render_saved_query(target_path / "semantic_manifest.json", week)
    )
    if single_row:
        answer = wrap_single_row(f"{SAVED_QUERY}|{week}", answer, order_by="t.region__name")
    return stamp + "\n" + join_statements(preamble(manifest) + [answer])


def _parse_vars(pairs: list[str]) -> dict[str, str]:
    """The repeated --var key=value arguments as a dict of dbt vars."""
    out = {}
    for pair in pairs:
        key, sep, value = pair.partition("=")
        if not sep or not key:
            raise RenderError(f"--var takes key=value, not {pair!r}")
        out[key] = value
    return out


def main(argv: list[str] | None = None) -> int:
    """Render one string and write it to lake_sql/ (or stdout). Returns the process exit code."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--target",
        required=True,
        help=f"a target in dbt/profiles.yml: {LAKE_TARGET} for the lake string, local for the debug warehouse",
    )
    parser.add_argument(
        "--week", default="last-complete", help="last-complete (production) or a pinned Monday"
    )
    parser.add_argument(
        "--var", action="append", default=[], help="a dbt var as key=value; repeatable"
    )
    parser.add_argument(
        "--single-row", action="store_true", help="return one row: the answer as JSON plus its md5"
    )
    parser.add_argument(
        "--no-compile", action="store_true", help="reuse dbt/target_<target> as it is"
    )
    parser.add_argument(
        "--stdout", action="store_true", help="print the SQL instead of writing lake_sql/"
    )
    args = parser.parse_args(argv)
    try:
        sql = render(
            args.target, args.week, _parse_vars(args.var), args.single_row, not args.no_compile
        )
    except RenderError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    if args.stdout:
        sys.stdout.write(sql)
        return 0
    OUT_DIR.mkdir(exist_ok=True)
    week_label = (
        args.week.replace("-", "") if args.week != "last-complete" else "last_complete_week"
    )
    suffix = ".single_row" if args.single_row else ""
    out = OUT_DIR / f"{SAVED_QUERY}.{args.target}.{week_label}{suffix}.sql"
    out.write_text(sql)
    print(f"wrote {out.relative_to(HERE)} ({len(sql.splitlines())} lines)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
