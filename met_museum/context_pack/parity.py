"""Run the context pack's models on Snowflake and on DuckDB over the same Managed Data Lake tables, and compare them.

Two lanes:

    compute  The same dbt models and the same checked queries on both engines, compared exactly. Every result is
             normalized (nulls, booleans, dates, floats to 12 significant digits) and hashed; a check passes only when
             both engines return the same rows in the same order.
    ai       Snowflake Cortex AI_FILTER, AI_CLASSIFY, AI_COMPLETE and AI_AGG over the connector's visual_description,
             against DuckDB Python functions with the same names and argument shapes on a `claude` (Anthropic API) or
             `ollama` (local model) backend. Two models never answer identically, so this lane reports an agreement
             rate per function, never pass or fail.

Neither engine needs the models built. The script compiles the dbt project for each engine (targets snowflake and
lake), then inlines every model as a CTE ahead of each checked query, so nothing is created in Snowflake or the lake.

Usage, from this directory:

    python parity.py --lane compute --lake-config lake.json --snowflake-connection <connection name> \
        --snowflake-database <catalog-linked database> --output parity_output_<date>.txt

    python parity.py --lane ai --backend claude --sample 25 ...

Secrets never pass on the command line. The Polaris client secret is read from --polaris-secret-file (default
secrets/polaris_client_secret) and the Anthropic API key from ANTHROPIC_API_KEY or --anthropic-key-file. The Snowflake
connection comes from ~/.snowflake/connections.toml by name.
"""

from __future__ import annotations

# For command-line arguments
import argparse

# For fingerprinting normalized result sets
import hashlib

# For reading the compiled manifests and the lake config
import json

# For the local model backend's HTTP calls
import os

# For matching the compiled ref prefixes
import re

# For calling dbt compile
import subprocess

# For the exit code and stderr
import sys

# For timestamps in the report
from datetime import date, datetime, timezone

# For exact decimal normalization
from decimal import Decimal

# For paths
from pathlib import Path

# For type hints
from typing import Any, Callable, Sequence

HERE = Path(__file__).resolve().parent
DBT_DIR = HERE / "dbt"
DBT = os.environ.get("DBT_BIN", str(Path.home() / ".local/bin/dbt"))

# The compile-only database and schema each target's refs resolve to. They never exist: every ref to them is replaced
# by the model's CTE name before a query runs.
SNOWFLAKE_COMPILE_DATABASE = "MET_PACK"
SNOWFLAKE_COMPILE_SCHEMA = "MET_MUSEUM_CONTEXT"

# The six tables the connector writes, as the dbt source names them.
SOURCE_TABLES = (
    "department",
    "object",
    "object_constituent",
    "object_tag",
    "object_visual_attribute",
    "object_depicted_element",
)

# Models the checked queries read. The time spine is left out: it reads nothing from the lake.
SKIP_MODELS = {"metricflow_time_spine"}

# Every check returns an aggregate, so a result this long means a broken query, not a big answer.
MAX_RESULT_ROWS = 100_000

# The local Ollama endpoint for the --backend ollama judge, unless OLLAMA_HOST overrides it.
DEFAULT_OLLAMA_HOST = "http://localhost:11434"

# The subject categories the connector's validator accepts, the label set AI_CLASSIFY picks from.
SUBJECT_CATEGORIES = [
    "landscape",
    "portrait",
    "religious",
    "mythological",
    "history",
    "genre_scene",
    "still_life",
    "animal",
    "architecture",
    "marine",
    "abstract",
    "other",
]

# The hero question: public-domain landscapes with cypress trees from the 1880s. Every live row is public domain.
HERO_SQL = """
select o.object_id, o.title, o.artist_display_name, o.object_date, o.subject_category, o.object_url
from fct_objects_enriched as o
where o.subject_category = 'landscape'
  and o.object_begin_date between 1880 and 1889
  and exists (
    select 1 from fct_object_depicted_element as e
    where e.object_id = o.object_id
      and (' ' || e.element || ' ' like '% cypress %' or ' ' || e.element || ' ' like '% cypresses %')
  )
order by o.object_id
"""

# The same question answered from the curators' tags alone, with no AI column, for contrast.
HERO_CURATOR_SQL = """
select o.object_id, o.title, o.artist_display_name, o.object_date, o.object_url
from fct_objects_enriched as o
where o.object_begin_date between 1880 and 1889
  and exists (select 1 from fct_object_tag as t where t.object_id = o.object_id and lower(t.term) = 'landscapes')
  and exists (select 1 from fct_object_tag as t where t.object_id = o.object_id and lower(t.term) like 'cypress%')
order by o.object_id
"""

# The compute lane: name, SQL over the model CTEs. Every query orders its rows fully, so equal results hash equal.
COMPUTE_CHECKS: list[tuple[str, str]] = [
    (
        "model_row_counts",
        """
select 'stg_met__department' as model, count(*) as row_count from stg_met__department
union all select 'stg_met__object', count(*) from stg_met__object
union all select 'stg_met__object_constituent', count(*) from stg_met__object_constituent
union all select 'stg_met__object_tag', count(*) from stg_met__object_tag
union all select 'stg_met__object_visual_attribute', count(*) from stg_met__object_visual_attribute
union all select 'stg_met__object_depicted_element', count(*) from stg_met__object_depicted_element
union all select 'fct_objects_enriched', count(*) from fct_objects_enriched
union all select 'fct_object_tag', count(*) from fct_object_tag
union all select 'fct_object_depicted_element', count(*) from fct_object_depicted_element
order by model
""",
    ),
    (
        "stamp_rule_rows_dropped",
        """
select 'object_constituent' as source_table,
  (select count(*) from {src}."object_constituent") as source_rows,
  (select count(*) from stg_met__object_constituent) as current_rows
union all select 'object_tag',
  (select count(*) from {src}."object_tag"),
  (select count(*) from stg_met__object_tag)
union all select 'object_depicted_element',
  (select count(*) from {src}."object_depicted_element"),
  (select count(*) from stg_met__object_depicted_element)
order by source_table
""",
    ),
    ("fct_objects_enriched_all_rows", "select * from fct_objects_enriched order by object_id"),
    ("fct_object_tag_all_rows", "select * from fct_object_tag order by object_tag_key"),
    (
        "fct_object_depicted_element_all_rows",
        "select * from fct_object_depicted_element order by object_element_key",
    ),
    (
        "metric_public_domain_object_count",
        "select count(object_id) as public_domain_object_count, "
        "sum(case when is_enriched then 1 else 0 end) as enriched_object_count from fct_objects_enriched",
    ),
    (
        "agents_objects_by_subject",
        """
select subject_category, count(object_id) as public_domain_object_count,
  sum(case when is_enriched then 1 else 0 end) as enriched_object_count
from fct_objects_enriched
where department_name = 'European Paintings'
group by subject_category
order by public_domain_object_count desc, subject_category nulls first
""",
    ),
    (
        "metric_curator_tag_agreement",
        """
select sum(curator_tags_matched) as curator_tags_matched_total,
  sum(curator_tags_matched + curator_tags_missed) as curator_tags_scored_total,
  cast(sum(curator_tags_matched) as double)
    / nullif(sum(curator_tags_matched + curator_tags_missed), 0) as curator_tag_agreement
from fct_objects_enriched
""",
    ),
    (
        "saved_query_objects_by_department_decade",
        """
select department_name, object_decade, count(object_id) as public_domain_object_count
from fct_objects_enriched
group by department_name, object_decade
order by department_name, object_decade nulls first
""",
    ),
    ("hero_question", HERO_SQL),
    ("hero_question_curator_tags_only", HERO_CURATOR_SQL),
]


# --------------------------------------------------------------------------------------------------------------------
# Compiling and inlining the dbt models
# --------------------------------------------------------------------------------------------------------------------


def compile_target(target: str, dbt_vars: dict[str, str], env: dict[str, str]) -> Path:
    """
    Compile the dbt project for one target into dbt/target_<target>/ and return that directory.
    Args:
        target: the profiles.yml target, snowflake or lake.
        dbt_vars: the --vars mapping.
        env: extra environment for the compile (the snowflake target reads its profile from the environment).
    """
    target_path = DBT_DIR / f"target_{target}"
    (DBT_DIR / ".duckdb" / "lake").mkdir(parents=True, exist_ok=True)
    command = [
        DBT,
        "compile",
        "--profiles-dir",
        ".",
        "--target",
        target,
        "--vars",
        json.dumps(dbt_vars),
        "--target-path",
        str(target_path.name),
        "--static-analysis",
        "off",
    ]
    result = subprocess.run(
        command,
        cwd=DBT_DIR,
        env={**os.environ, **env},
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"dbt compile --target {target} failed:\n{result.stdout[-4000:]}\n{result.stderr[-2000:]}"
        )
    return target_path


def model_ctes(target_path: Path, ref_prefix: re.Pattern[str]) -> str:
    """
    Return every compiled model as a CTE, in dependency order, with refs rewritten to the CTE names.
    Args:
        target_path: the compile's target directory, holding manifest.json and compiled/.
        ref_prefix: matches the database.schema. prefix the target's refs compile to.
    """
    manifest = json.loads((target_path / "manifest.json").read_text())
    models = {
        unique_id: node
        for unique_id, node in manifest["nodes"].items()
        if node["resource_type"] == "model" and node["name"] not in SKIP_MODELS
    }
    ordered: list[str] = []
    visiting: set[str] = set()

    def visit(unique_id: str) -> None:
        if unique_id in ordered:
            return
        if unique_id in visiting:
            raise RuntimeError(f"dependency cycle at {unique_id}")
        visiting.add(unique_id)
        for parent in models[unique_id]["depends_on"]["nodes"]:
            if parent in models:
                visit(parent)
        visiting.discard(unique_id)
        ordered.append(unique_id)

    for unique_id in sorted(models):
        visit(unique_id)

    ctes = []
    for unique_id in ordered:
        node = models[unique_id]
        compiled_file = (
            target_path / "compiled" / node["package_name"] / node["original_file_path"]
        )
        sql = ref_prefix.sub("", compiled_file.read_text()).strip().rstrip(";")
        ctes.append(f"{node['name']} as (\n{sql}\n)")
    return "with " + ",\n".join(ctes)


def with_models(ctes: str, query: str) -> str:
    """
    Put a checked query after the model CTEs. A query that starts with its own WITH has its CTEs appended instead.
    Args:
        ctes: the output of model_ctes.
        query: the checked query.
    """
    stripped = query.strip()
    if stripped.lower().startswith("with "):
        return f"{ctes},\n{stripped[5:]}"
    return f"{ctes}\n{stripped}"


# --------------------------------------------------------------------------------------------------------------------
# Engines
# --------------------------------------------------------------------------------------------------------------------


class Engine:
    """One query engine: a name, the model CTEs compiled for it, and how to run SQL and reach the source schema."""

    def __init__(
        self,
        name: str,
        ctes: str,
        source_schema: str,
        run: Callable[[str], tuple[list[str], list[tuple]]],
    ):
        self.name = name
        self.ctes = ctes
        self.source_schema = source_schema
        self.run = run

    def query(self, sql: str) -> tuple[list[str], list[tuple]]:
        """
        Run a checked query after the model CTEs, and return the lowercase column names and the rows.
        Args:
            sql: the checked query; {src} expands to the connector's source schema.
        """
        return self.run(with_models(self.ctes, sql.replace("{src}", self.source_schema)))


def connect_snowflake(connection_name: str, role: str | None):
    """
    Open a Snowflake connection by name from ~/.snowflake/connections.toml, with the session in UTC.
    Args:
        connection_name: the connection's name in connections.toml.
        role: a role that can read the catalog-linked database; the connection's default role when None.
    """
    import snowflake.connector

    options = {"role": role} if role else {}
    connection = snowflake.connector.connect(connection_name=connection_name, **options)
    connection.cursor().execute("alter session set timezone = 'UTC'")
    return connection


def fetch_bounded(cursor, sql: str) -> list[tuple]:
    """
    Fetch a query's rows, up to MAX_RESULT_ROWS.
    Args:
        cursor: a Snowflake cursor or a DuckDB relation that has run the query.
        sql: the query, named in the error.
    Returns:
        The rows.
    Raises:
        RuntimeError: when the query returns more than MAX_RESULT_ROWS rows.
    """
    rows = cursor.fetchmany(MAX_RESULT_ROWS + 1)
    if len(rows) > MAX_RESULT_ROWS:
        raise RuntimeError(f"A check returned more than {MAX_RESULT_ROWS} rows: {sql[:200]}")
    return rows


def snowflake_engine(args: argparse.Namespace) -> Engine:
    """
    Compile the models for Snowflake and return an Engine over the catalog-linked database.
    Args:
        args: the parsed command line.
    """
    env = {
        "SNOWFLAKE_ACCOUNT": "compile-only",
        "SNOWFLAKE_USER": "compile-only",
        "SNOWFLAKE_PRIVATE_KEY_PATH": "/dev/null",
        "SNOWFLAKE_ROLE": "compile-only",
        "SNOWFLAKE_WAREHOUSE": "compile-only",
        "SNOWFLAKE_DATABASE": SNOWFLAKE_COMPILE_DATABASE,
        "SNOWFLAKE_SCHEMA": SNOWFLAKE_COMPILE_SCHEMA,
    }
    target_path = compile_target(
        "snowflake",
        {
            "lake_database": args.snowflake_database,
            "lake_schema": args.lake_schema,
            "fivetran_deleted_tables": args.fivetran_deleted_tables,
        },
        env,
    )
    prefix = re.compile(
        rf"\b{SNOWFLAKE_COMPILE_DATABASE}\.{SNOWFLAKE_COMPILE_SCHEMA}\.", re.IGNORECASE
    )
    connection = connect_snowflake(args.snowflake_connection, args.snowflake_role)

    def run(sql: str) -> tuple[list[str], list[tuple]]:
        cursor = connection.cursor()
        cursor.execute(sql)
        return [column[0].lower() for column in cursor.description], fetch_bounded(cursor, sql)

    return Engine(
        "snowflake",
        model_ctes(target_path, prefix),
        f'{args.snowflake_database}."{args.lake_schema}"',
        run,
    )


def connect_duckdb_lake(lake: dict[str, str], client_secret: str):
    """
    Attach the Managed Data Lake's Polaris catalog in a new DuckDB session as `lake`, with storage credentials vended
    by the catalog.
    Args:
        lake: endpoint, catalog, client_id and region.
        client_secret: the Polaris client secret. Passed as a bound parameter and never printed.
    """
    import duckdb

    connection = duckdb.connect()
    connection.execute("install iceberg; load iceberg; install httpfs; load httpfs;")
    connection.execute("set timezone = 'UTC'")
    connection.execute(f"set s3_region = '{lake['region']}'")
    connection.execute(
        "create secret lake_catalog (type iceberg, client_id ?, client_secret ?, oauth2_scope ?, oauth2_server_uri ?)",
        [
            lake["client_id"],
            client_secret,
            lake.get("scope", "PRINCIPAL_ROLE:ALL"),
            lake["endpoint"] + "/v1/oauth/tokens",
        ],
    )
    connection.execute(
        f"attach '{lake['catalog']}' as lake (type iceberg, secret lake_catalog, endpoint '{lake['endpoint']}', default_region '{lake['region']}')"
    )
    return connection


def lake_tables_with_fivetran_deleted(args: argparse.Namespace) -> list[str]:
    """
    Return the connector tables in the lake that have a _fivetran_deleted column. The staging models filter on the
    column only where it exists (macro live_rows). Neither compile has a live connection to look, so both get this
    list as var fivetran_deleted_tables; both engines read the same Iceberg tables, so one list serves both.
    Args:
        args: the parsed command line.
    """
    lake = json.loads(Path(args.lake_config).read_text())
    connection = connect_duckdb_lake(lake, Path(args.polaris_secret_file).read_text().strip())
    try:
        found = []
        for table in SOURCE_TABLES:
            described = connection.execute(
                f'describe lake."{args.lake_schema}"."{table}"'
            ).fetchall()
            if "_fivetran_deleted" in {row[0].lower() for row in described}:
                found.append(table)
        return found
    finally:
        connection.close()


def duckdb_engine(args: argparse.Namespace) -> tuple[Engine, Any]:
    """
    Compile the models for DuckDB and return an Engine over the attached lake, plus the raw connection.
    Args:
        args: the parsed command line.
    """
    lake = json.loads(Path(args.lake_config).read_text())
    client_secret = Path(args.polaris_secret_file).read_text().strip()
    target_path = compile_target(
        "lake",
        {
            "lake_catalog": "lake",
            "lake_schema": args.lake_schema,
            "fivetran_deleted_tables": args.fivetran_deleted_tables,
        },
        {},
    )
    prefix = re.compile(r'"met_pack"\."met"\.')
    connection = connect_duckdb_lake(lake, client_secret)

    def run(sql: str) -> tuple[list[str], list[tuple]]:
        relation = connection.execute(sql)
        return [column[0].lower() for column in relation.description], fetch_bounded(relation, sql)

    return (
        Engine("duckdb", model_ctes(target_path, prefix), f'lake."{args.lake_schema}"', run),
        connection,
    )


# --------------------------------------------------------------------------------------------------------------------
# Normalizing and comparing results
# --------------------------------------------------------------------------------------------------------------------


def normalize(value: Any) -> str:
    """
    Render one value the same way whichever engine returned it.
    Args:
        value: a value from a result row.
    """
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return str(int(value))
        return f"{float(value):.12g}"
    if isinstance(value, float):
        return f"{value:.12g}"
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str):
        # Quoted, so the string "NULL" or "true" never matches SQL NULL or a boolean.
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def fingerprint(rows: Sequence[Sequence[str]]) -> str:
    """
    Return a short sha256 of normalized rows.
    Args:
        rows: rows of normalized values.
    """
    digest = hashlib.sha256()
    for row in rows:
        digest.update("\x1f".join(row).encode("utf-8"))
        digest.update(b"\x1e")
    return digest.hexdigest()[:16]


def compare(name: str, sql: str, engines: Sequence[Engine], out: Callable[[str], None]) -> bool:
    """
    Run one checked query on every engine, print both results' fingerprints, and return whether they match exactly.
    Args:
        name: the check's name.
        sql: the checked query.
        engines: the engines to compare.
        out: the report writer.
    """
    results = {}
    for engine in engines:
        columns, rows = engine.query(sql)
        results[engine.name] = (
            columns,
            [tuple(normalize(value) for value in row) for row in rows],
        )
    (left_name, (left_columns, left_rows)), (right_name, (right_columns, right_rows)) = (
        results.items()
    )
    passed = left_columns == right_columns and left_rows == right_rows
    out(f"\n[{'PASS' if passed else 'FAIL'}] {name}")
    for engine_name, (columns, rows) in results.items():
        out(f"  {engine_name:<9} rows={len(rows):<6} sha256={fingerprint(rows)}")
    show_rows = len(left_rows) <= 25
    if show_rows:
        out("  " + " | ".join(left_columns))
        for row in left_rows:
            out("  " + " | ".join(row))
    if not passed:
        if left_columns != right_columns:
            out(f"  columns differ: {left_name}={left_columns} {right_name}={right_columns}")
        differing = [
            (index, left, right)
            for index, (left, right) in enumerate(zip(left_rows, right_rows))
            if left != right
        ]
        for index, left, right in differing[:5]:
            out(f"  row {index}: {left_name}={left}")
            out(f"  row {index}: {right_name}={right}")
        if len(left_rows) != len(right_rows):
            out(
                f"  row counts differ: {left_name}={len(left_rows)} {right_name}={len(right_rows)}"
            )
    return passed


def run_compute_lane(engines: Sequence[Engine], out: Callable[[str], None]) -> bool:
    """
    Run every compute check on both engines and return whether all of them match.
    Args:
        engines: the Snowflake and DuckDB engines.
        out: the report writer.
    """
    out("\n== Compute lane: the same models and queries on both engines, compared exactly ==")
    results = [compare(name, sql, engines, out) for name, sql in COMPUTE_CHECKS]
    passed = sum(results)
    out(f"\nCompute lane: {passed} of {len(results)} checks identical on both engines.")
    return all(results)


# --------------------------------------------------------------------------------------------------------------------
# The AI lane
# --------------------------------------------------------------------------------------------------------------------

AI_FILTER_QUESTION = "Does this image description mention trees? "
AI_COMPLETE_PROMPT = (
    "Answer with one word, the season this scene most likely shows: spring, summer, autumn, winter or unknown. "
    "Scene: "
)
AI_AGG_TASK = "List the three subjects these descriptions mention most often, lowercase, comma-separated, nothing else."


class LocalAI:
    """The DuckDB side of the AI lane: the four functions, answered by Claude through the Anthropic API or by a local
    model through Ollama."""

    def __init__(self, backend: str, model: str, api_key: str | None):
        self.backend = backend
        self.model = model
        self.calls = 0
        if backend == "claude":
            import anthropic

            self.client = anthropic.Anthropic(api_key=api_key)

    def complete(self, prompt: str) -> str:
        """
        Send one prompt and return the text answer.
        Args:
            prompt: the prompt.
        """
        self.calls += 1
        if self.backend == "claude":
            message = self.client.messages.create(
                model=self.model, max_tokens=200, messages=[{"role": "user", "content": prompt}]
            )
            return "".join(block.text for block in message.content if block.type == "text").strip()
        import urllib.request

        request = urllib.request.Request(
            os.environ.get("OLLAMA_HOST", DEFAULT_OLLAMA_HOST) + "/api/generate",
            data=json.dumps({"model": self.model, "prompt": prompt, "stream": False}).encode(
                "utf-8"
            ),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=120) as response:
            return json.loads(response.read())["response"].strip()

    def ai_filter(self, prompt: str) -> bool:
        """AI_FILTER: true when the model answers the yes-or-no prompt yes."""
        answer = self.complete(prompt + "\nAnswer only yes or no.")
        return answer.lower().startswith("yes")

    def ai_classify(self, text: str, labels: list[str]) -> str:
        """AI_CLASSIFY: the one label from labels that fits text best."""
        answer = self.complete(
            f"Classify the text into exactly one of these labels: {', '.join(labels)}.\n"
            f"Answer with the label only.\nText: {text}"
        )
        cleaned = answer.strip().strip(".").lower()
        return cleaned if cleaned in labels else "unparsed:" + cleaned[:40]

    def ai_complete(self, model: str, prompt: str) -> str:
        """AI_COMPLETE: the model's answer to prompt. The model argument is accepted for shape and ignored."""
        del model
        return self.complete(prompt)

    def ai_agg(self, texts: str, task: str) -> str:
        """AI_AGG over the newline-joined texts of a group (DuckDB has no Python aggregate UDFs)."""
        return self.complete(f"{task}\n\nDescriptions:\n{texts}")


def register_ai_functions(connection: Any, local: LocalAI) -> None:
    """
    Register the four AI functions in the DuckDB session under their Snowflake names.
    Args:
        connection: the DuckDB connection.
        local: the backend.
    """
    # DuckDB 1.5 moved the type constants from duckdb.typing to duckdb.sqltypes.
    try:
        from duckdb.sqltypes import BOOLEAN, VARCHAR
    except ImportError:
        from duckdb.typing import BOOLEAN, VARCHAR

    connection.create_function("ai_filter", local.ai_filter, [VARCHAR], BOOLEAN)
    connection.create_function("ai_classify", local.ai_classify, [VARCHAR, "VARCHAR[]"], VARCHAR)
    connection.create_function("ai_complete", local.ai_complete, [VARCHAR, VARCHAR], VARCHAR)
    connection.create_function("ai_agg", local.ai_agg, [VARCHAR, VARCHAR], VARCHAR)


def sql_literal(text: str) -> str:
    """Quote a string as a SQL literal."""
    return "'" + text.replace("'", "''") + "'"


def ai_sample_sql(sample: int | None) -> str:
    """
    Return the query for the AI lane's sample, taken round-robin across subject categories so every category,
    landscapes among them, is in it: the first object of each category by id, then the second, and so on.
    Args:
        sample: how many enriched objects to send, or None for every enriched object in sample order.
    """
    limit = f" limit {int(sample)}" if sample is not None else ""
    return (
        "select object_id, subject_category, visual_description, "
        "row_number() over (partition by subject_category order by object_id) as sample_rank "
        "from fct_objects_enriched where visual_description is not null "
        f"order by sample_rank, object_id{limit}"
    )


def ai_queries(sample: int, cortex_model: str) -> dict[str, dict[str, str]]:
    """
    Return the per-function queries for both engines: object_id, answer, over the first `sample` enriched objects.
    DuckDB reads the sample from the ai_sample temp table, because DuckDB 1.5 cannot apply LIMIT to an Iceberg scan.
    Args:
        sample: how many enriched objects to send.
        cortex_model: the Cortex model AI_COMPLETE uses.
    """
    base = ai_sample_sql(sample)
    duck_base = f"select * from ai_sample order by sample_rank, object_id limit {int(sample)}"
    labels = "[" + ", ".join(sql_literal(label) for label in SUBJECT_CATEGORIES) + "]"
    filter_prompt = sql_literal(AI_FILTER_QUESTION)
    complete_prompt = sql_literal(AI_COMPLETE_PROMPT)
    return {
        "AI_FILTER": {
            "snowflake": f"with s as ({base}) select object_id, "
            f"ai_filter(prompt({sql_literal(AI_FILTER_QUESTION + '{0}')}, visual_description)) as answer "
            "from s order by object_id",
            "duckdb": f"with s as ({duck_base}) select object_id, ai_filter({filter_prompt} || visual_description) "
            "as answer from s order by object_id",
        },
        "AI_CLASSIFY": {
            "snowflake": f"with s as ({base}) select object_id, "
            f"ai_classify(visual_description, {labels}):labels[0]::varchar as answer from s order by object_id",
            "duckdb": f"with s as ({duck_base}) select object_id, ai_classify(visual_description, {labels}) as answer "
            "from s order by object_id",
        },
        "AI_COMPLETE": {
            "snowflake": f"with s as ({base}) select object_id, "
            f"ai_complete({sql_literal(cortex_model)}, {complete_prompt} || visual_description) as answer "
            "from s order by object_id",
            "duckdb": f"with s as ({duck_base}) select object_id, "
            f"ai_complete({sql_literal(cortex_model)}, {complete_prompt} || visual_description) as answer "
            "from s order by object_id",
        },
        "AI_AGG": {
            "snowflake": f"with s as ({base}) select subject_category as object_id, "
            f"ai_agg(visual_description, {sql_literal(AI_AGG_TASK)}) as answer from s "
            "group by subject_category order by subject_category",
            "duckdb": f"with s as ({duck_base}) select subject_category as object_id, "
            f"ai_agg(string_agg(visual_description, chr(10) order by object_id), {sql_literal(AI_AGG_TASK)}) "
            "as answer from s group by subject_category order by subject_category",
        },
    }


def answer_key(function: str, answer: Any) -> Any:
    """
    Reduce an answer to what agreement is measured on.
    Args:
        function: the AI function.
        answer: the raw answer.
    """
    if answer is None:
        return None
    if function == "AI_FILTER":
        return bool(answer)
    text = str(answer).strip().strip('"').strip().lower()
    if function == "AI_COMPLETE":
        words = re.findall(r"[a-z]+", text)
        return words[0] if words else ""
    if function == "AI_AGG":
        return frozenset(term.strip(" .") for term in text.split(",") if term.strip(" ."))
    return text


def run_ai_lane(
    engines: dict[str, Engine],
    local: LocalAI,
    args: argparse.Namespace,
    out: Callable[[str], None],
) -> bool:
    """
    Run the four AI functions on both engines over the same sample and report agreement per function.
    Returns False when the engines returned different keys for any function: the rates would then compare
    different samples, so the lane is invalid.
    Args:
        engines: the engines by name.
        local: the DuckDB backend.
        args: the parsed command line.
        out: the report writer.
    """
    out(
        f"\n== AI lane: Cortex against DuckDB functions on the {args.backend} backend ({local.model}) =="
    )
    out(
        "Agreement rates, not equality: two models answering the same prompt are expected to differ."
    )
    # DuckDB 1.5 fails with "IcebergScan serialization not implemented" when LIMIT reaches an Iceberg scan, so the
    # enriched rows are copied into a temp table first and the sample is limited there.
    duck = engines["duckdb"]
    duck.run(
        f"create or replace temp table ai_sample as {with_models(duck.ctes, ai_sample_sql(None))}"
    )
    _, sampled = duck.query("select count(*) from ai_sample")
    if not sampled[0][0]:
        out("\n  INVALID: no enriched rows to compare. Run an enrichment sync first.")
        return False
    valid = True
    for function, queries in ai_queries(args.sample, args.cortex_model).items():
        answers = {}
        for name, engine in engines.items():
            _, rows = engine.query(queries[name])
            answers[name] = {row[0]: answer_key(function, row[1]) for row in rows}
        only = {
            name: sorted(set(answers[name]) - set(answers[other]), key=str)
            for name, other in (("snowflake", "duckdb"), ("duckdb", "snowflake"))
        }
        if only["snowflake"] or only["duckdb"]:
            valid = False
            out(
                f"\n  {function:<12} INVALID: the engines returned different keys "
                f"(only snowflake: {only['snowflake'][:10]}, only duckdb: {only['duckdb'][:10]})"
            )
            continue
        keys = sorted(answers["snowflake"], key=str)
        if function == "AI_AGG":
            overlaps = []
            for key in keys:
                # A null answer has no terms, so it overlaps nothing.
                left = answers["snowflake"][key] or frozenset()
                right = answers["duckdb"][key] or frozenset()
                union = left | right
                overlaps.append(len(left & right) / len(union) if union else 1.0)
            rate = sum(overlaps) / len(overlaps) if overlaps else 0.0
            out(f"\n  {function:<12} groups={len(keys):<4} mean term overlap (Jaccard)={rate:.2f}")
        else:
            agreed = sum(answers["snowflake"][key] == answers["duckdb"][key] for key in keys)
            rate = agreed / len(keys) if keys else 0.0
            out(
                f"\n  {function:<12} rows={len(keys):<4} agreement={agreed}/{len(keys)} ({rate:.0%})"
            )
            if function == "AI_FILTER":
                true_counts = {
                    name: sum(bool(answers[name][key]) for key in keys) for name in answers
                }
                out(
                    f"    true answers: snowflake={true_counts['snowflake']} duckdb={true_counts['duckdb']}"
                )
        for key in keys[:10]:
            out(
                f"    {key}: snowflake={_short(answers['snowflake'][key])} duckdb={_short(answers['duckdb'][key])}"
            )
    out(f"\n  {args.backend} calls made: {local.calls}")
    return valid


def _short(value: Any) -> str:
    """Render an answer key briefly for the report."""
    if isinstance(value, frozenset):
        return "{" + ", ".join(sorted(value)) + "}"
    return str(value)[:60]


# --------------------------------------------------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------------------------------------------------


def parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    """Parse the command line."""
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--lane", choices=["compute", "ai", "both"], default="compute")
    parser.add_argument(
        "--lake-config",
        default=str(HERE / "lake.json"),
        help="endpoint, catalog, client_id, region",
    )
    parser.add_argument(
        "--polaris-secret-file", default=str(HERE / "secrets" / "polaris_client_secret")
    )
    parser.add_argument(
        "--lake-schema", default="met_museum", help="the connection's destination schema"
    )
    parser.add_argument(
        "--snowflake-connection", required=True, help="a connection name in connections.toml"
    )
    parser.add_argument("--snowflake-role", default=None, help="default: the connection's role")
    parser.add_argument(
        "--snowflake-database", required=True, help="the catalog-linked database over the lake"
    )
    parser.add_argument("--backend", choices=["claude", "ollama"], default="claude")
    parser.add_argument("--local-model", default=None, help="default: claude-sonnet-5 or llama3.2")
    parser.add_argument("--cortex-model", default="claude-sonnet-5")
    parser.add_argument(
        "--anthropic-key-file", default=str(HERE / "secrets" / "anthropic_api_key")
    )
    parser.add_argument("--sample", type=int, default=25)
    parser.add_argument("--output", default=None, help="also write the report here")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the requested lanes and return 0 when the compute lane, if run, matched exactly."""
    args = parse_args(argv)
    lines: list[str] = []

    def out(line: str) -> None:
        print(line, flush=True)
        lines.append(line)

    started = datetime.now(timezone.utc).replace(microsecond=0)
    out(f"Met Museum context pack parity, {started.isoformat()}")
    out(f'Snowflake: {args.snowflake_database}."{args.lake_schema}" (catalog-linked database)')
    out(f'DuckDB:    lake."{args.lake_schema}" (Polaris catalog attached in place)')

    args.fivetran_deleted_tables = lake_tables_with_fivetran_deleted(args)
    out(f"Tables with _fivetran_deleted: {', '.join(args.fivetran_deleted_tables) or 'none'}")
    snowflake = snowflake_engine(args)
    duck, duck_connection = duckdb_engine(args)
    compute_ok = ai_ok = True
    if args.lane in ("compute", "both"):
        compute_ok = run_compute_lane([snowflake, duck], out)
    if args.lane in ("ai", "both"):
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if args.backend == "claude" and not api_key and Path(args.anthropic_key_file).exists():
            api_key = Path(args.anthropic_key_file).read_text().strip()
        model = args.local_model or ("claude-sonnet-5" if args.backend == "claude" else "llama3.2")
        local = LocalAI(args.backend, model, api_key)
        register_ai_functions(duck_connection, local)
        ai_ok = run_ai_lane({"snowflake": snowflake, "duckdb": duck}, local, args, out)

    if args.output:
        Path(args.output).write_text("\n".join(lines) + "\n")
    return 0 if compute_ok and ai_ok else 1


if __name__ == "__main__":
    sys.exit(main())
