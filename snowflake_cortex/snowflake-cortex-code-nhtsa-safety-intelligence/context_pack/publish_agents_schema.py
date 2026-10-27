"""Publish the context pack's dbt definitions and agent guidance into Agents Schema (<database>.AGENTS) on Snowflake.

Agents Schema (https://github.com/dbt-labs/agents_schema) is a set of tables an agent can query for what a model,
column or metric means. Run this after each dbt Production run. It writes:

    AGENTS.DBT_MODEL, DBT_COLUMN, DBT_DEPENDENCY      from the run's manifest.json
    AGENTS.OSI_MODEL, OSI_DATASET, OSI_FIELD,
           OSI_METRIC, OSI_RELATIONSHIP               from the run's semantic_manifest.json, converted to OSI
    AGENTS.ROOT skill/<key>                           from agent/AGENTS.md

Agents Schema has no dbt metric table. Its canonical semantic-layer source is OSI (Open Semantic Interchange), so
the `osi` step converts the dbt Semantic Layer to OSI with MetricFlow's own converter
(metricflow.converters.msi_to_osi). The converter emits OSI 0.1.1 and the agents-schema CLI validates 0.2.0.dev0,
so `shim_osi` bridges the two. Each metric also gets its dbt definition and, when a Semantic Layer token is given,
the SQL the Semantic Layer compiles for it (compileSql, which runs no query). The synonyms and usage rules in
agent/osi_context.yaml are added as OSI `ai_context`.

Steps, from this directory:

    python publish_agents_schema.py fetch --run-id <dbt_run_id> --work-dir agents_schema_work
    python publish_agents_schema.py osi --work-dir agents_schema_work --model-name nhtsa_recalls_context \\
        --context agent/osi_context.yaml
    python publish_agents_schema.py publish --work-dir agents_schema_work \\
        --skill nhtsa-recalls-agent=agent/AGENTS.md --uses-schema <pack_schema>

Environment (or KEY=VALUE lines in a file passed with --env-file; values are never printed):

    fetch    DBT_HOST, DBT_ACCOUNT_ID, DBT_TOKEN,      a dbt platform token that can read the run's artifacts,
             DBT_PROD_ENV_ID                           and the Production environment the run must belong to
    osi      DBT_HOST, DBT_PROD_ENV_ID, DBT_TOKEN      a Semantic Layer token, for compiled SQL; optional
    publish  WAREHOUSE_CREDENTIALS                     the agents-schema CLI's Snowflake credentials, a JSON object

`fetch` and `osi` only read. `publish` writes to Snowflake: the CLI creates <database>.AGENTS if needed, merges
the skill rows into AGENTS.ROOT, and replaces the AGENTS.DBT_* tables, the AGENTS.SKILL_USE table and the
AGENTS.OSI_* tables. Replacing them drops what another project published
there and every grant on those tables, so give the pack a database of its own and keep reader access with
sql/05_reader_grants.sql.

`fetch` accepts only a successful run in the Production environment (DBT_PROD_ENV_ID), whose schemas are the ones the
pack's agent reads, and refuses a manifest with no models or a semantic manifest with no metrics. It removes
<work-dir>/osi, and `publish` refuses to run without OSI files converted from the fetched semantic manifest, so the
DBT_* and OSI_* tables always come from the same run.

Needs agents-schema 0.0.11 and metricflow 0.213.0, which bring PyYAML and jsonschema. Tested on Python 3.12.
"""

from __future__ import annotations

# For the fetch, osi, skills and publish subcommands
import argparse

# For stamping the OSI output with the semantic manifest it came from
import hashlib

# For the dbt artifacts, the API request bodies and the OSI round trip
import json

# For reading settings from the process environment
import os

# For parsing KEY=VALUE lines in an --env-file
import re

# For clearing a previous run's output directories
import shutil

# For running the agents-schema CLI
import subprocess

# For exiting with an error message that names the failed step
import sys

# For the HTTP errors the dbt APIs can return
import urllib.error

# For calling the dbt Admin API and the Semantic Layer GraphQL API
import urllib.request

# For the work directory and its files
from pathlib import Path

# For reading the context YAML and writing the OSI documents
import yaml

# The OSI version the agents-schema CLI's vendored osi-schema.json accepts.
OSI_VERSION = "0.2.0.dev0"
COMPILE_SQL = (
    "mutation($env: BigInt!, $metrics: [MetricInput!]!) "
    "{ compileSql(environmentId: $env, metrics: $metrics) { sql } }"
)
HTTP_TIMEOUT_SECONDS = 60
# Written next to the OSI files: the sha256 of the semantic_manifest.json they were converted from
OSI_SOURCE_STAMP = "source.sha256"
# dbt platform endpoints, filled in from DBT_HOST and DBT_ACCOUNT_ID
ADMIN_API_URL = "https://{host}/api/v2/accounts/{account_id}{path}"
# A multicell host <prefix>.<region>.dbt.com serves the Semantic Layer at <prefix>.semantic-layer.<region>.dbt.com
SEMANTIC_LAYER_MULTICELL_URL = "https://{prefix}.semantic-layer.{rest}/api/graphql"
# A single-cell host <region>.dbt.com serves it at semantic-layer.<host>
SEMANTIC_LAYER_SINGLE_CELL_URL = "https://semantic-layer.{host}/api/graphql"


def _env_value(raw: str) -> str:
    """A shell env-file value. A value wrapped in one matching pair of quotes loses that pair and anything after
    it; an unquoted value loses an inline ` # comment`. A quote that is part of the value (a token ending in `"`)
    and a `#` with no space before it stay.
    """
    raw = raw.strip()
    quoted = re.match(r"""(['"])(.*)\1\s*(?:#.*)?$""", raw)
    if quoted:
        return quoted.group(2)
    return re.sub(r"\s+#.*$", "", raw)


def read_env_file(path: str) -> dict[str, str]:
    """KEY=VALUE pairs from a shell env file (optionally `export`ed). Values are never printed."""
    env = {}
    for line in Path(path).expanduser().read_text().splitlines():
        match = re.match(r"\s*(?:export\s+)?([A-Z_][A-Z0-9_]*)=(.*)", line)
        if match:
            env[match.group(1)] = _env_value(match.group(2))
    return env


def settings(env_file: str | None) -> dict[str, str]:
    """The process environment, with an --env-file's values on top."""
    return {**os.environ, **(read_env_file(env_file) if env_file else {})}


def require(env: dict[str, str], *names: str) -> None:
    """Exit naming every missing setting, never its value."""
    missing = [name for name in names if not env.get(name)]
    if missing:
        sys.exit(
            f"missing {', '.join(missing)}: set them in the environment or in a file passed with --env-file"
        )


def require_json_credentials(raw: str) -> None:
    """Exit unless WAREHOUSE_CREDENTIALS is a JSON object, naming no part of it.

    The agents-schema CLI falls back to YAML when the value is not JSON, and its YAML error quotes the text
    around the fault, which is part of the secret. Checking here first means the CLI only ever sees an object.
    """
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        sys.exit("WAREHOUSE_CREDENTIALS is not valid JSON (value not shown)")
    if not isinstance(parsed, dict):
        sys.exit("WAREHOUSE_CREDENTIALS must be a JSON object (value not shown)")


def require_int(env: dict[str, str], name: str) -> int:
    """A setting that must be an integer, such as a dbt environment ID. Exits naming the setting, not its value."""
    try:
        return int(env[name])
    except ValueError:
        sys.exit(f"{name} must be an integer")


def sha256_of(path: Path) -> str:
    """Hex sha256 of a file's bytes."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def host_of(dbt_host: str) -> str:
    """A dbt host as a bare name: abc123.us1.dbt.com."""
    return dbt_host.removeprefix("https://").rstrip("/")


def admin_api(env: dict[str, str], path: str) -> dict:
    """GET a dbt platform Admin API v2 path and return the parsed JSON."""
    url = ADMIN_API_URL.format(
        host=host_of(env["DBT_HOST"]), account_id=env["DBT_ACCOUNT_ID"], path=path
    )
    request = urllib.request.Request(
        url, headers={"Authorization": f"Token {env['DBT_TOKEN']}", "Accept": "application/json"}
    )
    try:
        with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        sys.exit(f"dbt Admin API {path}: HTTP {exc.code}")
    except ValueError:
        # A proxy or login page answers 200 with HTML.
        sys.exit(f"dbt Admin API {path}: the response is not JSON")
    except urllib.error.URLError as exc:
        sys.exit(f"dbt Admin API {path}: {exc.reason}")


def fetch(args: argparse.Namespace) -> None:
    """Download manifest.json and semantic_manifest.json from a successful dbt platform run into <work-dir>."""
    env = settings(args.env_file)
    require(env, "DBT_HOST", "DBT_ACCOUNT_ID", "DBT_TOKEN", "DBT_PROD_ENV_ID")
    prod_env_id = require_int(env, "DBT_PROD_ENV_ID")
    response = admin_api(env, f"/runs/{args.run_id}/")
    run = response.get("data") if isinstance(response, dict) else None
    if not isinstance(run, dict):
        run = {}
    if run.get("status_humanized") != "Success":
        sys.exit(f"run {args.run_id} is not a successful run: {run.get('status_humanized')}")
    # A CI or development run builds into other schemas, so the published lineage would point the agent at nothing.
    if run.get("environment_id") != prod_env_id:
        sys.exit(f"run {args.run_id} is not in the Production environment (DBT_PROD_ENV_ID)")
    manifest = admin_api(env, f"/runs/{args.run_id}/artifacts/manifest.json")
    semantic = admin_api(env, f"/runs/{args.run_id}/artifacts/semantic_manifest.json")
    # publish replaces every DBT_* and OSI_* table, so an artifact that describes nothing would empty them.
    nodes = manifest.get("nodes") if isinstance(manifest, dict) else None
    if not isinstance(nodes, dict):
        nodes = {}
    models = [
        n for n in nodes.values() if isinstance(n, dict) and n.get("resource_type") == "model"
    ]
    if not models:
        sys.exit(f"run {args.run_id}: manifest.json has no models; nothing was written")
    if not isinstance(semantic, dict):
        semantic = {}
    metrics = semantic.get("metrics")
    if not isinstance(metrics, list) or not metrics:
        sys.exit(f"run {args.run_id}: semantic_manifest.json has no metrics; nothing was written")
    # A malformed entry is named, not reported as "no metrics", and stops the run before a file is written.
    for position, metric in enumerate(metrics):
        if not isinstance(metric, dict):
            sys.exit(
                f"run {args.run_id}: semantic_manifest.json metric {position} is not an object; "
                "nothing was written"
            )
        name = metric.get("name")
        if not isinstance(name, str) or not name:
            sys.exit(
                f"run {args.run_id}: semantic_manifest.json metric {position} has no name; nothing was written"
            )
    # The osi step reads every entry of these lists by key, so an entry that is not an object stops the run here.
    for key in ("semantic_models", "saved_queries"):
        entries = semantic.get(key, [])
        if not isinstance(entries, list):
            sys.exit(
                f"run {args.run_id}: semantic_manifest.json `{key}` is not a list; nothing was written"
            )
        for position, entry in enumerate(entries):
            if not isinstance(entry, dict):
                sys.exit(
                    f"run {args.run_id}: semantic_manifest.json `{key}` entry {position} is not an object; "
                    "nothing was written"
                )

    work = Path(args.work_dir)
    # OSI files converted from an earlier run's semantic manifest must not be published with this run's manifest.
    stale_osi = work / "osi"
    if stale_osi.exists():
        shutil.rmtree(stale_osi)
        print(f"removed {stale_osi}: run osi again for this run")
    target = work / "dbt" / "target"
    target.mkdir(parents=True, exist_ok=True)
    (target / "manifest.json").write_text(json.dumps(manifest))
    (target / "semantic_manifest.json").write_text(json.dumps(semantic))
    print(
        f"run {args.run_id} (job {run.get('job_definition_id')}, finished {run.get('finished_at')})"
    )
    print(f"manifest: {len(models)} models -> {target / 'manifest.json'}")
    print(
        f"semantic manifest: {len(semantic.get('semantic_models', []))} semantic models, "
        f"{len(semantic.get('metrics', []))} metrics, {len(semantic.get('saved_queries', []))} saved queries "
        f"-> {target / 'semantic_manifest.json'}"
    )


def convert_to_osi(semantic: dict, model_name: str) -> dict:
    """Run MetricFlow's MSI-to-OSI converter on a semantic manifest. Returns the OSI 0.1.1 document as a dict."""
    try:
        from metricflow.converters.msi_to_osi import MSIToOSIConverter
        from metricflow_semantic_interfaces.implementations.semantic_manifest import (
            PydanticSemanticManifest,
        )
    except ImportError:
        sys.exit(
            "the osi step needs metricflow 0.213.0 or later: pip install 'metricflow==0.213.0'"
        )
    # MetricFlow's models keep pydantic's v1-style API (parse_obj, .json) under pydantic 2.
    result = MSIToOSIConverter().convert(
        PydanticSemanticManifest.parse_obj(semantic), osi_model_name=model_name
    )
    for (
        issue
    ) in result.issues:  # elements the converter dropped or degraded, such as conversion metrics
        print(f"  converter: {issue.issue_type.name} {issue.element_name}")
    return json.loads(result.output.json(exclude_none=True))


def shim_osi(doc: dict) -> dict:
    """Bring the converter's OSI 0.1.1 output to the 0.2.0.dev0 shape the agents-schema CLI validates against.

    0.2.0.dev0 has no document-level `dialects` (each expression names its own) and calls a relationship's
    many side `from`, not `from_dataset`. Anything else that differs fails validation in `validate_osi`.
    """
    doc = json.loads(json.dumps(doc))
    doc.pop("dialects", None)
    doc["version"] = OSI_VERSION
    for model in doc.get("semantic_model", []):
        for rel in model.get("relationships", []):
            if "from_dataset" in rel:
                rel["from"] = rel.pop("from_dataset")
    return doc


def _ai_context(entry: dict) -> dict:
    """An overlay entry as an OSI ai_context object: instructions and synonyms, nothing empty."""
    ctx = {}
    if entry.get("instructions"):
        ctx["instructions"] = " ".join(str(entry["instructions"]).split())
    if entry.get("synonyms"):
        ctx["synonyms"] = [str(s) for s in entry["synonyms"]]
    return ctx


def _extension(data: dict) -> dict:
    """An OSI custom extension from dbt. OSI carries the data as a JSON string."""
    return {"vendor_name": "DBT", "data": json.dumps(data, sort_keys=True)}


def enrich_osi(doc: dict, semantic: dict, context: dict, compiled_sql: dict[str, str]) -> dict:
    """Add the pack's ai_context and the dbt definition of each metric to a single-model OSI document.

    context       the overlay: `model`, `datasets.<name>` and `metrics.<name>` entries with instructions/synonyms.
                  A name the semantic manifest does not have is an error, so the overlay cannot drift silently.
    compiled_sql  metric name -> the SQL the dbt Semantic Layer compiles for it; may be empty.
    """
    doc = json.loads(json.dumps(doc))
    (model,) = doc["semantic_model"]
    datasets = {d["name"]: d for d in model["datasets"]}
    metrics = {m["name"]: m for m in model.get("metrics", [])}
    for kind, known in (("datasets", datasets), ("metrics", metrics)):
        unknown = sorted(set(context.get(kind) or {}) - set(known))
        if unknown:
            raise ValueError(f"overlay {kind} not in the semantic manifest: {', '.join(unknown)}")

    if context.get("model"):
        model["ai_context"] = _ai_context(context["model"])
    model["custom_extensions"] = [
        _extension(
            {
                "saved_queries": [
                    {
                        "name": q["name"],
                        "description": " ".join((q.get("description") or "").split()),
                        **{k: q["query_params"].get(k) for k in ("metrics", "group_by", "where")},
                    }
                    for q in semantic.get("saved_queries", [])
                ],
            }
        )
    ]
    for name, entry in (context.get("datasets") or {}).items():
        datasets[name]["ai_context"] = _ai_context(entry)

    saved_by_metric: dict[str, list[str]] = {}
    for q in semantic.get("saved_queries", []):
        for m in q["query_params"].get("metrics") or []:
            saved_by_metric.setdefault(m, []).append(q["name"])
    for sm_metric in semantic.get("metrics", []):
        osi_metric = metrics.get(sm_metric["name"])
        if osi_metric is None:  # dropped by the converter, which already said why
            continue
        params = sm_metric.get("type_params") or {}
        agg = params.get("metric_aggregation_params") or {}
        data = {
            "label": sm_metric.get("label"),
            "type": sm_metric.get("type"),
            "semantic_model": agg.get("semantic_model"),
            "agg": agg.get("agg"),
            "expr": agg.get("expr") or params.get("expr"),
            "agg_time_dimension": agg.get("agg_time_dimension"),
            "filter": sm_metric.get("filter"),
            "saved_queries": saved_by_metric.get(sm_metric["name"], []),
        }
        if sm_metric["name"] in compiled_sql:
            data["compiled_sql"] = compiled_sql[sm_metric["name"]]
        osi_metric["custom_extensions"] = [_extension(data)]
        if sm_metric["name"] in (context.get("metrics") or {}):
            osi_metric["ai_context"] = _ai_context(context["metrics"][sm_metric["name"]])
    return doc


def validate_osi(doc: dict) -> None:
    """Validate against the OSI schema the agents-schema CLI vendors, so nothing it would reject gets written."""
    from importlib import resources

    import jsonschema

    schema = json.loads((resources.files("agents_schema") / "osi-schema.json").read_text())
    errors = sorted(
        jsonschema.Draft202012Validator(schema).iter_errors(doc),
        key=lambda e: list(e.absolute_path),
    )
    if errors:
        lines = [
            f"  /{'/'.join(str(p) for p in e.absolute_path)}: {e.message}" for e in errors[:10]
        ]
        raise ValueError("OSI document fails the CLI's osi-schema.json:\n" + "\n".join(lines))


def semantic_layer_url(dbt_host: str) -> str:
    """GraphQL URL of the Semantic Layer. A multicell host <prefix>.<region>.dbt.com serves it at
    <prefix>.semantic-layer.<region>.dbt.com; a single-cell host <region>.dbt.com at semantic-layer.<host>.
    """
    host = host_of(dbt_host)
    first, _, rest = host.partition(".")
    if rest.count(".") >= 2:  # abc123.us1.dbt.com
        return SEMANTIC_LAYER_MULTICELL_URL.format(prefix=first, rest=rest)
    return SEMANTIC_LAYER_SINGLE_CELL_URL.format(host=host)


def compile_metric_sql(env: dict[str, str], metric_names: list[str]) -> dict[str, str]:
    """The SQL the dbt Semantic Layer compiles for each metric on its own (compileSql runs nothing)."""
    url = semantic_layer_url(env["DBT_HOST"])
    env_id = require_int(env, "DBT_PROD_ENV_ID")
    compiled = {}
    for name in metric_names:
        body = json.dumps(
            {
                "query": COMPILE_SQL,
                "variables": {"env": env_id, "metrics": [{"name": name}]},
            }
        ).encode()
        request = urllib.request.Request(
            url,
            body,
            {"Authorization": f"Bearer {env['DBT_TOKEN']}", "Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
                payload = json.load(response)
        except urllib.error.HTTPError as exc:
            sys.exit(f"compileSql {name}: HTTP {exc.code}")
        except urllib.error.URLError as exc:
            sys.exit(f"compileSql {name}: {exc.reason} ({url})")
        except ValueError:
            sys.exit(f"compileSql {name}: the response is not JSON ({url})")
        if not isinstance(payload, dict):
            sys.exit(f"compileSql {name}: the response is not a JSON object")
        errors = payload.get("errors")
        if errors:
            first = errors[0] if isinstance(errors, list) else errors
            message = first.get("message") if isinstance(first, dict) else first
            sys.exit(f"compileSql {name}: {message}")
        data = payload.get("data")
        result = data.get("compileSql") if isinstance(data, dict) else None
        sql = result.get("sql") if isinstance(result, dict) else None
        if not isinstance(sql, str) or not sql.strip():
            sys.exit(f"compileSql {name}: the response has no compiled SQL")
        compiled[name] = result["sql"]
    return compiled


def osi(args: argparse.Namespace) -> None:
    """Write <work-dir>/osi/<model-name>.osi.yaml from the fetched semantic manifest. Writes nothing remote."""
    work = Path(args.work_dir)
    source = work / "dbt" / "target" / "semantic_manifest.json"
    if not source.exists():
        sys.exit(f"no {source}; run fetch first")
    semantic = json.loads(source.read_text())
    context = yaml.safe_load(Path(args.context).read_text()) if args.context else {}

    doc = shim_osi(convert_to_osi(semantic, args.model_name))
    names = [m["name"] for m in doc["semantic_model"][0].get("metrics", [])]
    env = settings(args.env_file)
    if args.no_compiled_sql:
        compiled = {}
    else:
        require(env, "DBT_HOST", "DBT_PROD_ENV_ID", "DBT_TOKEN")
        compiled = compile_metric_sql(env, names)
    doc = enrich_osi(doc, semantic, context, compiled)
    validate_osi(doc)

    out_dir = work / "osi"
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True)
    out = out_dir / f"{args.model_name}.osi.yaml"
    out.write_text(yaml.safe_dump(doc, sort_keys=False, width=120, allow_unicode=True))
    (out_dir / OSI_SOURCE_STAMP).write_text(sha256_of(source) + "\n")
    (model,) = doc["semantic_model"]
    print(
        f"osi: {len(model['datasets'])} datasets, "
        f"{sum(len(d.get('fields', [])) for d in model['datasets'])} fields, {len(names)} metrics "
        f"({len(compiled)} with compiled SQL), {len(model.get('relationships', []))} relationships -> {out}"
    )


def stage_skills(skills: list[str], uses_schemas: list[str], skills_dir: Path) -> None:
    """Copy each KEY=PATH guidance file to <skills_dir>/<KEY>.md with `uses:` frontmatter prepended.

    The frontmatter ties the skill to the pack schema in AGENTS.ROOT without editing the source file.
    """
    if skills_dir.exists():
        shutil.rmtree(skills_dir)
    skills_dir.mkdir(parents=True)
    for spec in skills:
        key, sep, src = spec.partition("=")
        if not sep or not key or not src:
            sys.exit(f"--skill must be KEY=PATH, got {spec!r}")
        body = Path(src).read_text()
        if body.startswith("---"):
            sys.exit(f"{src} already has frontmatter; merge uses: by hand")
        # Each schema is written as a JSON string, which YAML reads as a string: a schema named on, null or 2026
        # would otherwise load as a boolean, a null or an integer.
        schemas = "".join(f"    - {json.dumps(s)}\n" for s in uses_schemas)
        front = f"---\nuses:\n  schemas:\n{schemas}---\n\n"
        (skills_dir / f"{key}.md").write_text(front + body if uses_schemas else body)
        print(f"skill/{key} <- {src}")


def require_osi_matches_manifest(work: Path) -> None:
    """Exit unless <work>/osi was converted from the semantic_manifest.json now in <work>/dbt/target.

    `publish` writes AGENTS.DBT_* from the manifest and AGENTS.OSI_* from the OSI files. Without this check a
    fetch of run B followed by a publish would pair run B's models with run A's metrics.
    """
    stamp = work / "osi" / OSI_SOURCE_STAMP
    semantic = work / "dbt" / "target" / "semantic_manifest.json"
    if not stamp.exists() or not semantic.exists():
        sys.exit(f"{work / 'osi'} has no {OSI_SOURCE_STAMP} for the fetched run; run osi again")
    if stamp.read_text().strip() != sha256_of(semantic):
        sys.exit(
            f"{work / 'osi'} was converted from another run's semantic manifest; run osi again"
        )


def publish(args: argparse.Namespace) -> None:
    """Run `agents-schema dbt`, `osi` and `skills` against <database>.AGENTS."""
    work = Path(args.work_dir)
    if not (work / "dbt" / "target" / "manifest.json").exists():
        sys.exit(f"no manifest under {work}/dbt/target; run fetch first")
    env = settings(args.env_file)
    require(env, "WAREHOUSE_CREDENTIALS")
    require_json_credentials(env["WAREHOUSE_CREDENTIALS"])
    cli = shutil.which(args.cli)
    if not cli:
        sys.exit(
            f"{args.cli} not found: pip install 'agents-schema==0.0.11', or pass --cli <path>"
        )

    # fetch removes osi/, so publishing DBT_* alone would leave an earlier run's OSI_* tables beside them.
    if not list((work / "osi").glob("*.osi.yaml")):
        sys.exit(f"no OSI files under {work / 'osi'} for the fetched run; run osi first")
    require_osi_matches_manifest(work)
    steps = [
        [cli, "dbt", "--project-dir", str(work / "dbt")],
        [cli, "osi", "--osi-dir", str(work / "osi")],
    ]
    if args.skill:
        stage_skills(args.skill, args.uses_schema, work / "skills")
        steps.append(
            [cli, "skills", "--skills-dir", str(work / "skills"), "--provider", args.provider]
        )
    for cmd in steps:
        print("$ agents-schema", " ".join(cmd[1:]))
        result = subprocess.run(cmd, env=env, check=False)
        if result.returncode != 0:
            sys.exit(f"agents-schema {cmd[1]} failed with exit code {result.returncode}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="command", required=True)
    f = sub.add_parser(
        "fetch", help="download manifest.json and semantic_manifest.json from a dbt platform run"
    )
    f.add_argument("--run-id", required=True, type=int)
    f.add_argument("--work-dir", required=True)
    o = sub.add_parser(
        "osi", help="convert the fetched semantic manifest to <work-dir>/osi/*.osi.yaml"
    )
    o.add_argument("--work-dir", required=True)
    o.add_argument(
        "--model-name", required=True, help="OSI semantic model name, usually the dbt project name"
    )
    o.add_argument(
        "--context", help="YAML overlay of ai_context for the model, its datasets and metrics"
    )
    o.add_argument(
        "--no-compiled-sql", action="store_true", help="skip compileSql; metrics carry no SQL"
    )
    p = sub.add_parser(
        "publish", help="publish the manifest, OSI files and skills into <database>.AGENTS"
    )
    p.add_argument("--work-dir", required=True)
    p.add_argument("--skill", action="append", default=[], help="KEY=PATH, published as skill/KEY")
    p.add_argument(
        "--uses-schema", action="append", default=[], help="schema the skills read (uses.schemas)"
    )
    p.add_argument("--provider", default="user", help="AGENTS.ROOT provider for the skill rows")
    p.add_argument("--cli", default="agents-schema", help="the agents-schema executable")
    for command in (f, o, p):
        command.add_argument("--env-file", help="KEY=VALUE file read on top of the environment")
    args = parser.parse_args()
    {"fetch": fetch, "osi": osi, "publish": publish}[args.command](args)


if __name__ == "__main__":
    main()
