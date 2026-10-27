"""Render the context pack's Snowflake DDL templates in sql/ as SQL you can run as is.

The dbt project in dbt/ builds the models. These templates create what dbt cannot: the dbt MCP connection, the Cortex
Search service and the Cortex Agent, plus the teardown. They name no account, database, schema or dbt project, and
CREATE AGENT ... FROM SPECIFICATION embeds names and the dbt project_id inside $$ ... $$, where Snowflake substitutes
no variables. So this script fills them in:

    {{database}}         <pack database>, the database that holds the pack schema and AGENTS
    {{pack}}             <pack database>.<pack schema>, where dbt built the models and the pack's objects go
    {{agents}}           <pack database>.AGENTS, where the Agents Schema publish writes the dbt manifest and skills
    {{warehouse}}        the warehouse the search service refreshes on
    {{target_lag}}       how far the search index may lag the campaign model (default 1 day)
    {{api_integration}}  the account-level API integration for the dbt MCP server
    {{mcp_server}}       <pack>.<MCP server name>, the external MCP server object
    {{dbt_host}}         the dbt platform host with its static subdomain, for example abc123.us1.dbt.com
    {{project_id}}       the dbt platform project the agent must query, and no other
    {{reader_role}}      a role that asks the agent without owning its objects (05_reader_grants.sql and
                         98_reader_revoke.sql only)

Usage, from this directory:
    python render_sql.py --pack-database <pack_database> --pack-schema <pack_schema> --warehouse <warehouse> \
        --dbt-host <dbt_host> --project-id <project_id> --output rendered/pack.sql \
        sql/00_dbt_mcp.sql sql/02_agents_context.sql sql/03_search_service.sql sql/04_agent.sql

    python render_sql.py --pack-database <pack_database> --pack-schema <pack_schema> --warehouse <warehouse> \
        --reader-role <reader_role> --output rendered/reader_grants.sql sql/05_reader_grants.sql

Only the values the given templates use are required. The rendered SQL goes to standard output, or to --output.
"""

from __future__ import annotations

# For command-line arguments
import argparse

# For matching placeholders and validating values
import re

# For the exit code and stderr
import sys

# For type hints
from collections.abc import Mapping, Sequence

# For reading the templates and writing the output
from pathlib import Path

# Plain identifiers only. Anything else is refused, so a value passed on the command line can never inject SQL or
# close the $$ ... $$ around an agent specification. Snowflake allows $ in an unquoted identifier, but two of them
# end a dollar-quoted string, so $ is refused too.
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# A bare host name: lowercase labels joined by dots, no scheme, port, path or user info.
HOST = re.compile(
    r"^(?=.{4,253}$)[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+$"
)

# A dbt platform project id is a positive integer.
PROJECT_ID = re.compile(r"^[1-9][0-9]{0,18}$")

# A Cortex Search TARGET_LAG: a count and a unit, for example '1 day' or '30 minutes'.
TARGET_LAG = re.compile(r"^[1-9][0-9]{0,4} (?:second|minute|hour|day)s?$")

# {{name}}, with optional spaces inside the braces.
PLACEHOLDER = re.compile(r"\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}")

# The command-line flag that supplies each placeholder, so a missing value names its fix.
FLAGS = {
    "database": "--pack-database and --pack-schema",
    "pack": "--pack-database and --pack-schema",
    "agents": "--pack-database and --pack-schema",
    "warehouse": "--warehouse",
    "target_lag": "--target-lag",
    "api_integration": "--api-integration",
    # The server lives in the pack schema, so its name needs the pack too; --mcp-server has a default.
    "mcp_server": "--pack-database and --pack-schema (and --mcp-server, if it is not the default)",
    "dbt_host": "--dbt-host",
    "project_id": "--project-id",
    "reader_role": "--reader-role",
}

DEFAULT_TARGET_LAG = "1 day"
DEFAULT_API_INTEGRATION = "NHTSA_PACK_DBT_MCP"
DEFAULT_MCP_SERVER = "DBT_MCP_SERVER"
AGENTS_SCHEMA = "AGENTS"


class RenderError(ValueError):
    """A template or an argument that cannot be rendered into safe SQL."""


def check(value: str, pattern: re.Pattern[str], what: str, expected: str) -> str:
    """Return value when pattern matches it in full, otherwise raise RenderError naming what it was for."""
    if not pattern.match(value):
        raise RenderError(f"{what} must be {expected}, got {value!r}")
    return value


def check_identifier(value: str, what: str) -> str:
    """Return value when it is a plain SQL identifier."""
    return check(value, IDENTIFIER, what, "a plain identifier (letters, digits or _)")


def build_values(
    pack_database: str | None = None,
    pack_schema: str | None = None,
    warehouse: str | None = None,
    target_lag: str | None = DEFAULT_TARGET_LAG,
    api_integration: str | None = DEFAULT_API_INTEGRATION,
    mcp_server: str | None = DEFAULT_MCP_SERVER,
    dbt_host: str | None = None,
    project_id: str | None = None,
    reader_role: str | None = None,
) -> dict[str, str]:
    """Return the value of every placeholder that can be filled from the arguments, after validating each one.

    A placeholder whose argument is None is left out, and render() refuses a template that uses it.
    """
    if (pack_database is None) != (pack_schema is None):
        raise RenderError("--pack-database and --pack-schema go together")
    values: dict[str, str] = {}
    if pack_database is not None and pack_schema is not None:
        pack = f"{check_identifier(pack_database, '--pack-database')}.{check_identifier(pack_schema, '--pack-schema')}"
        values["database"] = pack_database
        values["pack"] = pack
        # Agents Schema always publishes into a schema named AGENTS, in the database it connects to.
        values["agents"] = f"{pack_database}.{AGENTS_SCHEMA}"
        if mcp_server is not None:
            values["mcp_server"] = f"{pack}.{check_identifier(mcp_server, '--mcp-server')}"
    if warehouse is not None:
        values["warehouse"] = check_identifier(warehouse, "--warehouse")
    if target_lag is not None:
        values["target_lag"] = check(
            target_lag, TARGET_LAG, "--target-lag", "a count and a unit, like '1 day'"
        )
    if api_integration is not None:
        values["api_integration"] = check_identifier(api_integration, "--api-integration")
    if dbt_host is not None:
        values["dbt_host"] = check(
            dbt_host, HOST, "--dbt-host", "a bare lowercase host name, like abc123.us1.dbt.com"
        )
    if project_id is not None:
        values["project_id"] = check(project_id, PROJECT_ID, "--project-id", "a positive integer")
    if reader_role is not None:
        values["reader_role"] = check_identifier(reader_role, "--reader-role")
    return values


def render(template: str, values: Mapping[str, str]) -> str:
    """Fill every placeholder in template. Raise RenderError for a placeholder with no value.

    SQL comment lines are copied unchanged, so a template's header can name its placeholders. Lines inside an agent
    specification are YAML, not SQL comments, so they are always filled.
    """

    def replace(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in FLAGS:
            raise RenderError(f"unknown placeholder {{{{{name}}}}}")
        if name not in values:
            raise RenderError(f"no value for placeholder {{{{{name}}}}}; pass {FLAGS[name]}")
        return values[name]

    lines = template.splitlines(keepends=True)
    return "".join(
        line if line.lstrip().startswith("--") else PLACEHOLDER.sub(replace, line)
        for line in lines
    )


def render_files(paths: Sequence[Path], values: Mapping[str, str]) -> str:
    """Render each template in order and join them, each under a comment naming its file."""
    parts = []
    for path in paths:
        try:
            template = path.read_text(encoding="utf-8")
        except OSError as error:
            raise RenderError(f"cannot read template {path}: {error}") from error
        parts.append(f"-- Rendered from {path.name}\n{render(template, values)}")
    return "\n".join(parts)


def main(argv: Sequence[str] | None = None) -> int:
    """Parse the command line, render the templates and write the SQL. Return the process exit code."""
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--pack-database", help="database dbt built the models in; the pack's objects go there too"
    )
    parser.add_argument("--pack-schema", help="schema dbt built the models in")
    parser.add_argument("--warehouse", help="warehouse the search service refreshes on")
    parser.add_argument(
        "--target-lag", default=DEFAULT_TARGET_LAG, help="search index lag (default: %(default)s)"
    )
    parser.add_argument(
        "--api-integration", default=DEFAULT_API_INTEGRATION, help="default: %(default)s"
    )
    parser.add_argument("--mcp-server", default=DEFAULT_MCP_SERVER, help="default: %(default)s")
    parser.add_argument("--dbt-host", help="dbt platform host with its static subdomain")
    parser.add_argument("--project-id", help="dbt platform project the agent queries")
    parser.add_argument(
        "--reader-role",
        help="role granted read access to the AGENTS tables (05_reader_grants.sql)",
    )
    parser.add_argument("--output", type=Path, help="file to write instead of standard output")
    parser.add_argument(
        "templates", nargs="+", type=Path, help="template files, rendered in the order given"
    )
    args = parser.parse_args(argv)

    try:
        values = build_values(
            pack_database=args.pack_database,
            pack_schema=args.pack_schema,
            warehouse=args.warehouse,
            target_lag=args.target_lag,
            api_integration=args.api_integration,
            mcp_server=args.mcp_server,
            dbt_host=args.dbt_host,
            project_id=args.project_id,
            reader_role=args.reader_role,
        )
        sql = render_files(args.templates, values)
        if args.output is None:
            sys.stdout.write(sql)
        else:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(sql, encoding="utf-8")
    except RenderError as error:
        print(f"render_sql.py: {error}", file=sys.stderr)
        return 2
    except OSError as error:
        print(f"render_sql.py: cannot write {args.output}: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
