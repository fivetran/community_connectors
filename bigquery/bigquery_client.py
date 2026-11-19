"""
This file contains the BigQuery client code for the connector: authentication, reading the
metadata of the tables listed in TABLE_SPECS (partitions, primary keys, sizes), sync planning, and
the partition-aware initial and incremental sync logic. Data is read with the BigQuery Storage Read
API so that column projection and row filters are applied on the server, and partition pruning
limits the data that is scanned.
"""

# For reading the service account key
import json

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

import base64  # For encoding BYTES values as text inside STRUCT and ARRAY values
import hashlib  # For the fingerprint of a table's settings
import math  # For rounding up lookbacks and checking that floats are finite
import random  # For the jitter of retry delays
import re  # For parsing table filters and table names
import threading  # For the locks that protect state shared by the worker threads
import time  # For retry delays and the checkpoint interval
from bisect import bisect_right  # For finding the first partition after the watermark
from concurrent.futures import ThreadPoolExecutor  # For reading metadata and streams in parallel
from dataclasses import dataclass  # For the plan and batch records
from datetime import date, datetime, time as dt_time, timedelta, timezone  # For date handling
from decimal import Decimal  # For NUMERIC values and filter literals

import requests  # For the exception types of failed network calls, which are retried
from google.api_core import exceptions as gexc  # For the exception types of BigQuery API errors
from google.auth import exceptions as auth_exceptions  # For authentication errors
from google.cloud import bigquery, bigquery_storage_v1  # BigQuery and Storage Read API clients
from google.cloud.bigquery_storage_v1 import types as bq_types  # For Storage Read API requests
from google.oauth2 import service_account  # For credentials from the service account key

# The tables to sync and the constants that control how they are read
from table_specs import (
    TABLE_SPECS,
    PREFERRED_CURSOR_COLUMN_NAMES,
    CURSOR_COLUMN_TYPES,
    READ_STREAMS_PER_TABLE,
    BATCH_TARGET_ROWS,
    FLUSH_MAX_VALUES,
    FLUSH_MAX_BYTES,
    FLUSH_MIN_VALUES,
    FLUSH_INTERVAL_SECONDS,
    ROW_WEIGHT_CELLS,
    LOOKBACK_DAYS,
    LOOKBACK_PARTITIONS,
    MODIFIED_MARGIN_SECONDS,
    SCHEMA_CHANGE_DEFAULT,
    STREAMING_GRACE_SECONDS,
    STREAMING_MIN_LOOKBACK,
    CURSOR_MARGIN_SECONDS,
    FULL_REFRESH_WARNING_BYTES,
    MAX_DISCOVERY_WORKERS,
    MAX_RETRY_ATTEMPTS,
    RETRY_BASE_DELAY_SECONDS,
    RETRY_MAX_DELAY_SECONDS,
    API_TIMEOUT_SECONDS,
    SETUP_TEST_TIMEOUT_SECONDS,
)

# For describing tables and durations in logs
from progress import (
    describe_global_settings,
    describe_plan,
    format_bytes,
    format_duration,
    plan_kind,
)

# OAuth scope required for metadata queries and for the Storage Read API
__BIGQUERY_SCOPE = "https://www.googleapis.com/auth/bigquery"

# Only plain tables can be read with the Storage Read API (views and external tables cannot)
__READABLE_TABLE_TYPE = "TABLE"

# Ingestion-time partitioned tables have no partition column, so rows are filtered on the
# _PARTITIONTIME pseudo-column (_PARTITIONDATE is its DATE form)
__INGESTION_TIME_COLUMN = "_PARTITIONTIME"
__INGESTION_PSEUDO_TYPES = {"_PARTITIONTIME": "TIMESTAMP", "_PARTITIONDATE": "DATE"}

# Special partition ids returned by INFORMATION_SCHEMA.PARTITIONS
__PARTITION_NULL = "__NULL__"
__PARTITION_UNPARTITIONED = "__UNPARTITIONED__"

# Replication strategies
__STRATEGY_FULL = "FULL"
__STRATEGY_INCREMENTAL = "INCREMENTAL"

# A table filter is COLUMN OPERATOR VALUE conditions joined by AND. See the README, Filters section.
__FILTER_OPERATORS = (">=", "<=", "<>", "!=", "=", ">", "<")
__FILTER_AND = re.compile(r"\s+and\s+", re.IGNORECASE)
__FILTER_CONDITION = re.compile(
    r"^\s*`?(?P<column>[A-Za-z_][A-Za-z0-9_]*)`?\s*(?P<operator>>=|<=|<>|!=|=|>|<)\s*"
    r"""(?:'(?P<single>(?:[^']|'')*)'|"(?P<double>(?:[^"]|"")*)"|(?P<bare>[^\s'"()=<>!][^\s'"()]*))\s*$"""
)

# Fivetran types that are declared in schema(), because the value alone does not show them. Other
# columns are inferred, which keeps the schema free to evolve. See the README, Data types section.
__BIGQUERY_TO_FIVETRAN_TYPE = {
    "FLOAT": "DOUBLE",
    "BYTES": "BINARY",
    "DATE": "NAIVE_DATE",
    "DATETIME": "NAIVE_DATETIME",
    "TIMESTAMP": "UTC_DATETIME",
    "JSON": "JSON",
    "RECORD": "JSON",
    "INTERVAL": "JSON",
    "RANGE": "JSON",
}

# BigQuery types that are not declared, because their Python values show the type
__INFERRED_TYPES = {"STRING", "INTEGER", "BOOLEAN", "TIME", "GEOGRAPHY"}

# NUMERIC and BIGNUMERIC are declared as DECIMAL, with the precision and scale of the column when
# it has them, otherwise 38 and 9. See the README, Data types section.
__MAX_DECIMAL_PRECISION = 38
__DEFAULT_DECIMAL_SCALE = 9

# Fivetran type names that can be used in the "column_types" override in TABLE_SPECS
__VALID_FIVETRAN_TYPES = {
    "BOOLEAN",
    "SHORT",
    "INT",
    "LONG",
    "FLOAT",
    "DOUBLE",
    "NAIVE_DATE",
    "NAIVE_DATETIME",
    "UTC_DATETIME",
    "BINARY",
    "XML",
    "STRING",
    "JSON",
}

# Standard SQL names mapped to the canonical names that the BigQuery client library reports
__TYPE_ALIASES = {
    "INT64": "INTEGER",
    "FLOAT64": "FLOAT",
    "BOOL": "BOOLEAN",
    "STRUCT": "RECORD",
    "DECIMAL": "NUMERIC",
    "BIGDECIMAL": "BIGNUMERIC",
}

# Column types that cannot be part of a primary key
__UNSUPPORTED_KEY_TYPES = {"RECORD", "JSON", "GEOGRAPHY", "INTERVAL", "RANGE"}

# Allowed characters in the dataset and table parts of a TABLE_SPECS key
__KEY_PART = re.compile(r"^[\w\-]+$")

# Allowed characters in a project id, checked before it is put in a back-quoted query
__IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9_\-:.]+$")

# Global cache so that schema() and update() share one pass over the table metadata
__CONTEXT = None
__TABLE_PLAN_LIST = None

# Serializes updates to the shared state dictionary and checkpoint calls from worker threads
__STATE_LOCK = threading.Lock()

# Protects the cache of dataset locations
__LOCATION_LOCK = threading.Lock()

# Values and Arrow bytes upserted since the last checkpoint, across all tables. Guarded by
# __STATE_LOCK.
__BUFFER = {"values": 0, "bytes": 0, "since": time.monotonic()}

# Reasons of a 403 that is a quota or rate limit, which waiting can fix
__QUOTA_REASONS = frozenset(
    {"rateLimitExceeded", "quotaExceeded", "userRateLimitExceeded", "concurrentLimitExceeded"}
)

# Marks a value that _convert_scalar did not convert, because it is not a scalar type
__NOT_SCALAR = object()

# The time the sync started, so relative filter values use the same window for every table
__REFERENCE_TIME = {"value": None}

# Python types that need no conversion before they are sent
__PLAIN_TYPES = frozenset({str, int, bool})

# Relative filter values: today, today-90d, now, now-12h, now-7d
__RELATIVE_VALUE = re.compile(r"^(today|now)(?:-(\d+)([dh]))?$")

# Shortest length of a partition in hours, used to turn a lookback in days into partitions
__HOURS_PER_PARTITION = {"HOUR": 1, "DAY": 24, "MONTH": 24 * 28, "YEAR": 24 * 365}

# Data that was skipped or read in a degraded way, by reason, reported with op.warning at the end of
# update(). Only 10 warnings can be shown per sync, so they are aggregated.
__SKIPPED = {}


@dataclass
class ClientContext:
    """
    Holds the clients and the project that is billed, shared by all tables.
    Attributes:
        bq_client: BigQuery client used for metadata and INFORMATION_SCHEMA queries
        read_client: BigQuery Storage Read API client used to read table data
        billing_project: project that is billed for jobs and read sessions (the key's project)
        locations: cache of dataset locations, used for INFORMATION_SCHEMA query jobs
    """

    bq_client: object
    read_client: object
    billing_project: str
    locations: dict


@dataclass
class TableRef:
    """
    A table named by a key of TABLE_SPECS.
    Attributes:
        project: project that contains the table
        dataset: dataset the table belongs to
        table: table name
        spec: the TABLE_SPECS entry of the table
        key: name used in state and logs: dataset.table, or project.dataset.table when the table
            is not in the billing project
    """

    project: str
    dataset: str
    table: str
    spec: dict
    key: str


@dataclass
class TablePlan:
    """
    Plan for syncing a single table, built from its BigQuery metadata and its TABLE_SPECS entry.
    Attributes:
        stream: table name, also used as the destination table name
        dataset: dataset the table belongs to, also used as the destination schema
        columns: names of the columns to read
        column_types: BigQuery type of every selected column
        explicit_columns: columns whose Fivetran type is declared in the schema (a type name,
            or a dict for DECIMAL columns with precision and scale). Other columns are inferred.
        json_columns: columns of BigQuery type JSON, parsed before upsert
        primary_keys: primary key columns, possibly empty
        strategy: FULL or INCREMENTAL
        partition_column: column the table is partitioned on, or empty when not column-partitioned
        partition_kind: TIME or RANGE for column-partitioned tables, otherwise empty
        granularity: HOUR, DAY, MONTH or YEAR for TIME partitioning
        range_interval: interval for RANGE partitioning
        cursor_column: cursor column for tables that are not column-partitioned, or empty
        lookback: number of recent partitions re-read on every incremental sync
        filter_condition: table filter written like a WHERE clause, or None
        num_rows: row count reported by BigQuery metadata
        num_bytes: size in bytes reported by BigQuery metadata
        full_by_default: True when the table is fully read on every sync only because it has no
            partition and no cursor column, not because the table specification asks for it
        project: project that contains the table
        key: name that identifies the table in state and logs, for example sales.orders
        fingerprint: short hash of the settings that decide which rows are read and how. When it
            differs from the one saved in state, the saved progress no longer applies and the
            table is read again from the start.
        schema_signature: dict of the selected columns and their BigQuery types, saved in state to
            notice columns that are added or change type
        on_schema_change: resync or keep, what to do when a column is added or changes type
        truncate_on_resync: True to mark the rows that are not sent again as deleted whenever the
            table is read again from the start
        streaming_buffer: True when BigQuery reports rows in the table's streaming buffer, which
            means rows are being streamed into it
        cursor_margin_seconds: how far before the last cursor value a cursor-based read starts
        options: the options of the table's TABLE_SPECS entry, shown in the logs
    """

    stream: str
    columns: list
    column_types: dict
    explicit_columns: dict
    json_columns: set
    primary_keys: list
    strategy: str
    partition_column: str = ""
    partition_kind: str = ""
    granularity: str = ""
    range_interval: int = 0
    cursor_column: str = ""
    lookback: int = LOOKBACK_PARTITIONS
    filter_condition: str = None
    num_rows: int = 0
    num_bytes: int = 0
    dataset: str = ""
    full_by_default: bool = False
    project: str = ""
    key: str = ""
    fingerprint: str = ""
    schema_signature: dict = None
    on_schema_change: str = SCHEMA_CHANGE_DEFAULT
    truncate_on_resync: bool = False
    streaming_buffer: bool = False
    cursor_margin_seconds: float = CURSOR_MARGIN_SECONDS
    options: dict = None


@dataclass
class Batch:
    """
    A group of contiguous partitions read with one read session.
    Attributes:
        restriction: row restriction parts for this batch, joined with AND
        last_key: highest partition key in the batch (an int), or None for the NULL partition
        label: text used in logs
    """

    restriction: list
    last_key: int
    label: str


def _note_skipped(reason, detail):
    """
    Remember that data was skipped or read in a degraded way, so update() can warn the user.
    Args:
        reason: sentence that explains why the data was skipped
        detail: table name or other short text that identifies what was skipped
    """
    with __STATE_LOCK:
        __SKIPPED.setdefault(reason, []).append(detail)


def report_skipped():
    """
    Show one dashboard warning for each reason that data was skipped or read in a degraded way.
    The details are also kept in the logs. Call this once at the end of update().
    """
    with __STATE_LOCK:
        skipped = {reason: list(details) for reason, details in __SKIPPED.items()}
    for reason, details in skipped.items():
        examples = ", ".join(details[:5]) + (", ..." if len(details) > 5 else "")
        op.warning(message=f"{reason} ({len(details)}): {examples}")


def _is_quota_error(error):
    """
    Check whether a 403 from BigQuery is a quota or rate limit and not a permission problem.
    Args:
        error: google.api_core.exceptions.Forbidden
    Returns:
        True when one of the reasons that the error carries is a quota or rate limit reason.
    """
    details = getattr(error, "errors", None) or []
    reasons = {str(item.get("reason", "")) for item in details if isinstance(item, dict)}
    return bool(reasons & __QUOTA_REASONS)


def _wait_before_retry(description, attempt, error):
    """
    Wait before the next attempt of a failed call, with exponential backoff and jitter, or raise
    the error when the attempts are used up.
    Args:
        description: text used in log messages
        attempt: number of the attempt that just failed, starting at 1
        error: the exception that the attempt raised
    Raises:
        The error, when this was the last attempt.
    """
    if attempt == MAX_RETRY_ATTEMPTS:
        log.error(f"{description} failed after {attempt} attempts")
        raise error
    delay = min(RETRY_MAX_DELAY_SECONDS, RETRY_BASE_DELAY_SECONDS * 2 ** (attempt - 1))
    # Jitter keeps parallel workers from retrying at the same moment
    delay += random.uniform(0, delay / 2)
    log.warning(f"{description}: transient error, retry {attempt} in {delay:.0f}s: {error}")
    time.sleep(delay)


def _retry(call, description):
    """
    Run a BigQuery API call and retry transient failures with exponential backoff and jitter. A
    call is attempted MAX_RETRY_ATTEMPTS times in total. Quota and rate limit errors are retried
    too, including the ones that BigQuery reports as a 403.
    Args:
        call: zero-argument function that performs the API call
        description: text used in log messages
    Returns:
        The result of the call.
    Raises:
        RuntimeError: for permission, not-found and rejected-credential errors, which retrying
        cannot fix.
    """
    transient = (
        gexc.ServiceUnavailable,
        gexc.InternalServerError,
        gexc.TooManyRequests,
        gexc.ResourceExhausted,
        gexc.Aborted,
        gexc.DeadlineExceeded,
        gexc.GatewayTimeout,
        ConnectionError,
        auth_exceptions.TransportError,
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
    )
    for attempt in range(1, MAX_RETRY_ATTEMPTS + 1):
        try:
            return call()
        except transient as error:
            _wait_before_retry(description, attempt, error)
        except gexc.Forbidden as error:
            if not _is_quota_error(error):
                raise RuntimeError(
                    f"{description} failed (Forbidden). Check that the service account has access "
                    f"and that the project and dataset exist: {error}"
                )
            _wait_before_retry(description, attempt, error)
        except auth_exceptions.GoogleAuthError as error:
            # Raised when Google rejects the key, for example because it was disabled or deleted
            raise RuntimeError(
                f"{description} failed: Google rejected the service account key "
                f"({type(error).__name__}). Check that the key is valid and has not been disabled "
                f"or deleted: {error}"
            )
        except (gexc.Unauthorized, gexc.NotFound) as error:
            raise RuntimeError(
                f"{description} failed ({type(error).__name__}). Check that the service account "
                f"has access and that the project and dataset exist: {error}"
            )


def _parse_service_account_key(configuration):
    """
    Read the service account key from the configuration. The key is never logged, and error
    messages do not include any part of it.
    Args:
        configuration: dict with connector configuration
    Returns:
        The service account key as a dict.
    """
    try:
        key_info = json.loads(configuration["service_account_key"])
    except (ValueError, TypeError):
        # "from None" keeps the text of the key, which the JSON error holds, out of the traceback
        raise ValueError(
            "service_account_key must be the service account key JSON as a string."
        ) from None
    if not isinstance(key_info, dict) or key_info.get("type") != "service_account":
        raise ValueError("service_account_key must be a Google service account key.")
    return key_info


def _check_project(configuration, key_info):
    """
    Check that the project_id setting is the project of the service account key, so that a key
    from another project is caught when the connection is set up. Reads and queries are billed to
    this project. Tables in another project are written as project.dataset.table in TABLE_SPECS.
    Args:
        configuration: dict with connector configuration
        key_info: the service account key as a dict
    Returns:
        The project id.
    Raises:
        ValueError: when project_id is missing, invalid, or does not match the key.
    """
    project = (configuration.get("project_id") or "").strip()
    key_project = key_info.get("project_id")
    if not project:
        raise ValueError("project_id is required.")
    if not __IDENTIFIER_PATTERN.fullmatch(project):
        raise ValueError("project_id contains unsupported characters.")
    if not key_project:
        raise ValueError("The service account key does not contain a project_id.")
    if project != key_project:
        raise ValueError(
            f"project_id '{project}' does not match the project of the service account key "
            f"('{key_project}'). Enter the project that the service account belongs to. To read "
            "tables from another project, write them as project.dataset.table in TABLE_SPECS."
        )
    return project


def _normalize_name(name):
    """
    Approximate how Fivetran normalizes a schema or table name in the destination: camelCase is
    split, characters other than letters, digits and underscores become underscores, and the name
    is lower-cased.
    Args:
        name: dataset or table name
    Returns:
        The normalized name.
    """
    split = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name)
    return re.sub(r"[^A-Za-z0-9_]", "_", split).lower()


def _check_name_collisions(items, description, hint=None):
    """
    Fail when different BigQuery names would become the same name in the destination, because
    their data would be silently merged. BigQuery names are case-sensitive, so datasets such as
    NETSUITE and netsuite can both exist, and columns such as userId and user_id become one name.
    Args:
        items: list of tuples of names, for example (dataset,) or (dataset, table)
        description: what the names are, used in the error message
        hint: what to do about it, used in the error message. Defaults to listing only one.
    Raises:
        ValueError: that lists the names that collide.
    """
    groups = {}
    for item in items:
        groups.setdefault(tuple(_normalize_name(name) for name in item), []).append(".".join(item))
    collisions = [names for names in groups.values() if len(names) > 1]
    if collisions:
        examples = "; ".join(" and ".join(names) for names in collisions[:3])
        hint = hint or "List only one of them in TABLE_SPECS."
        raise ValueError(
            f"{len(collisions)} group(s) of {description} would become the same name in the "
            f"destination and their data would be merged: {examples}. {hint}"
        )


def _create_context(configuration):
    """
    Create the BigQuery clients.
    Args:
        configuration: dict with connector configuration
    Returns:
        A new ClientContext instance.
    """
    key_info = _parse_service_account_key(configuration)
    billing_project = _check_project(configuration, key_info)
    try:
        credentials = service_account.Credentials.from_service_account_info(
            key_info, scopes=[__BIGQUERY_SCOPE]
        )
    except (ValueError, auth_exceptions.GoogleAuthError):
        # The library's message can describe the key, so it is not passed on
        raise ValueError(
            "service_account_key could not be used. Check that it is the complete JSON key of a "
            "service account."
        ) from None
    return ClientContext(
        bq_client=bigquery.Client(credentials=credentials, project=billing_project),
        read_client=bigquery_storage_v1.BigQueryReadClient(credentials=credentials),
        billing_project=billing_project,
        locations={},
    )


def get_context(configuration):
    """
    Build, or return the cached, clients and settings for this run.
    Args:
        configuration: dict with connector configuration
    Returns:
        A ClientContext instance.
    """
    global __CONTEXT
    if __CONTEXT is None:
        __CONTEXT = _create_context(configuration)
        log.info(f"Connected to BigQuery project {__CONTEXT.billing_project}")
    return __CONTEXT


def _dataset_location(ctx, project, dataset):
    """
    Get the location of a dataset, which INFORMATION_SCHEMA query jobs need. Locations are read
    when first needed and cached.
    Args:
        ctx: ClientContext
        project: project that contains the dataset
        dataset: dataset id
    Returns:
        The location, for example US or EU.
    """
    with __LOCATION_LOCK:
        if (project, dataset) not in ctx.locations:
            info = _retry(
                lambda: ctx.bq_client.get_dataset(
                    f"{project}.{dataset}", timeout=API_TIMEOUT_SECONDS
                ),
                f"Reading dataset {project}.{dataset}",
            )
            ctx.locations[(project, dataset)] = info.location
        return ctx.locations[(project, dataset)]


def _schema_change_options(name, spec):
    """
    Read and check the options that control what happens when the schema or settings of a table
    change.
    Args:
        name: table name, used in error messages
        spec: the table's entry in TABLE_SPECS, or an empty dict
    Returns:
        A tuple of (on_schema_change, truncate_on_resync).
    Raises:
        ValueError: when an option has an invalid value.
    """
    policy = spec.get("on_schema_change", SCHEMA_CHANGE_DEFAULT)
    if policy not in ("resync", "keep"):
        raise ValueError(f'{name}: on_schema_change must be "resync" or "keep".')
    truncate = spec.get("truncate_on_resync", False)
    if not isinstance(truncate, bool):
        raise ValueError(f"{name}: truncate_on_resync must be True or False.")
    token = spec.get("resync_token")
    if token is not None and (isinstance(token, bool) or not isinstance(token, (str, int))):
        raise ValueError(f"{name}: resync_token must be text or a whole number.")
    return policy, truncate


def _cursor_margin(name, spec):
    """
    Read and check how far before the last cursor value a cursor-based read starts.
    Args:
        name: table name, used in error messages
        spec: the table's entry in TABLE_SPECS, or an empty dict
    Returns:
        The margin in seconds.
    Raises:
        ValueError: when the option is not a number that is 0 or more.
    """
    value = spec.get("cursor_margin_seconds", CURSOR_MARGIN_SECONDS)
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        raise ValueError(f"{name}: cursor_margin_seconds must be a number that is 0 or more.")
    return value


def _note_bignumeric_columns(name, table, spec, types):
    """
    Warn on the dashboard about selected BIGNUMERIC columns that keep the default type. BIGNUMERIC
    holds more digits than the DECIMAL(38, 9) that the connector declares for it, so values with
    more than 9 decimal places are rounded and values with more than 29 integer digits become
    NULL. NUMERIC fits DECIMAL(38, 9) exactly, so it needs no warning.
    Args:
        name: table name
        table: bigquery.Table
        spec: the table's entry in TABLE_SPECS, or an empty dict
        types: dict of the selected columns and their BigQuery types
    """
    overridden = set(spec.get("column_types") or {})
    for field in table.schema:
        is_selected_default = field.name in types and field.name not in overridden
        if is_selected_default and _normalize_type(field.field_type) == "BIGNUMERIC":
            _note_skipped(
                "BIGNUMERIC columns are stored as DECIMAL(38, 9), which rounds beyond 9 decimal "
                "places and gives NULL above 29 integer digits (set column_types to choose "
                "another type)",
                f"{name}.{field.name}",
            )


def _validate_specs_offline(refs):
    """
    Check the options of the TABLE_SPECS entries that can be checked without calling BigQuery: the
    form of primary_keys, the options for schema changes, and the syntax of the filter. The rest is
    checked against the table metadata when the schema is read.
    Args:
        refs: list of TableRef
    Raises:
        ValueError: that names the entry and what is wrong.
    """
    for ref in refs:
        _schema_change_options(ref.key, ref.spec)
        _cursor_margin(ref.key, ref.spec)
        keys = ref.spec.get("primary_keys")
        if keys:
            valid = isinstance(keys, (list, tuple)) and len(set(keys)) == len(keys)
            if not valid:
                raise ValueError(
                    f"{ref.key}: primary_keys must be a list of different column names."
                )
        if ref.spec.get("filter"):
            _parse_filter(ref.spec["filter"])


def _probe_access(ctx, ref):
    """
    Make the one call of the connection test: read the metadata of the first listed table, with a
    short timeout and no retries. It shows that Google accepts the key and that the service account
    can reach the table, which is the access every sync needs.
    Args:
        ctx: ClientContext
        ref: TableRef of the table to read
    Raises:
        RuntimeError: with a message that says what failed, without any credential in it.
    """
    name = f"{ref.project}.{ref.dataset}.{ref.table}"
    try:
        ctx.bq_client.get_table(name, retry=None, timeout=SETUP_TEST_TIMEOUT_SECONDS)
    except gexc.NotFound:
        raise RuntimeError(
            f"Table {ref.key} was not found, or the service account cannot see it. Check its name "
            "in TABLE_SPECS and that the service account has BigQuery Data Viewer on it."
        )
    except (gexc.Forbidden, gexc.Unauthorized):
        raise RuntimeError(
            f"The service account cannot read {ref.key}. Grant it BigQuery Data Viewer on the table."
        )
    except auth_exceptions.GoogleAuthError as error:
        raise RuntimeError(
            f"Google rejected the service account key ({type(error).__name__}). Check that the "
            "key is valid and has not been disabled or deleted."
        )
    except (gexc.GoogleAPICallError, requests.exceptions.RequestException) as error:
        raise RuntimeError(
            f"BigQuery did not answer within {SETUP_TEST_TIMEOUT_SECONDS} seconds "
            f"({type(error).__name__}). Try the test again."
        )


def check_connection(configuration):
    """
    Check the setup for the connection test of the setup form. It is quick on purpose: it checks
    the settings, the form of every TABLE_SPECS entry, and makes one short call to BigQuery. Whether
    every table exists, can be read and has a primary key is checked in full when the schema is
    read and when a sync starts. Nothing is cached, so the test always uses the values that were
    just entered.
    Args:
        configuration: dict with connector configuration
    Raises:
        ValueError or RuntimeError: with a message that describes what failed, without any
        credential in it.
    """
    ctx = _create_context(configuration)
    refs, _ = _table_references(ctx.billing_project)
    if not refs:
        raise ValueError(
            "TABLE_SPECS does not list a table to sync. Add an entry for each table, for example "
            '"sales.orders": {"primary_keys": ["order_id"]}.'
        )
    _validate_specs_offline(refs)
    _probe_access(ctx, refs[0])


def _normalize_type(field_type):
    """
    Normalize a BigQuery column type name to the legacy names used by the client library.
    Args:
        field_type: type name as reported by BigQuery
    Returns:
        Upper-case type name.
    """
    name = (field_type or "").upper()
    return __TYPE_ALIASES.get(name, name)


def _infer_cursor_column(columns):
    """
    Infer a cursor column for a table that is not partitioned on a column.
    Only a TIMESTAMP, DATETIME or DATE column with a name from PREFERRED_CURSOR_COLUMN_NAMES is
    used, because any other column, such as a creation date, would miss updates to existing rows.
    Args:
        columns: list of (column_name, bigquery_type) tuples
    Returns:
        The cursor column name, or an empty string when none is suitable.
    """
    candidates = [name for name, column_type in columns if column_type in CURSOR_COLUMN_TYPES]
    by_lower_name = {name.lower(): name for name in candidates}
    for preferred in PREFERRED_CURSOR_COLUMN_NAMES:
        if preferred in by_lower_name:
            return by_lower_name[preferred]
    return ""


def _decimal_type(source_field):
    """
    Build the Fivetran DECIMAL type for a NUMERIC or BIGNUMERIC column. The precision and scale
    declared on the column are used when they fit a destination decimal, otherwise the defaults.
    Args:
        source_field: bigquery.SchemaField of a NUMERIC or BIGNUMERIC column
    Returns:
        A dict like {"type": "DECIMAL", "precision": 38, "scale": 9}.
    """
    precision, scale = source_field.precision, source_field.scale
    if not precision or precision > __MAX_DECIMAL_PRECISION:
        precision, scale = __MAX_DECIMAL_PRECISION, __DEFAULT_DECIMAL_SCALE
    return {"type": "DECIMAL", "precision": precision, "scale": scale or 0}


def _column_type_overrides(table_name, column_types, selected_types):
    """
    Validate the "column_types" override of a table in TABLE_SPECS.
    Each value is a Fivetran type name, or for DECIMAL a dict with "type", "precision" and "scale".
    Args:
        table_name: name of the table, used in error messages
        column_types: dict from TABLE_SPECS, or None
        selected_types: dict of the selected columns and their BigQuery types
    Returns:
        The validated overrides, keyed by column name.
    """
    if not column_types:
        return {}
    if not isinstance(column_types, dict):
        raise ValueError(f"{table_name}: column_types must be a dictionary.")
    for column, fivetran_type in column_types.items():
        if column not in selected_types:
            raise ValueError(f"{table_name}: column_types names an unknown column '{column}'.")
        if isinstance(fivetran_type, dict):
            is_decimal = str(fivetran_type.get("type", "")).upper() == "DECIMAL"
            has_sizes = all(isinstance(fivetran_type.get(k), int) for k in ("precision", "scale"))
            valid = is_decimal and has_sizes
        else:
            valid = str(fivetran_type).upper() in __VALID_FIVETRAN_TYPES
        if not valid:
            raise ValueError(
                f"{table_name}: column_types for '{column}' must be one of "
                f"{', '.join(sorted(__VALID_FIVETRAN_TYPES))} or "
                '{"type": "DECIMAL", "precision": 18, "scale": 2}.'
            )
    return dict(column_types)


def _is_unsupported_key_type(field):
    """
    Check whether a column cannot be part of a primary key.
    Args:
        field: bigquery.SchemaField
    Returns:
        True for arrays and for record, JSON, geography, interval and range columns.
    """
    if field.mode == "REPEATED":
        return True
    return _normalize_type(field.field_type) in __UNSUPPORTED_KEY_TYPES


def _resolve_primary_keys(name, spec, table, column_names):
    """
    Decide the primary key of a table. Keys set in TABLE_SPECS win, then the key declared in
    BigQuery. Keys are never guessed from column names or data. A table without a key is not
    synced, see _require_primary_keys.
    Args:
        name: table name
        spec: the table's entry in TABLE_SPECS, or an empty dict
        table: bigquery.Table
        column_names: set of the table's column names
    Returns:
        List of primary key columns, possibly empty.
    """
    configured = spec.get("primary_keys")
    if configured:
        if isinstance(configured, str) or not isinstance(configured, (list, tuple)):
            raise ValueError(f"{name}: primary_keys must be a list of column names.")
        keys = list(configured)
        if len(set(keys)) != len(keys):
            raise ValueError(f"{name}: primary_keys lists the same column more than once.")
        unknown = [key for key in keys if key not in column_names]
        if unknown:
            raise ValueError(
                f"{name}: primary_keys in TABLE_SPECS name columns that do not exist: "
                f"{', '.join(unknown)}"
            )
    else:
        constraints = getattr(table, "table_constraints", None)
        declared = getattr(getattr(constraints, "primary_key", None), "columns", None)
        keys = list(declared) if declared else []
    fields = {field.name: field for field in table.schema}
    unsupported = [key for key in keys if _is_unsupported_key_type(fields[key])]
    if unsupported:
        raise ValueError(
            f"{name}: primary key column(s) {', '.join(unsupported)} have a type that cannot be "
            "a key (array, record, JSON, geography, interval or range)."
        )
    return keys


def _detect_partitioning(name, table):
    """
    Find the column a table is partitioned on. A table partitioned by ingestion time has no
    column, so the _PARTITIONTIME pseudo-column is used in its place.
    Args:
        name: table name
        table: bigquery.Table
    Returns:
        A tuple of (partition_column, partition_kind, granularity, range_interval). The column is
        an empty string when the table is not partitioned.
    """
    time_partitioning = table.time_partitioning
    range_partitioning = table.range_partitioning
    if time_partitioning is not None and time_partitioning.field:
        return time_partitioning.field, "TIME", (time_partitioning.type_ or "DAY").upper(), 0
    if range_partitioning is not None and range_partitioning.field:
        interval = int(range_partitioning.range_.interval)
        return range_partitioning.field, "RANGE", "", interval
    if time_partitioning is not None:
        granularity = (time_partitioning.type_ or "DAY").upper()
        return __INGESTION_TIME_COLUMN, "TIME", granularity, 0
    return "", "", "", 0


def _with_pseudo_columns(column_types, partition_column):
    """
    Add the partition pseudo-columns of an ingestion-time partitioned table to a column type map,
    so they can be used in partition restrictions and table filters. They are not read as data.
    Args:
        column_types: dict mapping column names to BigQuery types
        partition_column: the table's partition column, or an empty string
    Returns:
        A new dict that includes _PARTITIONTIME and _PARTITIONDATE when they apply.
    """
    if partition_column == __INGESTION_TIME_COLUMN:
        return {**column_types, **__INGESTION_PSEUDO_TYPES}
    return dict(column_types)


def _lookback_partitions(name, spec, partition_kind, granularity):
    """
    Decide how many of the most recent already-synced partitions are re-read on every incremental
    sync. The default is given in days and converted using the partition size, so it means the same
    amount of data for hourly, daily, monthly and yearly tables.
    Args:
        name: table name, used in error messages
        spec: the table's entry in TABLE_SPECS, or an empty dict
        partition_kind: TIME or RANGE for partitioned tables, otherwise an empty string
        granularity: HOUR, DAY, MONTH or YEAR for TIME partitioning
    Returns:
        The number of partitions.
    """
    for key in ("lookback_partitions", "lookback_days"):
        value = spec.get(key)
        if value is not None and (
            isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0
        ):
            raise ValueError(f"{name}: {key} must be a number that is 0 or more.")
    if spec.get("lookback_partitions") is not None:
        return int(spec["lookback_partitions"])
    if partition_kind != "TIME":
        return LOOKBACK_PARTITIONS
    days = spec["lookback_days"] if spec.get("lookback_days") is not None else LOOKBACK_DAYS
    if days == 0:
        return 0
    hours_per_partition = __HOURS_PER_PARTITION.get(granularity, 24)
    return max(1, math.ceil(days * 24 / hours_per_partition))


def _choose_strategy(name, spec, partition_column, all_columns):
    """
    Choose the replication strategy and, for tables not partitioned on a column, the cursor column.
    A FULL sync of a partitioned table still reads partition by partition, so it can checkpoint
    and resume, but it never uses a watermark.
    Args:
        name: table name
        spec: the table's entry in TABLE_SPECS, or an empty dict
        partition_column: the table's partition column, or an empty string
        all_columns: list of (column_name, bigquery_type) tuples
    Returns:
        A tuple of (strategy, cursor_column).
    """
    requested = (spec.get("strategy") or "").upper()
    cursor_column = ""
    if not partition_column:
        cursor_column = spec.get("cursor_column") or _infer_cursor_column(all_columns)
        configured_type = dict(all_columns).get(cursor_column)
        if spec.get("cursor_column") and configured_type not in CURSOR_COLUMN_TYPES:
            raise ValueError(
                f"{name}: cursor_column '{cursor_column}' must be a TIMESTAMP, DATETIME or DATE column."
            )
    if requested == __STRATEGY_FULL or not (partition_column or cursor_column):
        return __STRATEGY_FULL, ""
    return __STRATEGY_INCREMENTAL, cursor_column


def _select_columns(spec, all_columns, required_columns):
    """
    Apply the include and exclude lists of a table. Key and cursor columns are always read.
    Args:
        spec: the table's entry in TABLE_SPECS, or an empty dict
        all_columns: list of (column_name, bigquery_type) tuples
        required_columns: columns that must always be read
    Returns:
        List of the selected (column_name, bigquery_type) tuples.
    """
    include = {c.lower() for c in (spec.get("include") or [])}
    exclude = {c.lower() for c in (spec.get("exclude") or [])}
    required = {c.lower() for c in required_columns}
    selected = []
    for column, column_type in all_columns:
        name_lower = column.lower()
        projected = (not include or name_lower in include) and name_lower not in exclude
        if name_lower in required or projected:
            selected.append((column, column_type))
    return selected


def _fivetran_type(name, source_field):
    """
    Choose the Fivetran type to declare for one BigQuery column. Repeated columns (arrays) are
    JSON. A column whose value shows its own type is not declared.
    Args:
        name: table name, used in log messages
        source_field: bigquery.SchemaField
    Returns:
        A Fivetran type name or a DECIMAL type dict, or None when the type is left to inference.
    """
    if source_field.mode == "REPEATED":
        return "JSON"
    field_type = _normalize_type(source_field.field_type)
    if field_type in ("NUMERIC", "BIGNUMERIC"):
        return _decimal_type(source_field)
    if field_type in __BIGQUERY_TO_FIVETRAN_TYPE:
        return __BIGQUERY_TO_FIVETRAN_TYPE[field_type]
    if field_type not in __INFERRED_TYPES:
        log.warning(
            f"{name}.{source_field.name}: BigQuery type {field_type} has no mapping, so Fivetran "
            "infers its type. Set column_types in TABLE_SPECS."
        )
    return None


def _fivetran_column_types(name, table, spec, types):
    """
    Build the Fivetran types to declare in schema(), for the selected columns whose type cannot be
    inferred from the value. "column_types" in TABLE_SPECS wins over the mapping and can declare
    any column.
    Args:
        name: table name
        table: bigquery.Table
        spec: the table's entry in TABLE_SPECS, or an empty dict
        types: dict of the selected columns and their BigQuery types
    Returns:
        Dict mapping column names to a Fivetran type name or a DECIMAL type dict.
    """
    explicit = {}
    for source_field in table.schema:
        if source_field.name in types:
            fivetran_type = _fivetran_type(name, source_field)
            if fivetran_type is not None:
                explicit[source_field.name] = fivetran_type
    explicit.update(_column_type_overrides(name, spec.get("column_types"), types))
    return explicit


def _reject_moving_window(filter_condition, column_types):
    """
    Refuse a relative filter value, such as today-7d, that is used as an upper bound or with an
    equality. In an incremental sync the window of such a filter moves forward over rows that were
    already passed, and those rows would never be read. A relative value used as a lower bound only
    shrinks the window, which loses nothing.
    Args:
        filter_condition: filter text written like a WHERE clause
        column_types: dict mapping every column of the table to its BigQuery type
    Raises:
        ValueError: that names the condition and the safe alternatives.
    """
    for column, operator, value in _parse_filter(filter_condition):
        if column_types[column] not in ("DATE", "DATETIME", "TIMESTAMP"):
            continue
        if __RELATIVE_VALUE.match(str(value).strip().lower()) and operator not in (">", ">="):
            raise ValueError(
                f"Table filter condition on '{column}': the relative value {value} can only be "
                f"used with > or >= in an incremental sync. With {operator}, rows that come into "
                "the window later would never be read. Use a fixed value, or set the strategy "
                "of the table to FULL."
            )


def _validated_filter(spec, all_columns, partition_column, strategy):
    """
    Get the table filter from the table specification and check it, so that an invalid filter
    fails when the table metadata is read instead of in the middle of a sync.
    Args:
        spec: the table's entry in TABLE_SPECS, or an empty dict
        all_columns: list of (column_name, bigquery_type) tuples
        partition_column: the table's partition column, or an empty string
        strategy: FULL or INCREMENTAL
    Returns:
        The filter text from the specification, or None when there is none.
    """
    filter_condition = spec.get("filter")
    if not filter_condition:
        # Missing, empty text or an empty list: no filter
        return None
    column_types = _with_pseudo_columns(dict(all_columns), partition_column)
    _build_filter_restriction(filter_condition, column_types)
    if strategy == __STRATEGY_INCREMENTAL:
        _reject_moving_window(filter_condition, column_types)
    return filter_condition


def _fingerprint(spec, layout):
    """
    Hash the settings that decide which rows of a table are read and how, so that a change to
    any of them can be noticed. Saved progress is only valid for the settings it was made with: a
    wider filter would never read the older partitions that now fall inside it, and a changed
    partition column or key makes a saved watermark meaningless. Columns that BigQuery adds to the
    table later are not part of the hash: they are compared separately, see _schema_changes, so
    that on_schema_change decides whether the table is read again. The resync_token is part of the
    hash only when it is set, so that changing it forces a re-read.
    Args:
        spec: the table's entry in TABLE_SPECS, or an empty dict
        layout: dict of the values chosen for the table, such as its partitioning and keys
    Returns:
        A 16 character hexadecimal string.
    """
    options = {
        "include": sorted(str(column).lower() for column in (spec.get("include") or [])),
        "exclude": sorted(str(column).lower() for column in (spec.get("exclude") or [])),
        "column_types": spec.get("column_types") or {},
    }
    if spec.get("resync_token") is not None:
        options["resync_token"] = str(spec["resync_token"])
    text = json.dumps({**layout, **options}, sort_keys=True, default=str)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def _field_signature(field):
    """
    Describe the type of a column in a short text that changes when the type changes. A record
    column is described by a hash of its fields, so a field added inside it is noticed without
    saving every nested name in state.
    Args:
        field: bigquery.SchemaField
    Returns:
        Text such as INTEGER, NUMERIC(10,2), TIMESTAMP[] or RECORD#1a2b3c4d.
    """
    text = _normalize_type(field.field_type)
    if text == "RECORD":
        nested = ",".join(f"{sub.name}:{_field_signature(sub)}" for sub in field.fields)
        text += "#" + hashlib.sha256(nested.encode("utf-8")).hexdigest()[:8]
    elif text in ("NUMERIC", "BIGNUMERIC") and field.precision:
        text += f"({field.precision},{field.scale or 0})"
    return text + ("[]" if field.mode == "REPEATED" else "")


def _schema_signature(table, types):
    """
    Describe the selected columns of a table, to notice when a column is added or changes type.
    Args:
        table: bigquery.Table
        types: dict of the selected columns and their BigQuery types
    Returns:
        Dict mapping each selected column to the text from _field_signature.
    """
    return {field.name: _field_signature(field) for field in table.schema if field.name in types}


def _build_plan(ref, table):
    """
    Build the sync plan for one table from its BigQuery metadata and its TABLE_SPECS entry.
    Args:
        ref: TableRef of the table
        table: bigquery.Table returned by get_table
    Returns:
        A TablePlan, or None when the table has no usable columns.
    """
    name, spec = ref.key, ref.spec
    all_columns = [(f.name, _normalize_type(f.field_type)) for f in table.schema]
    if not all_columns:
        log.warning(f"{name}: no columns found, skipping")
        _note_skipped("Tables without columns were not synced", name)
        return None

    primary_keys = _resolve_primary_keys(name, spec, table, {c for c, _ in all_columns})
    partition_column, partition_kind, granularity, range_interval = _detect_partitioning(
        name, table
    )
    strategy, cursor_column = _choose_strategy(name, spec, partition_column, all_columns)
    required = primary_keys + ([cursor_column] if cursor_column else [])
    selected = _select_columns(spec, all_columns, required)
    types = dict(selected)

    filter_condition = _validated_filter(spec, all_columns, partition_column, strategy)
    on_schema_change, truncate_on_resync = _schema_change_options(name, spec)
    cursor_margin_seconds = _cursor_margin(name, spec)
    _note_bignumeric_columns(name, table, spec, types)

    return TablePlan(
        stream=ref.table,
        dataset=ref.dataset,
        project=ref.project,
        key=ref.key,
        columns=[c for c, _ in selected],
        column_types=_with_pseudo_columns(types, partition_column),
        explicit_columns=_fivetran_column_types(name, table, spec, types),
        json_columns={c for c, t in types.items() if t == "JSON"},
        primary_keys=primary_keys,
        strategy=strategy,
        partition_column=partition_column,
        partition_kind=partition_kind,
        granularity=granularity,
        range_interval=range_interval,
        cursor_column=cursor_column,
        lookback=_lookback_partitions(name, spec, partition_kind, granularity),
        filter_condition=filter_condition,
        num_rows=int(table.num_rows or 0),
        num_bytes=int(table.num_bytes or 0),
        full_by_default=strategy == __STRATEGY_FULL and not spec.get("strategy"),
        fingerprint=_fingerprint(
            spec,
            {
                "primary_keys": primary_keys,
                "partition_column": partition_column,
                "partition_kind": partition_kind,
                "granularity": granularity,
                "range_interval": range_interval,
                "strategy": strategy,
                "cursor_column": cursor_column,
                "filter": filter_condition,
            },
        ),
        schema_signature=_schema_signature(table, types),
        on_schema_change=on_schema_change,
        truncate_on_resync=truncate_on_resync,
        streaming_buffer=getattr(table, "streaming_buffer", None) is not None,
        cursor_margin_seconds=cursor_margin_seconds,
        options={name_: value for name_, value in spec.items() if name_ != "skip"},
    )


def _parse_table_key(spec_key, billing_project):
    """
    Split a TABLE_SPECS key into its project, dataset and table. The key is dataset.table, for a
    table in the billing project, or project.dataset.table. Dataset and table names cannot contain
    a dot, so the parts are taken from the right and a project id may contain dots.
    Args:
        spec_key: the key of a TABLE_SPECS entry
        billing_project: the project of the service account key
    Returns:
        A tuple of (project, dataset, table).
    Raises:
        ValueError: when the key is not one table.
    """
    parts = spec_key.strip().rsplit(".", 2) if isinstance(spec_key, str) else []
    if len(parts) == 2:
        project, (dataset, table) = billing_project, parts
    elif len(parts) == 3:
        project, dataset, table = parts
    else:
        project = dataset = table = ""
    valid = all(
        part and pattern.fullmatch(part)
        for part, pattern in (
            (project, __IDENTIFIER_PATTERN),
            (dataset, __KEY_PART),
            (table, __KEY_PART),
        )
    )
    if not valid:
        raise ValueError(
            f"TABLE_SPECS key {spec_key!r} must name one table, as dataset.table or "
            "project.dataset.table. Wildcards and table names without a dataset are not supported."
        )
    return project, dataset, table


def _table_references(billing_project):
    """
    Read the tables to sync from TABLE_SPECS. Every key is one table. An entry with skip set to
    True is left out.
    Args:
        billing_project: the project of the service account key
    Returns:
        A tuple of (list of TableRef, number of entries that are skipped).
    Raises:
        ValueError: when a key or entry is invalid, or a table is listed twice.
    """
    refs, seen, skipped = [], {}, 0
    for spec_key, spec in TABLE_SPECS.items():
        project, dataset, table = _parse_table_key(spec_key, billing_project)
        if not isinstance(spec, dict):
            raise ValueError(f"{spec_key}: the TABLE_SPECS entry must be a dictionary.")
        key = (
            f"{dataset}.{table}" if project == billing_project else f"{project}.{dataset}.{table}"
        )
        if key in seen:
            raise ValueError(f"TABLE_SPECS lists {key} twice, as {seen[key]!r} and {spec_key!r}.")
        seen[key] = spec_key
        skip = spec.get("skip", False)
        if not isinstance(skip, bool):
            raise ValueError(f"{key}: skip must be True or False.")
        if skip:
            skipped += 1
        else:
            refs.append(TableRef(project, dataset, table, spec, key))
    return refs, skipped


def _load_tables(ctx, refs):
    """
    Read the metadata of the listed tables in parallel. A table that does not exist or cannot be
    read fails with a message that names it.
    Args:
        ctx: ClientContext
        refs: list of TableRef
    Returns:
        List of bigquery.Table in the same order.
    """

    def load(ref):
        """Read the metadata of one table."""
        return _retry(
            lambda: ctx.bq_client.get_table(
                f"{ref.project}.{ref.dataset}.{ref.table}", timeout=API_TIMEOUT_SECONDS
            ),
            f"Reading metadata for {ref.key}",
        )

    with ThreadPoolExecutor(max_workers=MAX_DISCOVERY_WORKERS) as executor:
        return list(executor.map(load, refs))


def _require_readable_tables(refs, tables):
    """
    Check that every listed table is a plain table. Views, materialized views and external tables
    cannot be read with the Storage Read API.
    Args:
        refs: list of TableRef
        tables: list of bigquery.Table in the same order
    Raises:
        ValueError: that names the tables that cannot be read.
    """
    unreadable = [
        f"{ref.key} ({table.table_type})"
        for ref, table in zip(refs, tables)
        if table.table_type != __READABLE_TABLE_TYPE
    ]
    if unreadable:
        raise ValueError(
            f"TABLE_SPECS lists table(s) that cannot be read with the Storage Read API: "
            f"{', '.join(unreadable)}. Views and external tables are not supported. List the "
            'table they read from, or set "skip" for the entry.'
        )


def _require_primary_keys(plans):
    """
    Check that every table has a primary key, declared in BigQuery or set in TABLE_SPECS. Rows of
    a table without a key cannot be updated in the destination, so every changed row would become
    a new row next to its old version.
    Args:
        plans: list of TablePlan
    Raises:
        ValueError: that names the tables without a primary key.
    """
    missing = [plan.key for plan in plans if not plan.primary_keys]
    if missing:
        raise ValueError(
            f"TABLE_SPECS lists table(s) without a primary key: {', '.join(missing)}. Every synced "
            'table needs one. Add "primary_keys" to the entry (a list of column names, for a '
            'composite key), declare a primary key on the table in BigQuery, or set "skip".'
        )


def _log_summary(plans, skipped, seconds):
    """
    Log a short summary of the table metadata: how many tables, rows and bytes, how the tables are
    read, and the largest tables. The details of each table are only logged at the DEBUG level,
    and again when the table starts to sync.
    Args:
        plans: list of TablePlan
        skipped: number of TABLE_SPECS entries that are skipped
        seconds: how long reading the metadata took
    """
    kinds = {"partition": 0, "cursor": 0, "full": 0}
    for plan in plans:
        kinds[plan_kind(plan)] += 1
        log.debug(f"{plan.key}: {describe_plan(plan, plan.strategy)}")
    datasets = len({(plan.project, plan.dataset) for plan in plans})
    skipped_text = f" ({skipped:,} skipped by skip)" if skipped else ""
    log.info(
        f"Table metadata read in {format_duration(seconds)}: {len(plans):,} table(s) to sync in "
        f"{datasets:,} dataset(s), {sum(p.num_rows for p in plans):,} rows, "
        f"{format_bytes(sum(p.num_bytes for p in plans))}{skipped_text}"
    )
    log.info(
        f"Strategies: {kinds['partition']} incremental by partition, {kinds['cursor']} "
        f"incremental by cursor column, {kinds['full']} full reload"
    )
    if len(plans) > 1:
        largest = sorted(plans, key=lambda plan: plan.num_bytes, reverse=True)[:5]
        names = ", ".join(f"{plan.key} ({format_bytes(plan.num_bytes)})" for plan in largest)
        log.info(f"Largest tables: {names}")


def _log_plans(plans, skipped, seconds):
    """
    Log the summary, and add a dashboard note for large tables that are read in full on every
    sync.
    Args:
        plans: list of TablePlan
        skipped: number of TABLE_SPECS entries that are skipped
        seconds: how long reading the metadata took
    """
    _log_summary(plans, skipped, seconds)
    detection, reading = describe_global_settings()
    log.info(f"config: {detection}")
    log.info(f"config: {reading}")
    for plan in plans:
        if plan.full_by_default and plan.num_bytes >= FULL_REFRESH_WARNING_BYTES:
            _note_skipped(
                "Large tables have no partition and no update-style cursor column, so every sync "
                "reads them in full (set cursor_column or strategy in TABLE_SPECS, or partition "
                "the source table)",
                f"{plan.key} ({plan.num_bytes / 1e9:.1f} GB)",
            )


def _build_plans(ctx):
    """
    Read the metadata of the tables in TABLE_SPECS and build a validated sync plan for each. Any
    mistake in TABLE_SPECS fails here, before data is read.
    Args:
        ctx: ClientContext
    Returns:
        A tuple of (list of TablePlan, number of entries that are skipped).
    Raises:
        ValueError or RuntimeError: when a table is missing, unreadable, has no primary key or has
        an invalid entry.
    """
    refs, skipped = _table_references(ctx.billing_project)
    if not refs:
        reason = f"All {skipped} entries are skipped" if skipped else "TABLE_SPECS is empty"
        raise ValueError(
            f"{reason}, so there is no table to sync. Add an entry for each table, for example "
            '"sales.orders": {"primary_keys": ["order_id"]}.'
        )
    _check_name_collisions([(ref.dataset, ref.table) for ref in refs], "tables")
    tables = _load_tables(ctx, refs)
    _require_readable_tables(refs, tables)
    plans = [_build_plan(ref, table) for ref, table in zip(refs, tables)]
    plans = [plan for plan in plans if plan is not None]
    _require_primary_keys(plans)
    for plan in plans:
        _check_name_collisions(
            [(column,) for column in plan.columns],
            f"columns of {plan.key}",
            "Use exclude in TABLE_SPECS to leave all but one of them out.",
        )
    return plans, skipped


def get_table_plans(configuration):
    """
    Get a sync plan for every table in TABLE_SPECS. The result is cached so that schema() and
    update() share one pass over the table metadata.
    Args:
        configuration: dict with connector configuration
    Returns:
        List of TablePlan instances, all with a primary key.
    """
    global __TABLE_PLAN_LIST
    if __TABLE_PLAN_LIST is not None:
        return __TABLE_PLAN_LIST

    started = time.monotonic()
    plans, skipped = _build_plans(get_context(configuration))
    _log_plans(plans, skipped, time.monotonic() - started)
    __TABLE_PLAN_LIST = plans
    return plans


def _temporal_literal(bigquery_type, value):
    """
    Build a typed SQL literal for a DATE, TIMESTAMP or DATETIME value. The value is parsed and
    formatted again, so it cannot break out of the literal. A TIMESTAMP is written in UTC.
    Args:
        bigquery_type: DATE, TIMESTAMP or DATETIME
        value: a date or datetime, or text in ISO 8601 form
    Returns:
        The SQL literal as a string.
    """
    if bigquery_type == "DATE":
        if isinstance(value, datetime):
            value = value.date()
        parsed = value if isinstance(value, date) else date.fromisoformat(str(value))
        return f"DATE '{parsed.isoformat()}'"
    parsed = (
        value
        if isinstance(value, datetime)
        else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    )
    if bigquery_type == "TIMESTAMP":
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        parsed = parsed.astimezone(timezone.utc)
        return f"TIMESTAMP '{parsed.strftime('%Y-%m-%d %H:%M:%S.%f')}+00'"
    return f"DATETIME '{parsed.replace(tzinfo=None).strftime('%Y-%m-%d %H:%M:%S.%f')}'"


def _literal(bigquery_type, value):
    """
    Build a typed SQL literal for a row restriction. Values are parsed and re-formatted, never
    concatenated as given, so a value cannot break out of the literal.
    Args:
        bigquery_type: BigQuery type of the column the literal is compared with
        value: python value (date, datetime, int) or string from configuration
    Returns:
        The SQL literal as a string.
    """
    if bigquery_type in ("DATE", "TIMESTAMP", "DATETIME"):
        return _temporal_literal(bigquery_type, value)
    if bigquery_type == "INTEGER":
        return str(int(value))
    if bigquery_type == "FLOAT":
        number = float(value)
        if not math.isfinite(number):
            raise ValueError("a filter value on a FLOAT column must be a finite number")
        return repr(number)
    if bigquery_type in ("NUMERIC", "BIGNUMERIC"):
        return f"{bigquery_type} '{Decimal(str(value))}'"
    if bigquery_type == "BOOLEAN":
        text = str(value).lower()
        if text not in ("true", "false"):
            # A typo would otherwise read the wrong rows without any error
            raise ValueError("a filter value on a BOOLEAN column must be true or false")
        return "TRUE" if text == "true" else "FALSE"
    if bigquery_type == "STRING":
        escaped = (
            str(value)
            .replace("\\", "\\\\")
            .replace("'", "\\'")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
        )
        return f"'{escaped}'"
    raise ValueError(f"Filters are not supported on columns of type {bigquery_type}.")


def begin_sync():
    """
    Mark the start of a sync. Relative filter values such as "today-90d" are resolved against this
    time, so every table and batch of the sync uses the same window.
    """
    __REFERENCE_TIME["value"] = datetime.now(timezone.utc)


def _reference_time():
    """
    Get the time the sync started, starting the sync now if begin_sync was not called.
    Returns:
        A timezone-aware datetime in UTC.
    """
    if __REFERENCE_TIME["value"] is None:
        begin_sync()
    return __REFERENCE_TIME["value"]


def _resolve_filter_value(column_type, value):
    """
    Turn a relative filter value such as "today-90d" or "now-12h" into a date or time. Other values
    are returned unchanged. Relative values only apply to DATE, DATETIME and TIMESTAMP columns.
    Args:
        column_type: BigQuery type of the filtered column
        value: the value from the table filter
    Returns:
        A datetime for a relative value, otherwise the value as given.
    """
    if column_type not in ("DATE", "DATETIME", "TIMESTAMP") or not isinstance(value, str):
        return value
    match = __RELATIVE_VALUE.match(value.strip().lower())
    if not match:
        return value
    anchor, amount, unit = match.groups()
    moment = _reference_time()
    if anchor == "today":
        if unit == "h":
            raise ValueError(
                "A relative value that starts with today can only use days, such as today-7d."
            )
        moment = moment.replace(hour=0, minute=0, second=0, microsecond=0)
    if amount:
        moment -= timedelta(days=int(amount)) if unit == "d" else timedelta(hours=int(amount))
    return moment


def _split_filter(text):
    """
    Split a filter into its conditions at every AND that is not inside a quoted value.
    Args:
        text: the filter text
    Returns:
        List of condition texts.
    Raises:
        ValueError: if a quote is not closed.
    """
    parts, current, quote, i = [], [], None, 0
    while i < len(text):
        char = text[i]
        if quote:
            current.append(char)
            if char == quote:
                if text[i + 1 : i + 2] == quote:
                    current.append(quote)
                    i += 1
                else:
                    quote = None
        elif char in "'\"":
            quote = char
            current.append(char)
        else:
            match = __FILTER_AND.match(text, i)
            if match:
                parts.append("".join(current))
                current = []
                i = match.end()
                continue
            current.append(char)
        i += 1
    if quote:
        raise ValueError(f"Table filter {text!r} has a quote that is not closed.")
    parts.append("".join(current))
    return parts


def _parse_filter(text):
    """
    Read a table filter written like a WHERE clause into its conditions.
    Args:
        text: for example "event_date >= today-90d AND country = 'IE'"
    Returns:
        List of (column, operator, value) tuples, with the operator <> turned into !=.
    Raises:
        ValueError: with an example of the syntax, when the text cannot be read.
    """
    if not isinstance(text, str):
        raise ValueError(
            "A table filter must be text written like a WHERE clause, for example "
            "\"event_date >= today-90d AND country = 'IE'\"."
        )
    conditions = []
    for part in _split_filter(text):
        match = __FILTER_CONDITION.match(part)
        if not match:
            raise ValueError(
                f"Cannot read the table filter condition {part.strip()!r}. Write conditions as "
                'COLUMN OPERATOR VALUE joined by AND, for example "event_date >= today-90d AND '
                f"country = 'IE'\". Operators: {', '.join(__FILTER_OPERATORS)}. OR, IN, LIKE and "
                "parentheses are not supported. Put values that contain spaces in quotes."
            )
        if match["single"] is not None:
            value = match["single"].replace("''", "'")
        elif match["double"] is not None:
            value = match["double"].replace('""', '"')
        else:
            value = match["bare"]
        operator = "!=" if match["operator"] == "<>" else match["operator"]
        conditions.append((match["column"], operator, value))
    return conditions


def _filter_clause(column, operator, value, column_types):
    """
    Check one filter condition and turn it into a row restriction.
    Args:
        column: column name
        operator: one of the supported operators
        value: the value as text, possibly a relative value such as today-90d
        column_types: dict mapping every column of the table to its BigQuery type
    Returns:
        The restriction, for example "`event_date` >= DATE '2023-01-01'".
    """
    if column not in column_types:
        raise ValueError(f"Table filter column '{column}' does not exist in the table.")
    column_type = column_types[column]
    resolved = _resolve_filter_value(column_type, value)
    try:
        literal = _literal(column_type, resolved)
    except (ValueError, ArithmeticError) as error:
        raise ValueError(
            f"Table filter value {value!r} is not valid for column '{column}' "
            f"of type {column_type}: {error}"
        )
    return f"`{column}` {operator} {literal}"


def _build_filter_restriction(filter_condition, column_types):
    """
    Check a table filter and turn it into a row restriction string. All conditions must match.
    Args:
        filter_condition: filter text written like a WHERE clause
        column_types: dict mapping every column of the table to its BigQuery type
    Returns:
        The restriction, for example "`event_date` >= DATE '2023-01-01' AND `country` = 'IE'".
    """
    return " AND ".join(
        _filter_clause(column, operator, value, column_types)
        for column, operator, value in _parse_filter(filter_condition)
    )


def _add_months(value, months):
    """
    Add months to the first day of a month.
    Args:
        value: datetime on the first day of a month
        months: number of months to add
    Returns:
        datetime on the first day of the resulting month.
    """
    index = value.year * 12 + (value.month - 1) + months
    return value.replace(year=index // 12, month=index % 12 + 1)


def _partition_bounds(plan, partition_id):
    """
    Convert a partition id into a half-open range [lower, upper) in the partition column's type.
    Args:
        plan: TablePlan of a column-partitioned table
        partition_id: partition id from INFORMATION_SCHEMA.PARTITIONS
    Returns:
        A tuple of (lower, upper) values.
    """
    if plan.partition_kind == "RANGE":
        start = int(partition_id)
        return start, start + plan.range_interval

    formats = {"HOUR": "%Y%m%d%H", "DAY": "%Y%m%d", "MONTH": "%Y%m", "YEAR": "%Y"}
    lower = datetime.strptime(partition_id, formats[plan.granularity])
    if plan.granularity == "HOUR":
        upper = lower + timedelta(hours=1)
    elif plan.granularity == "DAY":
        upper = lower + timedelta(days=1)
    elif plan.granularity == "MONTH":
        upper = _add_months(lower, 1)
    else:
        upper = lower.replace(year=lower.year + 1)

    if plan.column_types[plan.partition_column] == "DATE":
        return lower.date(), upper.date()
    return lower, upper


def _parse_partition_row(plan, row):
    """
    Convert one row of INFORMATION_SCHEMA.PARTITIONS into a partition dict. The
    __UNPARTITIONED__ partition holds rows that BigQuery has not moved into a partition yet, such
    as streamed rows, and values outside the partition range. It cannot be read by partition, so
    it is reported and left out. Its rows are counted by _fetch_partitions.
    Args:
        plan: TablePlan
        row: row with partition_id, total_rows and last_modified_time
    Returns:
        A dict with id, key, rows and modified, or None for the __UNPARTITIONED__ partition.
    """
    partition_id = row["partition_id"]
    if partition_id == __PARTITION_UNPARTITIONED:
        if (row["total_rows"] or 0) > 0:
            log.warning(
                f"{plan.key}: {row['total_rows']} rows are waiting in the "
                f"{__PARTITION_UNPARTITIONED} partition (streamed rows, or values outside the "
                "partition range)."
            )
            _note_skipped(
                "Rows are waiting in BigQuery's __UNPARTITIONED__ partition (streamed rows, or "
                "values outside the partition range). Rows after the newest partition are read, "
                "and older rows are not synced until BigQuery moves them into their partition",
                plan.key,
            )
        return None
    return {
        "id": partition_id,
        "key": None if partition_id == __PARTITION_NULL else int(partition_id),
        "rows": int(row["total_rows"] or 0),
        "modified": row["last_modified_time"],
    }


def _fetch_partitions(ctx, plan):
    """
    List the partitions of a table with their row counts and last modified times.
    Querying INFORMATION_SCHEMA.PARTITIONS needs the bigquery.jobs.create permission and scans
    only a small amount of metadata.
    Args:
        ctx: ClientContext
        plan: TablePlan of a column-partitioned table
    Returns:
        A tuple of (list of dicts with id, key, rows and modified, sorted by partition key, number
        of rows waiting in the __UNPARTITIONED__ partition). The NULL partition, if any, is
        returned with key None.
    """
    sql = (
        "SELECT partition_id, total_rows, last_modified_time "
        f"FROM `{plan.project}.{plan.dataset}`.INFORMATION_SCHEMA.PARTITIONS "
        "WHERE table_name = @table_name"
    )
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("table_name", "STRING", plan.stream)]
    )
    rows = _retry(
        lambda: list(
            ctx.bq_client.query(
                sql,
                job_config=job_config,
                location=_dataset_location(ctx, plan.project, plan.dataset),
            ).result(timeout=API_TIMEOUT_SECONDS)
        ),
        f"{plan.key}: listing partitions",
    )
    partitions = [p for p in (_parse_partition_row(plan, row) for row in rows) if p is not None]
    partitions.sort(key=lambda p: (p["key"] is not None, p["key"] or 0))
    unpartitioned_rows = sum(
        int(row["total_rows"] or 0)
        for row in rows
        if row["partition_id"] == __PARTITION_UNPARTITIONED
    )
    return partitions, unpartitioned_rows


def _parse_time(value):
    """
    Parse an ISO timestamp stored in state.
    Args:
        value: ISO 8601 string, or None
    Returns:
        A timezone-aware datetime, or None.
    """
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _changed(partition, start_point, seen):
    """
    Check whether BigQuery shows a partition as modified since the start point. A partition
    modified after the start point has changed. A partition modified shortly before it, within
    MODIFIED_MARGIN_SECONDS, is also read unless the previous sync already saw exactly this
    modification time, because a change can become visible slightly out of order. Without the
    record of what was seen, the partition that sets the high-water mark would be read again on
    every sync.
    Args:
        partition: dict with the partition id and its modification time
        start_point: the high-water mark of the last completed sync, or the time an unfinished
            initial load started at
        seen: dict of partition id to modification time (ISO text) that the previous sync saw
            within the margin before the start point
    Returns:
        True when the partition has to be read.
    """
    modified = partition["modified"]
    if modified is None:
        return False
    if modified > start_point:
        return True
    in_margin = modified > start_point - timedelta(seconds=MODIFIED_MARGIN_SECONDS)
    return in_margin and seen.get(partition["id"]) != modified.isoformat()


def _recent_modifications(partitions, mark):
    """
    Record the partitions modified within the margin before a high-water mark, with the exact
    modification times seen, so the next sync does not read them again unless they change.
    Args:
        partitions: list of partitions as returned by _fetch_partitions
        mark: the high-water mark, a timezone-aware datetime
    Returns:
        Dict of partition id to modification time as ISO text.
    """
    start = mark - timedelta(seconds=MODIFIED_MARGIN_SECONDS)
    return {
        p["id"]: p["modified"].isoformat()
        for p in partitions
        if p["modified"] is not None and p["modified"] > start
    }


def _select_partitions(partitions, watermark, start_point, seen, lookback, completed):
    """
    Decide which partitions to read in this sync.
    - No watermark: every partition.
    - Otherwise: every partition after the watermark, and every partition that BigQuery shows as
      modified since the start point, however old it is (see _changed). This is what picks up
      late-arriving and corrected rows. While an initial load is still unfinished, the start point
      is the time the load started at, so changes made between an interrupted run and its resume
      are found too.
    - Once a load has completed, the last `lookback` partitions up to the watermark are re-read in
      addition. The default is 0, so this is only an optional extra margin.
    Args:
        partitions: all partitions sorted by key, excluding the NULL partition
        watermark: highest partition key fully synced, or None
        start_point: high-water mark of the last completed sync, or the start of the initial load
        seen: partitions the previous sync saw within the margin before the start point
        lookback: number of already-synced partitions to re-read when the load has completed
        completed: True when an earlier sync of the table ran to the end
    Returns:
        List of selected partitions in key order.
    """
    if watermark is None:
        return list(partitions)
    keys = [p["key"] for p in partitions]
    # bisect_right gives the first partition after the watermark even if the watermark's own
    # partition no longer exists
    first_index = bisect_right(keys, watermark)
    if completed:
        first_index = max(0, first_index - lookback)
    return [
        p
        for index, p in enumerate(partitions)
        if index >= first_index or _changed(p, start_point, seen)
    ]


def _make_batches(plan, partitions, selected):
    """
    Group the selected partitions into batches. A batch only spans partitions that are next to each
    other in the table, so an old partition that was modified is read on its own instead of
    reading everything between it and the recent partitions.
    Args:
        plan: TablePlan
        partitions: all partitions sorted by key, excluding the NULL partition
        selected: partitions to read, in key order
    Returns:
        List of Batch instances.
    """
    position = {p["key"]: index for index, p in enumerate(partitions)}
    column = plan.partition_column
    column_type = plan.column_types[column]

    batches = []
    group = []
    group_rows = 0

    def flush():
        """Turn the partitions collected so far into one batch."""
        if not group:
            return
        lower, _ = _partition_bounds(plan, group[0]["id"])
        _, upper = _partition_bounds(plan, group[-1]["id"])
        restriction = [
            f"`{column}` >= {_literal(column_type, lower)}",
            f"`{column}` < {_literal(column_type, upper)}",
        ]
        ids = [p["id"] for p in group]
        batches.append(
            Batch(
                restriction=restriction,
                last_key=group[-1]["key"],
                label=f"partitions {ids[0]}..{ids[-1]} ({len(ids)})",
            )
        )

    for partition in selected:
        contiguous = group and position[partition["key"]] == position[group[-1]["key"]] + 1
        if group and (not contiguous or group_rows >= BATCH_TARGET_ROWS):
            flush()
            group, group_rows = [], 0
        group.append(partition)
        group_rows += partition["rows"]
    flush()
    return batches


def _convert_scalar(value, keep_bytes):
    """
    Convert a scalar value read from BigQuery into a type the Connector SDK can send. A float
    that is not finite becomes NULL, because it cannot be sent.
    Args:
        value: python value converted from an Arrow column
        keep_bytes: True for a BINARY column, which is sent as bytes
    Returns:
        The converted value, or the marker __NOT_SCALAR when the value is not a scalar type that
        needs conversion, for example a dict or a list.
    """
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.isoformat()
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, (date, dt_time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (bytes, bytearray)):
        if keep_bytes:
            return bytes(value)
        return base64.b64encode(bytes(value)).decode("ascii")
    if all(hasattr(value, attribute) for attribute in ("months", "days", "nanoseconds")):
        # An INTERVAL, which arrives as a (months, days, nanoseconds) tuple
        return {"months": value.months, "days": value.days, "nanoseconds": value.nanoseconds}
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    return __NOT_SCALAR


def _to_json_safe(value, keep_bytes=False):
    """
    Convert a value read from BigQuery into a type the Connector SDK can send.
    Args:
        value: python value converted from an Arrow column
        keep_bytes: True for a BINARY column, which is sent as bytes. Otherwise, and for bytes
            inside a STRUCT or ARRAY, bytes become Base64 text.
    Returns:
        A JSON-compatible value, or bytes when keep_bytes is set.
    """
    if type(value) in __PLAIN_TYPES:
        return value
    if value is None:
        return None
    converted = _convert_scalar(value, keep_bytes)
    if converted is not __NOT_SCALAR:
        return converted
    if isinstance(value, dict):
        return {key: _to_json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_json_safe(item) for item in value]
    return value


def _format_cursor(value):
    """
    Convert a cursor value to a string that can be stored in state.
    Args:
        value: date or datetime read from the cursor column
    Returns:
        ISO 8601 string.
    """
    if isinstance(value, datetime) and value.tzinfo is not None:
        return value.astimezone(timezone.utc).isoformat()
    return value.isoformat()


def _parse_cursor(plan, text):
    """
    Convert a cursor stored in state back to a value of the cursor column's type.
    Args:
        plan: TablePlan
        text: ISO 8601 string from state
    Returns:
        A date or datetime.
    """
    if plan.column_types[plan.cursor_column] == "DATE":
        return date.fromisoformat(text)
    return datetime.fromisoformat(text)


def _reset_buffer():
    """
    Start counting the buffer again after a checkpoint, which flushed every buffered row. Call it
    while holding __STATE_LOCK.
    """
    __BUFFER["values"] = 0
    __BUFFER["bytes"] = 0
    __BUFFER["since"] = time.monotonic()


def _flush_if_due(state, added, added_bytes):
    """
    Checkpoint the current, unchanged state when enough values or bytes are buffered, or when some
    values are buffered and a minute has passed since the last checkpoint. The SDK holds upserted
    rows in memory until a checkpoint, and one checkpoint flushes every table, so the counts are
    shared by all tables. The watermark in state is not moved here, so a failure still re-reads the
    whole batch.
    Args:
        state: shared state dictionary
        added: number of values (rows x columns) upserted since the previous call
        added_bytes: size in bytes of the Arrow data that those rows were made from
    """
    with __STATE_LOCK:
        __BUFFER["values"] += added
        __BUFFER["bytes"] += added_bytes
        buffered = __BUFFER["values"]
        waited = time.monotonic() - __BUFFER["since"]
        too_many_values = buffered >= FLUSH_MAX_VALUES
        too_many_bytes = __BUFFER["bytes"] >= FLUSH_MAX_BYTES
        interval_due = buffered >= FLUSH_MIN_VALUES and waited >= FLUSH_INTERVAL_SECONDS
        if too_many_values or too_many_bytes or interval_due:
            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
            # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
            op.checkpoint(state=state)
            _reset_buffer()


def _highest_cursor(plan, columns):
    """
    Find the highest cursor value in a page, and how many rows have no cursor value.
    Args:
        plan: TablePlan
        columns: list of the page's columns as python lists, in the order of plan.columns
    Returns:
        A tuple of (highest cursor value or None, number of rows with a NULL cursor value).
    """
    if not plan.cursor_column:
        return None, 0
    values = columns[plan.columns.index(plan.cursor_column)]
    present = [v for v in values if v is not None]
    return (max(present) if present else None), len(values) - len(present)


def _parse_json_columns(plan, rows):
    """
    Turn the text of every JSON column into the value it holds, so that it is sent as JSON.
    Args:
        plan: TablePlan
        rows: list of row dicts, changed in place
    """
    for column in plan.json_columns:
        for row in rows:
            if isinstance(row.get(column), str):
                row[column] = json.loads(row[column])


def _page_rows(plan, batch):
    """
    Turn an Arrow page into the rows to upsert. Cells that are NULL are left out: for an upsert,
    Fivetran sets every column that is missing from the data to NULL, so the result in the
    destination is the same, and fewer cells are sent. Sending a cell costs the same whatever its
    value, and many tables are mostly NULL, so this is the biggest saving in a sync.
    Args:
        plan: TablePlan
        batch: pyarrow RecordBatch with the columns of the plan
    Returns:
        A tuple of (list of row dicts, highest cursor value in the page or None, number of rows
        with a NULL cursor value).
    """
    names = plan.columns
    columns = [batch.column(name).to_pylist() for name in names]
    binary = {name for name in names if plan.explicit_columns.get(name) == "BINARY"}
    rows = [
        {
            name: (value if type(value) in __PLAIN_TYPES else _to_json_safe(value, name in binary))
            for name, value in zip(names, values)
            if value is not None
        }
        for values in zip(*columns)
    ]
    _parse_json_columns(plan, rows)
    highest, null_cursor_rows = _highest_cursor(plan, columns)
    return rows, highest, null_cursor_rows


def _read_stream(ctx, plan, state, session, stream_name, progress):
    """
    Read one Storage Read API stream and upsert every row.
    Args:
        ctx: ClientContext
        plan: TablePlan
        state: shared state dictionary
        session: the ReadSession the stream belongs to
        stream_name: name of the stream to read
        progress: TableProgress of the table, which counts the rows and logs the progress
    Returns:
        A tuple of (rows_upserted, highest_cursor_value_seen).
    """
    reader = ctx.read_client.read_rows(stream_name)
    count = 0
    highest = None
    for page in reader.rows(session).pages:
        arrow_page = page.to_arrow()
        rows, page_highest, null_cursor_rows = _page_rows(plan, arrow_page)
        if page_highest is not None and (highest is None or page_highest > highest):
            highest = page_highest
        if null_cursor_rows:
            progress.add_null_cursor(null_cursor_rows)
        sent = 0
        for row in rows:
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            op.upsert(table=plan.stream, schema=plan.dataset, data=row)
            sent += len(row)
        count += len(rows)
        # Count the cells sent, a weight per row, and the page size, which the SDK holds in memory
        _flush_if_due(state, sent + ROW_WEIGHT_CELLS * len(rows), arrow_page.nbytes)
        progress.add_rows(len(rows))
    return count, highest


def _create_read_session(ctx, plan, restriction):
    """
    Create a Storage Read API session. Column selection and the row restriction are applied by
    BigQuery, and partition pruning happens when the restriction filters on the partition column.
    Args:
        ctx: ClientContext
        plan: TablePlan
        restriction: list of row restriction parts, joined with AND
    Returns:
        The ReadSession.
    """
    parts = list(restriction)
    if plan.filter_condition:
        parts.append(_build_filter_restriction(plan.filter_condition, plan.column_types))
    read_options = bq_types.ReadSession.TableReadOptions(selected_fields=plan.columns)
    if parts:
        read_options.row_restriction = " AND ".join(parts)

    request_session = bq_types.ReadSession(
        table=f"projects/{plan.project}/datasets/{plan.dataset}/tables/{plan.stream}",
        data_format=bq_types.DataFormat.ARROW,
        read_options=read_options,
    )
    return _retry(
        lambda: ctx.read_client.create_read_session(
            parent=f"projects/{ctx.billing_project}",
            read_session=request_session,
            max_stream_count=READ_STREAMS_PER_TABLE,
            timeout=API_TIMEOUT_SECONDS,
        ),
        f"{plan.key}: creating read session",
    )


def _read_batch(ctx, plan, state, restriction, label, progress):
    """
    Read all rows that match the restriction using parallel Storage Read API streams.
    Args:
        ctx: ClientContext
        plan: TablePlan
        state: shared state dictionary
        restriction: list of row restriction parts, joined with AND
        label: text used in logs
        progress: TableProgress of the table
    Returns:
        A tuple of (rows_upserted, highest_cursor_value_seen).
    """
    session = _create_read_session(ctx, plan, restriction)
    progress.add_scan(session.estimated_total_bytes_scanned)
    log.debug(
        f"{plan.key}: reading {label} with {len(session.streams)} stream(s), "
        f"estimated scan {session.estimated_total_bytes_scanned:,} bytes"
    )
    if not session.streams:
        return 0, None

    with ThreadPoolExecutor(max_workers=len(session.streams)) as executor:
        futures = [
            executor.submit(_read_stream, ctx, plan, state, session, stream.name, progress)
            for stream in session.streams
        ]
        results = [future.result() for future in futures]

    total = sum(count for count, _ in results)
    highs = [high for _, high in results if high is not None]
    return total, (max(highs) if highs else None)


def _save_state(state, stream, table_state):
    """
    Store the state of one table and checkpoint, at most once per FLUSH_INTERVAL_SECONDS. Many small
    tables or batches would otherwise checkpoint far more than once a minute. The state is always
    updated, so a checkpoint that is skipped here is saved by the next checkpoint, which also
    flushes the rows upserted before it. At worst a failure re-reads about a minute of tables,
    which is safe because upserts are idempotent. Worker threads share one state dictionary, so
    the update and the checkpoint are done under a lock.
    Args:
        state: shared state dictionary
        stream: table name
        table_state: new state for the table
    """
    with __STATE_LOCK:
        state[stream] = table_state
        if time.monotonic() - __BUFFER["since"] < FLUSH_INTERVAL_SECONDS:
            return
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
        # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
        op.checkpoint(state=state)
        # The checkpoint flushed every buffered row, so the shared count starts again
        _reset_buffer()


def save_final_state(state):
    """
    Checkpoint the state once more at the end of the sync, because _save_state can skip a
    checkpoint. Call this once, after every table has finished.
    Args:
        state: shared state dictionary
    """
    with __STATE_LOCK:
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
        # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
        op.checkpoint(state=state)
        _reset_buffer()


def _read_without_partition_listing(ctx, plan, state, previous, sync_start, error, progress):
    """
    Read a partitioned table in one pass, in full, because its partitions could not be listed. This
    is used when listing fails with a permission, not-found or rejected-key error, which retrying
    cannot fix, so every sync of the table reads all of it until the listing works again. The
    previous watermark is kept so a later sync with the permission continues incrementally.
    Args:
        ctx: ClientContext
        plan: TablePlan
        state: shared state dictionary
        previous: the table's state from before this sync
        sync_start: time the sync of this table started
        error: the error raised when listing the partitions
        progress: TableProgress of the table
    """
    log.warning(
        f"{plan.key}: could not list partitions ({error}); reading the whole table. "
        "Grant bigquery.jobs.create to enable partition-aware syncs."
    )
    _note_skipped(
        "Partitions could not be listed, so the whole table was read instead of only new "
        "partitions (grant bigquery.jobs.create)",
        plan.key,
    )
    progress.expect(plan.num_rows, exact=not plan.filter_condition)
    _read_batch(ctx, plan, state, [], "whole table", progress)
    # No listing means no BigQuery time, so this machine's clock marks the point that was read
    _save_state(
        state,
        plan.key,
        _partition_state(
            plan, previous.get("watermark"), sync_start, None, {}, previous.get("streaming_seen")
        ),
    )


def _comparable(column_type, value):
    """
    Turn a filter value into a value that can be compared with partition bounds.
    Args:
        column_type: BigQuery type of the filtered column
        value: the value, possibly already resolved from a relative value such as today-90d
    Returns:
        An int, a date, or a naive UTC datetime.
    Raises:
        ValueError: for a column type that cannot be compared with partition bounds.
    """
    if column_type == "INTEGER":
        return int(value)
    if column_type == "DATE":
        if isinstance(value, datetime):
            return value.date()
        return value if isinstance(value, date) else date.fromisoformat(str(value))
    if column_type not in ("DATETIME", "TIMESTAMP"):
        raise ValueError(f"Partitions cannot be compared on a {column_type} column.")
    parsed = value
    if not isinstance(parsed, datetime):
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed.astimezone(timezone.utc).replace(tzinfo=None) if parsed.tzinfo else parsed


def _may_match(operator, lower, upper, value):
    """
    Check whether some value in the half-open range [lower, upper) can satisfy a condition. The
    check is conservative: it only says no when no value can match.
    Args:
        operator: one of >, >=, <, <=, =, !=
        lower: start of the partition, included
        upper: end of the partition, excluded
        value: the value the column is compared with
    Returns:
        False only when the partition cannot contain a matching row.
    """
    if operator in (">", ">="):
        return upper > value
    if operator == "<":
        return lower < value
    if operator == "<=":
        return lower <= value
    if operator == "=":
        return lower <= value < upper
    return True


def _partition_tests(plan):
    """
    Find the conditions of the table filter that are on the partition column, which can rule out
    whole partitions. For an ingestion-time table, _PARTITIONDATE counts as the partition column.
    Args:
        plan: TablePlan
    Returns:
        List of (column type, operator, comparable value), possibly empty.
    """
    if not plan.filter_condition or not plan.partition_column:
        return []
    names = {plan.partition_column}
    if plan.partition_column == __INGESTION_TIME_COLUMN:
        names.add("_PARTITIONDATE")
    tests = []
    for column, operator, value in _parse_filter(plan.filter_condition):
        if column in names:
            column_type = plan.column_types[column]
            try:
                resolved = _comparable(column_type, _resolve_filter_value(column_type, value))
            except (ValueError, TypeError):
                continue
            tests.append((column_type, operator, resolved))
    return tests


def _partition_passes(plan, partition, tests):
    """
    Check a partition against the conditions of the filter that are on the partition column.
    Args:
        plan: TablePlan
        partition: dict with the partition id
        tests: result of _partition_tests
    Returns:
        False when the filter rules out every row of the partition.
    """
    lower, upper = _partition_bounds(plan, partition["id"])
    for column_type, operator, value in tests:
        start, end = lower, upper
        if column_type == "DATE" and isinstance(lower, datetime):
            # Hourly or ingestion-time bounds, seen as the days they cover
            start, end = lower.date(), (upper - timedelta(microseconds=1)).date() + timedelta(
                days=1
            )
        try:
            if not _may_match(operator, start, end, value):
                return False
        except TypeError:
            continue
    return True


def _explain_selection(selected, watermark, start_point, seen):
    """
    Count why the selected partitions are read, for the log line of the sync.
    Args:
        selected: partitions that are read, in key order
        watermark: highest partition key fully synced, or None
        start_point: high-water mark of the last completed sync, or the start of the initial load
        seen: partitions the previous sync saw within the margin before the start point
    Returns:
        Text such as "new=2 modified=1 lookback=1", or "reason=no_saved_progress".
    """
    if watermark is None:
        return "reason=no_saved_progress"
    older = [p for p in selected if p["key"] <= watermark]
    modified = sum(1 for p in older if _changed(p, start_point, seen))
    return f"new={len(selected) - len(older)} modified={modified} lookback={len(older) - modified}"


def _plan_partition_batches(
    plan, partitions, watermark, start_point, seen, completed, lookback, streaming=False
):
    """
    Decide which partitions to read and group them into batches. Partitions that the table filter
    rules out are left out, so no read session is made for them. Rows with a NULL partition column
    are read first, but only until a load has completed or when they changed, and never when the
    filter has a condition on the partition column, because no comparison matches NULL. While rows
    are streamed into the table, a last batch reads the rows that are not in a listed partition.
    Args:
        plan: TablePlan
        partitions: all partitions of the table, including the NULL partition
        watermark: highest partition key fully synced, or None
        start_point: high-water mark of the last completed sync, or the start of the initial load
        seen: partitions the previous sync saw within the margin before the start point
        completed: True when an earlier sync of the table ran to the end
        lookback: number of already-synced partitions to read again, once a load has completed
        streaming: True when rows are, or recently were, streamed into the table
    Returns:
        A tuple of (list of Batch instances in the order they should be read, number of rows in
        the selected partitions).
    """
    tests = _partition_tests(plan)
    dated = [p for p in partitions if p["key"] is not None]
    ordered = [p for p in dated if _partition_passes(plan, p, tests)] if tests else dated
    null_partition = None if tests else next((p for p in partitions if p["key"] is None), None)
    selected = _select_partitions(ordered, watermark, start_point, seen, lookback, completed)
    batches = _make_batches(plan, ordered, selected)
    expected_rows = sum(p["rows"] for p in selected)

    if null_partition is not None and (
        not completed or _changed(null_partition, start_point, seen)
    ):
        batches.insert(
            0,
            Batch(
                restriction=[f"`{plan.partition_column}` IS NULL"],
                last_key=None,
                label="NULL partition",
            ),
        )
        expected_rows += null_partition["rows"]
    if streaming:
        batches.append(_streaming_batch(plan, dated))
    reason = _explain_selection(selected, watermark, start_point, seen)
    outside = f" outside_filter={len(dated) - len(ordered):,}" if tests else ""
    tail = " streaming_tail=yes" if streaming else ""
    log.info(
        f"{plan.key}: plan partitions={len(dated):,}{outside} read={len(selected):,} {reason}"
        f"{tail} rows~{expected_rows:,} batches={len(batches)}"
    )
    return batches, expected_rows


def _iso(moment):
    """
    Format a time for state.
    Args:
        moment: a datetime, or None
    Returns:
        ISO 8601 text, or None.
    """
    return moment.isoformat() if moment is not None else None


def _partition_state(plan, watermark, high_water, baseline, seen, streaming_seen):
    """
    Build the state saved for a partitioned table.
    Args:
        plan: TablePlan
        watermark: highest partition key fully synced, or None
        high_water: latest modification time BigQuery reported when the last sync completed, or
            None while no sync has completed
        baseline: latest modification time BigQuery reported when the current initial load
            started, or None once a sync has completed
        seen: partitions modified within the margin before the high-water mark or the baseline,
            with the modification times that were seen
        streaming_seen: ISO time a streaming buffer was last seen on the table, or None
    Returns:
        A dict of JSON-compatible values.
    """
    return {
        "watermark": watermark,
        "high_water": _iso(high_water),
        "baseline": _iso(baseline),
        "recent": seen,
        "streaming_seen": streaming_seen,
        "fingerprint": plan.fingerprint,
        "schema": plan.schema_signature,
    }


def _is_streaming(plan, saved, now, unpartitioned_rows=0):
    """
    Decide whether rows are, or recently were, streamed into the table. That is the case while the
    table has a streaming buffer, while rows wait in the __UNPARTITIONED__ partition, and for
    STREAMING_GRACE_SECONDS after either was last seen.
    Args:
        plan: TablePlan
        saved: ISO time streaming was last seen on the table, from state, or None
        now: time this sync started
        unpartitioned_rows: number of rows waiting in the __UNPARTITIONED__ partition
    Returns:
        True when streamed rows may not show in the partition metadata yet.
    """
    if plan.streaming_buffer or unpartitioned_rows > 0:
        return True
    seen = _parse_time(saved)
    return seen is not None and (now - seen).total_seconds() < STREAMING_GRACE_SECONDS


def _streaming_lookback(plan, saved, now, unpartitioned_rows=0):
    """
    Decide how many of the newest partitions to read again because rows are streamed into the
    table. Streamed rows can be read at once, but BigQuery's partition metadata does not show them
    until they leave the streaming buffer, so a modified partition is not noticed in that time.
    Args:
        plan: TablePlan
        saved: ISO time streaming was last seen on the table, from state, or None
        now: time this sync started
        unpartitioned_rows: number of rows waiting in the __UNPARTITIONED__ partition
    Returns:
        A tuple of (partitions to read again, ISO time to save as the last time streaming was seen).
    """
    seen_now = plan.streaming_buffer or unpartitioned_rows > 0
    lookback = plan.lookback
    if _is_streaming(plan, saved, now, unpartitioned_rows):
        lookback = max(plan.lookback, STREAMING_MIN_LOOKBACK)
    return lookback, (now.isoformat() if seen_now else saved)


def _streaming_batch(plan, dated):
    """
    Build the batch that reads rows that BigQuery has not put in a listed partition yet. Streamed
    rows can be read straight away, but they are not in the partition listing until BigQuery moves
    them. For a column-partitioned table they are found by their value, after the newest listed
    partition. For an ingestion-time table they have a NULL _PARTITIONTIME until then.
    Args:
        plan: TablePlan with a partition column
        dated: the listed partitions that have a key, sorted by key
    Returns:
        A Batch that does not move the watermark.
    """
    column = plan.partition_column
    if column == __INGESTION_TIME_COLUMN:
        return Batch([f"`{column}` IS NULL"], None, "rows still in the streaming buffer")
    if not dated:
        return Batch([], None, "rows that are not in a listed partition")
    _, upper = _partition_bounds(plan, dated[-1]["id"])
    start = _literal(plan.column_types[column], upper)
    return Batch([f"`{column}` >= {start}"], None, "rows after the newest partition")


def _latest_modification(partitions):
    """
    Find the latest modification time among the listed partitions, which is the high-water mark.
    It comes from BigQuery's own clock, so it can be compared with the modification times of the
    next sync without any difference between clocks.
    Args:
        partitions: list of partitions as returned by _fetch_partitions
    Returns:
        A timezone-aware datetime, or None when no partition has a modification time.
    """
    times = [p["modified"] for p in partitions if p["modified"] is not None]
    return max(times) if times else None


def _read_batches(ctx, plan, state, progress, batches, watermark, carry):
    """
    Read the batches of a partitioned table one by one. The watermark moves forward and is saved in
    state after every batch, with a checkpoint at most once per FLUSH_INTERVAL_SECONDS, so an
    interrupted sync resumes after the last batch that was checkpointed.
    Args:
        ctx: ClientContext
        plan: TablePlan with a partition column
        state: shared state dictionary
        progress: TableProgress of the table
        batches: list of Batch instances
        watermark: highest partition key fully synced so far, or None
        carry: tuple of (high_water, baseline, seen, streaming_seen) saved unchanged until the
            whole table has finished
    Returns:
        The watermark after the last batch.
    """
    for number, batch in enumerate(batches, start=1):
        rows, _ = _read_batch(ctx, plan, state, batch.restriction, batch.label, progress)
        if batch.last_key is not None and (watermark is None or batch.last_key > watermark):
            watermark = batch.last_key
        # The high-water mark only moves when the whole table has finished, so an interrupted
        # sync looks for modified partitions from the same point again on the next run
        _save_state(state, plan.key, _partition_state(plan, watermark, *carry))
        progress.batch_done(number, len(batches), rows, batch.label)
    return watermark


def _sync_partitioned(ctx, plan, state, progress):
    """
    Sync a column-partitioned table one batch of partitions at a time.
    Args:
        ctx: ClientContext
        plan: TablePlan with a partition column
        state: shared state dictionary
        progress: TableProgress of the table
    """
    sync_start = datetime.now(timezone.utc)
    previous = dict(state.get(plan.key) or {})
    watermark = previous.get("watermark")
    # last_sync_time is the name that earlier versions used for the high-water mark
    high_water = _parse_time(previous.get("high_water") or previous.get("last_sync_time"))
    baseline = _parse_time(previous.get("baseline"))
    seen = dict(previous.get("recent") or {})
    if plan.strategy == __STRATEGY_FULL:
        watermark, high_water, baseline, seen = None, None, None, {}

    try:
        partitions, unpartitioned_rows = _fetch_partitions(ctx, plan)
    except RuntimeError as error:
        # _retry raises RuntimeError only for errors that retrying cannot fix. Transient errors are
        # not caught here, so they fail the sync instead of turning it into a full table read.
        _read_without_partition_listing(ctx, plan, state, previous, sync_start, error, progress)
        return

    # The latest modification BigQuery shows now. Anything written after this has a later time, so
    # it is read by the next sync. It is the high-water mark once this sync completes.
    listing_mark = _latest_modification(partitions) or sync_start
    if watermark is not None and high_water is None and baseline is None:
        # An unfinished load whose start was not recorded, so there is no way to tell which of the
        # partitions it already read have changed since. Reading everything again is the safe choice.
        log.warning(f"{plan.key}: the saved progress has no start point, reading the table again")
        _note_skipped("Saved progress could not be verified, so tables were read again", plan.key)
        watermark = None
    completed = high_water is not None
    if not completed and baseline is None:
        baseline = listing_mark
        seen = _recent_modifications(partitions, listing_mark)
    start_point = high_water if completed else baseline

    saved_streaming = previous.get("streaming_seen")
    streaming = _is_streaming(plan, saved_streaming, sync_start, unpartitioned_rows)
    lookback, streaming_seen = _streaming_lookback(
        plan, saved_streaming, sync_start, unpartitioned_rows
    )
    if streaming:
        log.info(
            f"{plan.key}: streaming detected lookback={lookback} unlisted_rows=read "
            f"grace={STREAMING_GRACE_SECONDS}s"
        )
        _note_skipped(
            "Rows are streamed into these tables, and BigQuery's partition metadata can lag "
            "behind them by an hour or more, so their newest partition and the rows that are not "
            "in a listed partition yet are read on every sync",
            plan.key,
        )

    batches, expected_rows = _plan_partition_batches(
        plan, partitions, watermark, start_point, seen, completed, lookback, streaming
    )
    progress.expect(expected_rows, exact=not plan.filter_condition)
    carry = (high_water, None if completed else baseline, seen, streaming_seen)
    watermark = _read_batches(ctx, plan, state, progress, batches, watermark, carry)
    finished = _partition_state(
        plan,
        watermark,
        listing_mark,
        None,
        _recent_modifications(partitions, listing_mark),
        streaming_seen,
    )
    _save_state(state, plan.key, finished)


def _cursor_start(plan, text):
    """
    Work out where a cursor-based read starts: the saved cursor value minus the margin, which is
    whole days (at least one) for a DATE column.
    Args:
        plan: TablePlan
        text: ISO 8601 cursor value from state
    Returns:
        A date or datetime.
    """
    value = _parse_cursor(plan, text)
    margin = plan.cursor_margin_seconds
    if plan.column_types[plan.cursor_column] == "DATE":
        return value - timedelta(days=max(1, math.ceil(margin / 86400)))
    return value - timedelta(seconds=margin)


def _sync_cursor_or_full(ctx, plan, state, progress):
    """
    Sync a table that is not partitioned on a column, either with a cursor column or in full.
    Streams are not ordered, so the cursor only moves forward once the whole read has finished.
    Args:
        ctx: ClientContext
        plan: TablePlan
        state: shared state dictionary
        progress: TableProgress of the table
    """
    restriction = []
    cursor_text = (state.get(plan.key) or {}).get("cursor")
    if plan.strategy == __STRATEGY_INCREMENTAL and cursor_text:
        cursor_type = plan.column_types[plan.cursor_column]
        column = plan.cursor_column
        start = _literal(cursor_type, _cursor_start(plan, cursor_text))
        # The read starts a margin before the last cursor value, to find rows that were committed
        # late. A comparison never matches NULL, so rows with no cursor value are always read.
        restriction.append(f"(`{column}` >= {start} OR `{column}` IS NULL)")

    # The table size is exact for a full read. With a cursor or a filter it is an upper bound.
    progress.expect(plan.num_rows, exact=not restriction and not plan.filter_condition)
    _, highest = _read_batch(
        ctx,
        plan,
        state,
        restriction,
        "incremental" if restriction else "full table",
        progress,
    )
    if progress.null_cursor_rows:
        _note_skipped(
            "Rows with no value in the cursor column cannot be found by it, so they are read "
            "again on every sync",
            f"{plan.key} ({progress.null_cursor_rows:,} rows)",
        )
    new_state = {
        "cursor": _format_cursor(highest) if highest is not None else cursor_text,
        "fingerprint": plan.fingerprint,
        "schema": plan.schema_signature,
    }
    _save_state(state, plan.key, new_state)


def _sync_mode(plan, state):
    """
    Describe what kind of sync a table gets in this run, from its strategy and saved state.
    Args:
        plan: TablePlan
        state: shared state dictionary
    Returns:
        "full reload", "initial load", "resuming initial load" or "incremental".
    """
    previous = state.get(plan.key) or {}
    if plan.strategy == __STRATEGY_FULL:
        return "full reload"
    if plan.partition_column:
        if previous.get("high_water") or previous.get("last_sync_time"):
            return "incremental"
        return "resuming initial load" if previous.get("watermark") is not None else "initial load"
    return "incremental" if previous.get("cursor") else "initial load"


def _schema_changes(saved, current):
    """
    Compare the columns saved in state with the columns of the table now.
    Args:
        saved: dict of column to type text that was saved in state
        current: dict of column to type text of the table now
    Returns:
        A tuple of three sorted lists: added columns, columns whose type changed, and removed
        columns.
    """
    added = sorted(column for column in current if column not in saved)
    changed = sorted(c for c in current if c in saved and saved[c] != current[c])
    removed = sorted(column for column in saved if column not in current)
    return added, changed, removed


def _describe_schema_changes(added, changed, removed):
    """
    Describe schema changes for a log line and a dashboard warning, naming at most five columns of
    each kind.
    Args:
        added: list of added columns
        changed: list of columns whose type changed
        removed: list of removed columns
    Returns:
        Text such as "added: discount, region; type changed: amount".
    """
    parts = []
    for label, columns in (("added", added), ("type changed", changed), ("removed", removed)):
        if columns:
            more = ", ..." if len(columns) > 5 else ""
            parts.append(f"{label}: {', '.join(columns[:5])}{more}")
    return "; ".join(parts)


def _drop_state(plan, state):
    """
    Forget the saved progress of a table, so that it is read again from the start.
    Args:
        plan: TablePlan
        state: shared state dictionary
    """
    with __STATE_LOCK:
        state.pop(plan.key, None)


def _reset_if_changed(plan, state):
    """
    Drop the saved progress of a table when it no longer applies, so the table is read again from
    the start. That is the case when the settings it was made with changed (the partitioning, key,
    filter, column options or resync_token), and, unless on_schema_change is keep, when a column
    was added or changed type in the source. Removed columns never cause a re-read. State saved
    before fingerprints or schemas were recorded has none, and is kept.
    Args:
        plan: TablePlan
        state: shared state dictionary
    Returns:
        True when the saved progress was dropped.
    """
    previous = state.get(plan.key) or {}
    return _drop_if_settings_changed(plan, state, previous) or _apply_schema_change(
        plan, state, previous
    )


def _drop_if_settings_changed(plan, state, previous):
    """
    Drop the saved progress of a table when the settings it was made with changed, which the
    fingerprint shows. State saved before fingerprints were recorded has none, and is kept.
    Args:
        plan: TablePlan
        state: shared state dictionary
        previous: the table's saved state
    Returns:
        True when the saved progress was dropped.
    """
    saved_fingerprint = previous.get("fingerprint")
    if saved_fingerprint is None or saved_fingerprint == plan.fingerprint:
        return False
    log.warning(
        f"{plan.key}: the partitioning, keys, filter, column options or resync_token changed "
        "since the last sync, so the saved progress is dropped and the table is read again "
        "from the start"
    )
    _note_skipped("Table settings changed, so tables were read again from the start", plan.key)
    _drop_state(plan, state)
    return True


def _apply_schema_change(plan, state, previous):
    """
    Compare the columns saved in state with the table now, and drop the saved progress when a
    column was added or changed type and on_schema_change is resync. Otherwise report the change
    once and remember the new schema. State saved before schemas were recorded has none, and is
    kept.
    Args:
        plan: TablePlan
        state: shared state dictionary
        previous: the table's saved state
    Returns:
        True when the saved progress was dropped.
    """
    saved_schema = previous.get("schema")
    if saved_schema is None:
        return False
    added, changed, removed = _schema_changes(saved_schema, plan.schema_signature)
    if not (added or changed or removed):
        return False
    description = _describe_schema_changes(added, changed, removed)
    if (added or changed) and plan.on_schema_change == "resync":
        log.warning(
            f"{plan.key}: the source schema changed ({description}), so the saved progress is "
            "dropped and the table is read again from the start"
        )
        _note_skipped(
            "The source schema changed, so tables were read again from the start",
            f"{plan.key} ({description})",
        )
        _drop_state(plan, state)
        return True
    if added or changed:
        log.warning(
            f"{plan.key}: the source schema changed ({description}), and on_schema_change is "
            "keep, so rows that were already synced were not read again"
        )
        _note_skipped(
            "The source schema changed and on_schema_change is keep, so existing rows were not "
            "read again and have no value for added columns",
            f"{plan.key} ({description})",
        )
    else:
        log.info(f"{plan.key}: columns were removed from the source ({description}); kept")
    # Remember the new schema, so that the change is only reported once
    with __STATE_LOCK:
        state[plan.key] = {**previous, "schema": dict(plan.schema_signature)}
    return False


def sync_table(ctx, plan, state, run):
    """
    Sync one table with the strategy chosen in its plan, and log when it starts, completes or fails.
    Args:
        ctx: ClientContext
        plan: TablePlan
        state: shared state dictionary
        run: RunProgress of this sync
    Returns:
        The name of the table that was synced.
    """
    if _reset_if_changed(plan, state) and plan.truncate_on_resync:
        # A deferred truncate marks the rows that are not sent again as deleted, but only after
        # this sync succeeds, so a failed sync deletes nothing
        op.truncate(table=plan.stream, schema=plan.dataset, deferred=True)
    progress = run.start_table(plan, _sync_mode(plan, state))
    try:
        if plan.partition_column:
            _sync_partitioned(ctx, plan, state, progress)
        else:
            _sync_cursor_or_full(ctx, plan, state, progress)
    except Exception as error:
        run.fail_table(progress, error)
        raise
    run.finish_table(progress)
    return plan.key
