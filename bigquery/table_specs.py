"""
This file lists the tables to sync (TABLE_SPECS) and holds the constants that control how they are
read. Every option is explained in the README, in the Table specifications section.
"""

# Candidate cursor columns, in order of preference, for tables that are not column-partitioned.
# Only columns that change when a row changes belong here, so a creation date is never inferred.
PREFERRED_CURSOR_COLUMN_NAMES = [
    "_fivetran_synced",
    "system_modstamp",
    "last_modified_date",
    "updated_at",
    "last_updated",
    "last_modified",
    "modified_at",
    "updated_on",
    "modified_on",
    "update_time",
    "updated",
    "modified_date",
    "updated_date",
    "last_update_date",
    "last_modified_time",
    "updated_timestamp",
]

# BigQuery types that can be a cursor column
CURSOR_COLUMN_TYPES = {"TIMESTAMP", "DATETIME", "DATE"}

# Tables synced at the same time, and Storage Read API streams per table batch
MAX_PARALLEL_TABLES = 2
READ_STREAMS_PER_TABLE = 4

# Target rows per batch of partitions. Progress is saved after every batch.
BATCH_TARGET_ROWS = 5000000

# Memory: the connector checkpoints when this many values, or this many bytes of Arrow data, are
# buffered, or after the interval once the minimum is buffered. Lower the limits if a sync runs
# close to the 4 GB memory limit.
FLUSH_MAX_VALUES = 4000000
FLUSH_MAX_BYTES = 256 * 1024 * 1024
FLUSH_MIN_VALUES = 500000
FLUSH_INTERVAL_SECONDS = 60

# Extra cells that each row counts for, because a row costs memory beyond its cells
ROW_WEIGHT_CELLS = 24

# Optional lookback: the newest already-synced partitions that are re-read on every sync. Modified
# partitions are always re-read, so 0 is the default.
LOOKBACK_DAYS = 0
LOOKBACK_PARTITIONS = 0

# Streamed tables: while a table has a streaming buffer or rows waiting in __UNPARTITIONED__, and
# for this many seconds after, at least this many of the newest partitions are re-read, and the
# rows that are not in a listed partition yet are read. Google documents up to 90 minutes.
STREAMING_GRACE_SECONDS = 7200
STREAMING_MIN_LOOKBACK = 1

# Partitions modified within this many seconds before the last sync's high-water mark are checked
# again, because changes can become visible slightly out of order.
MODIFIED_MARGIN_SECONDS = 600

# What to do when a column is added or changes type in the source: "resync" or "keep"
SCHEMA_CHANGE_DEFAULT = "resync"

# How far before the last cursor value a cursor-based read starts, in seconds (whole days for DATE)
CURSOR_MARGIN_SECONDS = 3600

# Worker threads that read the metadata of the tables
MAX_DISCOVERY_WORKERS = 8

# Size at which a table that is read in full on every sync triggers a dashboard warning
FULL_REFRESH_WARNING_BYTES = 100000000000

# How often a long running table logs its progress, in seconds
PROGRESS_LOG_INTERVAL_SECONDS = 60

# Retries of transient errors: attempts in total, first delay and delay cap in seconds
MAX_RETRY_ATTEMPTS = 4
RETRY_BASE_DELAY_SECONDS = 5
RETRY_MAX_DELAY_SECONDS = 60

# Timeout of metadata calls, partition queries and read session creation, in seconds
API_TIMEOUT_SECONDS = 120

# Timeout of the single call that the connection test makes, in seconds
SETUP_TEST_TIMEOUT_SECONDS = 15

# The tables to sync: one entry per table, written as "dataset.table" or "project.dataset.table".
# Every table needs a primary key, declared in BigQuery or set with "primary_keys". All options are
# described in the README, in the Table specifications section. For example:
#   "analytics.events": {"primary_keys": ["event_id"], "filter": "event_date >= today-90d"},
TABLE_SPECS = {}
