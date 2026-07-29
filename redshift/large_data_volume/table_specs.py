"""
This file contains table specifications and constants for Redshift connector.
Each table specification includes details such as table name, primary keys,
replication strategy, replication key, and columns to include or exclude.
You can modify the lists to add or change table configurations as needed.
"""

# Preferred timestamp column names for inferring replication keys
# If your tables have any of these column names, they will be prioritized when automatically selecting a replication key
# You can add or modify these names based on your database schema conventions
PREFERRED_TS_COLUMN_NAMES = [
    "updated_at",
    "last_updated",
    "last_update",
    "last_modified",
    "modified_at",
    "modified_on",
    "updated_on",
    "update_time",
    "updated",
]

# Set of Redshift data types that represent timestamps or dates
# These are used to identify potential replication key columns if not explicitly specified
# This is only used when replication_key is not set in table spec and strategy is INCREMENTAL
TIMESTAMP_TYPE_NAMES = {
    "timestamp",
    "timestamp without time zone",
    "timestamp with time zone",
    "timestamptz",
    "date",
}

# Number of rows after which to checkpoint progress
# This ensures that the connector can resume from the last successful sync in case of interruptions
# Adjust this value based on your data volume and performance considerations
CHECKPOINT_EVERY_ROWS = 50000

# Chunk size for chunked cursor processing
# When chunking is enabled, large tables are processed in chunks to avoid cursor size limits
# Each chunk will contain this many rows (based on replication_key ordering)
# Adjust this value based on your table size and memory constraints
CHUNK_SIZE = 10000

# List of table specifications for the Redshift connector
# Each dictionary in the list defines a table and its sync configuration
# You can modify this list to add or change table configurations as needed
# Each table spec includes:
# - name: The name of the table in the format "schema.table"
# - primary_keys: List of primary key columns for the table. If None, the connector will attempt to fetch them from the source database.
# - strategy: Replication strategy, either "FULL" or "INCREMENTAL"
# - replication_key: Column used for incremental replication (if applicable).
# - include: List of columns to include in the sync (empty list means all columns)
# - exclude: List of columns to exclude from the sync (empty list means no exclusions)
# - use_chunking: (Optional) Boolean to enable/disable chunking for this specific table.
#                 If not specified, defaults to False
#                 Note: Chunking is only applicable for tables with INCREMENTAL strategy and replication_key.
# - column_types: (Optional) Dict mapping column names to explicit Fivetran type strings.
#                 Use this to declare columns that contain only NULL values during a sync, which
#                 Fivetran cannot type-infer automatically. Declared columns are merged with
#                 auto-detected special types (date, timestamp, super); auto-detected types win
#                 on conflict. If omitted or empty, no additional columns are declared.
# - filter: (Optional) Dict with keys "column", "operator", and "value" to apply a static
#           WHERE condition to every sync of this table.
#           The filter is ANDed with any incremental bookmark conditions.
#           Supported operators: >, >=, <, <=, =, !=
#           Example: {"column": "createddate", "operator": ">", "value": "2020-01-10"}
#           If omitted, no extra filter is applied.

TABLE_SPECS = [
    {
        "name": "tickit.users",  # Name of the table from the Redshift database
        "primary_keys": [
            "userid"
        ],  # Primary key column(s) for the table. If None, the connector will fetch the primary key(s) from the source database.
        "strategy": "FULL",  # Replication strategy: FULL or INCREMENTAL
        "replication_key": None,  # Column used for incremental replication. If None, the connector will attempt to infer it for INCREMENTAL sync strategy.
        "include": [],  # List of columns to include in the sync. An empty list means all columns are included.
        "exclude": [],  # List of columns to exclude from the sync. An empty list means no columns are excluded.
    },
    {
        "name": "tickit.category",
        "primary_keys": ["catid"],
        "strategy": "INCREMENTAL",
        "include": [],
        "exclude": [],
    },  # No replication_key specified. The replication key will be inferred as the sync strategy is INCREMENTAL
    {
        "name": "tickit.date",
        "primary_keys": ["dateid"],
        "strategy": "INCREMENTAL",
        "replication_key": None,  # The replication key will be inferred as the sync strategy is INCREMENTAL
        "include": [],
        "exclude": [],
        "use_chunking": True,  # Enable chunking for this table
    },
    {
        "name": "tickit.event",
        "primary_keys": ["eventid", "venueid"],
        "strategy": "INCREMENTAL",
        "replication_key": None,  # The replication key will be inferred as the sync strategy is INCREMENTAL
        "include": [],
        "exclude": [],
        "use_chunking": True,  # Enable chunking for this table
    },
    {
        "name": "tickit.listing",
        "primary_keys": ["sellerid", "listid", "eventid"],
        "strategy": "FULL",
        "replication_key": None,
        "include": [],
        "exclude": [],
    },
    {
        "name": "tickit.sales",
        "primary_keys": ["buyerid", "sellerid", "salesid", "listid"],
        "strategy": "FULL",
        "replication_key": None,
        "include": [],
        "exclude": [],
        "column_types": {
            # Explicitly declare columns that may contain only NULL values so Fivetran
            # creates them in the destination even when no non-null data is observed.
            # Example: "quantity_sold": "SHORT", "commission": "FLOAT"
        },
        # Optional: apply a static WHERE condition to every sync of this table.
        # Example:"filter": {"column": "saletime", "operator": ">", "value": "2008-01-01"}
        "filter": None,
    },
    {
        "name": "tickit.venue",
        "primary_keys": ["venueid"],
        "strategy": "FULL",
        "replication_key": None,
        "include": [],
        "exclude": [],
    },
]
