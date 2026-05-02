"""
Constants for EHI high-volume connector.
Tune these values for your environment. Table filtering and max_workers
are configured in configuration.json, not here.
"""

BATCH_SIZE = 1_000
CHECKPOINT_INTERVAL = 25_000
MAX_WORKERS = 4

MAX_RETRIES = 5
BASE_RETRY_DELAY = 5.0
MAX_RETRY_DELAY = 300.0
CONNECTION_TIMEOUT_HOURS = 8

# SQLSTATE codes for transient SQL Server errors safe to retry.
# Using standardised SQLSTATE codes avoids false matches on error message text.
RETRYABLE_SQLSTATES = frozenset(
    {
        "40001",  # Serialization failure / deadlock victim
        "HYT00",  # Query timeout expired
        "HYT01",  # Connection timeout expired
        "08S01",  # Communication link failure (TCP-level)
        "08001",  # Client unable to establish connection
        "08003",  # Connection not open
        "08007",  # Connection failure during transaction
    }
)

# SQL Server lock timeout (native error 1222) arrives as SQLSTATE HY000
# with the native error code embedded in the message string.
LOCK_TIMEOUT_NATIVE_ERROR = "1222"

KNOWN_REPLICATION_KEY_PATTERNS = [
    "_LastUpdatedInstant",
    "UpdatedAt",
    "updated_at",
    "UpdatedDate",
    "updated_date",
    "ModifiedDate",
    "modified_date",
    "ModifiedAt",
    "modified_at",
    "LastModified",
    "last_modified",
    "DateModified",
    "date_modified",
    "LastUpdated",
    "last_updated",
]
