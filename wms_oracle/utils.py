"""Constants, entity list, custom exceptions, and timestamp utilities."""

from datetime import datetime, timezone

# ── Entity list ───────────────────────────────────────────────────────────────

ORACLE_WMS_ENTITIES = [
    "order_dtl",
    "inventory",
    "container",
    "container_lock_xref",
    "order_hdr",
    "allocation",
    "inventory_attribute",
    "item",
    "batch_number",
    "purchase_order_dtl",
    "ib_shipment",
    "ib_shipment_dtl",
    "purchase_order_hdr",
    "inventory_lock",
    "location",
    "item_metric",
    "ib_container",
    "history_activity",
    "putaway_type",
    "order_type",
    "purchase_order_status",
    "order_status",
    "inventory_status",
    "vendor",
    "company",
    "facility",
    #   "inventory_history",  # Uncomment if your Oracle WMS instance supports this entity
]


# ── Custom exceptions ─────────────────────────────────────────────────────────


class OrderingNotSupportedError(Exception):
    """Raised when the API returns 400 for a request with an ordering parameter.
    Indicates the entity does not support ordering on that field. Do not retry."""

    pass


# ── Sync tuning constants ─────────────────────────────────────────────────────

API_VERSION = "v10"
DEFAULT_PAGE_SIZE = 1000
MIN_PAGE_SIZE = 25
CHECKPOINT_INTERVAL_PAGES = 10
MAX_RETRIES = 5
INITIAL_BACKOFF_SECONDS = 1
MAX_CONCURRENT_ENTITIES = 10
DEFAULT_MAX_PAGES = 100
BACKFILL_WINDOW_DAYS = 30  # Width of each backfill fetch window
BACKFILL_MAX_EMPTY_WINDOWS = 12  # Stop after this many consecutive empty windows (~1 year)
# Max wall-clock seconds between incremental checkpoints
INCREMENTAL_CHECKPOINT_INTERVAL_SECONDS = 600


# ── Configuration validation ──────────────────────────────────────────────────


def validate_configuration(configuration: dict):
    """Raise ValueError if any required configuration fields are missing or invalid."""
    for key in ["base_url", "username", "password"]:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")

    if not configuration.get("base_url", "").startswith("https://"):
        raise ValueError("base_url must start with https://")

    try:
        int(configuration.get("page_size", str(DEFAULT_PAGE_SIZE)))
    except ValueError:
        raise ValueError("page_size must be a valid integer")


# ── Timestamp utilities ───────────────────────────────────────────────────────


def get_current_timestamp() -> str:
    """Return the current UTC time as an ISO string with timezone offset."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def normalize_timestamp_to_oracle_format(timestamp_str: str) -> str:
    """Round a timestamp to second precision, preserving timezone offset.
    Oracle WMS rejects sub-second precision in query parameters.
    """
    try:
        dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        return dt.isoformat(timespec="seconds")
    except (ValueError, AttributeError):
        return timestamp_str


def to_utc(timestamp_str: str) -> str:
    """Convert any ISO timestamp string to UTC with full microsecond precision."""
    try:
        dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc).isoformat()
    except (ValueError, AttributeError):
        return timestamp_str
