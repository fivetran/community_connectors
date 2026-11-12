"""Configuration constants and limits for the Deltek Vantagepoint connector.

This module centralizes retry/timeout/pagination parameters and the Hubs incremental-sync
constants. Any API-level changes (page size, timeout values, the audit column name) should be
reviewed and updated here rather than made configurable, since they typically require code
changes elsewhere to handle properly.
"""

# --------------------------------------------------------------------------------------
# HTTP retry/timeout/pagination constants
# --------------------------------------------------------------------------------------

__MAX_RETRIES = 3
__BASE_DELAY_SECONDS = 2
__REQUEST_TIMEOUT_SECONDS = 60

# Number of records requested per page. Vantagepoint's Hubs endpoints return a bare JSON array with
# no pagination envelope, so a page shorter than this size signals the last page.
__PAGE_SIZE = 200

# --------------------------------------------------------------------------------------
# Incremental sync / monthly full resync constants
# --------------------------------------------------------------------------------------

# Default cursor value used the first time a table is synced, if no override is provided via the
# optional "initial_sync_start_date" configuration value (see get_initial_cursor() in
# sync_operations.py). Using the epoch here means a first sync with no override pulls a Hub
# table's entire history. The previous value was set to 0001-01-01 and this throws errors.
__DEFAULT_CURSOR = "1901-01-01T00:00:00"

# The audit column present on every Hub entity, used for both server-side incremental filtering
# (via filterHash) and result ordering.
__CURSOR_FIELD = "ModDate"

# Default day of the month that triggers the monthly full truncate-and-resync (see
# should_run_full_resync() in sync_operations.py), if the optional "full_resync_day_of_month"
# configuration value is blank or invalid. 1 = the first day of the month.
__DEFAULT_FULL_RESYNC_DAY_OF_MONTH = 1

# Format used for state["last_full_resync_month"] - a calendar year-month, not a timestamp, since
# the monthly full resync is tracked per calendar month, not as a rolling interval.
__STATE_MONTH_FORMAT = "%Y-%m"
