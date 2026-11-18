"""
This file contains the progress logging for the connector. It keeps the logs short and readable:
one line when a table starts (with its strategy and size), a progress line about once a minute
for tables that take long, one line when a table completes, and one summary line per sync.
Per-table details of the table metadata are only logged at the DEBUG level.
"""

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

import threading  # For the locks that protect counters shared by the worker threads
import time  # For measuring how long tables take

# How often a long running table logs its progress
from table_specs import PROGRESS_LOG_INTERVAL_SECONDS

# Number of other running tables that are named in a table's completion line. A single underscore
# is used because names with two underscores are renamed inside classes.
_MAX_RUNNING_NAMES = 3


def format_bytes(count):
    """
    Format a number of bytes for logs.
    Args:
        count: number of bytes
    Returns:
        Text such as 164.0 MB.
    """
    size = float(count)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1000 or unit == "TB":
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1000


def format_duration(seconds):
    """
    Format a duration for logs.
    Args:
        seconds: number of seconds
    Returns:
        Text such as 45s, 4m12s or 1h05m.
    """
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m{seconds % 60:02d}s"
    return f"{seconds // 3600}h{(seconds % 3600) // 60:02d}m"


def plan_kind(plan):
    """
    Classify how a table is read.
    Args:
        plan: TablePlan
    Returns:
        "partition" for an incremental sync by partition, "cursor" for an incremental sync by
        cursor column, otherwise "full".
    """
    if plan.strategy != "INCREMENTAL":
        return "full"
    return "partition" if plan.partition_column else "cursor"


def describe_filter(filter_condition):
    """
    Describe the filter of a table for logs.
    Args:
        filter_condition: filter text written like a WHERE clause, or None
    Returns:
        The filter text on one line, or an empty string.
    """
    return " ".join(str(filter_condition).split()) if filter_condition else ""


def _describe_strategy(plan):
    """
    Describe how a table is read, for the line that is logged when the table starts.
    Args:
        plan: TablePlan
    Returns:
        Text such as "partition column event_date (DAY), lookback 2".
    """
    if plan.partition_column:
        unit = plan.granularity if plan.partition_kind == "TIME" else "integer range"
        detail = f"partition column {plan.partition_column} ({unit})"
        if plan.strategy == "INCREMENTAL":
            return f"{detail}, lookback {plan.lookback}"
        return f"{detail}, strategy FULL"
    if plan.cursor_column:
        return f"cursor column {plan.cursor_column}"
    if plan.full_by_default:
        return "no partition or cursor column"
    return "strategy FULL"


def describe_plan(plan, mode):
    """
    Describe a table for the line that is logged when the table starts.
    Args:
        plan: TablePlan
        mode: initial load, resuming initial load, incremental or full reload
    Returns:
        A one-line description with the mode, strategy, primary key, size and filter.
    """
    keys = ", ".join(plan.primary_keys)
    text = (
        f"{mode} | {_describe_strategy(plan)} | primary key: {keys} | "
        f"{plan.num_rows:,} rows, {format_bytes(plan.num_bytes)}, {len(plan.columns)} columns"
    )
    filter_text = describe_filter(plan.filter_condition)
    return f"{text} | filter: {filter_text}" if filter_text else text


class TableProgress:
    """
    Counts the rows, bytes scanned and batches of one table, and logs the progress of a long
    running table about once a minute. Several read streams update it at the same time.
    """

    def __init__(self, key):
        """
        Args:
            key: dataset.table name used in logs
        """
        self.key = key
        self.started = time.monotonic()
        self.rows = 0
        self.scanned_bytes = 0
        self.expected = 0
        self.exact = False
        self.null_cursor_rows = 0
        self._last_log = self.started
        self._lock = threading.Lock()

    def expect(self, rows, exact):
        """
        Set the number of rows that this sync is expected to read, from table metadata.
        Args:
            rows: expected number of rows, or 0 when unknown
            exact: True when no filter can make the real number smaller than this estimate
        """
        self.expected = rows
        self.exact = exact

    def add_scan(self, scanned_bytes):
        """
        Add the bytes that BigQuery estimates a read session will scan.
        Args:
            scanned_bytes: estimated bytes scanned
        """
        with self._lock:
            self.scanned_bytes += scanned_bytes

    def add_null_cursor(self, count):
        """
        Count rows that were read with a NULL value in the cursor column.
        Args:
            count: number of rows
        """
        with self._lock:
            self.null_cursor_rows += count

    def _rows_text(self):
        """Describe the rows read so far, with the expectation when there is one."""
        if not self.expected:
            return f"{self.rows:,} rows read"
        if self.exact:
            percent = min(100, int(100 * self.rows / self.expected))
            return f"{self.rows:,} of ~{self.expected:,} rows read ({percent}%)"
        return f"{self.rows:,} rows read (up to ~{self.expected:,} expected)"

    def _rate_text(self, now):
        """Describe the elapsed time and the read speed."""
        elapsed = now - self.started
        rate = self.rows / elapsed if elapsed > 0 else 0
        return f"{format_duration(elapsed)}, {rate:,.0f} rows/s"

    def add_rows(self, count):
        """
        Add rows that were just read, and log the progress if a minute has passed since the last
        line about this table.
        Args:
            count: number of rows read
        """
        with self._lock:
            self.rows += count
            now = time.monotonic()
            if now - self._last_log < PROGRESS_LOG_INTERVAL_SECONDS:
                return
            self._last_log = now
            message = f"{self.key}: progress - {self._rows_text()}, {self._rate_text(now)}"
        log.info(message)

    def batch_done(self, number, total, rows, label):
        """
        Log a finished batch of partitions.
        Args:
            number: position of the batch
            total: number of batches
            rows: rows read in the batch
            label: description of the batch
        """
        with self._lock:
            now = time.monotonic()
            self._last_log = now
            message = (
                f"{self.key}: batch {number}/{total} done [{label}] - {rows:,} rows; "
                f"{self._rows_text()}, {self._rate_text(now)}"
            )
        log.info(message)

    def seconds(self):
        """Seconds since the table started."""
        return time.monotonic() - self.started


class RunProgress:
    """
    Tracks all tables of one sync: how many have started and finished, which are running, and the
    totals. Tables are synced in parallel, so every method is thread safe.
    """

    def __init__(self, total_tables):
        """
        Args:
            total_tables: number of tables in this sync
        """
        self.total = total_tables
        self.started_count = 0
        self.done = 0
        self.failed = 0
        self.rows = 0
        self.scanned_bytes = 0
        self.started = time.monotonic()
        self._running = []
        self._lock = threading.Lock()

    def start_table(self, plan, mode):
        """
        Log that a table starts, with its strategy and size.
        Args:
            plan: TablePlan
            mode: initial load, resuming initial load, incremental or full reload
        Returns:
            The TableProgress of the table.
        """
        progress = TableProgress(plan.key)
        with self._lock:
            self.started_count += 1
            position = self.started_count
            self._running.append(plan.key)
        log.info(f"{plan.key}: started ({position}/{self.total}) - {describe_plan(plan, mode)}")
        return progress

    def _stop_running(self, progress):
        """Remove a table from the running list and return the names of the others."""
        self._running.remove(progress.key)
        return list(self._running)

    def finish_table(self, progress):
        """
        Log that a table completed, with its row count, duration and estimated scan.
        Args:
            progress: TableProgress of the table
        """
        with self._lock:
            self.done += 1
            self.rows += progress.rows
            self.scanned_bytes += progress.scanned_bytes
            others = self._stop_running(progress)
            done = self.done
        suffix = ""
        if others:
            names = ", ".join(others[:_MAX_RUNNING_NAMES])
            hidden = len(others) - _MAX_RUNNING_NAMES
            more = f" and {hidden} more" if hidden > 0 else ""
            suffix = f"; running: {names}{more}"
        log.info(
            f"{progress.key}: completed in {format_duration(progress.seconds())} - "
            f"{progress.rows:,} rows read, ~{format_bytes(progress.scanned_bytes)} scanned "
            f"({done}/{self.total} tables done{suffix})"
        )

    def fail_table(self, progress, error):
        """
        Log that a table failed. The error is raised again by the caller so the sync fails.
        Args:
            progress: TableProgress of the table
            error: the exception
        """
        with self._lock:
            self.failed += 1
            self._stop_running(progress)
        log.error(
            f"{progress.key}: FAILED after {format_duration(progress.seconds())} and "
            f"{progress.rows:,} rows - {type(error).__name__}: {error}"
        )

    def summary(self):
        """
        Describe the whole sync, for the last line of the log.
        Returns:
            A one-line summary with tables, rows, estimated scan and duration.
        """
        with self._lock:
            return (
                f"Sync finished in {format_duration(time.monotonic() - self.started)} - "
                f"{self.done}/{self.total} tables, {self.rows:,} rows read, "
                f"~{format_bytes(self.scanned_bytes)} scanned (estimated)"
            )
