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

# Settings that are described in the logs, see describe_settings
from table_specs import (
    BATCH_TARGET_ROWS,
    CURSOR_MARGIN_SECONDS,
    FLUSH_MAX_BYTES,
    FLUSH_MAX_VALUES,
    LOOKBACK_DAYS,
    LOOKBACK_PARTITIONS,
    MAX_PARALLEL_TABLES,
    MODIFIED_MARGIN_SECONDS,
    PROGRESS_LOG_INTERVAL_SECONDS,
    READ_STREAMS_PER_TABLE,
    SCHEMA_CHANGE_DEFAULT,
    STREAMING_GRACE_SECONDS,
    STREAMING_MIN_LOOKBACK,
)

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


def _lookback_source(plan):
    """Say whether the lookback of a table is the default or set in TABLE_SPECS."""
    options = plan.options or {}
    chosen = "lookback_partitions" in options or "lookback_days" in options
    return "spec" if chosen else "default"


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


def describe_strategy(plan):
    """
    Describe which data a table reads on every sync after the first, as key=value pairs.
    Args:
        plan: TablePlan
    Returns:
        Text such as "strategy=partition column=event_date granularity=DAY detect_modified=auto".
    """
    if plan.partition_column and plan.strategy == "INCREMENTAL":
        unit = plan.granularity if plan.partition_kind == "TIME" else "RANGE"
        return (
            f"strategy=partition column={plan.partition_column} granularity={unit} "
            f"detect_modified=auto margin={MODIFIED_MARGIN_SECONDS}s "
            f"lookback={plan.lookback} ({_lookback_source(plan)}) streaming_tail=auto"
        )
    if plan.partition_column:
        return f"strategy=full column={plan.partition_column} reason=spec"
    if plan.cursor_column and plan.strategy == "INCREMENTAL":
        return (
            f"strategy=cursor column={plan.cursor_column} "
            f"margin={plan.cursor_margin_seconds:g}s null_cursor=read"
        )
    reason = "no_partition_or_cursor_column" if plan.full_by_default else "spec"
    return f"strategy=full reason={reason}"


def _format_option(name, value):
    """Format one TABLE_SPECS option as name=value, without printing long lists."""
    if name in ("include", "exclude", "column_types") and hasattr(value, "__len__"):
        return f"{name}={len(value)}_columns"
    if isinstance(value, (list, tuple)):
        return f"{name}={','.join(str(item) for item in value)}"
    text = " ".join(str(value).split())
    return f'{name}="{text}"' if " " in text else f"{name}={text}"


def describe_settings(plan):
    """
    List the options set for a table in TABLE_SPECS and the defaults that apply, as key=value
    pairs.
    Args:
        plan: TablePlan
    Returns:
        Text such as "spec: primary_keys=id filter="d >= today" defaults: on_schema_change=resync".
    """
    options = plan.options or {}
    chosen = " ".join(_format_option(name, value) for name, value in options.items())
    defaults = []
    if "on_schema_change" not in options:
        defaults.append(f"on_schema_change={plan.on_schema_change}")
    if plan.cursor_column and "cursor_margin_seconds" not in options:
        defaults.append(f"cursor_margin_seconds={plan.cursor_margin_seconds:g}")
    text = f"spec: {chosen or 'none'}"
    return text + (f" defaults: {' '.join(defaults)}" if defaults else "")


def describe_global_settings():
    """
    List the settings of table_specs.py that apply to all tables, for the start of a sync.
    Returns:
        A tuple of two lines of key=value text: the change detection and the read settings.
    """
    detection = (
        f"detect_modified=auto modified_margin={MODIFIED_MARGIN_SECONDS}s "
        f"lookback_days={LOOKBACK_DAYS} lookback_partitions={LOOKBACK_PARTITIONS} "
        f"streaming_grace={STREAMING_GRACE_SECONDS}s "
        f"streaming_min_lookback={STREAMING_MIN_LOOKBACK} "
        f"cursor_margin={CURSOR_MARGIN_SECONDS}s on_schema_change={SCHEMA_CHANGE_DEFAULT}"
    )
    reading = (
        f"parallel_tables={MAX_PARALLEL_TABLES} read_streams={READ_STREAMS_PER_TABLE} "
        f"batch_rows={BATCH_TARGET_ROWS} flush_values={FLUSH_MAX_VALUES} "
        f"flush_bytes={FLUSH_MAX_BYTES}"
    )
    return detection, reading


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
        log.info(f"{plan.key}: {describe_strategy(plan)}")
        log.info(f"{plan.key}: {describe_settings(plan)}")
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
