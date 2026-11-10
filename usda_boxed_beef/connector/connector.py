"""
USDA Boxed Beef connector - Syncs wholesale boxed beef prices from the USDA Agricultural Marketing Service (AMS)
Livestock Mandatory Reporting (LMR) DataMart API: the daily Choice and Select cutout, composite primal values,
negotiated prices for each boxed beef cut, ground beef and trimmings (report 2453, LM_XB403), and weekly Prime
cut prices (report 2460, LM_XB456).

The first sync reads every report date from the configured start date in windows of up to 180 days. Later syncs
read by published date instead. AMS corrects a report by republishing the same report date with a new published
time, so reading by published date picks up a correction whenever it is issued, with no fixed lookback.

See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading configuration from a JSON file
import json

# For rejecting NaN and infinity in the request delay
import math

# For date windows and the current date in US Central time
from datetime import date, datetime, timedelta

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For the setup form and the result type returned by its setup test
from fivetran_connector_sdk import ConfigurationForm, Test

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like upsert(), update(), delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# The setup form's field layout, kept in its own module so the connector stays readable
from setup_form import build_form

# Every destination table and the sections of each supported report
from tables import CUTOUT_VALUES_TABLE, REPORTS, REPORTS_TABLE, TABLE_CONFIG

# Row building: number, date and item parsing
from transforms import BUILDERS, SOURCE_TIME_ZONE, RowIssues

# The paced, retrying client for the LMR DataMart API
from usda_client import UsdaApiError, UsdaClient, fetch_section

# Configuration defaults and limits
__DEFAULT_REPORT_IDS = "2453,2460"
__DEFAULT_START_DATE = "2024-01-01"
# The lower bound start_date is validated against: the lowest first_report_date in tables.REPORTS
__EARLIEST_START_DATE = min(report["first_report_date"] for report in REPORTS.values())
__DEFAULT_MAX_WINDOW_DAYS = 180
# The API documents 180 days as the longest date range one request may span
__MAX_WINDOW_DAYS_CEILING = 180
__DEFAULT_MAX_WINDOWS_PER_SYNC = 50
__MAX_WINDOWS_PER_SYNC_CEILING = 500
__DEFAULT_REQUEST_DELAY_SECONDS = 1.0
__MIN_REQUEST_DELAY_SECONDS = 0.5
__MAX_REQUEST_DELAY_SECONDS = 30.0

# Each incremental sync re-reads this many days of published dates before its saved position, so a report the
# API publishes a little after its published time is not missed. Upserts make the re-read idempotent.
__INCREMENTAL_OVERLAP_DAYS = 3
# Both reports publish at least once in any 14 days, so a backfill window this long with no report is unexpected
__MIN_DAYS_TO_EXPECT_A_REPORT = 14

# The date fields the API filters on: report_date for the first sync, published_date afterwards
__BACKFILL_FIELD = "report_date"
__INCREMENTAL_FIELD = "published_date"


def _is_placeholder(value) -> bool:
    """
    Check for an unset value or an unedited angle-bracket placeholder.

    Args:
        value: the configuration value to check

    Returns:
        True when the value is missing, empty, or a <PLACEHOLDER> string
    """
    if value is None:
        return True
    if not isinstance(value, str):
        return False
    stripped = value.strip()
    return not stripped or (stripped.startswith("<") and stripped.endswith(">"))


def _optional_str(configuration: dict, key: str, default: str) -> str:
    """
    Read an optional setting, treating a missing, blank or placeholder value as unset.

    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        key: the configuration key to read
        default: the value to use when the key is unset

    Returns:
        The configured value as a stripped string, or the default
    """
    value = configuration.get(key)
    if _is_placeholder(value):
        return default
    return str(value).strip()


def parse_report_ids_config(value: str) -> list:
    """
    Parse the comma-separated report ids.

    Args:
        value: such as "2453,2460"

    Returns:
        The report ids in the order given, without duplicates

    Raises:
        ValueError: if the list is empty or names a report this connector does not map
    """
    report_ids = []
    for part in value.split(","):
        report_id = part.strip()
        if not report_id:
            continue
        if report_id not in REPORTS:
            raise ValueError(
                f"report_ids: {report_id!r} is not supported; use any of {', '.join(sorted(REPORTS))}"
            )
        if report_id not in report_ids:
            report_ids.append(report_id)
    if not report_ids:
        raise ValueError("report_ids must name at least one report")
    return report_ids


def parse_start_date_config(value: str) -> date:
    """
    Parse the first report date to sync.

    Args:
        value: a YYYY-MM-DD date

    Returns:
        The date

    Raises:
        ValueError: if the value is not a date, is before the API's history, or is in the future
    """
    # date.fromisoformat also takes forms such as 20240101 and 2024-W01-1, so the shape is checked first
    try:
        if not (value.isascii() and len(value) == 10 and value[4] == "-" and value[7] == "-"):
            raise ValueError(value)
        start_date = date.fromisoformat(value)
    except ValueError as e:
        raise ValueError("start_date must be a YYYY-MM-DD date") from e
    if start_date < date.fromisoformat(__EARLIEST_START_DATE):
        raise ValueError(f"start_date must be {__EARLIEST_START_DATE} or later")
    if start_date > get_current_date():
        raise ValueError("start_date must not be in the future")
    return start_date


def parse_positive_int_config(key: str, value: str, ceiling: int) -> int:
    """
    Parse a positive whole number with a ceiling.

    Args:
        key: the configuration key, for the error message
        value: the configured text
        ceiling: the largest value allowed

    Returns:
        The number

    Raises:
        ValueError: if the value is not a whole number between 1 and the ceiling
    """
    # int() also reads digits from other scripts, a sign, underscores and spaces, so only ASCII digits are accepted
    try:
        if not (value.isascii() and value.isdecimal()):
            raise ValueError(value)
        number = int(value)
    except ValueError as e:
        raise ValueError(f"{key} must be a whole number") from e
    if number <= 0 or number > ceiling:
        raise ValueError(f"{key} must be between 1 and {ceiling}")
    return number


def parse_request_delay_config(value: str) -> float:
    """
    Parse the least number of seconds between two requests.

    Args:
        value: the configured text

    Returns:
        The delay in seconds

    Raises:
        ValueError: if the value is not a finite number in the allowed range
    """
    # float() also reads digits from other scripts and underscores, so only ASCII digits and a point are accepted
    try:
        if not (value.isascii() and value.replace(".", "", 1).isdecimal()):
            raise ValueError(value)
        seconds = float(value)
    except ValueError as e:
        raise ValueError("request_delay_seconds must be a number") from e
    if not math.isfinite(seconds):
        raise ValueError("request_delay_seconds must be a finite number")
    if seconds < __MIN_REQUEST_DELAY_SECONDS or seconds > __MAX_REQUEST_DELAY_SECONDS:
        raise ValueError(
            f"request_delay_seconds must be between {__MIN_REQUEST_DELAY_SECONDS} and {__MAX_REQUEST_DELAY_SECONDS}"
        )
    return seconds


def validate_configuration(configuration: dict) -> dict:
    """
    Validate the configuration dictionary to ensure all required parameters are present and valid.

    Every setting is optional; a missing, blank or placeholder value takes its default.

    Args:
        configuration: a dictionary that holds the configuration settings for the connector.

    Returns:
        The parsed settings

    Raises:
        ValueError: if any configuration parameter is invalid.
    """
    return {
        "report_ids": parse_report_ids_config(
            _optional_str(configuration, "report_ids", __DEFAULT_REPORT_IDS)
        ),
        "start_date": parse_start_date_config(
            _optional_str(configuration, "start_date", __DEFAULT_START_DATE)
        ),
        "max_window_days": parse_positive_int_config(
            "max_window_days",
            _optional_str(configuration, "max_window_days", str(__DEFAULT_MAX_WINDOW_DAYS)),
            __MAX_WINDOW_DAYS_CEILING,
        ),
        "max_windows_per_sync": parse_positive_int_config(
            "max_windows_per_sync",
            _optional_str(
                configuration, "max_windows_per_sync", str(__DEFAULT_MAX_WINDOWS_PER_SYNC)
            ),
            __MAX_WINDOWS_PER_SYNC_CEILING,
        ),
        "request_delay_seconds": parse_request_delay_config(
            _optional_str(
                configuration, "request_delay_seconds", str(__DEFAULT_REQUEST_DELAY_SECONDS)
            )
        ),
    }


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-code/connector-sdk-methods#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {"table": table_name, "primary_key": config["primary_key"], "columns": config["columns"]}
        for table_name, config in TABLE_CONFIG.items()
    ]


def get_current_date() -> date:
    """
    Return today's date in US Central time, the time zone the API dates and publishes reports in.

    Returns:
        The current date in US Central time
    """
    return datetime.now(SOURCE_TIME_ZONE).date()


def report_progress(
    start_date, backfill_through, backfill_complete: bool, published_through, last_report_date=None
) -> dict:
    """
    Build one report's saved progress. Every key is always written, so no checkpoint can drop one.

    Args:
        start_date: the first report date the backfill reads
        backfill_through: the last report date the backfill has read, or None before the first window
        backfill_complete: whether the backfill has reached the current date
        published_through: the last published date the incremental sync has read
        last_report_date: the latest report date the backfill has loaded, or None before the first report

    Returns:
        The progress as JSON-safe values
    """
    return {
        "start_date": start_date.isoformat(),
        "backfill_through": backfill_through.isoformat() if backfill_through else None,
        "backfill_complete": backfill_complete,
        "published_through": published_through.isoformat(),
        "last_report_date": last_report_date.isoformat() if last_report_date else None,
    }


def _saved_date(saved: dict, key: str, slug_id: str):
    """
    Read one date from a report's saved progress.

    Args:
        saved: the report's saved progress
        key: the key to read
        slug_id: the report id, for the error message

    Returns:
        The date, or None when the key is unset

    Raises:
        RuntimeError: if the saved value is not a date
    """
    value = saved.get(key)
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError) as e:
        raise RuntimeError(f"Saved state for report {slug_id} has an unreadable {key}") from e


def load_progress(saved, start_date: date, today: date, slug_id: str) -> dict:
    """
    Read a report's saved progress, or start a new backfill.

    A new backfill also records today as the published date the incremental sync starts from, so a correction
    published while the backfill runs is read by the first incremental sync. Moving start_date earlier than the
    saved one starts the backfill again from the new date; moving it later keeps the rows already loaded.

    Args:
        saved: the report's saved progress, or None on its first sync
        start_date: the configured first report date
        today: the current date in US Central time
        slug_id: the report id

    Returns:
        A dict with start_date, backfill_through, backfill_complete, published_through and last_report_date
    """
    saved = saved if isinstance(saved, dict) else {}
    saved_start = _saved_date(saved, "start_date", slug_id)
    published_through = _saved_date(saved, "published_through", slug_id) or today
    if saved_start is None or start_date < saved_start:
        if saved_start is not None:
            log.info(
                f"Report {slug_id}: start_date moved earlier to {start_date}, so the backfill starts again"
            )
        return {
            "start_date": start_date,
            "backfill_through": None,
            "backfill_complete": False,
            "published_through": min(published_through, today),
            "last_report_date": None,
        }
    return {
        "start_date": saved_start,
        "backfill_through": _saved_date(saved, "backfill_through", slug_id),
        "backfill_complete": bool(saved.get("backfill_complete")),
        "published_through": published_through,
        # Progress saved before last_report_date existed has no such key: the backfill loaded the dates through
        # backfill_through, so no gap is measured across them
        "last_report_date": _saved_date(
            saved,
            "last_report_date" if "last_report_date" in saved else "backfill_through",
            slug_id,
        ),
    }


def sync_window(
    client: UsdaClient,
    slug_id: str,
    field: str,
    window_start,
    window_end,
    issues,
    counts,
    report_dates=None,
) -> int:
    """
    Read every section of one report for one date window and upsert the rows.

    Args:
        client: the paced session
        slug_id: the report id
        field: report_date or published_date
        window_start: the first date of the window
        window_end: the last date of the window
        issues: counts of values the sync could not use
        counts: rows written per table, updated in place
        report_dates: when given, the report date of every reports row written is added to it

    Returns:
        The number of rows written for the window
    """
    written = 0
    # The two cutout sections each carry half of a cutout_values row, keyed here by report date
    cutout_rows = {}
    for section in REPORTS[slug_id]["sections"]:
        build_rows = BUILDERS[section["builder"]]
        raw_rows = fetch_section(client, slug_id, section["name"], field, window_start, window_end)
        for raw in raw_rows:
            for row in build_rows(slug_id, section, raw, issues):
                if section["table"] == CUTOUT_VALUES_TABLE:
                    cutout_rows.setdefault(row["report_date"], {}).update(
                        {key: value for key, value in row.items() if value is not None}
                    )
                    continue
                if report_dates is not None and section["table"] == REPORTS_TABLE:
                    report_dates.add(date.fromisoformat(row["report_date"]))
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The first argument is the name of the destination table.
                # The second argument is a dictionary containing the record to be upserted.
                op.upsert(table=section["table"], data=row)
                counts[section["table"]] = counts.get(section["table"], 0) + 1
                written += 1

    cutout_columns = TABLE_CONFIG[CUTOUT_VALUES_TABLE]["columns"]
    for report_date in sorted(cutout_rows):
        row = {column: cutout_rows[report_date].get(column) for column in cutout_columns}
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table=CUTOUT_VALUES_TABLE, data=row)
        counts[CUTOUT_VALUES_TABLE] = counts.get(CUTOUT_VALUES_TABLE, 0) + 1
        written += 1
    return written


def sync_report(
    client: UsdaClient, slug_id: str, settings: dict, today: date, state: dict, issues, counts
):
    """
    Sync one report: continue its backfill by report date, or read newly published reports by published date.

    A sync that runs the backfill does not also run the incremental read, because the backfill has just read
    every report date through today. The next sync starts reading published dates from the day the backfill
    started.

    Args:
        client: the paced session
        slug_id: the report id
        settings: the parsed configuration
        today: the current date in US Central time
        state: the whole state dictionary, checkpointed after every window
        issues: counts of values the sync could not use
        counts: rows written per table, updated in place
    """
    if not isinstance(state.get("reports"), dict):
        state["reports"] = {}
    reports_state = state["reports"]
    progress = load_progress(reports_state.get(slug_id), settings["start_date"], today, slug_id)
    window_days = settings["max_window_days"]
    windows_left = settings["max_windows_per_sync"]

    if not progress["backfill_complete"]:
        first_report_date = date.fromisoformat(REPORTS[slug_id]["first_report_date"])
        while not progress["backfill_complete"] and windows_left > 0:
            if progress["backfill_through"] is None:
                window_start = progress["start_date"]
            else:
                window_start = progress["backfill_through"] + timedelta(days=1)
            window_end = min(window_start + timedelta(days=window_days - 1), today)
            report_dates = set()
            written = sync_window(
                client,
                slug_id,
                __BACKFILL_FIELD,
                window_start,
                window_end,
                issues,
                counts,
                report_dates,
            )
            # The backfill never reads these report dates again, so each gap between report dates long enough to be
            # unexpected is reported once, when the report after it arrives or the backfill reaches today. The last
            # report date is saved with the progress, so a gap that spans windows or syncs is still found.
            previous = progress["last_report_date"] or (
                max(progress["start_date"], first_report_date) - timedelta(days=1)
            )
            for report_date in sorted(report_dates):
                if (report_date - previous).days - 1 >= __MIN_DAYS_TO_EXPECT_A_REPORT:
                    report_empty_dates(
                        slug_id, previous + timedelta(days=1), report_date - timedelta(days=1)
                    )
                previous = max(previous, report_date)
            if report_dates:
                progress["last_report_date"] = previous
            # Today's report may not be published yet, so the gap at the end runs to yesterday
            yesterday = today - timedelta(days=1)
            trailing_gap = (yesterday - previous).days >= __MIN_DAYS_TO_EXPECT_A_REPORT
            if window_end >= today and trailing_gap:
                report_empty_dates(slug_id, previous + timedelta(days=1), yesterday)
            log.info(
                f"Report {slug_id}: report dates {window_start} to {window_end}, {written} rows"
            )
            progress["backfill_through"] = window_end
            progress["backfill_complete"] = window_end >= today
            reports_state[slug_id] = report_progress(**progress)
            windows_left -= 1

            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
            # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
            op.checkpoint(state=state)

        if not progress["backfill_complete"]:
            log.info(
                f"Report {slug_id}: backfill read through {progress['backfill_through']}; "
                "the next sync continues it"
            )
        return

    window_start = progress["published_through"] - timedelta(days=__INCREMENTAL_OVERLAP_DAYS)
    while window_start <= today and windows_left > 0:
        window_end = min(window_start + timedelta(days=window_days - 1), today)
        written = sync_window(
            client, slug_id, __INCREMENTAL_FIELD, window_start, window_end, issues, counts
        )
        log.info(
            f"Report {slug_id}: published dates {window_start} to {window_end}, {written} rows"
        )
        progress["published_through"] = max(progress["published_through"], window_end)
        reports_state[slug_id] = report_progress(**progress)
        windows_left -= 1
        window_start = window_end + timedelta(days=1)

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
        # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
        op.checkpoint(state=state)


def report_empty_dates(slug_id: str, first: date, last: date):
    """
    Report a run of report dates the API returned no report for, which the backfill does not read again.

    Args:
        slug_id: the report id
        first: the first report date of the run
        last: the last report date of the run so far
    """
    message = (
        f"The API returned no report {slug_id} for report dates {first} to {last}, which the backfill does not "
        "read again. Move start_date earlier to read them again."
    )
    log.warning(message)
    op.warning(message)


def report_issues(issues: RowIssues):
    """
    Report values the sync could not use, in the connector log and on the Fivetran dashboard.

    Args:
        issues: counts of values the sync could not use
    """
    if issues.unparseable_numbers:
        message = f"Wrote null for {issues.unparseable_numbers} value(s) the API sent that were not numbers."
        log.warning(message)
        op.warning(message)
    if issues.rows_without_key:
        message = (
            f"Skipped {issues.rows_without_key} row(s) with no report date or item description."
        )
        log.warning(message)
        op.warning(message)
    if issues.rows_with_prices_without_trades:
        message = (
            f"Wrote null prices on {issues.rows_with_prices_without_trades} row(s) without a readable trade "
            "count above zero."
        )
        log.warning(message)
        op.warning(message)
    if issues.unparseable_published_dates:
        message = (
            f"Wrote a null published_at for {issues.unparseable_published_dates} row(s) whose published "
            "date was not in the API's format."
        )
        log.warning(message)
        op.warning(message)


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """
    log.warning("Example: Source Examples : USDA Boxed Beef")

    settings = validate_configuration(configuration)
    today = get_current_date()
    client = UsdaClient(settings["request_delay_seconds"])
    issues = RowIssues()
    counts = {}

    try:
        for slug_id in settings["report_ids"]:
            sync_report(client, slug_id, settings, today, state, issues, counts)

        report_issues(issues)

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
        # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
        op.checkpoint(state=state)

        summary = ", ".join(f"{table} {count}" for table, count in sorted(counts.items()))
        if not summary:
            summary = "no new rows"
        log.info(f"Sync complete in {client.request_count} request(s): {summary}")
    finally:
        client.close()


def verify_api_access(configuration: dict):
    """
    Setup test: validate the configuration and read one week of the first report's Summary section.

    Args:
        configuration: a dictionary that holds the configuration settings for the connector.

    Returns:
        Test().success() or Test().failure(reason)
    """
    test = Test()
    try:
        settings = validate_configuration(configuration)
    except ValueError as e:
        return test.failure(str(e))
    client = UsdaClient(settings["request_delay_seconds"])
    today = get_current_date()
    try:
        fetch_section(
            client,
            settings["report_ids"][0],
            "Summary",
            __BACKFILL_FIELD,
            today - timedelta(days=7),
            today,
        )
    except UsdaApiError as e:
        return test.failure(f"The USDA LMR DataMart API could not be read: {e}")
    finally:
        client.close()
    return test.success()


def configuration_form() -> ConfigurationForm:
    """
    Declare the setup form Fivetran renders for this connector and the setup test it can run.

    The field layout lives in setup_form.py; this function supplies the defaults and the setup test, which needs
    the connector's own validation code.

    Returns:
        ConfigurationForm with one field per configuration key and one setup test
    """
    return build_form(
        defaults={
            "report_ids": __DEFAULT_REPORT_IDS,
            "start_date": __DEFAULT_START_DATE,
            "earliest_start_date": __EARLIEST_START_DATE,
            "max_window_days": __DEFAULT_MAX_WINDOW_DAYS,
            "max_windows_per_sync": __DEFAULT_MAX_WINDOWS_PER_SYNC,
            "request_delay_seconds": __DEFAULT_REQUEST_DELAY_SECONDS,
            "min_request_delay_seconds": __MIN_REQUEST_DELAY_SECONDS,
        },
        tests=[("Read the USDA LMR DataMart API", verify_api_access)],
    )


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema, configuration_form=configuration_form)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
#
# IMPORTANT: The recommended way to test your connector is using the Fivetran debug command:
#   fivetran debug
#
# This local testing block is provided as a convenience for quick debugging during development,
# such as using IDE debug tools (breakpoints, step-through debugging, etc.).
# Note: This method is not called by Fivetran when executing your connector in production.
# Always test using 'fivetran debug' prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
