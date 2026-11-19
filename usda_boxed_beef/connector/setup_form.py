"""Setup form for the USDA boxed beef connector. One field per configuration.json key, all optional."""

# For declaring the setup form and its fields
from fivetran_connector_sdk import ConfigurationForm, form_field


def build_form(defaults: dict, tests: list) -> ConfigurationForm:
    """
    Build the setup form from the connector's defaults and setup tests.

    Args:
        defaults: the default value of every configuration key, shown in the field descriptions
        tests: (label, function) pairs; each function takes the configuration dict and returns
            Test().success() or Test().failure(reason)

    Returns:
        ConfigurationForm with one field per configuration key and the given setup tests
    """
    form = ConfigurationForm()
    form.add_field(
        form_field.TextField(
            "report_ids",
            label="USDA report ids",
            description=(
                "Comma-separated LMR report ids: 2453 (daily Choice and Select cutout and cuts) and "
                f"2460 (weekly Prime cuts). Optional. Defaults to {defaults['report_ids']}."
            ),
            placeholder=defaults["report_ids"],
        )
    )
    form.add_field(
        form_field.TextField(
            "start_date",
            label="First report date to sync",
            description=(
                "YYYY-MM-DD. The initial sync reads every report from this date forward. "
                f"Optional. Defaults to {defaults['start_date']}; history starts on {defaults['earliest_start_date']}."
            ),
            placeholder=defaults["start_date"],
        )
    )
    form.add_field(
        form_field.TextField(
            "max_window_days",
            label="Days per request",
            description=(
                "Report dates covered by one request. The API allows at most 180. "
                f"Optional. Defaults to {defaults['max_window_days']}."
            ),
            placeholder=str(defaults["max_window_days"]),
        )
    )
    form.add_field(
        form_field.TextField(
            "max_windows_per_sync",
            label="Windows per report per sync",
            description=(
                "The most date windows one sync reads for each report. A long backfill continues on the next "
                f"sync. Optional. Defaults to {defaults['max_windows_per_sync']}."
            ),
            placeholder=str(defaults["max_windows_per_sync"]),
        )
    )
    form.add_field(
        form_field.TextField(
            "request_delay_seconds",
            label="Seconds between requests",
            description=(
                "The least time between two requests. USDA blocks high-frequency callers. "
                f"Optional. Defaults to {defaults['request_delay_seconds']}; the minimum is "
                f"{defaults['min_request_delay_seconds']}."
            ),
            placeholder=str(defaults["request_delay_seconds"]),
        )
    )
    for label, func in tests:
        form.add_test(label=label, func=func)
    return form
