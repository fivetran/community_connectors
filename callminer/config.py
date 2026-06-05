"""
Configuration parsing and validation utilities.
"""

from typing import Dict, Any
from fivetran_connector_sdk import Logging as log


def validate_configuration(configuration: Dict[str, Any]) -> None:
    """
    Validate required configuration parameters.

    Args:
        configuration: Configuration dictionary to validate

    Raises:
        ValueError: If any required configuration is missing
    """
    if not configuration.get("client_id"):
        log.error("client_id not found in configuration")
        raise ValueError("client_id is required in configuration")

    if not configuration.get("client_secret"):
        log.error("client_secret not found in configuration")
        raise ValueError("client_secret is required in configuration")

    if not configuration.get("initial_start_date"):
        log.error("initial_start_date not found in configuration")
        raise ValueError("initial_start_date is required in configuration")


def parse_configuration(configuration: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse and validate configuration values.

    Args:
        configuration: Configuration dictionary

    Returns:
        Dictionary with keys:
            - client_id: Client ID string
            - client_secret: Client secret string
            - initial_start_date: ISO format date string
            - max_records: Integer or None (no limit)
            - increment_days: Integer (default 10)
            - max_threads: Integer (default 8)
            - max_polls: Integer (default 60)
            - email_recipients: List of email strings
            - data_types: List of data type strings
            - test_job_id: String or None (optional, for testing mode)
    """
    # Parse max_records (None means no limit)
    max_records = None
    max_records_str = configuration.get("max_records", "")
    if max_records_str:
        try:
            max_records = int(max_records_str)
            if max_records <= 0:
                max_records = None
        except ValueError:
            log.warning(f"Invalid max_records value: {max_records_str}")
            max_records = None

    # Parse increment_days (default to 10)
    increment_days = 10
    increment_days_str = configuration.get("increment_days", "10")
    if increment_days_str:
        try:
            increment_days = int(increment_days_str)
            if increment_days <= 0:
                log.warning(
                    f"Invalid increment_days value: {increment_days_str}, " f"using default: 10"
                )
                increment_days = 10
        except ValueError:
            log.warning(
                f"Invalid increment_days value: {increment_days_str}, " f"using default: 10"
            )
            increment_days = 10

    # Parse max_threads (default to 8)
    max_threads = 8
    max_threads_str = configuration.get("max_threads", "8")
    if max_threads_str:
        try:
            max_threads = int(max_threads_str)
            if max_threads <= 0 or max_threads > 16:
                log.warning(
                    f"Invalid max_threads value: {max_threads_str}, "
                    f"using default: 8 (valid range: 1-16)"
                )
                max_threads = 8
        except ValueError:
            log.warning(f"Invalid max_threads value: {max_threads_str}, " f"using default: 8")
            max_threads = 8

    # Parse max_polls (default to 60)
    max_polls = 60
    max_polls_str = configuration.get("max_polls", "60")
    if max_polls_str:
        try:
            max_polls = int(max_polls_str)
            if max_polls <= 0:
                log.warning(f"Invalid max_polls value: {max_polls_str}, " f"using default: 60")
                max_polls = 60
        except ValueError:
            log.warning(f"Invalid max_polls value: {max_polls_str}, " f"using default: 60")
            max_polls = 60

    # Parse email recipients
    email_recipients = []
    email_recipients_str = configuration.get("email_recipients", "")
    if email_recipients_str:
        email_recipients = [
            email.strip() for email in email_recipients_str.split(",") if email.strip()
        ]

    # Parse data types
    data_types_str = configuration.get("data_types", "Contacts")
    data_types = [dt.strip() for dt in data_types_str.split(",") if dt.strip()]

    # Parse test_job_id (optional, for testing with existing jobs)
    test_job_id = configuration.get("test_job_id", "").strip()
    if test_job_id:
        log.info(f"TESTING MODE: Using existing job ID: {test_job_id}")

    return {
        "client_id": configuration.get("client_id"),
        "client_secret": configuration.get("client_secret"),
        "initial_start_date": configuration.get("initial_start_date"),
        "max_records": max_records,
        "increment_days": increment_days,
        "max_threads": max_threads,
        "max_polls": max_polls,
        "email_recipients": email_recipients,
        "data_types": data_types,
        "test_job_id": test_job_id if test_job_id else None,
    }
