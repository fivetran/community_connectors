"""
This connector syncs report data from Amazon Video Central API to the Fivetran destination.
It supports OAuth 2.0 authentication with automatic refresh token rotation, limit/offset pagination,
and ZIP-compressed CSV report files. Tables are created dynamically based on report group,
channel or studio, territory, and report type.

The connector distinguishes between an initial sync and incremental syncs. During the initial sync,
it checkpoints after each channel/studio so that a failed sync can resume from the last saved
position rather than starting over.

See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.
"""

# For reading configuration from a JSON file
import json

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like upsert(), update(), delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# For making HTTP requests to the Amazon Video Central API
import requests

# For handling ZIP archives containing CSV report files
import zipfile

# For parsing CSV report data
import csv

# For in-memory byte and string streams
import io

# For generating and formatting sync timestamps
from datetime import datetime, timezone

# For type hints
from typing import Dict, List, Any, Optional, Callable

# For exponential backoff delays
import time

# For Fernet symmetric encryption of refresh tokens
from cryptography.fernet import Fernet

# For base64 encoding/decoding of encrypted tokens
import base64

# For cleaning table names
import re

__TOKEN_URL = "https://api.amazon.co.uk/auth/o2/token"
__BASE_URL = "https://videocentral.amazon.com/apis/v1"
__DEFAULT_MAX_RETRIES = 5
__DEFAULT_BASE_DELAY_SECONDS = 30
__REQUEST_TIMEOUT = 30
__PAGE_SIZE = 50


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters with correct values.
    This function is called at the start of the update method to ensure that the connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing, empty, or invalid.
    """
    required_configs = [
        "refresh_token",
        "client_id",
        "client_secret",
        "account_id",
        "report_group_ids",
        "initial_sync_start",
        "fernet_key",
    ]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")
        if not str(configuration[key]).strip():
            raise ValueError(f"Configuration value for '{key}' must not be empty")

    # Validate report_group_ids contains at least one non-blank entry
    report_groups = [
        rg.strip() for rg in configuration["report_group_ids"].split(",") if rg.strip()
    ]
    if not report_groups:
        raise ValueError(
            "Configuration value for 'report_group_ids' must contain at least one report group ID"
        )

    # Validate initial_sync_start is a valid ISO-8601 timestamp
    try:
        datetime.fromisoformat(configuration["initial_sync_start"].replace("Z", "+00:00"))
    except ValueError:
        raise ValueError(
            "Configuration value for 'initial_sync_start' must be a valid ISO-8601 timestamp (e.g. 2020-01-01T00:00:00.000Z)"
        )

    # Validate fernet_key is a valid Fernet key
    try:
        Fernet(configuration["fernet_key"].encode())
    except (ValueError, TypeError):
        raise ValueError(
            "Configuration value for 'fernet_key' must be a valid base64-encoded Fernet key"
        )


def encrypt_token(token: str, fernet_key: str) -> str:
    """
    Encrypt a token using Fernet encryption.

    Args:
        token: The token to encrypt
        fernet_key: Base64-encoded Fernet key

    Returns:
        Encrypted token as base64 string
    """
    try:
        # Fernet constructor expects the base64-encoded string directly
        f = Fernet(fernet_key.encode())

        # Encrypt the token
        encrypted_token = f.encrypt(token.encode())

        # Return as base64 string for safe storage
        return base64.urlsafe_b64encode(encrypted_token).decode()
    except Exception as e:
        log.error(f"Failed to encrypt token: {str(e)}")
        raise RuntimeError(f"Token encryption failed: {str(e)}")


def decrypt_token(encrypted_token: str, fernet_key: str) -> str:
    """
    Decrypt a token using Fernet encryption.

    Args:
        encrypted_token: Base64-encoded encrypted token
        fernet_key: Base64-encoded Fernet key

    Returns:
        Decrypted token as string
    """
    try:
        # Fernet constructor expects the base64-encoded string directly
        f = Fernet(fernet_key.encode())

        # Decode the encrypted token from base64
        encrypted_bytes = base64.urlsafe_b64decode(encrypted_token.encode())

        # Decrypt the token
        decrypted_token = f.decrypt(encrypted_bytes)

        return decrypted_token.decode()
    except Exception as e:
        log.error(f"Failed to decrypt token: {str(e)}")
        raise RuntimeError(f"Token decryption failed: {str(e)}")


def retry_with_backoff(
    func: Callable,
    max_retries: int = __DEFAULT_MAX_RETRIES,
    base_delay: int = __DEFAULT_BASE_DELAY_SECONDS,
    operation_name: str = "operation",
) -> Any:
    """
    Execute a function with exponential backoff retry logic for 429 errors.

    Args:
        func: Function to execute that returns a requests.Response object
        max_retries: Maximum number of retry attempts (default: 5)
        base_delay: Base delay in seconds for exponential backoff (default: 30)
        operation_name: Name of the operation for logging purposes

    Returns:
        The successful response from the function

    Raises:
        Exception: When max retries exceeded or non-retryable error occurs
    """
    for attempt in range(max_retries + 1):
        response = func()

        # Retry on rate limiting (429) and server errors (5xx)
        if response.status_code in [429, 500, 502, 503, 504]:
            if attempt < max_retries:
                delay = min(
                    base_delay * (2**attempt), 300
                )  # Exponential backoff capped at 5 minutes
                log.warning(
                    f"Retrying {operation_name} (status {response.status_code}) in {delay} seconds (attempt {attempt + 1}/{max_retries + 1})"
                )
                time.sleep(delay)
                continue
            else:
                log.error(
                    f"Max retries exceeded for {operation_name} after {max_retries + 1} attempts (status {response.status_code})"
                )
                raise RuntimeError(
                    f"Max retries exceeded for {operation_name} - status {response.status_code}"
                )

        # Return the response for the calling function to handle
        return response

    # This should never be reached, but included for completeness
    raise RuntimeError(f"Unexpected error in {operation_name} retry logic")


def _post_token_request(data: dict, headers: dict) -> requests.Response:
    """
    Send the OAuth token refresh request to the Amazon token endpoint.

    Args:
        data: Form-encoded request body containing token refresh parameters.
        headers: HTTP headers to include with the token request.

    Returns:
        The HTTP response returned by the token endpoint.
    """
    return requests.post(__TOKEN_URL, data=data, headers=headers, timeout=__REQUEST_TIMEOUT)


def _get_api_request(url: str, headers: dict, params: Optional[Dict]) -> requests.Response:
    """
    Send a GET request to an Amazon Video Central API endpoint.

    Args:
        url: The API endpoint URL to request.
        headers: HTTP headers to include with the API request.
        params: Optional query parameters to include in the request.

    Returns:
        The HTTP response returned by the API endpoint.
    """
    return requests.get(url, headers=headers, params=params, timeout=__REQUEST_TIMEOUT)


def _get_download_request(download_url: str) -> requests.Response:
    """
    Download a report file from the provided URL.

    Args:
        download_url: The fully qualified URL for the report download.

    Returns:
        The HTTP response containing the downloadable file content.
    """
    return requests.get(download_url, timeout=__REQUEST_TIMEOUT)


def get_access_token(configuration: Dict[str, Any], state: Dict[str, Any]) -> tuple[str, str]:
    """
    Get a new access token using the refresh token with retry logic for rate limiting.

    Args:
        configuration: Configuration dictionary containing auth credentials
        state: Current sync state (may contain updated refresh token)

    Returns:
        Tuple of (access_token, new_refresh_token)
    """
    # Get Fernet key from configuration
    fernet_key = configuration["fernet_key"]

    # Use refresh token from state if available, otherwise from configuration
    if "encrypted_refresh_token" in state:
        # Token is encrypted, decrypt it
        try:
            refresh_token = decrypt_token(state["encrypted_refresh_token"], fernet_key)
        except RuntimeError as e:
            # If decryption fails (e.g., due to key change), fall back to configuration
            log.warning(f"Failed to decrypt refresh token from state: {str(e)}")
            log.info("Falling back to refresh token from configuration")
            refresh_token = configuration["refresh_token"]
    elif "refresh_token" in state:
        # Token is not encrypted (backward compatibility)
        refresh_token = state["refresh_token"]
    else:
        # Use refresh token from configuration (first time or fallback)
        refresh_token = configuration["refresh_token"]

    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": configuration["client_id"],
        "client_secret": configuration["client_secret"],
    }

    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    log.info("Refreshing access token")

    response = retry_with_backoff(
        lambda: _post_token_request(data, headers), operation_name="token refresh"
    )

    if response.status_code != 200:
        log.error(f"Failed to refresh token: {response.status_code} - {response.text}")
        raise RuntimeError(f"Token refresh failed: {response.status_code}")

    token_data = response.json()
    return token_data["access_token"], token_data["refresh_token"]


def make_api_request(
    url: str,
    access_token: str,
    params: Optional[Dict] = None,
) -> Dict[str, Any]:
    """
    Make an authenticated API request to Amazon Video Central with retry logic for rate limiting.

    Args:
        url: API endpoint URL
        access_token: Valid access token
        params: Optional query parameters

    Returns:
        JSON response data
    """
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

    response = retry_with_backoff(
        lambda: _get_api_request(url, headers, params), operation_name="API request"
    )

    if response.status_code == 401:
        log.error("Access token expired or invalid")
        raise RuntimeError("Authentication failed - token may be expired")
    elif response.status_code != 200:
        log.error(f"API request failed: {response.status_code} - {response.text}")
        raise RuntimeError(f"API request failed: {response.status_code}")

    return response.json()


def paginated_request(
    base_url: str,
    access_token: str,
    params: Optional[Dict] = None,
) -> List[Dict]:
    """
    Handle paginated API requests and return all results.
    Pagination uses limit/offset parameters, advancing until the API returns no 'next' field or an empty data array.

    Args:
        base_url: Base API endpoint URL
        access_token: Valid access token
        params: Optional query parameters

    Returns:
        List of all items from all pages
    """
    all_items = []
    offset = 0
    limit = __PAGE_SIZE

    if params is None:
        params = {}

    while True:
        current_params = params.copy()
        current_params.update({"limit": limit, "offset": offset})

        response_data = make_api_request(base_url, access_token, current_params)

        items = response_data.get("data", [])
        all_items.extend(items)

        # Check if there are more pages; stop if no 'next' field or empty page returned
        if not response_data.get("next") or len(items) == 0:
            break

        offset += limit

    return all_items


def download_and_parse_csv(download_url: str) -> List[Dict[str, Any]]:
    """
    Download a ZIP file from a pre-signed URL and parse the CSV inside with retry logic for rate limiting.

    Args:
        download_url: Pre-signed S3 URL to download ZIP file

    Returns:
        List of CSV records as dictionaries
    """

    try:
        response = retry_with_backoff(
            lambda: _get_download_request(download_url), operation_name="file download"
        )

        if response.status_code != 200:
            log.error(f"Failed to download file: {response.status_code}")
            raise RuntimeError(f"File download failed with status code: {response.status_code}")
    except Exception as e:
        # If retry exceeded, raise runtime error for Fivetran compliance
        log.error(f"File download failed after retries: {str(e)}")
        raise RuntimeError(f"File download failed after retries: {str(e)}")

    try:
        # Extract ZIP file
        with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
            # Get the first CSV file in the ZIP
            csv_files = [f for f in zip_file.namelist() if f.endswith(".csv")]

            if not csv_files:
                log.error("No CSV files found in ZIP archive")
                raise RuntimeError("No CSV files found in ZIP archive")

            csv_filename = csv_files[0]

            # Read and parse CSV
            with zip_file.open(csv_filename) as csv_file:
                csv_content = csv_file.read().decode("utf-8")

            # Parse CSV using DictReader
            csv_reader = csv.DictReader(io.StringIO(csv_content))
            records = []

            for row in csv_reader:
                # Add the CSV filename to each record
                row["csv_filename"] = csv_filename
                records.append(row)

            return records

    except Exception as e:
        log.error(f"Error processing ZIP file: {str(e)}")
        raise RuntimeError(f"Error processing ZIP file: {str(e)}")


def clean_table_name(name: str) -> str:
    """
    Clean a string to be used as a table name.
    Replaces special characters with underscores, collapses consecutive underscores, and lowercases the result.

    Args:
        name: Raw string to clean

    Returns:
        Cleaned table name
    """
    # Replace spaces and special characters with underscores
    cleaned = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    # Remove multiple consecutive underscores
    cleaned = re.sub(r"_+", "_", cleaned)
    # Remove leading/trailing underscores
    cleaned = cleaned.strip("_")
    # Convert to lowercase
    return cleaned.lower()


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

    log.warning("Example: Source Examples - Amazon Video Central")

    # Validate the configuration to ensure it contains all required values.
    validate_configuration(configuration=configuration)
    # Set sync time at the beginning to prevent data gaps
    current_sync_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4] + "Z"

    # Initialize summary counters
    total_reports_processed = 0
    total_csv_records = 0
    total_channels_processed = 0

    # Extract configuration parameters
    account_id = configuration["account_id"]
    report_group_ids = [
        rg.strip() for rg in configuration["report_group_ids"].split(",") if rg.strip()
    ]
    initial_sync_start = configuration["initial_sync_start"]

    # Get the state variable for the sync, if needed.
    # For the first sync, state will be an empty dictionary.
    # last_sync_time tracks the timestamp used as the lower bound for incremental report fetches.
    last_sync_time = state.get("last_sync_time", initial_sync_start)
    processed_report_groups = set(state.get("report_groups_processed", []))

    # Determine if this is an initial sync (no last_sync_time in state OR initial sync in progress)
    is_initial_sync = "last_sync_time" not in state or state.get("initial_sync_in_progress", False)

    # Get processed channels for initial sync progress tracking
    processed_channels = set(state.get("processed_channels", []))

    # Determine which report groups are new
    config_report_groups = set(report_group_ids)
    new_report_groups = config_report_groups - processed_report_groups
    existing_report_groups = config_report_groups & processed_report_groups

    if new_report_groups:
        log.info(
            f"Found {len(new_report_groups)} new report groups: {', '.join(sorted(new_report_groups))}"
        )
        log.info(f"New report groups will sync from: {initial_sync_start}")

    if existing_report_groups:
        log.info(
            f"Found {len(existing_report_groups)} existing report groups: {', '.join(sorted(existing_report_groups))}"
        )

    # Log sync mode
    if is_initial_sync:
        log.info("Running in INITIAL SYNC mode - will checkpoint after each channel/studio")
    else:
        log.info("Running in INCREMENTAL SYNC mode - will checkpoint after all reports")

    # Get access token and new refresh token
    access_token, new_refresh_token = get_access_token(configuration, state)

    # Process each report group
    for report_group_id in report_group_ids:
        report_group_id = report_group_id.strip()

        # Determine sync time for this report group
        if is_initial_sync:
            # During initial sync (including interrupted), always use initial_sync_start
            sync_time_for_group = initial_sync_start
            log.info(
                f"Processing report group: {report_group_id} (INITIAL SYNC from {sync_time_for_group})"
            )
        elif report_group_id in new_report_groups:
            # New report group in incremental sync
            sync_time_for_group = initial_sync_start
            log.info(
                f"Processing NEW report group: {report_group_id} (syncing from {sync_time_for_group})"
            )
        else:
            # Existing report group in incremental sync
            sync_time_for_group = last_sync_time
            log.info(
                f"Processing existing report group: {report_group_id} (syncing from {sync_time_for_group})"
            )

        # Get channels and studios
        channels_url = f"{__BASE_URL}/accounts/{account_id}/{report_group_id}"
        channels_studios = paginated_request(channels_url, access_token)

        log.info(
            f"Found {len(channels_studios)} channels/studios in report group: {report_group_id}"
        )

        # Process each channel/studio
        for channel_studio in channels_studios:
            channel_studio_id = channel_studio["id"]
            channel_studio_name = channel_studio["name"]
            territory = channel_studio.get("territory", "unknown")
            total_channels_processed += 1

            # Create unique channel identifier for progress tracking
            channel_key = f"{report_group_id}_{channel_studio_id}"

            # Skip if already processed in initial sync mode
            if is_initial_sync and channel_key in processed_channels:
                log.info(
                    f"Skipping already processed channel/studio: {channel_studio_name} ({territory})"
                )
                continue

            # Get report types for this channel/studio
            report_types_url = f"{__BASE_URL}/accounts/{account_id}/{report_group_id}/{channel_studio_id}/reportTypes"
            report_types = paginated_request(report_types_url, access_token)

            # Process each report type
            for report_type in report_types:
                report_type_id = report_type["id"]
                report_type_name = report_type["name"]
                cadence = report_type["cadence"]

                # Get reports for this report type using the appropriate sync time
                reports_url = f"{__BASE_URL}/accounts/{account_id}/{report_group_id}/{channel_studio_id}/reportTypes/{report_type_id}/reports"
                reports_params = {"modifiedDateGte": sync_time_for_group}

                # Make initial API call to check total count
                initial_response = make_api_request(
                    reports_url,
                    access_token,
                    {**reports_params, "limit": 1, "offset": 0},
                )
                total_reports = initial_response.get("total", 0)

                if total_reports == 0:
                    log.info(
                        f"No reports for {report_type_name} ({cadence}) in {report_group_id}/{channel_studio_name} ({territory})"
                    )
                    continue

                log.info(
                    f"Processing {total_reports} reports for {report_type_name} ({cadence}) in {report_group_id}/{channel_studio_name} ({territory})"
                )

                # If there are reports, get all of them
                reports = paginated_request(reports_url, access_token, reports_params)

                # Create table name from report group, channel name, territory, report type, and cadence
                table_name = clean_table_name(
                    f"{report_group_id}_{channel_studio_name}_{territory}_{report_type_name}_{cadence}"
                )

                # Process each report
                missing_urls_count = 0
                for report in reports:
                    total_reports_processed += 1
                    download_url = report.get("downloadUrl")
                    if not download_url:
                        missing_urls_count += 1
                        log.warning(
                            f"Report missing download URL - Report: {report.get('name', 'unknown')}, Date: {report.get('reportDateBegin', 'unknown')}, Modified: {report.get('modifiedDate', 'unknown')}, Rows: {report.get('numRows', 0)}"
                        )
                        continue

                    # Create unique report ID from components
                    report_id = f"{report_group_id}_{channel_studio_id}_{report_type_id}_{report.get('reportDateBegin', 'unknown')}"

                    # Create report metadata record
                    report_metadata = {
                        "report_id": report_id,
                        "report_group_id": report_group_id,
                        "channel_studio_id": channel_studio_id,
                        "channel_studio_name": channel_studio_name,
                        "channel_studio_territory": channel_studio.get("territory", ""),
                        "report_type_id": report_type_id,
                        "report_type_name": report_type_name,
                        "report_cadence": cadence,
                        "report_date_begin": report.get("reportDateBegin", ""),
                        "report_date_end": report.get("reportDateEnd", ""),
                        "report_modified_date": report.get("modifiedDate", ""),
                        "report_num_rows": report.get("numRows", 0),
                        "download_url": download_url,
                    }

                    # The 'upsert' operation is used to insert or update data in the destination table.
                    # The first argument is the name of the destination table.
                    # The second argument is a dictionary containing the record to be upserted.
                    op.upsert(table="report_metadata", data=report_metadata)

                    # Download and parse CSV data
                    csv_records = download_and_parse_csv(download_url)

                    for record in csv_records:
                        # Add report_id to link back to metadata
                        record["report_id"] = report_id
                        # The 'upsert' operation is used to insert or update data in the destination table.
                        # The first argument is the name of the destination table.
                        # The second argument is a dictionary containing the record to be upserted.
                        op.upsert(table=table_name, data=record)
                        total_csv_records += 1

                # Log summary if there were missing URLs
                if missing_urls_count > 0:
                    log.warning(
                        f"Completed processing {len(reports) - missing_urls_count}/{len(reports)} reports for {report_type_name} ({cadence}) in {report_group_id}/{channel_studio_name} ({territory}) ({missing_urls_count} missing download URLs)"
                    )

            # Checkpoint after each channel/studio in initial sync mode
            if is_initial_sync:
                # Add this channel to processed list
                processed_channels.add(channel_key)

                territory = channel_studio.get("territory", "unknown")
                log.info(
                    f"Checkpointing progress after {report_group_id}/{channel_studio_name} ({territory})"
                )

                checkpoint_state = {
                    "last_sync_time": current_sync_time,
                    "encrypted_refresh_token": encrypt_token(
                        new_refresh_token, configuration["fernet_key"]
                    ),
                    "report_groups_processed": report_group_ids,
                    "processed_channels": list(processed_channels),
                    "initial_sync_in_progress": True,
                }

                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
                # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
                op.checkpoint(state=checkpoint_state)

    # Final checkpoint - different behavior for initial vs incremental sync
    fernet_key = configuration["fernet_key"]
    encrypted_refresh_token = encrypt_token(new_refresh_token, fernet_key)

    if is_initial_sync:
        # Initial sync complete: clear processed_channels and initial_sync_in_progress from state
        log.info(
            "Initial sync completed - clearing processed channels and initial sync flag from state"
        )
        new_state = {
            "last_sync_time": current_sync_time,
            "encrypted_refresh_token": encrypted_refresh_token,
            "report_groups_processed": report_group_ids,
            # Note: processed_channels and initial_sync_in_progress are not included, so they will be cleared
        }
    else:
        # Incremental sync: standard final checkpoint
        new_state = {
            "last_sync_time": current_sync_time,
            "encrypted_refresh_token": encrypted_refresh_token,
            "report_groups_processed": report_group_ids,
        }

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
    # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
    op.checkpoint(state=new_state)

    log.info(
        f"Sync completed successfully - Processed {total_channels_processed} channels, {total_reports_processed} reports, {total_csv_records} CSV records"
    )


connector = Connector(update=update)

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
