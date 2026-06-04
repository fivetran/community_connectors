"""
Sync orchestration and job polling logic for CallMiner exports.
"""

import time
from typing import Dict, Any, Tuple
from datetime import datetime, timedelta
from fivetran_connector_sdk import Operations as op, Logging as log
from api_client import create_job, get_jobs_history, check_job_status, delete_job
from file_processing import download_and_stream_file, process_multi_type_zip_file
from state import get_data_type_state, update_data_type_state
from auth import refresh_token_if_needed


def determine_sync_strategy(
    state: Dict[str, Any], data_type: str, threshold_hours: int, initial_start_date_str: str
) -> Tuple[bool, datetime, int]:
    """
    Determine sync strategy based on state and time gap for a specific data type.

    Args:
        state: Current state dictionary
        data_type: Data type to check state for
        threshold_hours: Threshold for using LastNHours vs date range
        initial_start_date_str: Initial start date from configuration

    Returns:
        Tuple of (use_last_n_hours, current_start, last_n_hours)
    """
    current_time = datetime.utcnow()
    dt_state = get_data_type_state(state, data_type)

    if dt_state.get("last_synced_date"):
        last_synced = datetime.strptime(dt_state["last_synced_date"], "%Y-%m-%dT%H:%M:%S.%fZ")

        # Calculate hours since last sync
        hours_diff = (current_time - last_synced).total_seconds() / 3600

        # If gap is small enough, use LastNHours for efficiency
        if hours_diff <= threshold_hours:
            last_n_hours = int(hours_diff) + 1  # Round up to ensure overlap

            log.info(
                f"[{data_type}] Recent sync detected: {hours_diff:.2f} hours "
                f"since last sync ({dt_state['last_synced_date']}), "
                f"using LastNHours={last_n_hours} "
                f"(threshold: {threshold_hours} hours)"
            )

            return True, current_time, last_n_hours
        else:
            # Gap too large - use incremental date range logic
            log.info(
                f"[{data_type}] Large gap detected: {hours_diff:.2f} hours "
                f"since last sync ({dt_state['last_synced_date']}), "
                f"exceeds threshold of {threshold_hours} hours. "
                f"Using incremental date range logic."
            )
            return False, last_synced, 0
    else:
        # No state - initial sync
        log.info(f"[{data_type}] No previous sync detected. Starting initial sync.")
        current_start = datetime.strptime(initial_start_date_str, "%Y-%m-%dT%H:%M:%S.%fZ")
        return False, current_start, 0


def handle_completed_job(
    job_id: str,
    data_types_str: str,
    download_endpoint: str,
    bearer_token: str,
    state: Dict[str, Any],
    max_records: int = None,
    max_threads: int = 8,
):
    """
    Download and process a completed job.

    Args:
        job_id: Job ID
        data_types_str: Comma-separated data types string
        download_endpoint: Endpoint URL for downloading
        bearer_token: Bearer token for authentication
        state: State dictionary for checkpointing
        max_records: Maximum records to process (None for no limit)
        max_threads: Maximum number of threads for parallel processing
    """
    log.info(f"Job completed, downloading file for: {data_types_str}")

    file_stream = download_and_stream_file(download_endpoint, bearer_token)

    try:
        process_multi_type_zip_file(
            file_stream, job_id, data_types_str, state, max_records, max_threads
        )
        log.info(f"Sync completed successfully for: {data_types_str}")
    finally:
        file_stream.close()


def poll_and_process_single_job(
    job_id: str,
    data_types_str: str,
    client_id: str,
    client_secret: str,
    bearer_token: str,
    token_expires_at: datetime,
    state: Dict[str, Any],
    start_date: str = None,
    end_date: str = None,
    max_records: int = None,
    max_threads: int = 8,
    max_polls: int = 60,
    poll_interval: int = 60,
):
    """
    Poll a single job and process when complete.

    Args:
        job_id: The job ID to poll
        data_types_str: Comma-separated data types string
        client_id: Client ID for authentication
        client_secret: Client secret for authentication
        bearer_token: Bearer token for authentication
        token_expires_at: When current token expires
        state: State dictionary
        start_date: Start date of the job period (for resume logic)
        end_date: End date of the job period (for resume logic)
        max_records: Maximum records to process per file (None for no limit)
        max_threads: Maximum number of threads for parallel processing
        max_polls: Maximum number of polling attempts
        poll_interval: Seconds between polls

    Returns:
        Tuple of (updated_bearer_token, updated_expiration, job_id)
    """
    log.info(f"Polling job {job_id} for: {data_types_str}")

    for poll_count in range(max_polls):
        # Refresh token if needed before each poll
        bearer_token, token_expires_at = refresh_token_if_needed(
            client_id, client_secret, bearer_token, token_expires_at
        )

        log.info(f"Polling attempt {poll_count + 1}/{max_polls}")

        # Fetch jobs history
        jobs_history = get_jobs_history(bearer_token)

        # Check job status
        job_status = check_job_status(job_id, jobs_history=jobs_history)

        if not job_status["found"]:
            log.warning(f"Job {job_id} not found in history yet")
            time.sleep(poll_interval)
            continue

        status = job_status["status"]
        log.info(f"Job status: {status}")

        if status == "Completed":
            download_endpoint = job_status.get("download_endpoint")

            if not download_endpoint:
                log.error("Job completed but no download endpoint")
                raise ValueError(f"No download endpoint for completed job {job_id}")

            # Download and process the completed job
            log.info("Job completed, downloading and processing...")
            handle_completed_job(
                job_id,
                data_types_str,
                download_endpoint,
                bearer_token,
                state,
                max_records,
                max_threads,
            )

            log.info(f"Successfully processed job {job_id}")
            return bearer_token, token_expires_at, job_id

        elif status == "Failed":
            log.error(f"Job {job_id} failed")
            raise ValueError(f"Export job failed: {job_id}")

        # Job still processing, wait before next poll
        time.sleep(poll_interval)

    # Job didn't complete within timeout - store in state for resume
    log.error(
        f"Job {job_id} did not complete within timeout "
        f"({max_polls * poll_interval / 60:.0f} minutes)"
    )

    # Store job info in state to resume on next sync
    state["pending_job"] = {
        "job_id": job_id,
        "data_types": data_types_str,
        "start_date": start_date,
        "end_date": end_date,
        "created_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
    # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
    op.checkpoint(state=state)

    raise TimeoutError(
        f"Job {job_id} timed out after "
        f"{max_polls * poll_interval / 60:.0f} minutes. "
        f"Stored in state for resume on next sync."
    )


def sync_with_last_n_hours(
    client_id: str,
    client_secret: str,
    bearer_token: str,
    token_expires_at: datetime,
    data_type,  # Can be str or list of str
    last_n_hours: int,
    email_recipients: list,
    max_records: int,
    max_threads: int,
    max_polls: int,
    state: Dict[str, Any],
) -> Tuple[str, datetime]:
    """
    Sync using LastNHours strategy for recent data.

    Args:
        client_id: Client ID for authentication
        client_secret: Client secret for authentication
        bearer_token: Bearer token for authentication
        token_expires_at: When current token expires
        data_type: Data type string or list of data types to export
        last_n_hours: Number of hours to look back
        email_recipients: List of email addresses for notifications
        max_records: Maximum records to process (None for no limit)
        max_threads: Maximum number of threads for parallel processing
        max_polls: Maximum number of polling attempts
        state: State dictionary to update

    Returns:
        Tuple of (updated_bearer_token, updated_expiration)
    """
    # Refresh token if needed
    bearer_token, token_expires_at = refresh_token_if_needed(
        client_id, client_secret, bearer_token, token_expires_at
    )

    # Normalize to list
    data_types = [data_type] if isinstance(data_type, str) else data_type

    # Create job using LastNHours
    job_response = create_job(
        bearer_token=bearer_token,
        last_n_hours=last_n_hours,
        data_types=data_types,
        email_recipients=email_recipients,
    )
    job_id = job_response["job_id"]
    log.info(
        f"[{', '.join(data_types)}] Job created with ID: {job_id} " f"(LastNHours={last_n_hours})"
    )

    # Calculate approximate date range for state tracking
    current_time = datetime.utcnow()
    end_date_str = current_time.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    start_time = current_time - timedelta(hours=last_n_hours)
    start_date_str = start_time.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    # Poll and process this job
    result = poll_and_process_single_job(
        job_id=job_id,
        data_types_str=",".join(data_types),
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
        token_expires_at=token_expires_at,
        state=state,
        start_date=start_date_str,
        end_date=end_date_str,
        max_records=max_records,
        max_threads=max_threads,
        max_polls=max_polls,
        poll_interval=60,
    )
    bearer_token, token_expires_at, processed_job_id = result

    # Update state for all data types in this batch
    sync_timestamp = end_date_str

    for dt in data_types:
        update_data_type_state(state, dt, sync_timestamp)

    # Checkpoint after sync
    op.checkpoint(state=state)

    # Delete job after checkpoint
    delete_job(processed_job_id, bearer_token)

    log.info(f"[{', '.join(data_types)}] Successfully synced using LastNHours")

    return bearer_token, token_expires_at


def sync_incremental_periods(
    client_id: str,
    client_secret: str,
    bearer_token: str,
    token_expires_at: datetime,
    data_type,  # Can be str or list of str
    current_start: datetime,
    increment_days: int,
    email_recipients: list,
    max_records: int,
    max_threads: int,
    max_polls: int,
    state: Dict[str, Any],
) -> Tuple[str, datetime]:
    """
    Sync using incremental date range strategy.

    Args:
        client_id: Client ID for authentication
        client_secret: Client secret for authentication
        bearer_token: Bearer token for authentication
        token_expires_at: When current token expires
        data_type: Data type string or list of data types to export
        current_start: Start datetime for sync
        increment_days: Number of days per sync period
        email_recipients: List of email addresses for notifications
        max_records: Maximum records to process (None for no limit)
        max_threads: Maximum number of threads for parallel processing
        max_polls: Maximum number of polling attempts
        state: State dictionary to update

    Returns:
        Tuple of (updated_bearer_token, updated_expiration)
    """
    # Normalize to list
    data_types = [data_type] if isinstance(data_type, str) else data_type
    data_types_str = ", ".join(data_types)

    current_time = datetime.utcnow()

    log.info(
        f"[{data_types_str}] Starting from: " f"{current_start.strftime('%Y-%m-%dT%H:%M:%S.%fZ')}"
    )
    log.info(f"[{data_types_str}] Using {increment_days}-day sync periods")

    # Process data in configurable day increments
    period_count = 0

    while current_start < current_time:
        # Refresh token if needed before each job
        bearer_token, token_expires_at = refresh_token_if_needed(
            client_id, client_secret, bearer_token, token_expires_at
        )

        # Calculate end time for this period
        # End at 23:59:59 of the final day (subtract 1 to stay within range)
        current_end = current_start + timedelta(days=increment_days - 1)
        current_end = current_end.replace(hour=23, minute=59, second=59, microsecond=0)

        # Don't go beyond current time
        if current_end > current_time:
            current_end = current_time

        # Format dates for API
        start_date_str = current_start.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        end_date_str = current_end.strftime("%Y-%m-%dT%H:%M:%S.000Z")

        period_count += 1
        log.info(
            f"[{data_types_str}] Processing {increment_days}-day "
            f"period #{period_count}: {start_date_str} to {end_date_str}"
        )

        # Create job for this period
        job_response = create_job(
            bearer_token=bearer_token,
            start_date=start_date_str,
            end_date=end_date_str,
            data_types=data_types,
            email_recipients=email_recipients,
        )
        job_id = job_response["job_id"]
        log.info(f"[{data_types_str}] Job created with ID: {job_id}")

        # Poll and process this job
        result = poll_and_process_single_job(
            job_id=job_id,
            data_types_str=",".join(data_types),
            client_id=client_id,
            client_secret=client_secret,
            bearer_token=bearer_token,
            token_expires_at=token_expires_at,
            state=state,
            start_date=start_date_str,
            end_date=end_date_str,
            max_records=max_records,
            max_threads=max_threads,
            max_polls=max_polls,
            poll_interval=60,
        )
        bearer_token, token_expires_at, processed_job_id = result

        # Update state for all data types in this batch
        for dt in data_types:
            update_data_type_state(state, dt, end_date_str)

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
        # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
        op.checkpoint(state=state)

        # Delete job after checkpoint
        delete_job(processed_job_id, bearer_token)

        log.info(f"[{data_types_str}] Successfully synced period ending: " f"{end_date_str}")

        # Move to next period (start from next second after end)
        current_start = current_end + timedelta(seconds=1)

    log.info(
        f"[{data_types_str}] Incremental sync completed. "
        f"Processed {period_count} {increment_days}-day period(s)"
    )

    return bearer_token, token_expires_at
