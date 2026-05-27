"""
CallMiner API client functions for job management.
"""

import requests
from typing import Dict, Any
from fivetran_connector_sdk import Logging as log
from auth import retry_on_500_error


@retry_on_500_error(max_retries=3, initial_delay=1, backoff_factor=2)
def create_job(
    bearer_token: str,
    start_date: str = None,
    end_date: str = None,
    last_n_hours: int = None,
    data_types: list = None,
    email_recipients: list = None,
) -> Dict[str, Any]:
    """
    Create a new export job for contacts.

    Args:
        bearer_token: Bearer token for authentication
        start_date: Start date in ISO format (e.g., "2025-09-01T00:00:00.000Z")
        end_date: End date in ISO format (e.g., "2025-10-01T00:00:00.000Z")
        last_n_hours: Number of hours to look back (alternative to date range)
        data_types: List of data types to export (default: ["Contacts"])
        email_recipients: List of email addresses for notifications

    Returns:
        Dictionary containing:
            - job_id: The job ID
            - create_date: The job creation date
            - full_response: Complete API response
    """
    url = "https://api.callminer.net/bulkexport/api/export/job"

    headers = {"Authorization": f"Bearer {bearer_token}", "Content-Type": "application/json"}

    if data_types is None:
        data_types = ["Contacts"]

    if email_recipients is None:
        email_recipients = []

    # Generate descriptive name based on data types
    if len(data_types) == 1:
        job_name = f"{data_types[0]} Export"
    else:
        job_name = f"Bulk Export ({len(data_types)} data types)"

    # Build duration based on whether we're using LastNHours or custom date range
    if last_n_hours is not None:
        # Use LastNHours for subsequent syncs
        duration = {
            "LastNDays": None,
            "LastNHours": last_n_hours,
            "TimeFrame": None,
            "StartDate": None,
            "EndDate": None,
            "SearchMode": "NewAndUpdated",
        }
        log.info(f"Creating job for last {last_n_hours} hours")
    else:
        # Use custom date range for initial sync
        duration = {
            "LastNDays": None,
            "LastNHours": None,
            "TimeFrame": "Custom",
            "StartDate": start_date,
            "EndDate": end_date,
            "SearchMode": "NewAndUpdated",
        }
        log.info(f"Creating job for date range: {start_date} to {end_date}")

    payload = {
        "Name": job_name,
        "Duration": duration,
        "DataTypes": data_types,
        "SearchFilters": [],
        "NotificationMethod": "Email",
        "EmailRecipients": email_recipients,
    }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=(30, 60))
        response.raise_for_status()

        job_data = response.json()
        job_id = job_data.get("Id")
        create_date = job_data.get("CreateDate")

        if not job_id:
            log.severe(f"No job ID in response: {job_data}")
            raise ValueError("Failed to get job ID from response")

        log.info(f"Successfully created job with ID: {job_id}")

        return {"job_id": job_id, "create_date": create_date, "full_response": job_data}

    except requests.exceptions.RequestException as e:
        log.severe(f"Error creating job: {e}")
        raise


@retry_on_500_error(max_retries=3, initial_delay=1, backoff_factor=2)
def get_jobs_history(bearer_token: str) -> list:
    """
    Get the history of all jobs.

    Args:
        bearer_token: Bearer token for authentication

    Returns:
        List of job dictionaries containing job history
    """
    url = "https://api.callminer.net/bulkexport/api/export/history"

    headers = {"Authorization": f"Bearer {bearer_token}"}

    try:
        response = requests.get(url, headers=headers, timeout=(30, 60))
        response.raise_for_status()

        jobs = response.json()

        # If not a list, assume it's wrapped in an object
        if not isinstance(jobs, list):
            jobs = []

        log.info(f"Retrieved {len(jobs)} jobs from history")

        return jobs

    except requests.exceptions.RequestException as e:
        log.severe(f"Error getting jobs history: {e}")
        raise


@retry_on_500_error(max_retries=3, initial_delay=1, backoff_factor=2)
def delete_job(job_id: str, bearer_token: str) -> None:
    """
    Delete an export job.

    Args:
        job_id: The job ID to delete
        bearer_token: Bearer token for authentication
    """
    url = f"https://api.callminer.net/bulkexport/api/export/job/{job_id}"

    headers = {"Authorization": f"Bearer {bearer_token}"}

    log.info(f"Deleting job: {job_id}")

    try:
        response = requests.delete(url, headers=headers, timeout=(30, 60))
        response.raise_for_status()

        log.info(f"Successfully deleted job: {job_id}")

    except requests.exceptions.RequestException as e:
        log.severe(f"Error deleting job {job_id}: {e}")
        # Don't raise - deletion failure shouldn't fail the sync
        # Job will eventually expire on CallMiner's side


def check_job_status(
    job_id: str, bearer_token: str = None, jobs_history: list = None
) -> Dict[str, Any]:
    """
    Check the status of a job by searching the jobs history.

    Args:
        job_id: The ID of the job to check (matches ExportJobId in history)
        bearer_token: Bearer token for authentication
            (optional if jobs_history provided)
        jobs_history: Pre-fetched jobs history (optional, fetches if not provided)

    Returns:
        Dictionary containing job status information with keys:
            - status: Job status (e.g., "Completed", "Processing", "Failed")
            - download_endpoint: Download endpoint if job is completed (optional)
            - found: Boolean indicating if job was found in history
    """
    if jobs_history is None:
        if bearer_token is None:
            raise ValueError("Either jobs_history or bearer_token must be provided")
        jobs_history = get_jobs_history(bearer_token)

    for job in jobs_history:
        export_job_id = job.get("ExportJobId")

        if export_job_id == job_id:
            status = job.get("Status")
            download_endpoint = job.get("DownloadEndpoint")

            result = {"status": status, "found": True, "job_data": job}

            if download_endpoint:
                result["download_endpoint"] = download_endpoint

            return result

    return {"status": None, "found": False, "job_data": None}
