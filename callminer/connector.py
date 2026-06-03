"""
CallMiner Connector for Fivetran SDK.

Main entry point for the connector that orchestrates authentication,
job creation, polling, and data synchronization.
"""

from typing import Dict, Any
from datetime import datetime, timedelta
# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like upsert(), update(), delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Import from local modules
from auth import get_token
from config import validate_configuration, parse_configuration
from state import update_data_type_state
from sync import (
    determine_sync_strategy,
    poll_and_process_single_job,
    sync_with_last_n_hours,
    sync_incremental_periods,
)
from api_client import delete_job


def schema(configuration: Dict[str, Any]):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {"table": "ai_summaries", "primary_key": ["contact_id"]},
        {"table": "comments", "primary_key": ["comment_id"]},
        {"table": "contacts", "primary_key": ["id"]},
        {"table": "categories", "primary_key": ["contact_id", "category_id", "section_id"]},
        {
            "table": "category_components",
            "primary_key": ["contact_id", "category_id", "component_id", "start_time"],
        },
        {"table": "events_delay", "primary_key": ["contact_id", "start_time", "end_time"]},
        {"table": "events_overtalk", "primary_key": ["contact_id", "start_time", "end_time"]},
        {"table": "events_redaction", "primary_key": ["contact_id", "start_time", "end_time"]},
        {"table": "events_silence", "primary_key": ["contact_id", "start_time", "end_time"]},
        {"table": "scores", "primary_key": ["contact_id", "score_id"]},
        {
            "table": "score_indicators",
            "primary_key": ["contact_id", "score_id", "score_component_id"],
        },
        {"table": "tags", "primary_key": ["contact_id", "tag_id"]},
        {"table": "transcripts", "primary_key": ["contact_id", "start_time"]},
    ]


def update(configuration: Dict[str, Any], state: Dict[str, Any]):
"""
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-code/connector-sdk-methods#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """
    log.warning("Example: API Connector : CallMiner Connector")

    # Validate configuration
    validate_configuration(configuration)

    # Parse configuration values
    config = parse_configuration(configuration)

    log.info(f"Processing data types: {', '.join(config['data_types'])}")

    # Get access token for this sync run
    bearer_token, token_expires_at = get_token(config["client_id"], config["client_secret"])

    # Check for pending job from previous run (resume if timed out)
    if "pending_job" in state:
        pending = state["pending_job"]
        log.info("=" * 60)
        log.info("RESUMING: Found pending job from previous run")
        log.info(f"Job ID: {pending['job_id']}")
        log.info(f"Data types: {pending['data_types']}")
        log.info(f"Date range: {pending.get('start_date')} " f"to {pending.get('end_date')}")
        log.info("=" * 60)

        try:
            result = poll_and_process_single_job(
                job_id=pending["job_id"],
                data_types_str=pending["data_types"],
                client_id=config["client_id"],
                client_secret=config["client_secret"],
                bearer_token=bearer_token,
                token_expires_at=token_expires_at,
                state=state,
                start_date=pending.get("start_date"),
                end_date=pending.get("end_date"),
                max_records=config["max_records"],
                max_threads=config["max_threads"],
                max_polls=config["max_polls"],
                poll_interval=60,
            )
            bearer_token, token_expires_at, processed_job_id = result

            # Clear pending job on success
            del state["pending_job"]

            # Update last_synced_date for each data type to the end_date
            # Add 1 second to move to the next period start
            if pending.get("end_date"):
                # Handle format: 2025-11-03T23:59:59.000Z or 2025-11-03T23:59:59Z
                end_date_str = pending["end_date"].replace(".000Z", "Z")
                end_dt = datetime.strptime(end_date_str, "%Y-%m-%dT%H:%M:%SZ")
                next_start = end_dt + timedelta(seconds=1)
                next_start_str = next_start.strftime("%Y-%m-%dT%H:%M:%S.000Z")

                data_type_list = pending["data_types"].split(",")
                for dt in data_type_list:
                    update_data_type_state(state, dt.strip(), next_start_str)
                log.info(f"Updated last_synced_date to {next_start_str} " f"for resumed job")

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
        # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
            op.checkpoint(state=state)

            # Delete job after checkpoint
            delete_job(processed_job_id, bearer_token)

            log.info("Successfully resumed and completed pending job")
            # DON'T return - continue with normal sync to catch up

        except TimeoutError:
            # Job still not done, keep it in state and exit
            log.error("Pending job still not complete after resuming")
            raise
        except Exception as e:
            # Other error - clear pending job and let it fail
            log.error(f"Error resuming pending job: {e}")
            if "pending_job" in state:
                del state["pending_job"]
            raise

    # TESTING MODE: If test_job_id is provided, skip job creation
    if config.get("test_job_id"):
        log.info("=" * 60)
        log.info("TESTING MODE: Using existing job ID")
        log.info("=" * 60)

        # Poll and process the test job
        result = poll_and_process_single_job(
            job_id=config["test_job_id"],
            data_types_str=",".join(config["data_types"]),
            client_id=config["client_id"],
            client_secret=config["client_secret"],
            bearer_token=bearer_token,
            token_expires_at=token_expires_at,
            state=state,
            max_records=config["max_records"],
            max_threads=config["max_threads"],
            max_polls=config["max_polls"],
            poll_interval=30,
        )
        bearer_token, token_expires_at, processed_job_id = result

        # Note: In testing mode, we don't delete the job
        # so you can test repeatedly
        log.info("Testing mode completed successfully " "(job not deleted for reuse)")
        return

    # Calculate threshold: increment_days converted to hours
    threshold_hours = config["increment_days"] * 24

    # Group data types by sync strategy
    # Key: (use_last_n_hours, start_timestamp, last_n_hours)
    sync_groups = {}

    for data_type in config["data_types"]:
        # Determine sync strategy for this data type
        use_last_n_hours, current_start, last_n_hours = determine_sync_strategy(
            state, data_type, threshold_hours, config["initial_start_date"]
        )

        # Create grouping key
        if use_last_n_hours:
            group_key = ("last_n_hours", None, last_n_hours)
        else:
            group_key = ("incremental", current_start.isoformat(), 0)

        if group_key not in sync_groups:
            sync_groups[group_key] = {
                "data_types": [],
                "use_last_n_hours": use_last_n_hours,
                "current_start": current_start,
                "last_n_hours": last_n_hours,
            }

        sync_groups[group_key]["data_types"].append(data_type)

    # Process each group
    for group_key, group_info in sync_groups.items():
        data_types = group_info["data_types"]
        log.info(f"Syncing batch of {len(data_types)} data type(s): " f"{', '.join(data_types)}")

        if group_info["use_last_n_hours"]:
            # Recent sync - use LastNHours for efficiency
            bearer_token, token_expires_at = sync_with_last_n_hours(
                client_id=config["client_id"],
                client_secret=config["client_secret"],
                bearer_token=bearer_token,
                token_expires_at=token_expires_at,
                data_type=data_types,  # Now accepts list
                last_n_hours=group_info["last_n_hours"],
                email_recipients=config["email_recipients"],
                max_records=config["max_records"],
                max_threads=config["max_threads"],
                max_polls=config["max_polls"],
                state=state,
            )
        else:
            # Initial sync or large gap - use incremental date range logic
            bearer_token, token_expires_at = sync_incremental_periods(
                client_id=config["client_id"],
                client_secret=config["client_secret"],
                bearer_token=bearer_token,
                token_expires_at=token_expires_at,
                data_type=data_types,  # Now accepts list
                current_start=group_info["current_start"],
                increment_days=config["increment_days"],
                email_recipients=config["email_recipients"],
                max_records=config["max_records"],
                max_threads=config["max_threads"],
                max_polls=config["max_polls"],
                state=state,
            )

        log.info(f"Completed sync for batch: {', '.join(data_types)}")


# Initialize connector
connector = Connector(update=update, schema=schema)


if __name__ == "__main__":
    connector.debug()
