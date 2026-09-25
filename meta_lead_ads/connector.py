"""This connector demonstrates syncing Facebook (Meta) Lead Ads data into a single
leads table via the Meta Graph API. It supports incremental sync per lead-gen form,
using a created_time cursor that only advances once a form's pagination has fully
completed, since Meta's page ordering by created_time is not guaranteed.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

from __future__ import annotations

# For reading configuration from a JSON file when running this file directly
import json

# For resolving the local configuration.json path when running this file directly
import os
from typing import Any, Dict, List, Optional

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like upsert(), update(), delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# For building the Graph API base URL from the configured API version
from http_helpers import BASE_URL_TEMPLATE

# For page/form discovery and lead pagination against the Meta Graph API
import meta_helpers

# For validating and normalizing the raw configuration
import validator

# ---------------------------------------------------------------------------
# Configuration Contract
# ---------------------------------------------------------------------------
# Required keys in configuration.json:
#   system_user_access_token : str   (Meta system user token with pages_show_list, leads access)
#   page_ids                 : str   (Comma-separated list or "ALL" to auto-discover)
#   form_ids                 : str   (Comma-separated list or "ALL" to fetch all forms of each page)
#   initial_start_time       : str   (ISO8601 timestamp or date; used when no prior state exists)
#   graph_version            : str   (Optional override for the default Graph API version)
#   include_archived_forms   : str   ("true"/"false") Include archived forms
#   request_timeout_seconds  : str   (Default 30)


def _process_leads(
    page: Dict[str, Any],
    form: Dict[str, Any],
    leads: List[Dict[str, Any]],
    batch_max_created: Optional[str],
    current_max_cursor: Optional[str],
) -> tuple[int, Optional[str]]:
    """
    Transform and upsert a batch of leads, returning the row count and updated max cursor.
    Args:
        page: the page dictionary the leads' form belongs to.
        form: the lead-gen form dictionary the leads belong to.
        leads: the raw lead dictionaries returned by the Graph API for this page.
        batch_max_created: the maximum created_time seen in this batch, if any.
        current_max_cursor: the maximum created_time seen so far for this form.
    Returns:
        A tuple of (rows_written, new_max_cursor).
    """
    rows_written = 0
    for lead in leads:
        row = {
            "lead_id": lead.get("id"),
            "page_id": page.get("id"),
            "page_name": page.get("name"),
            "form_id": form.get("id"),
            "form_name": form.get("name"),
            "created_time": lead.get("created_time"),
            "ad_id": lead.get("ad_id"),
            "field_data": json.dumps(lead.get("field_data", []), ensure_ascii=False),
        }
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table="leads", data=row)
        rows_written += 1
    new_max_cursor = current_max_cursor
    if batch_max_created and (
        current_max_cursor is None or batch_max_created > current_max_cursor
    ):
        new_max_cursor = batch_max_created
    return rows_written, new_max_cursor


def _process_form(
    page: Dict[str, Any],
    form: Dict[str, Any],
    cfg: Dict[str, Any],
    forms_state: Dict[str, Dict[str, Optional[str]]],
) -> int:
    """
    Sync all leads for a single form, upserting each page and periodically flushing state.
    The form's resumable cursor only advances once its pagination fully completes, because
    Meta's Graph API does not guarantee that pages are returned in ascending created_time
    order; checkpointing a newer cursor mid-pagination could otherwise cause a later resume
    to skip leads on pages that had not been processed yet.
    Args:
        page: the page dictionary the form belongs to, including its page-scoped access token.
        form: the lead-gen form dictionary to sync leads for.
        cfg: the validated connector configuration.
        forms_state: the state dictionary of per-form cursors, updated in place.
    Returns:
        The number of lead rows written for this form.
    """
    form_id = form.get("id")
    resume_cursor = forms_state.get(form_id, {}).get("last_created_time")
    if resume_cursor is None:
        resume_cursor = cfg.get("initial_start_time")
    max_cursor = resume_cursor
    batch_index = 0
    written = 0
    rows_since_checkpoint = 0
    page_token = page.get("access_token")
    for batch, batch_max_created in meta_helpers.iterate_leads_for_form(
        page_token, form_id, cfg, since_time=resume_cursor
    ):
        batch_index += 1
        rows, max_cursor = _process_leads(page, form, batch, batch_max_created, max_cursor)
        written += rows
        rows_since_checkpoint += rows
        log.info(
            f"Form {form_id} batch {batch_index} leads={len(batch)} written={rows} "
            f"total_since_ckpt={rows_since_checkpoint} max_cursor={max_cursor}"
        )
        if rows_since_checkpoint >= cfg["check_point_limit"]:
            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # The form's own cursor is intentionally left unchanged here (see docstring above);
            # this checkpoint only flushes the upserted rows to the destination.
            # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
            op.checkpoint(state={"forms": forms_state})
            rows_since_checkpoint = 0
    if batch_index == 0:
        log.info(f"No new leads for form {form_id}")
    # The form's pagination is now fully complete, so its cursor is safe to advance.
    if max_cursor and max_cursor != resume_cursor:
        forms_state[form_id] = {"last_created_time": max_cursor}
    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
    # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
    op.checkpoint(state={"forms": forms_state})
    return written


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-code/connector-sdk-methods#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {
            "table": "leads",
            "primary_key": ["lead_id"],
            "columns": {
                "lead_id": "STRING",
                "page_id": "STRING",
                "page_name": "STRING",
                "form_id": "STRING",
                "form_name": "STRING",
                "created_time": "UTC_DATETIME",
                "ad_id": "STRING",
                "field_data": "STRING",
            },
        }
    ]


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
    log.warning("Example: SaaS & APIs : Meta Lead Ads")

    # Validate the configuration to ensure it contains all required values, and normalize
    # it into the typed dictionary the rest of the connector expects (see validator.py).
    cfg = validator.validate_configuration(configuration)
    base_url = BASE_URL_TEMPLATE.format(version=cfg["graph_version"])
    log.info(f"Using Graph API version {cfg['graph_version']} base={base_url}")

    state = state or {}
    forms_state: Dict[str, Dict[str, str]] = state.get("forms", {})

    pages = meta_helpers.discover_pages(cfg)
    log.info(f"Discovered {len(pages)} pages")

    total_leads = 0
    for page in pages:
        forms = meta_helpers.list_forms(page, cfg)
        log.info(f"Page {page.get('id')} forms={len(forms)}")
        for form in forms:
            total_leads += _process_form(page, form, cfg, forms_state)

    log.info(f"Sync complete. Total leads upserted: {total_leads}")


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

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
    config_path = os.environ.get("CONNECTOR_CONFIG", "configuration.json")
    with open(config_path, "r", encoding="utf-8") as config_file:
        local_configuration = json.load(config_file)

    # Test the connector locally
    connector.debug(configuration=local_configuration)
