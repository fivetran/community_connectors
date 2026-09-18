"""Facebook (Meta) Leads Connector (SDK >= 2.0.0)
=================================================

Single-table ingestion of Meta Lead Ads data using Fivetran Connector SDK 2.x pattern.

Features:
 - Incremental sync per form via created_time cursor (threshold-based checkpointing)
 - One destination table: leads (primary key: lead_id)
 - Transformation: each lead converted to a single row with page/form metadata, ad_id, created_time, and raw field_data serialized as JSON in field_data (handled by process_leads)
 - Raw form field data preserved verbatim for downstream modeling
 - Resilient HTTP with retry/backoff
 - Direct Operations calls (no generator/yield required in SDK >=2.0.0)

State structure (checkpoint):
{
    "forms": {
        "<form_id>": {"last_created_time": "2025-08-31T07:45:31+0000"}
    }
}

Incremental logic:
- initial_start_time is optional—if omitted starts from available leads without filtering.
- paginate each form fetching leads with created_time > last_created_time (if a cursor exists).
- Track max created_time; accumulate written rows and perform checkpoint only when accumulated rows reach configuration.
- final flush remaining rows after pagination.
"""

from __future__ import annotations

import json
import os
from typing import Dict, List, Any, Optional

from fivetran_connector_sdk import Connector, Logging as log, Operations as op
from http_helpers import BASE_URL_TEMPLATE
import meta_helpers
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
    current_cursor: Optional[str],
) -> tuple[int, Optional[str]]:
    """Transform & upsert a batch of leads, returning (rows_written, new_cursor)."""
    rows_written = 0
    new_cursor = current_cursor
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
        op.upsert("leads", row)
        rows_written += 1
    if batch_max_created and (
        current_cursor is None or batch_max_created > current_cursor
    ):
        new_cursor = batch_max_created
    return rows_written, new_cursor


def _maybe_checkpoint(
    form_id: str,
    forms_state: Dict[str, Dict[str, Optional[str]]],
    current_cursor: Optional[str],
    rows_since_checkpoint: int,
    cfg: Dict[str, Any],
    batch_index: int,
) -> int:
    """Flush checkpoint when the configured threshold is met."""
    if rows_since_checkpoint >= cfg["check_point_limit"]:
        _write_checkpoint(
            form_id=form_id,
            forms_state=forms_state,
            current_cursor=current_cursor,
            rows_flushed=rows_since_checkpoint,
            context=f"Checkpoint batch={batch_index}",
        )
        return 0
    return rows_since_checkpoint


def _write_checkpoint(
    form_id: str,
    forms_state: Dict[str, Dict[str, Optional[str]]],
    current_cursor: Optional[str],
    rows_flushed: int,
    context: str,
) -> None:
    """Persist the form's cursor into forms_state and checkpoint, unless there is nothing to flush."""
    if current_cursor is None or rows_flushed == 0:
        return
    forms_state[form_id] = {"last_created_time": current_cursor}
    op.checkpoint(state={"forms": forms_state})
    log.info(
        f"{context} form={form_id} rows_flushed={rows_flushed} last_created_time={current_cursor}"
    )


def _process_form(
    page: Dict[str, Any],
    form: Dict[str, Any],
    cfg: Dict[str, Any],
    forms_state: Dict[str, Dict[str, Optional[str]]],
) -> int:
    """Sync all leads for a single form, paginating, upserting, and checkpointing as it goes."""
    form_id = form.get("id")
    current_cursor = forms_state.get(form_id, {}).get("last_created_time")
    if current_cursor is None:
        current_cursor = cfg.get("initial_start_time")
    batch_index = 0
    written = 0
    page_token = page.get("access_token")
    rows_since_checkpoint = 0
    for batch, batch_max_created in meta_helpers.iterate_leads_for_form(
        page_token, form_id, cfg, since_time=current_cursor
    ):
        batch_index += 1
        rows, new_cursor = _process_leads(
            page, form, batch, batch_max_created, current_cursor
        )
        written += rows
        rows_since_checkpoint += rows
        current_cursor = new_cursor
        log.info(
            f"Form {form_id} batch {batch_index} leads={len(batch)} written={rows} total_since_ckpt={rows_since_checkpoint} cursor={current_cursor}"
        )
        rows_since_checkpoint = _maybe_checkpoint(
            form_id,
            forms_state,
            current_cursor,
            rows_since_checkpoint,
            cfg,
            batch_index,
        )
    # Final checkpoint if any rows remain
    _write_checkpoint(
        form_id=form_id,
        forms_state=forms_state,
        current_cursor=current_cursor,
        rows_flushed=rows_since_checkpoint,
        context="Final checkpoint",
    )
    if batch_index == 0:
        log.info(f"No new leads for form {form_id}")
    return written


# ---------------------------------------------------------------------------
# SDK Required Functions
# ---------------------------------------------------------------------------
def schema(configuration: Dict[str, str]):
    """Define the single leads table, its primary key, and explicit column types."""
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


def update(configuration: Dict[str, str], state: Dict[str, Any] | None):
    """Discover pages and forms, then sync leads for every in-scope form."""
    log.info("Starting update for Facebook Leads connector")
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


connector = Connector(update=update, schema=schema)


if __name__ == "__main__":
    config_path = os.environ.get("CONNECTOR_CONFIG", "configuration.json")
    with open(config_path, "r", encoding="utf-8") as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)
