"""ZoomInfo Connector SDK example — syncs ZoomInfo Go-To-Market data into Fivetran.

This is a comprehensive reference connector for the ZoomInfo Search and Enrich
APIs. It is intentionally broad rather than single-purpose: a real ZoomInfo
deployment needs contacts, companies, scoops, intent, news, and several
credit-bearing enrichments to be useful, and they share one auth flow, one
pagination scheme, and one incremental-state model. Keeping them in one
connector lets the example demonstrate how those concerns compose. Each
enrichment is opt-in via a configuration flag (see README), so a default run
only touches the free Search endpoints.

Module layout (the connector is split across files for readability):
- connector.py   — schema() + update() entrypoints and the Connector object.
- constants.py   — endpoint paths, tunables, default field lists, state keys.
- client.py      — OAuth token cache, retry transport, Search/Enrich pagination.
- config.py      — configuration parsing/validation and Search-filter building.
- transforms.py  — pure value coercion (int/float/datetime, cursor compare).
- sync.py        — one sync_* function per table plus enrichment orchestration.

Patterns demonstrated:
- OAuth 2.0 Client Credentials auth with in-process token caching and mid-sync
  401 refresh (see client.get_access_token).
- JSON:API page[size]/page[number] pagination with end-of-pages detection
  (see client.paginate).
- Incremental sync via per-endpoint server-side date filters, with a full-replace
  truncate+upsert fallback for the companies entity, which has no cursor
  (see config.apply_incremental_filter, sync.sync_companies).
- Bounded-memory streaming of high-volume per-company enrichments through a
  worker pool + queue (see sync._stream_per_company_enrich).
- Exponential-backoff retries on 429/5xx and intra-table checkpointing
  (see client.post_with_retry, constants.CHECKPOINT_EVERY_N_ROWS).

See the Connector SDK Technical Reference
(https://fivetran.com/docs/connectors/connector-sdk/technical-reference) and
Best Practices (https://fivetran.com/docs/connectors/connector-sdk/best-practices).
"""

# Import required classes from fivetran_connector_sdk for connector initialization,
# logging, and data operations (upsert, update, delete, checkpoint).
from fivetran_connector_sdk import Connector, Logging as log, Operations as op

# For reading the local configuration.json in the __main__ debug block.
import json

from constants import (
    STATE_CONTACTS_LAST_UPDATED,
    STATE_COMPANIES_LAST_UPDATED,
    STATE_SCOOPS_LAST_UPDATED,
    STATE_INTENT_LAST_UPDATED,
    STATE_NEWS_LAST_UPDATED,
)
from config import (
    _bool_config,
    validate_configuration,
    build_search_filter,
    get_enrich_config,
)
from sync import (
    sync_usage,
    sync_contacts,
    sync_companies,
    sync_scoops,
    sync_intent,
    sync_news,
    sync_companies_enriched,
    sync_scoops_enriched,
    sync_technologies,
    sync_corporate_hierarchy,
)


# ─────────────────────────────────────────────
# SCHEMA
# ─────────────────────────────────────────────
def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-code/connector-sdk-methods
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    # Validate configuration here too: schema() runs before update() during a
    # sync, so failing fast on bad credentials surfaces the error immediately.
    validate_configuration(configuration)

    # Core tables — always present
    tables = [
        {
            "table": "contacts",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "first_name": "STRING",
                "last_name": "STRING",
                "middle_name": "STRING",
                "job_title": "STRING",
                "company_id": "STRING",
                "company_name": "STRING",
                "contact_accuracy_score": "FLOAT",
                "has_email": "BOOLEAN",
                "has_direct_phone": "BOOLEAN",
                "has_mobile_phone": "BOOLEAN",
                "has_supplemental_email": "BOOLEAN",
                "direct_phone_do_not_call": "BOOLEAN",
                "mobile_phone_do_not_call": "BOOLEAN",
                "last_updated_date": "UTC_DATETIME",
                "valid_date": "UTC_DATETIME",
                "raw_attributes": "JSON",
            },
        },
        {
            "table": "companies",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "name": "STRING",
                "website": "STRING",
                "city": "STRING",
                "state": "STRING",
                "country": "STRING",
                "employee_count": "INT",
                "revenue": "LONG",
                "raw_attributes": "JSON",
            },
        },
        {
            "table": "scoops",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "company_id": "STRING",
                "company_name": "STRING",
                "description": "STRING",
                "topics": "JSON",
                "types": "JSON",
                "published_date": "UTC_DATETIME",
                "original_published_date": "UTC_DATETIME",
                "raw_attributes": "JSON",
            },
        },
        {
            "table": "usage",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "limit_type": "STRING",
                "description": "STRING",
                "current_usage": "LONG",
                "total_limit": "LONG",
                "usage_remaining": "LONG",
                "raw_attributes": "JSON",
            },
        },
    ]

    # Intent — only if topics are configured
    if configuration.get("intent_topics", "").strip():
        tables.append(
            {
                "table": "intent",
                "primary_key": ["id"],
                "columns": {
                    "id": "STRING",
                    "company_id": "STRING",
                    "company_name": "STRING",
                    "topic": "STRING",
                    "category": "STRING",
                    "signal_score": "FLOAT",
                    "audience_strength": "FLOAT",
                    "signal_date": "UTC_DATETIME",
                    "raw_attributes": "JSON",
                },
            }
        )

    # News — opt-in
    if _bool_config(configuration, "sync_news"):
        tables.append(
            {
                "table": "news",
                "primary_key": ["id"],
                "columns": {
                    "id": "STRING",
                    "company_id": "STRING",
                    "company_name": "STRING",
                    "all_companies": "JSON",
                    "title": "STRING",
                    "url": "STRING",
                    "domain": "STRING",
                    "image_url": "STRING",
                    "categories": "JSON",
                    "description": "STRING",
                    "page_date": "UTC_DATETIME",
                    "raw_attributes": "JSON",
                },
            }
        )

    # Contact Enrichment — opt-in
    if _bool_config(configuration, "enrich_contacts"):
        tables.append(
            {
                "table": "contacts_enriched",
                "primary_key": ["id"],
                "columns": {
                    "id": "STRING",
                    "match_status": "STRING",
                    "first_name": "STRING",
                    "last_name": "STRING",
                    "email": "STRING",
                    "phone": "STRING",
                    "mobile_phone": "STRING",
                    "job_title": "STRING",
                    "job_function": "STRING",
                    "management_level": "JSON",
                    "company_id": "STRING",
                    "company_name": "STRING",
                    "company_website": "STRING",
                    "company_revenue": "LONG",
                    "company_employee_count": "INT",
                    "company_industry": "STRING",
                    "city": "STRING",
                    "region": "STRING",
                    "state": "STRING",
                    "country": "STRING",
                    "direct_phone_do_not_call": "BOOLEAN",
                    "mobile_phone_do_not_call": "BOOLEAN",
                    "contact_accuracy_score": "FLOAT",
                    "last_updated_date": "UTC_DATETIME",
                    "valid_date": "UTC_DATETIME",
                    "raw_attributes": "JSON",
                },
            }
        )

    # Company Enrichment — opt-in
    if _bool_config(configuration, "enrich_companies"):
        tables.append(
            {
                "table": "companies_enriched",
                "primary_key": ["id"],
                "columns": {
                    "id": "STRING",
                    "match_status": "STRING",
                    "name": "STRING",
                    "website": "STRING",
                    "phone": "STRING",
                    "revenue": "LONG",
                    "revenue_range": "STRING",
                    "employee_count": "INT",
                    "employee_range": "STRING",
                    "employee_growth": "JSON",
                    "primary_industry": "STRING",
                    "industries": "JSON",
                    "city": "STRING",
                    "state": "STRING",
                    "country": "STRING",
                    "street": "STRING",
                    "zip_code": "STRING",
                    "metro_area": "STRING",
                    "description": "STRING",
                    "ticker": "STRING",
                    "type": "STRING",
                    "naics_codes": "JSON",
                    "sic_codes": "JSON",
                    "founded_year": "INT",
                    "company_status": "STRING",
                    "parent_id": "STRING",
                    "parent_name": "STRING",
                    "ultimate_parent_id": "STRING",
                    "ultimate_parent_name": "STRING",
                    "total_funding_amount": "LONG",
                    "recent_funding_amount": "LONG",
                    "recent_funding_date": "UTC_DATETIME",
                    "social_media_urls": "JSON",
                    "logo": "STRING",
                    "last_updated_date": "UTC_DATETIME",
                    "is_defunct": "BOOLEAN",
                    "raw_attributes": "JSON",
                },
            }
        )

    # Scoops Enrichment — opt-in
    if _bool_config(configuration, "enrich_scoops"):
        tables.append(
            {
                "table": "scoops_enriched",
                "primary_key": ["id"],
                "columns": {
                    "id": "STRING",
                    "company_id": "STRING",
                    "company_name": "STRING",
                    "description": "STRING",
                    "topics": "JSON",
                    "types": "JSON",
                    "published_date": "UTC_DATETIME",
                    "original_published_date": "UTC_DATETIME",
                    "link": "STRING",
                    "link_text": "STRING",
                    "raw_attributes": "JSON",
                },
            }
        )

    # Technologies — opt-in
    if _bool_config(configuration, "enrich_technologies"):
        tables.append(
            {
                "table": "technologies",
                "primary_key": ["id"],
                "columns": {
                    "id": "STRING",
                    "company_id": "STRING",
                    "technology_id": "STRING",
                    "attribute": "STRING",
                    "product": "STRING",
                    "vendor": "STRING",
                    "category": "STRING",
                    "category_parent": "STRING",
                    "description": "STRING",
                    "domain": "STRING",
                    "logo": "STRING",
                    "website": "STRING",
                    "created_date": "UTC_DATETIME",
                    "modified_date": "UTC_DATETIME",
                    "raw_attributes": "JSON",
                },
            }
        )

    # Corporate Hierarchy — opt-in
    if _bool_config(configuration, "enrich_corporate_hierarchy"):
        tables.append(
            {
                "table": "corporate_hierarchy",
                "primary_key": ["company_id"],
                "columns": {
                    "company_id": "STRING",
                    "match_status": "STRING",
                    "family_tree": "JSON",
                    "parentage": "JSON",
                    "raw_attributes": "JSON",
                },
            }
        )

    return tables


# ─────────────────────────────────────────────
# UPDATE (main sync entrypoint)
# ─────────────────────────────────────────────
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
    log.warning("Example: Source Examples : ZoomInfo")

    # Validate the configuration before doing any work so a misconfiguration
    # fails fast with a clear error instead of part-way through a sync.
    validate_configuration(configuration)

    # Cumulative state we'll checkpoint after each completed table. Starting
    # from the prior state means a resume picks up where the last run left off.
    cumulative_state = dict(state)

    # ── Usage (always — free, no opt-in) ──
    sync_usage(configuration)
    # Save the progress by checkpointing the state. This tells Fivetran it is
    # safe to write the data synced so far to the destination, and lets the next
    # sync resume from here. We checkpoint after each completed table below.
    op.checkpoint(state=cumulative_state)

    # ── Parse enrichment + filter config once ──
    enrich_config = get_enrich_config(configuration)
    search_filter = build_search_filter(configuration)
    log.info(f"Search filter for this sync: {search_filter}")

    # ── Contacts (Search + optional Enrich) ──
    contacts_latest = sync_contacts(
        configuration, state, enrich_config, search_filter, cumulative_state
    )
    if contacts_latest:
        cumulative_state[STATE_CONTACTS_LAST_UPDATED] = contacts_latest
    # Checkpoint after Contacts so its incremental cursor is persisted; the next
    # sync resumes from here even if a later table fails.
    op.checkpoint(state=cumulative_state)

    # ── Companies (Search + optional downstream enrichments) ──
    companies_latest, company_ids = sync_companies(configuration, state, search_filter)
    if companies_latest:
        cumulative_state[STATE_COMPANIES_LAST_UPDATED] = companies_latest
    # Checkpoint after Companies so its cursor is persisted before the
    # downstream enrichments (which depend on company_ids) run.
    op.checkpoint(state=cumulative_state)

    # ── Scoops (Search) ──
    scoops_latest = sync_scoops(configuration, state, search_filter, cumulative_state)
    if scoops_latest:
        cumulative_state[STATE_SCOOPS_LAST_UPDATED] = scoops_latest
    # Checkpoint after Scoops so its incremental cursor is persisted.
    op.checkpoint(state=cumulative_state)

    # ── Intent (Search — requires topics config) ──
    intent_latest = sync_intent(configuration, state, cumulative_state)
    if intent_latest:
        cumulative_state[STATE_INTENT_LAST_UPDATED] = intent_latest
    # Checkpoint after Intent so its incremental cursor is persisted.
    op.checkpoint(state=cumulative_state)

    # ── News (Search — opt-in) ──
    news_latest = sync_news(configuration, state, cumulative_state)
    if news_latest:
        cumulative_state[STATE_NEWS_LAST_UPDATED] = news_latest
    # Checkpoint after News so its incremental cursor is persisted.
    op.checkpoint(state=cumulative_state)

    # ── Company-based Enrichments (all use company_ids from Search) ──
    sync_companies_enriched(configuration, company_ids)
    # These enrich tables have no cursor of their own; checkpoint after each so
    # Fivetran durably flushes the rows synced so far before the next enrichment.
    op.checkpoint(state=cumulative_state)

    sync_scoops_enriched(configuration, company_ids, cumulative_state)
    # Checkpoint to durably flush the Scoops-enrich rows synced so far.
    op.checkpoint(state=cumulative_state)

    sync_technologies(configuration, company_ids, cumulative_state)
    # Checkpoint to durably flush the Technologies rows synced so far.
    op.checkpoint(state=cumulative_state)

    sync_corporate_hierarchy(configuration, company_ids)
    # Checkpoint to durably flush the Corporate-hierarchy rows synced so far.
    op.checkpoint(state=cumulative_state)

    log.info("ZoomInfo Fivetran Connector — sync complete")


# ─────────────────────────────────────────────
# CONNECTOR INIT
# ─────────────────────────────────────────────
# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run
# directly from the command line or IDE 'run' button.
#
# IMPORTANT: The recommended way to test your connector is using the Fivetran
# debug command:
#   fivetran debug
#
# This local testing block is provided as a convenience for quick debugging
# during development, such as using IDE debug tools (breakpoints, step-through
# debugging, etc.). Note: This method is not called by Fivetran when executing
# your connector in production. Always test using 'fivetran debug' prior to
# finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
