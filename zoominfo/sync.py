"""Sync orchestration for the ZoomInfo connector.

One sync_* function per destination table. Search tables (contacts, companies,
scoops, intent, news) page the free Search API; enrich tables spend credits via
batch or per-company Enrich endpoints. The per-company enrichments (scoops,
technologies) stream rows through a bounded worker-pool + queue so memory stays
flat regardless of volume (see _stream_per_company_enrich). All op.upsert /
op.checkpoint calls happen here on the main thread; worker threads only enqueue.
"""

# For parallelizing per-company enrichment fetches across a bounded worker pool.
from concurrent.futures import ThreadPoolExecutor

# For the bounded queue used to stream per-company enrich rows back to the main thread.
import queue

# For the worker-pool abort event.
import threading

# For serializing nested/list fields to JSON strings before upsert.
import json

# For catching requests.exceptions.RequestException in the intent-topics lookup.
import requests

from fivetran_connector_sdk import Logging as log, Operations as op

from constants import (
    ENDPOINT_CONTACTS,
    ENDPOINT_COMPANIES,
    ENDPOINT_SCOOPS,
    ENDPOINT_INTENT,
    ENDPOINT_NEWS,
    ENDPOINT_CONTACTS_ENRICH,
    ENDPOINT_COMPANIES_ENRICH,
    ENDPOINT_CORP_HIER_ENRICH,
    ENDPOINT_USAGE,
    SEARCH_BASE,
    ENRICH_BATCH_SIZE,
    ENRICH_WORKERS,
    ENRICH_QUEUE_MAX,
    CHECKPOINT_EVERY_N_ROWS,
    DEFAULT_COMPANY_ENRICH_FIELDS,
    DEFAULT_CORP_HIER_FIELDS,
    STATE_CONTACTS_LAST_UPDATED,
    STATE_SCOOPS_LAST_UPDATED,
    STATE_INTENT_LAST_UPDATED,
    STATE_NEWS_LAST_UPDATED,
)
from client import (
    get_access_token,
    get_headers,
    get_headers_get,
    get_with_retry,
    post_with_retry,
    build_body,
    paginate,
    paginate_enrich_scoops,
    paginate_enrich_technologies,
    _invalidate_token_cache,
)
from config import (
    _bool_config,
    _fields_config,
    apply_incremental_filter,
    should_enrich,
    matches_mgmt_level,
)
from transforms import (
    _safe_int,
    _safe_float,
    _safe_utc_datetime,
    _max_cursor,
)


# ─────────────────────────────────────────────
# ENRICH HELPERS
# ─────────────────────────────────────────────
def _enrich_post(configuration: dict, endpoint: str, attributes: dict) -> list:
    """
    Shared helper for all POST-based enrich calls.
    Handles 401 token refresh and returns data[] from the response.
    """
    token = get_access_token(configuration)
    body = build_body(endpoint, attributes)

    response = post_with_retry(
        f"{SEARCH_BASE}{endpoint}", headers=get_headers(token), json_body=body
    )

    if response.status_code == 401:
        log.warning(f"401 on {endpoint} — refreshing token and retrying...")
        _invalidate_token_cache()
        token = get_access_token(configuration)
        response = post_with_retry(
            f"{SEARCH_BASE}{endpoint}", headers=get_headers(token), json_body=body
        )

    if response.status_code != 200:
        raise RuntimeError(
            f"ZoomInfo Enrich API error on {endpoint} "
            f"[HTTP {response.status_code}]: {response.text}"
        )

    return response.json().get("data", [])


def enrich_contacts_batch(configuration: dict, person_ids: list, output_fields: list) -> list:
    """Enriches a batch of up to 25 contacts by personId."""
    return _enrich_post(
        configuration,
        ENDPOINT_CONTACTS_ENRICH,
        {
            "matchPersonInput": [{"personId": pid} for pid in person_ids],
            "outputFields": output_fields,
        },
    )


def enrich_companies_batch(configuration: dict, company_ids: list, output_fields: list) -> list:
    """Enriches a batch of up to 25 companies by companyId."""
    return _enrich_post(
        configuration,
        ENDPOINT_COMPANIES_ENRICH,
        {
            "matchCompanyInput": [{"companyId": cid} for cid in company_ids],
            "outputFields": output_fields,
        },
    )


def enrich_corp_hierarchy_batch(
    configuration: dict, company_ids: list, output_fields: list
) -> list:
    """Enriches corporate hierarchy for a batch of up to 25 companies."""
    return _enrich_post(
        configuration,
        ENDPOINT_CORP_HIER_ENRICH,
        {
            "matchCompanyInput": [{"companyId": cid} for cid in company_ids],
            "outputFields": output_fields,
        },
    )


# ─────────────────────────────────────────────
# SYNC FUNCTIONS — SEARCH (free)
# ─────────────────────────────────────────────
def sync_contacts(
    configuration: dict,
    state: dict,
    enrich_config: dict,
    search_filter: dict,
    cumulative_state: dict = None,
):
    """
    Syncs contacts from ZoomInfo Search, then optionally enriches a subset.

    Search pass:
    - Upserts basic contact records to the 'contacts' table
    - Collects contact IDs eligible for enrichment based on customer config

    Enrich pass (if enabled):
    - Batches eligible IDs into groups of 25
    - Calls Enrich API for full profile data
    - Upserts enriched records to the 'contacts_enriched' table
    - Logs match status and skips NO_MATCH records (no credit charged)
    - Applies managementLevel filter post-enrich if configured

    State semantics:
    - When prior state exists, only contacts with `lastUpdatedDate >= state`
      are fetched (incremental). Override with `full_refresh=true`.
    - `latest_date` is the max `lastUpdatedDate` seen this run; checkpointed
      back as the new state so the next run picks up from here.
    """
    log.info("Starting contacts sync...")

    latest_date = state.get("contacts_last_updated")
    count = 0
    enrich_queue = []

    effective_filter = apply_incremental_filter(
        search_filter, configuration, state, "contacts_last_updated", "lastUpdatedDateAfter"
    )

    for page_results in paginate(configuration, ENDPOINT_CONTACTS, effective_filter):
        for record in page_results:
            record_id = record.get("id")
            a = record.get("attributes", {})

            if count == 0:
                log.debug(f"Sample contact attribute keys: {list(a.keys())}")

            company = a.get("company") or {}
            company_id = company.get("id")
            company_nm = company.get("name")

            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            op.upsert(
                table="contacts",
                data={
                    "id": record_id,
                    "first_name": a.get("firstName"),
                    "last_name": a.get("lastName"),
                    "middle_name": a.get("middleName"),
                    "job_title": a.get("jobTitle"),
                    "company_id": company_id,
                    "company_name": company_nm,
                    "contact_accuracy_score": _safe_float(a.get("contactAccuracyScore")),
                    "has_email": a.get("hasEmail"),
                    "has_direct_phone": a.get("hasDirectPhone"),
                    "has_mobile_phone": a.get("hasMobilePhone"),
                    "has_supplemental_email": a.get("hasSupplementalEmail"),
                    "direct_phone_do_not_call": a.get("directPhoneDoNotCall"),
                    "mobile_phone_do_not_call": a.get("mobilePhoneDoNotCall"),
                    "last_updated_date": _safe_utc_datetime(a.get("lastUpdatedDate")),
                    "valid_date": _safe_utc_datetime(a.get("validDate")),
                    "raw_attributes": json.dumps(a) if a else None,
                },
            )
            count += 1

            record_date = a.get("lastUpdatedDate")
            latest_date = _max_cursor(latest_date, record_date)

            if enrich_config and record_id and should_enrich(a, enrich_config["filter"]):
                enrich_queue.append(record_id)

            # Intra-table checkpoint so a crash mid-pull doesn't re-fetch from
            # the previous run's cursor — resume picks up from the latest_date
            # we've seen so far.
            if cumulative_state is not None and count % CHECKPOINT_EVERY_N_ROWS == 0:
                if latest_date:
                    cumulative_state[STATE_CONTACTS_LAST_UPDATED] = latest_date
                # Save the progress by checkpointing the state. This is important for
                # ensuring that the sync process can resume from the correct position
                # in case of next sync or interruptions. You should checkpoint even if
                # you are not using incremental sync, as it tells Fivetran it is safe
                # to write to the destination. For large datasets, checkpoint regularly
                # (e.g., every N records) not only at the end. Learn more in our best
                # practices documentation:
                # https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets  # noqa: B950
                op.checkpoint(state=cumulative_state)

    log.info(f"Contacts sync complete — {count} records upserted")

    if enrich_config and enrich_queue:
        log.info(
            f"Starting contact enrichment — {len(enrich_queue)} contacts queued "
            f"(filter={enrich_config['filter']})"
        )
        _run_contact_enrichment(configuration, enrich_queue, enrich_config)

    return latest_date


def sync_companies(configuration: dict, state: dict, search_filter: dict):
    """
    Syncs companies from ZoomInfo Search Companies.
    Returns (None, list_of_company_ids) — companies has no incremental cursor,
    so the return tuple's first element is always None for forward-compat with
    the update() caller's signature.

    The ZoomInfo Search filter API does not expose a server-side lastUpdated
    field for the company entity (verified via /lookup/search), so every sync
    re-pulls the full universe. To make deletions propagate to the destination
    instead of leaving stale rows forever, we truncate then upsert.
    op.truncate() marks every previously-synced row with _fivetran_deleted=TRUE;
    rows we then upsert in this run land with _fivetran_deleted=FALSE.

    Crash-safety: we paginate the full universe into an in-memory buffer first,
    then truncate + upsert in one block. If pagination throws (network, 5xx,
    auth), the destination table is left untouched — vastly better than the
    naive "truncate then stream upserts" pattern, which leaves the destination
    with every row soft-deleted if pagination dies partway through.

    Confirmed response fields (from data[].attributes):
      name, website, city, state, country, employeeCount, revenue

    Note: record.id holds the company ID (not in attributes).
    """
    log.info("Starting companies sync (full-replace via truncate + upsert)...")

    # Phase 1: buffer the full universe before any destructive op. Tradeoff —
    # peak memory scales with universe × ~1 KB/row. At the documented 10K-company
    # scale this is ~10 MB, well under the 1 GB Fivetran cloud limit. For very
    # large universes, narrow the `countries` filter or split across multiple
    # connectors instead of raising PAGE_SIZE.
    buffered_rows = []
    company_ids = []

    for page_results in paginate(configuration, ENDPOINT_COMPANIES, search_filter):
        for record in page_results:
            record_id = record.get("id")
            a = record.get("attributes", {})

            if not buffered_rows:
                log.debug(f"Sample company attribute keys: {list(a.keys())}")

            buffered_rows.append(
                {
                    "id": record_id,
                    "name": a.get("name"),
                    "website": a.get("website"),
                    "city": a.get("city"),
                    "state": a.get("state"),
                    "country": a.get("country"),
                    "employee_count": _safe_int(a.get("employeeCount")),
                    "revenue": _safe_int(a.get("revenue")),
                    "raw_attributes": json.dumps(a) if a else None,
                }
            )
            if record_id:
                company_ids.append(record_id)

    log.info(f"Companies sync — buffered {len(buffered_rows)} rows; truncating and upserting...")

    # Phase 2: pagination succeeded — now do the destructive swap.
    # The 'truncate' operation soft-deletes every previously-synced row
    # (marks _fivetran_deleted=TRUE); rows re-upserted below land with
    # _fivetran_deleted=FALSE, so deletions on the ZoomInfo side propagate.
    op.truncate(table="companies")
    for row in buffered_rows:
        # Upsert one company row into the destination. 'upsert' inserts or
        # updates the record keyed by its primary key ('id').
        op.upsert(table="companies", data=row)

    log.info(f"Companies sync complete — {len(buffered_rows)} records upserted")
    return None, company_ids


def sync_scoops(
    configuration: dict, state: dict, search_filter: dict, cumulative_state: dict = None
):
    """
    Syncs scoops from ZoomInfo Search Scoops.

    Confirmed response fields (from data[].attributes):
      company: {id, name}, description, topics, types,
      publishedDate, originalPublishedDate

    Note: 'country' is the only confirmed working filter for scoops.
    """
    log.info("Starting scoops sync...")

    latest_date = state.get("scoops_last_updated")
    count = 0

    effective_filter = apply_incremental_filter(
        search_filter, configuration, state, "scoops_last_updated", "publishedStartDate"
    )

    for page_results in paginate(configuration, ENDPOINT_SCOOPS, effective_filter):
        for record in page_results:
            record_id = record.get("id")
            a = record.get("attributes", {})

            if count == 0:
                log.debug(f"Sample scoop attribute keys: {list(a.keys())}")

            company = a.get("company") or {}
            company_id = company.get("id")
            company_nm = company.get("name")

            topics = a.get("topics")
            types = a.get("types")

            # Upsert one scoop row into the destination. 'upsert' inserts or
            # updates the record keyed by its primary key ('id').
            op.upsert(
                table="scoops",
                data={
                    "id": record_id,
                    "company_id": company_id,
                    "company_name": company_nm,
                    "description": a.get("description"),
                    "topics": json.dumps(topics) if topics else None,
                    "types": json.dumps(types) if types else None,
                    "published_date": _safe_utc_datetime(a.get("publishedDate")),
                    "original_published_date": _safe_utc_datetime(a.get("originalPublishedDate")),
                    "raw_attributes": json.dumps(a) if a else None,
                },
            )
            count += 1

            record_date = a.get("publishedDate")
            latest_date = _max_cursor(latest_date, record_date)

            if cumulative_state is not None and count % CHECKPOINT_EVERY_N_ROWS == 0:
                if latest_date:
                    cumulative_state[STATE_SCOOPS_LAST_UPDATED] = latest_date
                # Checkpoint mid-table so progress is flushed and the next sync
                # can resume from the latest scoops cursor.
                op.checkpoint(state=cumulative_state)

    log.info(f"Scoops sync complete — {count} records upserted")
    return latest_date


def get_valid_intent_topics(configuration: dict) -> set:
    """
    Fetches the list of licensed intent topic names from the Lookup endpoint.
    Returns a set of valid topic name strings (case-sensitive).
    Returns empty set on any error — caller should skip validation if empty.
    """
    try:
        token = get_access_token(configuration)
        resp = get_with_retry(
            f"{SEARCH_BASE}/gtm/data/v1/lookup/intent-topics", headers=get_headers_get(token)
        )
        if resp.status_code != 200:
            log.warning(
                f"Could not fetch intent topics lookup [{resp.status_code}] — skipping validation"
            )
            return set()
        return {
            item.get("attributes", {}).get("name")
            for item in resp.json().get("data", [])
            if item.get("attributes", {}).get("name")
        }
    except (requests.exceptions.RequestException, ValueError, KeyError, TypeError) as e:
        # Network failures (RequestException), JSON decode errors (ValueError),
        # and malformed-payload access errors (KeyError/TypeError). Unexpected
        # exceptions are allowed to surface rather than be silently swallowed.
        log.warning(f"Intent topics lookup failed: {e} — skipping validation")
        return set()


def sync_intent(configuration: dict, state: dict, cumulative_state: dict = None):
    """
    Syncs Intent Signals from ZoomInfo Search Intent.
    Free endpoint — no credits consumed (records and requests counted).

    Requires at least 1 intent topic in the request (up to 50).
    Topics are customer-configurable via 'intent_topics' in configuration.
    Topics must be exact names from the customer's licensed intent topic list.
    Use GET /gtm/data/v1/lookup/intent-topics to see valid values.

    Response fields: company.{id,name}, topic, signalScore, audienceStrength,
                     signalDate, category

    Gracefully skips on 403 — customer may not have Intent product licensed
    or the app scope may not include api:data:intent.
    """
    topics_raw = configuration.get("intent_topics", "").strip()
    topics = [t.strip() for t in topics_raw.split(",") if t.strip()]

    if not topics:
        log.info("Intent sync skipped — no intent_topics configured")
        return state.get("intent_last_updated")

    # Validate topics against the licensed list — warn and drop invalid ones
    valid_topics = get_valid_intent_topics(configuration)
    if valid_topics:
        invalid = [t for t in topics if t not in valid_topics]
        if invalid:
            log.warning(
                f"Removing {len(invalid)} invalid intent topic(s): {invalid}. "
                f"Valid topics for this account: {sorted(valid_topics)}"
            )
            topics = [t for t in topics if t in valid_topics]
        if not topics:
            log.warning("Intent sync skipped — no valid topics remain after validation")
            return state.get("intent_last_updated")

    log.info(f"Starting intent sync — {len(topics)} topic(s)...")

    latest_date = state.get("intent_last_updated")
    count = 0

    intent_filter = apply_incremental_filter(
        {"topics": topics}, configuration, state, "intent_last_updated", "signalStartDate"
    )

    try:
        for page_results in paginate(configuration, ENDPOINT_INTENT, intent_filter):
            for record in page_results:
                record_id = record.get("id")
                a = record.get("attributes", {})

                if count == 0:
                    log.debug(f"Sample intent attribute keys: {list(a.keys())}")

                company = a.get("company") or {}
                company_id = company.get("id")
                company_nm = company.get("name")

                # Upsert one intent-signal row into the destination. 'upsert'
                # inserts or updates the record keyed by its primary key ('id').
                op.upsert(
                    table="intent",
                    data={
                        "id": record_id,
                        "company_id": company_id,
                        "company_name": company_nm,
                        "topic": a.get("topic"),
                        "category": a.get("category"),
                        "signal_score": _safe_float(a.get("signalScore")),
                        "audience_strength": _safe_float(a.get("audienceStrength")),
                        "signal_date": _safe_utc_datetime(a.get("signalDate")),
                        "raw_attributes": json.dumps(a) if a else None,
                    },
                )
                count += 1

                record_date = a.get("signalDate")
                latest_date = _max_cursor(latest_date, record_date)

                if cumulative_state is not None and count % CHECKPOINT_EVERY_N_ROWS == 0:
                    if latest_date:
                        cumulative_state[STATE_INTENT_LAST_UPDATED] = latest_date
                    # Checkpoint mid-table so progress is flushed and the next
                    # sync can resume from the latest intent cursor.
                    op.checkpoint(state=cumulative_state)

    except RuntimeError as e:
        if "[HTTP 403]" in str(e):
            log.warning(
                "Intent sync skipped — 403 Access Denied. "
                "Ensure your DevPortal app has the 'api:data:intent' scope and "
                "your ZoomInfo account includes the Intent product."
            )
            return state.get("intent_last_updated")
        raise

    log.info(f"Intent sync complete — {count} records upserted")
    return latest_date


def sync_news(configuration: dict, state: dict, cumulative_state: dict = None):
    """
    Syncs News articles from ZoomInfo Search News.
    Free endpoint — no credits consumed (records and requests counted).
    Opt-in via 'sync_news' configuration flag.

    Response fields: company.{id,name}, title, url, publishedDate, category, summary

    Gracefully skips on 403 — customer may not have News product licensed
    or the app scope may not include api:data:news.
    """
    if not _bool_config(configuration, "sync_news"):
        log.info("News sync skipped — sync_news not enabled")
        return state.get("news_last_updated")

    log.info("Starting news sync...")

    latest_date = state.get("news_last_updated")
    count = 0

    news_filter = apply_incremental_filter(
        {}, configuration, state, "news_last_updated", "pageDateMin"
    )

    try:
        for page_results in paginate(configuration, ENDPOINT_NEWS, news_filter):
            for record in page_results:
                record_id = record.get("id")
                a = record.get("attributes", {})

                if count == 0:
                    log.debug(f"Sample news attribute keys: {list(a.keys())}")

                # company is a LIST in News (an article can mention multiple companies)
                # Take the first company for the primary foreign key columns
                company_list = a.get("company") or []
                if isinstance(company_list, dict):
                    company_list = [company_list]
                primary_company = company_list[0] if company_list else {}
                company_id = primary_company.get("id")
                company_nm = primary_company.get("name")

                # Upsert one news row into the destination. 'upsert' inserts or
                # updates the record keyed by its primary key ('id').
                op.upsert(
                    table="news",
                    data={
                        "id": record_id,
                        "company_id": company_id,
                        "company_name": company_nm,
                        # all_companies stores the full list as JSON (articles can mention multiple)
                        "all_companies": json.dumps(company_list) if company_list else None,
                        "title": a.get("title"),
                        "url": a.get("url"),
                        "domain": a.get("domain"),
                        "image_url": a.get("imageUrl"),
                        # categories is a list (e.g. ["FINANCIAL_RESULTS"]) — store as JSON
                        "categories": (
                            json.dumps(a.get("categories")) if a.get("categories") else None
                        ),
                        # description is the article text; field is "description" not "summary"
                        "description": a.get("description"),
                        # date field is "pageDate" not "publishedDate"
                        "page_date": _safe_utc_datetime(a.get("pageDate")),
                        "raw_attributes": json.dumps(a) if a else None,
                    },
                )
                count += 1

                record_date = a.get("pageDate")
                latest_date = _max_cursor(latest_date, record_date)

                if cumulative_state is not None and count % CHECKPOINT_EVERY_N_ROWS == 0:
                    if latest_date:
                        cumulative_state[STATE_NEWS_LAST_UPDATED] = latest_date
                    # Checkpoint mid-table so progress is flushed and the next
                    # sync can resume from the latest news cursor.
                    op.checkpoint(state=cumulative_state)

    except RuntimeError as e:
        if "[HTTP 403]" in str(e):
            log.warning(
                "News sync skipped — 403 Access Denied. "
                "Ensure your DevPortal app has the 'api:data:news' scope and "
                "your ZoomInfo account includes the News product."
            )
            return state.get("news_last_updated")
        raise

    log.info(f"News sync complete — {count} records upserted")
    return latest_date


# ─────────────────────────────────────────────
# SYNC FUNCTIONS — ENRICH (cost credits)
# ─────────────────────────────────────────────
def _run_contact_enrichment(configuration: dict, person_ids: list, enrich_config: dict):
    """
    Enriches contacts in batches of 25 and upserts results to
    the 'contacts_enriched' table.

    Skips NO_MATCH records (no credit charged for those).
    Applies managementLevel filter post-enrich if configured.
    On 403 (license missing), logs a warning and returns — the table is
    declared in schema() but no rows are produced for this sync.
    """
    output_fields = enrich_config["output_fields"]
    mgmt_levels = enrich_config["mgmt_levels"]

    total = len(person_ids)
    enriched = 0
    skipped_match = 0
    skipped_mgmt = 0

    try:
        for i in range(0, total, ENRICH_BATCH_SIZE):
            batch = person_ids[i : i + ENRICH_BATCH_SIZE]
            batch_num = (i // ENRICH_BATCH_SIZE) + 1
            log.debug(f"Enriching contact batch {batch_num} ({len(batch)} contacts)...")

            results = enrich_contacts_batch(configuration, batch, output_fields)

            for record in results:
                record_id = record.get("id")
                a = record.get("attributes", {})
                match_status = record.get("meta", {}).get("matchStatus", "")

                if match_status == "NO_MATCH":
                    skipped_match += 1
                    log.debug(f"NO_MATCH for personId={record_id} — skipping")
                    continue

                if mgmt_levels and not matches_mgmt_level(a, mgmt_levels):
                    skipped_mgmt += 1
                    continue

                # companyName is nested under company.name, not a top-level field
                company = a.get("company") or {}
                # Upsert one enriched-contact row into the destination. 'upsert'
                # inserts or updates the record keyed by its primary key ('id').
                op.upsert(
                    table="contacts_enriched",
                    data={
                        "id": record_id,
                        "match_status": match_status,
                        "first_name": a.get("firstName"),
                        "last_name": a.get("lastName"),
                        "email": a.get("email"),
                        "phone": a.get("phone"),
                        "mobile_phone": a.get("mobilePhone"),
                        "job_title": a.get("jobTitle"),
                        "job_function": a.get("jobFunction"),
                        # managementLevel is a list (e.g. ["C-Level"]) — store as JSON string
                        "management_level": (
                            json.dumps(a.get("managementLevel"))
                            if a.get("managementLevel")
                            else None
                        ),
                        "company_id": company.get("id") or a.get("companyId"),
                        "company_name": company.get("name") or a.get("companyName"),
                        "company_website": a.get("companyWebsite"),
                        "company_revenue": _safe_int(a.get("companyRevenue")),
                        "company_employee_count": _safe_int(a.get("companyEmployeeCount")),
                        "company_industry": a.get("companyPrimaryIndustry"),
                        "city": a.get("city"),
                        "region": a.get("region"),
                        "state": a.get("state"),
                        "country": a.get("country"),
                        "direct_phone_do_not_call": a.get("directPhoneDoNotCall"),
                        "mobile_phone_do_not_call": a.get("mobilePhoneDoNotCall"),
                        "contact_accuracy_score": _safe_float(a.get("contactAccuracyScore")),
                        "last_updated_date": _safe_utc_datetime(a.get("lastUpdatedDate")),
                        "valid_date": _safe_utc_datetime(a.get("validDate")),
                        "raw_attributes": json.dumps(a) if a else None,
                    },
                )
                enriched += 1

    except RuntimeError as e:
        if "[HTTP 403]" in str(e):
            log.warning(
                "Contact enrichment skipped — 403 Access Denied. "
                "Ensure your ZoomInfo account includes Contact Enrich."
            )
            return
        raise

    log.info(
        f"Contact enrichment complete — {enriched} upserted, "
        f"{skipped_match} no-match, {skipped_mgmt} filtered by management level"
    )


def sync_companies_enriched(configuration: dict, company_ids: list):
    """
    Enriches companies in batches of 25 and upserts to 'companies_enriched'.
    Opt-in via 'enrich_companies' configuration flag.
    Costs 1 credit per new company record.

    Uses company IDs collected during sync_companies() Search pass.
    Gracefully skips on 403 — customer may not have Company Enrich licensed.
    """
    if not _bool_config(configuration, "enrich_companies"):
        log.info("Company enrichment skipped — enrich_companies not enabled")
        return

    if not company_ids:
        log.info("Company enrichment skipped — no company IDs available")
        return

    output_fields = _fields_config(
        configuration, "enrich_companies_output_fields", DEFAULT_COMPANY_ENRICH_FIELDS
    )

    log.info(f"Starting company enrichment — {len(company_ids)} companies queued...")

    total = len(company_ids)
    enriched = 0
    skipped_match = 0

    try:
        for i in range(0, total, ENRICH_BATCH_SIZE):
            batch = company_ids[i : i + ENRICH_BATCH_SIZE]
            batch_num = (i // ENRICH_BATCH_SIZE) + 1
            log.debug(f"Enriching company batch {batch_num} ({len(batch)} companies)...")

            results = enrich_companies_batch(configuration, batch, output_fields)

            for record in results:
                record_id = record.get("id")
                a = record.get("attributes", {})
                match_status = record.get("meta", {}).get("matchStatus", "")

                if match_status == "NO_MATCH":
                    skipped_match += 1
                    log.debug(f"NO_MATCH for companyId={record_id} — skipping")
                    continue

                # Upsert one enriched-company row into the destination. 'upsert'
                # inserts or updates the record keyed by its primary key ('id').
                op.upsert(
                    table="companies_enriched",
                    data={
                        "id": record_id,
                        "match_status": match_status,
                        "name": a.get("name"),
                        "website": a.get("website"),
                        "phone": a.get("phone"),
                        "revenue": _safe_int(a.get("revenue")),
                        "revenue_range": a.get("revenueRange"),
                        "employee_count": _safe_int(a.get("employeeCount")),
                        "employee_range": a.get("employeeRange"),
                        # employeeGrowth is a nested object — store as JSON
                        "employee_growth": (
                            json.dumps(a.get("employeeGrowth"))
                            if a.get("employeeGrowth")
                            else None
                        ),
                        "primary_industry": a.get("primaryIndustry"),
                        # industries is a list — store as JSON
                        "industries": (
                            json.dumps(a.get("industries")) if a.get("industries") else None
                        ),
                        "city": a.get("city"),
                        "state": a.get("state"),
                        "country": a.get("country"),
                        "street": a.get("street"),
                        "zip_code": a.get("zipCode"),
                        "metro_area": a.get("metroArea"),
                        "description": a.get("description"),
                        "ticker": a.get("ticker"),
                        "type": a.get("type"),
                        # naicsCodes / sicCodes are lists — store as JSON
                        "naics_codes": (
                            json.dumps(a.get("naicsCodes")) if a.get("naicsCodes") else None
                        ),
                        "sic_codes": json.dumps(a.get("sicCodes")) if a.get("sicCodes") else None,
                        "founded_year": _safe_int(a.get("foundedYear")),
                        "company_status": a.get("companyStatus"),
                        "parent_id": a.get("parentId"),
                        "parent_name": a.get("parentName"),
                        "ultimate_parent_id": a.get("ultimateParentId"),
                        "ultimate_parent_name": a.get("ultimateParentName"),
                        "total_funding_amount": _safe_int(a.get("totalFundingAmount")),
                        "recent_funding_amount": _safe_int(a.get("recentFundingAmount")),
                        "recent_funding_date": _safe_utc_datetime(a.get("recentFundingDate")),
                        # socialMediaUrls is a list of objects — store as JSON
                        "social_media_urls": (
                            json.dumps(a.get("socialMediaUrls"))
                            if a.get("socialMediaUrls")
                            else None
                        ),
                        "logo": a.get("logo"),
                        "last_updated_date": _safe_utc_datetime(a.get("lastUpdatedDate")),
                        "is_defunct": a.get("isDefunct"),
                        "raw_attributes": json.dumps(a) if a else None,
                    },
                )
                enriched += 1

    except RuntimeError as e:
        if "[HTTP 403]" in str(e):
            log.warning(
                "Company enrichment skipped — 403 Access Denied. "
                "Ensure your ZoomInfo account includes Company Enrich."
            )
            return
        raise

    log.info(f"Company enrichment complete — {enriched} upserted, {skipped_match} no-match")


def _stream_per_company_enrich(
    configuration: dict,
    company_ids: list,
    table_name: str,
    row_builder,
    license_label: str,
    checkpoint_state: dict,
):
    """
    Streaming per-company enrichment with bounded memory.

    Workers fetch a single page from the per-company enrich endpoint at a time,
    build row dicts via ``row_builder(company_id, record)``, and push them onto
    a bounded queue. The main thread drains the queue and calls ``op.upsert``
    on each row, so memory holds at most ENRICH_QUEUE_MAX rows in flight
    instead of accumulating an entire company's record list per worker.

    Calls ``op.checkpoint(state=checkpoint_state)`` every CHECKPOINT_EVERY_N_ROWS
    successful upserts so Fivetran flushes buffered rows to the destination
    incrementally rather than buffering an entire million-row table in memory.
    Tech/scoops enrich tables don't have their own incremental cursor, so
    ``checkpoint_state`` is just the cumulative state of preceding tables —
    the resume semantics belong to those tables, not this one.

    SDK constraint: ``op.upsert`` and ``op.checkpoint`` only run on the main
    thread. Workers never call them directly; they only push to the queue.

    Sentinels on the queue (instead of row dicts):
        ("done", company_id)           — worker finished one company cleanly
        ("403", company_id)            — license missing; main thread sets the
                                         abort event and reports the warning
        ("error", company_id, exc)     — non-403 fetch failure for one company;
                                         main thread logs and continues
    """
    abort = threading.Event()
    work_queue: queue.Queue = queue.Queue(maxsize=ENRICH_QUEUE_MAX)

    def worker(cid: str):
        """Fetches one company's enrich rows and pushes them onto the queue.

        Runs in a thread-pool worker. Never calls op.upsert/op.checkpoint
        directly (SDK constraint — main-thread only); instead enqueues row
        dicts plus a terminal sentinel: ("done"/"403"/"error", cid[, exc]).
        """
        if abort.is_set():
            work_queue.put(("done", cid))
            return
        sent = False
        try:
            for page_results in row_builder.paginate(configuration, cid):
                if abort.is_set():
                    break
                for record in page_results:
                    row = row_builder.build_row(cid, record)
                    if row is not None:
                        work_queue.put(row)
        except RuntimeError as e:
            if "[HTTP 403]" in str(e):
                work_queue.put(("403", cid))
                sent = True
                return
            work_queue.put(("error", cid, e))
            sent = True
            return
        except (requests.exceptions.RequestException, ValueError, KeyError, TypeError) as e:
            work_queue.put(("error", cid, e))
            sent = True
            return
        finally:
            if not sent:
                work_queue.put(("done", cid))

    total_rows = 0
    companies_processed = 0
    license_missing = False
    expected_signals = len(company_ids)

    with ThreadPoolExecutor(max_workers=ENRICH_WORKERS) as pool:
        for cid in company_ids:
            pool.submit(worker, cid)

        while companies_processed < expected_signals:
            item = work_queue.get()
            if isinstance(item, dict):
                # A missing license means this table must be empty for the sync
                # (per README). Workers already running in parallel may have
                # queued rows before abort.set() landed — discard them rather
                # than partially populating the table, but keep draining the
                # queue so workers finish cleanly.
                if license_missing:
                    continue
                # Upsert one enriched row into the destination. op.upsert runs
                # only on the main thread (SDK constraint); workers enqueue rows.
                op.upsert(table=table_name, data=item)
                total_rows += 1
                if total_rows % CHECKPOINT_EVERY_N_ROWS == 0:
                    # Checkpoint periodically so Fivetran flushes buffered rows
                    # to the destination incrementally instead of holding the
                    # whole table in memory.
                    op.checkpoint(state=checkpoint_state)
                continue
            tag = item[0]
            cid = item[1]
            if tag == "done":
                companies_processed += 1
            elif tag == "403":
                if not license_missing:
                    log.warning(
                        f"{license_label} enrichment skipped — 403 Access Denied. "
                        f"Ensure your ZoomInfo account includes {license_label} Enrich."
                    )
                    license_missing = True
                    abort.set()
                companies_processed += 1
            elif tag == "error":
                exc = item[2]
                log.warning(f"{license_label} enrich failed for companyId={cid}: {exc}")
                companies_processed += 1

            if companies_processed % 100 == 0 and companies_processed > 0:
                log.info(
                    f"{license_label} enrichment progress: "
                    f"{companies_processed}/{expected_signals} companies"
                )

    if license_missing:
        return 0
    return total_rows


class _ScoopsEnrichBuilder:
    """Row-builder bundle for the per-company Scoops Enrich endpoint."""

    paginate = staticmethod(paginate_enrich_scoops)

    @staticmethod
    def build_row(company_id: str, record: dict) -> dict:
        """Maps one Scoops Enrich API record to a destination row dict."""
        record_id = record.get("id")
        a = record.get("attributes", {})
        company = a.get("company") or {}
        topics = a.get("topics")
        types = a.get("types")
        return {
            "id": record_id,
            "company_id": company_id,
            "company_name": company.get("name"),
            "description": a.get("description"),
            "topics": json.dumps(topics) if topics else None,
            "types": json.dumps(types) if types else None,
            "published_date": _safe_utc_datetime(a.get("publishedDate")),
            "original_published_date": _safe_utc_datetime(a.get("originalPublishedDate")),
            "link": a.get("link"),
            "link_text": a.get("linkText"),
            "raw_attributes": json.dumps(a) if a else None,
        }


def sync_scoops_enriched(configuration: dict, company_ids: list, cumulative_state: dict):
    """
    Enriches scoops per company and upserts to 'scoops_enriched'.
    Opt-in via 'enrich_scoops' configuration flag.
    Costs 1 credit per company enriched (each scoop counts as a record).

    Streams rows through a bounded queue (see _stream_per_company_enrich) so
    memory stays bounded regardless of company count or per-company scoop
    volume. ``op.upsert`` calls stay on the main thread for SDK safety.
    """
    if not _bool_config(configuration, "enrich_scoops"):
        log.info("Scoops enrichment skipped — enrich_scoops not enabled")
        return

    if not company_ids:
        log.info("Scoops enrichment skipped — no company IDs available")
        return

    log.info(
        f"Starting scoops enrichment — {len(company_ids)} companies, "
        f"{ENRICH_WORKERS} parallel workers (streaming, queue_max={ENRICH_QUEUE_MAX})..."
    )

    total = _stream_per_company_enrich(
        configuration,
        company_ids,
        table_name="scoops_enriched",
        row_builder=_ScoopsEnrichBuilder,
        license_label="Scoops",
        checkpoint_state=cumulative_state,
    )
    log.info(f"Scoops enrichment complete — {total} scoops across {len(company_ids)} companies")


class _TechnologiesEnrichBuilder:
    """Row-builder bundle for the per-company Technologies Enrich endpoint."""

    paginate = staticmethod(paginate_enrich_technologies)

    @staticmethod
    def build_row(company_id: str, record: dict) -> dict:
        """Maps one Technologies Enrich API record to a destination row dict."""
        tech_id = record.get("id")
        a = record.get("attributes", {})
        return {
            "id": f"{company_id}_{tech_id}",
            "company_id": company_id,
            "technology_id": tech_id,
            "attribute": a.get("attribute"),
            "product": a.get("product"),
            "vendor": a.get("vendor"),
            "category": a.get("category"),
            "category_parent": a.get("categoryParent"),
            "description": a.get("description"),
            "domain": a.get("domain"),
            "logo": a.get("logo"),
            "website": a.get("website"),
            "created_date": _safe_utc_datetime(a.get("createdDate")),
            "modified_date": _safe_utc_datetime(a.get("modifiedDate")),
            "raw_attributes": json.dumps(a) if a else None,
        }


def sync_technologies(configuration: dict, company_ids: list, cumulative_state: dict):
    """
    Enriches company technology stacks and upserts to 'technologies'.
    Opt-in via 'enrich_technologies' configuration flag.
    Costs 1 credit per company enriched.

    Each technology record is a separate row keyed by {company_id}_{tech_id}.
    A single company can have thousands of technology records, which is why
    this uses the streaming queue helper — accumulating per-company row lists
    OOM'd the connector in Fivetran cloud (1 GB limit, ~99K rows from 100
    companies). Streams rows through a bounded queue so memory stays flat.

    Confirmed response fields (from live API probe):
      id (technology id), attribute, product, vendor, category, categoryParent,
      description, domain, logo, website, createdDate, modifiedDate

    Input format: flat {"companyId": id} — NOT matchCompanyInput.
    No outputFields param — API returns all fields automatically.
    Pagination uses meta.totalResults only (no meta.page.total).
    """
    if not _bool_config(configuration, "enrich_technologies"):
        log.info("Technologies sync skipped — enrich_technologies not enabled")
        return

    if not company_ids:
        log.info("Technologies sync skipped — no company IDs available")
        return

    log.info(
        f"Starting technologies enrichment — {len(company_ids)} companies, "
        f"{ENRICH_WORKERS} parallel workers (streaming, queue_max={ENRICH_QUEUE_MAX})..."
    )

    total = _stream_per_company_enrich(
        configuration,
        company_ids,
        table_name="technologies",
        row_builder=_TechnologiesEnrichBuilder,
        license_label="Technologies",
        checkpoint_state=cumulative_state,
    )
    log.info(f"Technologies enrichment complete — {total} technology records upserted")


def sync_corporate_hierarchy(configuration: dict, company_ids: list):
    """
    Enriches corporate hierarchy for companies and upserts to 'corporate_hierarchy'.
    Opt-in via 'enrich_corporate_hierarchy' configuration flag.
    Costs 1 credit per company enriched.

    Returns the full family tree structure: parent company, subsidiaries,
    acquisitions, former names, and locations.
    """
    if not _bool_config(configuration, "enrich_corporate_hierarchy"):
        log.info("Corporate hierarchy sync skipped — enrich_corporate_hierarchy not enabled")
        return

    if not company_ids:
        log.info("Corporate hierarchy sync skipped — no company IDs available")
        return

    output_fields = _fields_config(
        configuration, "enrich_corp_hier_output_fields", DEFAULT_CORP_HIER_FIELDS
    )

    log.info(f"Starting corporate hierarchy enrichment — {len(company_ids)} companies queued...")

    total = 0
    skipped_match = 0

    try:
        for i in range(0, len(company_ids), ENRICH_BATCH_SIZE):
            batch = company_ids[i : i + ENRICH_BATCH_SIZE]
            batch_num = (i // ENRICH_BATCH_SIZE) + 1
            log.debug(
                f"Enriching corporate hierarchy batch {batch_num} ({len(batch)} companies)..."
            )

            results = enrich_corp_hierarchy_batch(configuration, batch, output_fields)

            for record in results:
                record_id = record.get("id")
                a = record.get("attributes", {})
                match_status = record.get("meta", {}).get("matchStatus", "")

                if match_status == "NO_MATCH":
                    skipped_match += 1
                    continue

                # PK is company_id (not record_id): the response 'id' is the
                # input matchId we sent, which is not stable across batches/syncs.
                # company_id (from attributes) is the stable ZoomInfo company ID.
                company_id = a.get("companyId") or record_id

                # Upsert one corporate-hierarchy row into the destination.
                # 'upsert' inserts or updates the record keyed by 'company_id'.
                op.upsert(
                    table="corporate_hierarchy",
                    data={
                        "company_id": company_id,
                        "match_status": match_status,
                        # parentage and familyTree are nested structures — store as JSON blobs
                        "family_tree": (
                            json.dumps(a.get("familyTree")) if a.get("familyTree") else None
                        ),
                        "parentage": (
                            json.dumps(a.get("parentage")) if a.get("parentage") else None
                        ),
                        "raw_attributes": json.dumps(a) if a else None,
                    },
                )
                total += 1

    except RuntimeError as e:
        if "[HTTP 403]" in str(e):
            log.warning(
                "Corporate hierarchy enrichment skipped — 403 Access Denied. "
                "Ensure your ZoomInfo account includes Corporate Hierarchy Enrich."
            )
            return
        raise

    log.info(
        f"Corporate hierarchy enrichment complete — {total} upserted, {skipped_match} no-match"
    )


def sync_usage(configuration: dict):
    """
    Fetches current API usage and limits from ZoomInfo and upserts to 'usage'.
    Always runs — no opt-in required. Free endpoint.

    Useful for customers to monitor credit consumption and rate limits
    directly in their data warehouse.

    Response shape (confirmed against live tenant):
      data: [{attributes: {usage: [
        {limitType: "requestLimit",      currentUsage, totalLimit, usageRemaining, description},
        {limitType: "recordLimit",       ...},
        {limitType: "uniqueRedeemLimit", ...},
        ...
      ]}}]

    Each limitType becomes its own row keyed by `limit_type`.
    """
    log.info("Fetching API usage data...")

    token = get_access_token(configuration)
    response = get_with_retry(f"{SEARCH_BASE}{ENDPOINT_USAGE}", headers=get_headers_get(token))

    if response.status_code == 401:
        _invalidate_token_cache()
        token = get_access_token(configuration)
        response = get_with_retry(f"{SEARCH_BASE}{ENDPOINT_USAGE}", headers=get_headers_get(token))

    # /users/usage acts as our auth preflight. A 401/403 here is unambiguous —
    # the credentials are bad or have been revoked, not a missing-product-license
    # situation (every authenticated tenant has access to /usage). Raise so the
    # Fivetran dashboard surfaces "auth failed" instead of silently completing
    # with empty enrich tables (every downstream enrich would 403 too, and our
    # 403-soft-skip logic would mask the real problem).
    if response.status_code in (401, 403):
        raise RuntimeError(
            f"ZoomInfo auth preflight failed on {ENDPOINT_USAGE} "
            f"[HTTP {response.status_code}]. Check that your client_id and "
            f"client_secret are valid and not revoked. Details: {response.text[:200]}"
        )

    if response.status_code != 200:
        log.warning(
            f"Usage data fetch failed [HTTP {response.status_code}]: {response.text[:200]}"
        )
        return

    payload = response.json()
    log.debug(f"Usage response: {json.dumps(payload)[:500]}")

    data = payload.get("data", [])
    if isinstance(data, dict):
        data = [data]

    upserted = 0
    for item in data:
        attrs = (item or {}).get("attributes", {}) or {}
        usage_rows = attrs.get("usage", [])
        for row in usage_rows:
            limit_type = row.get("limitType")
            if not limit_type:
                continue
            # Upsert one usage row into the destination. 'upsert' inserts or
            # updates the record keyed by its primary key ('id' = limitType).
            op.upsert(
                table="usage",
                data={
                    "id": limit_type,
                    "limit_type": limit_type,
                    "description": row.get("description"),
                    "current_usage": _safe_int(row.get("currentUsage")),
                    "total_limit": _safe_int(row.get("totalLimit")),
                    "usage_remaining": _safe_int(row.get("usageRemaining")),
                    "raw_attributes": json.dumps(row) if row else None,
                },
            )
            upserted += 1

    log.info(f"Usage data synced — {upserted} limit type(s) recorded")
