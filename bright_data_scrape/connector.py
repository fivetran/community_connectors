"""This connector syncs web scraping data from Bright Data's Web Scraper API to Fivetran destination.
See the Technical Reference documentation
(https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation
(https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading configuration from a JSON file
import json

# For parsing URLs
from urllib.parse import urlparse

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like upsert(), update(), delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Helper functions for data processing and validation
from helpers import (
    collect_all_fields,
    perform_scrape,
    process_scrape_result,
    validate_configuration,
)

# Table name constant
__SCRAPE_TABLE = "scrape_results"

# Linkedin Post By URL dataset ids
__LINKEDIN_POST_BY_URL_DATASET_ID = "gd_d85r5d60186q96c883"


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
            "table": __SCRAPE_TABLE,
            "primary_key": [
                "url",
                "result_index",
            ],
            "columns": {
                "url": "STRING",
                "result_index": "INT",
            },
        }
    ]


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-code/connector-sdk-methods#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """
    log.warning("Example: Connectors : Bright Data Web Scraper")

    # Validate the configuration to ensure it contains all required values
    validate_configuration(configuration=configuration)

    api_token = configuration.get("api_token")
    dataset_id = configuration.get("dataset_id")
    scrape_url_input = configuration.get("scrape_url", "")
    new_state = dict(state) if state else {}

    urls = parse_scrape_urls(scrape_url_input)

    if not urls:
        message = f"No URLs provided in configuration; scrape_url input: {scrape_url_input}"
        log.error(message)
        raise RuntimeError(message)
    sync_scrape_urls(api_token, dataset_id, urls, new_state)


def parse_scrape_urls(scrape_url_input):
    """
    Parse URLs from configuration input, supporting multiple formats.
    Args:
        scrape_url_input: The scrape_url configuration value (various formats supported).
    Returns:
        list: List of URL strings.
    """
    if not scrape_url_input:
        return []

    if isinstance(scrape_url_input, list):
        return [
            item.strip() for item in scrape_url_input if isinstance(item, str) and item.strip()
        ]

    if isinstance(scrape_url_input, str):
        # Try parsing as JSON first (e.g. '["https://..."]' or '"https://..."')
        try:
            parsed = json.loads(scrape_url_input)
            if isinstance(parsed, list):
                return [item.strip() for item in parsed if isinstance(item, str) and item.strip()]
            if isinstance(parsed, str) and parsed.strip():
                return [parsed.strip()]
        except (json.JSONDecodeError, TypeError):
            # Not valid JSON – treat as plain string (single URL or delimited list)
            pass

        # Prefer newlines for multi-URL input so commas in query strings are safer.
        if "\n" in scrape_url_input:
            return [item.strip() for item in scrape_url_input.split("\n") if item.strip()]

        # Comma-split only when the unsplit string is not itself a valid URL, or when
        # every comma-delimited token is a valid URL (true multi-URL list).
        # This preserves query commas like https://example.com/search?q=a,b.
        if "," in scrape_url_input:
            stripped = scrape_url_input.strip()
            candidates = [item.strip() for item in scrape_url_input.split(",") if item.strip()]
            if len(candidates) > 1 and all(_is_valid_url(c) for c in candidates):
                return candidates
            if _is_valid_url(stripped):
                return [stripped]
            return candidates

        # Single URL (or invalid string – downstream validation can filter)
        return [scrape_url_input.strip()] if scrape_url_input.strip() else []

    return []


def _is_valid_url(url: str) -> bool:
    """Return True if the string has a valid URL structure (scheme and netloc)."""
    if not url or not isinstance(url, str) or not url.strip():
        return False
    parsed = urlparse(url.strip())
    return bool(parsed.scheme and parsed.netloc)


def sync_scrape_urls(api_token, dataset_id, urls, state):
    """
    Sync scrape results for the requested URLs.

    Incremental behavior: URLs already recorded in state["last_scrape_urls"] are
    skipped. Only newly configured URLs are scraped. Reset connector state in
    Fivetran to re-scrape previously synced URLs.
    Args:
        api_token: Bright Data API token.
        dataset_id: ID of the dataset to use for scraping.
        urls: List of URLs to scrape (processed in batch by API).
        state: State dictionary for tracking sync progress.
    """
    valid_urls = []
    for url in urls:
        if _is_valid_url(url):
            valid_urls.append(url.strip())
        else:
            log.warning(f"Skipping invalid URL: {url}")

    if not valid_urls:
        log.warning("No valid URLs to sync after filtering invalid entries")
        raise RuntimeError("No valid URLs configured for sync")

    previously_synced = {
        url for url in (state.get("last_scrape_urls") or []) if isinstance(url, str)
    }
    # Drop state entries that are no longer in the current config so a URL removed
    # and later re-added will be scraped again.
    previously_synced &= set(valid_urls)
    urls_to_scrape = [url for url in valid_urls if url not in previously_synced]

    if not urls_to_scrape:
        log.info(
            f"All {len(valid_urls)} configured URL(s) were already synced; "
            f"nothing new to scrape"
        )
        state["last_scrape_urls"] = valid_urls
        state["last_scrape_count"] = 0
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
        # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
        op.checkpoint(state)
        return

    skipped_count = len(valid_urls) - len(urls_to_scrape)
    if skipped_count:
        log.info(
            f"Skipping {skipped_count} previously synced URL(s); "
            f"scraping {len(urls_to_scrape)} new URL(s)"
        )
    else:
        log.info(f"Starting scrape sync for {len(urls_to_scrape)} URL(s)")

    # Fetch scrape results for new URLs only
    # Apply dataset-specific query parameters when needed
    if dataset_id == __LINKEDIN_POST_BY_URL_DATASET_ID:
        scrape_results = perform_scrape(
            api_token=api_token,
            dataset_id=dataset_id,
            url=urls_to_scrape,
            extra_query_params={"discover_by": "profile_url", "type": "discover_new"},
        )
    else:
        scrape_results = perform_scrape(
            api_token=api_token,
            dataset_id=dataset_id,
            url=urls_to_scrape,
        )

    # Normalize results to always be a list
    if not isinstance(scrape_results, list):
        scrape_results = [scrape_results]

    if not scrape_results:
        log.warning("No scrape results returned from API")
        # Still mark URLs as synced to avoid repeatedly triggering empty jobs.
        state["last_scrape_urls"] = list(dict.fromkeys([*previously_synced, *urls_to_scrape]))
        state["last_scrape_count"] = 0
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
        # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
        op.checkpoint(state)
        return

    # Process and flatten results
    processed_results = process_scrape_results(scrape_results, urls_to_scrape)

    if not processed_results:
        log.warning("No processed results to upsert")
        state["last_scrape_urls"] = list(dict.fromkeys([*previously_synced, *urls_to_scrape]))
        state["last_scrape_count"] = 0
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
        # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
        op.checkpoint(state)
        return

    log.info(f"Upserting {len(processed_results)} scrape results to Fivetran")

    all_fields = collect_all_fields(processed_results)

    # Upsert each result
    process_and_upsert_results(processed_results, all_fields)

    # Persist incremental progress: previously synced + newly scraped URLs
    state["last_scrape_urls"] = list(dict.fromkeys([*previously_synced, *urls_to_scrape]))
    state["last_scrape_count"] = len(processed_results)

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
    # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
    op.checkpoint(state)

    log.info(f"Completed scrape sync. Total synced: {len(processed_results)} results")


def _extract_result_url(result, fallback_url=None):
    """Prefer the URL embedded in a scrape result payload over index-based attribution."""
    if isinstance(result, dict):
        input_field = result.get("input")
        if isinstance(input_field, dict) and input_field.get("url"):
            return str(input_field["url"])
        if result.get("url"):
            return str(result["url"])
    return fallback_url


def process_scrape_results(scrape_results, urls):
    """
    Process and flatten scrape results.
    Args:
        scrape_results: List of scrape results from API.
        urls: List of URLs that were scraped.
    Returns:
        list: List of processed result dictionaries.
    """
    processed_results = []
    url_result_counts = {url: 0 for url in urls}
    skipped_unattributed = 0

    # Flat batch responses are not guaranteed to align 1:1 with request URL order.
    if len(urls) > 1 and len(scrape_results) != len(urls):
        log.warning(
            f"Result count ({len(scrape_results)}) differs from URL count ({len(urls)}). "
            f"Attributing results by embedded URL when available."
        )

    # Flatten nested list payloads into individual result items.
    flat_items = []
    for source_idx, result in enumerate(scrape_results):
        if isinstance(result, list):
            for item in result:
                flat_items.append((item, source_idx))
        else:
            flat_items.append((result, source_idx))

    allow_index_fallback = len(urls) == 1 or len(scrape_results) == len(urls)

    for result, source_idx in flat_items:
        fallback_url = None
        if allow_index_fallback:
            if len(urls) == 1:
                fallback_url = urls[0]
            elif source_idx < len(urls):
                fallback_url = urls[source_idx]

        result_url = _extract_result_url(result, fallback_url=fallback_url)
        if not result_url:
            skipped_unattributed += 1
            continue

        result_index = url_result_counts.get(result_url, 0)
        processed_results.append(process_scrape_result(result, result_url, result_index))
        url_result_counts[result_url] = result_index + 1

    missing_urls = [url for url in urls if url_result_counts.get(url, 0) == 0]
    if missing_urls:
        log.warning(
            f"No result found for {len(missing_urls)} URL(s): "
            f"{', '.join(missing_urls[:5])}"
            f"{' (and more)' if len(missing_urls) > 5 else ''}"
        )

    if skipped_unattributed:
        log.warning(
            f"Skipped {skipped_unattributed} result(s) that could not be attributed to a URL"
        )

    return processed_results


def process_and_upsert_results(processed_results, all_fields):
    """
    Process and upsert scrape result records.
    Args:
        processed_results: List of processed result dictionaries.
        all_fields: List of all field names discovered from results.
    """
    primary_keys = {"url": str, "result_index": int}
    primary_key_errors = []
    for result in processed_results:
        # Ensure primary keys are always present with correct types
        for pk, pk_type in primary_keys.items():
            if pk not in result:
                primary_key_errors.append(f"Primary key '{pk}' missing from result")
                result[pk] = pk_type() if pk_type == str else 0
            else:
                current_value = result[pk]
                if not isinstance(current_value, pk_type):
                    try:
                        if pk_type == str:
                            result[pk] = str(current_value)
                        elif pk_type == int:
                            if isinstance(current_value, str):
                                cleaned = current_value.strip().strip("[]\"'")
                                result[pk] = int(cleaned) if cleaned.isdigit() else 0
                            else:
                                result[pk] = int(current_value)
                    except (ValueError, TypeError):
                        primary_key_errors.append(
                            f"Could not convert primary key '{pk}' to {pk_type.__name__}"
                        )
                        result[pk] = pk_type() if pk_type == str else 0
        row = {}
        for field in all_fields:
            row[field] = result.get(field)

        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table=__SCRAPE_TABLE, data=row)

    # Log primary key errors once after processing all results
    if primary_key_errors:
        unique_errors = list(set(primary_key_errors))
        log.warning(
            f"Primary key validation issues: {', '.join(unique_errors[:3])}"
            f"{' (and more)' if len(unique_errors) > 3 else ''}"
        )


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
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
