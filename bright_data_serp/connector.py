"""This connector syncs search engine results from Bright Data's SERP REST API to Fivetran.
See the Technical Reference documentation
(https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation
(https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading configuration from a JSON file
import json

# Helper functions for data processing and API interaction
from helpers import (
    collect_all_fields,
    perform_search,
    process_and_upsert_results,
    process_search_result,
)

# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

__SERP_TABLE = "search_results"
__CHECKPOINT_INTERVAL = 100
__VALID_SEARCH_ENGINES = {"google", "bing", "yandex"}
__VALID_RESPONSE_FORMATS = {"json", "html"}


def validate_configuration(configuration: dict) -> None:
    """
    Validate the configuration dictionary to ensure it contains all required parameters
    and that optional values have the expected types and constraints.
    This function is called at the start of the update method to ensure that the connector
    has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing or a value is invalid.
    """
    required_configs = ["api_token", "search_query"]
    for key in required_configs:
        if key not in configuration or not configuration.get(key):
            raise ValueError(f"Missing required configuration value: {key}")

    api_token = configuration.get("api_token")
    if not isinstance(api_token, str):
        raise ValueError("api_token must be a string")

    search_query = configuration.get("search_query")
    if not isinstance(search_query, (str, list)):
        raise ValueError("search_query must be a string or list of strings")
    if isinstance(search_query, list) and (
        not search_query
        or not all(isinstance(item, str) and item.strip() for item in search_query)
    ):
        raise ValueError("search_query list must contain non-empty strings")

    search_engine = configuration.get("search_engine")
    if search_engine:
        if not isinstance(search_engine, str):
            raise ValueError("search_engine must be a string")
        if search_engine.lower() not in __VALID_SEARCH_ENGINES:
            raise ValueError(
                f"search_engine must be one of: {', '.join(sorted(__VALID_SEARCH_ENGINES))}"
            )

    response_format = configuration.get("format")
    if response_format:
        if not isinstance(response_format, str):
            raise ValueError("format must be a string")
        if response_format.lower() not in __VALID_RESPONSE_FORMATS:
            raise ValueError(
                f"format must be one of: {', '.join(sorted(__VALID_RESPONSE_FORMATS))}"
            )

    country = configuration.get("country")
    if country:
        if not isinstance(country, str):
            raise ValueError("country must be a string")
        if len(country) != 2 or not country.isalpha():
            raise ValueError("country must be a 2-letter ISO 3166-1 alpha-2 country code")

    search_zone = configuration.get("search_zone")
    if search_zone is not None and search_zone != "":
        if not isinstance(search_zone, str) or not search_zone.strip():
            raise ValueError("search_zone must be a non-empty string")


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
            "table": __SERP_TABLE,
            "primary_key": [
                "query",
                "result_index",
            ],
            "columns": {
                "query": "STRING",
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
    log.warning("Example: Connectors : Bright Data SERP")

    validate_configuration(configuration=configuration)

    api_token = configuration.get("api_token")
    new_state = dict(state) if state else {}

    search_query_input = configuration.get("search_query", "")
    queries = parse_search_queries(search_query_input)

    if queries:
        sync_search_queries(
            configuration=configuration,
            queries=queries,
            api_token=api_token,
            state=new_state,
        )

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
    # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
    op.checkpoint(state=new_state)


def sync_search_queries(configuration: dict, queries: list, api_token: str, state: dict):
    """
    Fetch search results for the requested queries and upsert them to Fivetran.

    Processes one query at a time so progress can be checkpointed after each query's
    rows are upserted. See:
    https://fivetran.com/docs/connector-sdk/best-practices#checkpointregularlywhensyncing
    Args:
        configuration: Configuration dictionary containing search parameters.
        queries: List of search queries to execute.
        api_token: Bright Data API token.
        state: Current connector state.
    """
    search_engine = configuration.get("search_engine")
    country = configuration.get("country")
    search_zone = configuration.get("search_zone")
    response_format = configuration.get("format")

    completed_queries = [
        query for query in (state.get("completed_queries") or []) if isinstance(query, str)
    ]
    # Drop state entries that are no longer in the current config so a query removed
    # and later re-added will be searched again.
    completed_queries = [query for query in completed_queries if query in queries]
    total_upserted = int(state.get("last_search_count") or 0)

    for query in queries:
        if query in completed_queries:
            log.info(f"Skipping previously completed query: {query}")
            continue

        search_results = perform_search(
            api_token=api_token,
            query=query,
            search_engine=search_engine,
            country=country,
            zone=search_zone,
            response_format=response_format,
        )

        processed_results = process_search_results(search_results, [query])

        if not processed_results:
            log.warning(f"No search results returned from API for query: {query}")
            completed_queries.append(query)
            state["completed_queries"] = completed_queries
            state["last_search_queries"] = list(completed_queries)
            state["last_search_count"] = total_upserted
            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
            # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
            op.checkpoint(state=state)
            continue

        log.info(f"Upserting {len(processed_results)} search results for query: {query}")

        all_fields = collect_all_fields(processed_results)
        upserted_count = process_and_upsert_results(
            processed_results,
            all_fields,
            __SERP_TABLE,
            state=state,
            checkpoint_interval=__CHECKPOINT_INTERVAL,
        )
        total_upserted += upserted_count

        completed_queries.append(query)
        state["completed_queries"] = completed_queries
        state["last_search_queries"] = list(completed_queries)
        state["last_search_count"] = total_upserted

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
        # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
        op.checkpoint(state=state)
        log.info(
            f"Checkpointed after query '{query}' "
            f"({len(completed_queries)}/{len(queries)} queries complete)"
        )


def process_search_results(search_results, queries: list) -> list:
    """
    Normalize API results into flattened rows.
    Args:
        search_results: Raw search results from the Bright Data API.
        queries: List of queries that were executed.
    Returns:
        list: Processed result dictionaries ready for upsert.
    """
    processed_results = []

    if isinstance(search_results, list) and len(queries) > 1:
        for query_idx, query in enumerate(queries):
            if query_idx < len(search_results):
                query_results = search_results[query_idx]
                if isinstance(query_results, list):
                    for result_idx, result in enumerate(query_results):
                        processed_results.append(process_search_result(result, query, result_idx))
                elif isinstance(query_results, dict):
                    processed_results.append(process_search_result(query_results, query, 0))
    elif isinstance(search_results, list):
        for idx, result in enumerate(search_results):
            processed_results.append(process_search_result(result, queries[0], idx))
    elif isinstance(search_results, dict):
        processed_results.append(process_search_result(search_results, queries[0], 0))

    return processed_results


def parse_search_queries(search_query_input) -> list:
    """
    Normalize the search_query configuration value into a list of queries.
    Args:
        search_query_input: The search_query configuration value (various formats supported).
    Returns:
        list: List of normalized query strings.
    """
    if not search_query_input:
        return []

    if isinstance(search_query_input, list):
        return [
            item.strip() for item in search_query_input if isinstance(item, str) and item.strip()
        ]

    if isinstance(search_query_input, str):
        try:
            parsed = json.loads(search_query_input)
            if isinstance(parsed, list):
                return [item.strip() for item in parsed if isinstance(item, str) and item.strip()]
            if isinstance(parsed, str) and parsed.strip():
                return [parsed.strip()]
        except (json.JSONDecodeError, TypeError):
            pass

        if "," in search_query_input:
            return [item.strip() for item in search_query_input.split(",") if item.strip()]

        if "\n" in search_query_input:
            return [item.strip() for item in search_query_input.split("\n") if item.strip()]

        return [search_query_input.strip()] if search_query_input.strip() else []

    return []


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
