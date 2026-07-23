"""This connector syncs web unlocker results from Bright Data's Web Unlocker API to Fivetran.
See the Technical Reference documentation
(https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation
(https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading configuration from a JSON file
import json

# Helper functions for data processing, validation, and API interaction
from helpers import (
    collect_all_fields,
    perform_web_unlocker,
    process_and_upsert_results,
    process_unlocker_result,
    validate_configuration,
)

# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

__UNLOCKER_TABLE = "unlocker_results"


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {
            "table": __UNLOCKER_TABLE,
            "primary_key": [
                "requested_url",
                "result_index",
            ],
            "columns": {
                "requested_url": "STRING",
                "result_index": "INT",
            },
        }
    ]


def update(configuration: dict, state: dict):
    """
    Define the update function which lets you configure how your connector fetches data.
    See the technical reference documentation for more details on the update function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        state: a dictionary that holds the state of the connector.
    """
    log.warning("Example: Connectors : Bright Data Web Unlocker")

    validate_configuration(configuration=configuration)

    new_state = dict(state) if state else {}

    unlocker_url_input = configuration.get("unlocker_url", "")
    urls = parse_unlocker_urls(unlocker_url_input)

    if urls:
        sync_unlocker_urls(configuration=configuration, urls=urls, state=new_state)

    # Save the progress by checkpointing the state. This is important for ensuring that the sync
    # process can resume from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    op.checkpoint(state=new_state)


def sync_unlocker_urls(configuration: dict, urls: list, state: dict):
    """
    Fetch unlocker results for the requested URLs and upsert them to Fivetran.
    Args:
        configuration: Configuration dictionary containing unlocker parameters.
        urls: List of URLs to unlock/fetch.
        state: Current connector state.
    """
    api_token = configuration.get("api_token")
    country = configuration.get("country")
    data_format = configuration.get("data_format")
    format_param = configuration.get("format_param")
    method = configuration.get("method") or "GET"
    unlocker_zone = configuration.get("zone")

    payload = urls if len(urls) > 1 else urls[0]
    unlocker_results = perform_web_unlocker(
        api_token=api_token,
        url=payload,
        zone=unlocker_zone,
        country=country,
        method=method,
        format_param=format_param,
        data_format=data_format,
    )

    if not isinstance(unlocker_results, list):
        unlocker_results = [unlocker_results]

    processed_results = []
    for index, result in enumerate(unlocker_results):
        requested_url = result.get("requested_url") if isinstance(result, dict) else None
        if not requested_url:
            requested_url = urls[index % len(urls)]
        processed_results.append(process_unlocker_result(result, requested_url, index))

    if not processed_results:
        log.warning("No unlocker results returned from API")
        return

    log.info(f"Upserting {len(processed_results)} unlocker results to Fivetran")

    all_fields = collect_all_fields(processed_results)
    process_and_upsert_results(processed_results, all_fields, __UNLOCKER_TABLE)

    state["last_unlocker_urls"] = urls
    state["last_unlocker_count"] = len(processed_results)


def parse_unlocker_urls(unlocker_url_input) -> list:
    """
    Normalize the unlocker_url configuration value into a list of URLs.
    Args:
        unlocker_url_input: The unlocker_url configuration value (various formats supported).
    Returns:
        list: List of normalized URL strings.
    """
    if not unlocker_url_input:
        return []

    if isinstance(unlocker_url_input, list):
        return [
            item.strip() for item in unlocker_url_input if isinstance(item, str) and item.strip()
        ]

    if isinstance(unlocker_url_input, str):
        try:
            parsed = json.loads(unlocker_url_input)
            if isinstance(parsed, list):
                return [item.strip() for item in parsed if isinstance(item, str) and item.strip()]
            if isinstance(parsed, str) and parsed.strip():
                return [parsed.strip()]
        except (json.JSONDecodeError, TypeError):
            pass

        if "," in unlocker_url_input:
            return [item.strip() for item in unlocker_url_input.split(",") if item.strip()]

        if "\n" in unlocker_url_input:
            return [item.strip() for item in unlocker_url_input.split("\n") if item.strip()]

        return [unlocker_url_input.strip()] if unlocker_url_input.strip() else []

    return []


connector = Connector(update=update, schema=schema)


if __name__ == "__main__":
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    connector.debug(configuration=configuration)
