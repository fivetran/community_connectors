# Bright Data Web Unlocker Connector Example

## Connector overview

This connector syncs web page content from Bright Data's Web Unlocker API to your Fivetran destination. It fetches unlocked page data for one or more URLs, flattens nested JSON responses, and upserts results to an `unlocker_results` table.

## Requirements

- [Supported Python versions](https://github.com/fivetran/connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connector-sdk/setup-guide) to get started.

To initialize a new Connector SDK project using this connector as a starting point, run:

```bash
fivetran init <project-path> --template connectors/bright_data_serp
```
```fivetran init``` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with ```fivetran debug```. For more information on ```fivetran init```, refer to the [Connector SDK init documentation](https://fivetran.com/docs/connector-sdk/connector-development-and-configuration/connector-sdk-commands#fivetraninit).

Note: Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](https://github.com/fivetran/community_connectors/pull/41#configuration-file) section for details on the required configuration parameters.

## Features

- Fetches unlocked web content via Bright Data Web Unlocker API
- Supports multiple URL input formats (single URL, comma-separated, newline-separated, JSON array string)
- Flattens nested JSON structures for analysis
- Dynamically discovers fields from API responses
- Includes retry logic with exponential backoff for transient API errors
- Checkpoints state after each sync

## Configuration file

```json
{
  "api_token": "<YOUR_BRIGHT_DATA_API_TOKEN>",
  "unlocker_url": "<YOUR_UNLOCKER_URL>",
  "country": "us",
  "data_format": "markdown",
  "zone": "web_unlocker1",
  "format_param": "json"
}
```

Configuration parameters:

- `api_token` (required): Your Bright Data API token
- `unlocker_url` (required): URL or URLs to fetch via the Web Unlocker
- `zone` (optional): Bright Data unlocker zone identifier. Defaults to `web_unlocker1`
- `country` (optional): ISO 3166-1 alpha-2 country code. Defaults to `us`
- `method` (optional): HTTP method. Defaults to `GET`
- `format_param` (optional): Response format (`json` or `html`). Defaults to `json`
- `data_format` (optional): Content format such as `markdown` or `html`

## Requirements file

This connector does not require any additional Python packages beyond what is pre-installed in the Fivetran environment.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

The Bright Data API uses Bearer token authentication. Obtain your API token from the Bright Data dashboard at https://brightdata.com/cp/setting/users.

## Data handling

1. Configuration validation — refer to `validate_configuration()` in `helpers/validation.py`
2. URL parsing — refer to `parse_unlocker_urls()` in `connector.py`
3. API requests — refer to `perform_web_unlocker()` in `helpers/unlocker.py`
4. Result processing — refer to `process_unlocker_result()` in `helpers/data_processing.py`
5. Data upsertion — refer to `process_and_upsert_results()`
6. State checkpointing — refer to `op.checkpoint()` in `update()`

## Error handling

- Retry logic for transient HTTP errors (408, 429, 500, 502, 503, 504) with exponential backoff — refer to `_execute_unlocker_request()` in `helpers/unlocker.py`
- Non-retryable failures raise `RuntimeError` with API error details
- Primary key validation issues are logged once per sync via `log.warning()`

## Tables created

The connector creates a single table named `unlocker_results` with the following schema (refer to the `schema()` function):

    {
      "table": "unlocker_results",
      "primary_key": ["requested_url", "result_index"],
      "columns": {
        "requested_url": "STRING",
        "result_index": "INT"
      }
    }
## Additional files

- `helpers/validation.py` — Configuration parameter validation
- `helpers/unlocker.py` — Bright Data Web Unlocker API interaction and retry logic
- `helpers/data_processing.py` — Data flattening, field discovery, and upsert utilities

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
