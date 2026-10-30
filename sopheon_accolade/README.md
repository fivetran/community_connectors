---
name: Sopheon Accolade
description: "Run Sopheon Accolade reports through the REST API and load the results with $top/$skip paging"
category: "SaaS & APIs"
---

# Sopheon Accolade Connector Example

## Connector overview
This connector runs saved reports in Sopheon Accolade, a product and portfolio management platform, through its REST API and loads the result of each report into its own destination table. It suits teams that expose project, portfolio, or reporting data through Accolade reports and want it replicated to a warehouse.

For each configured report, the connector calls the `RunReport` endpoint, pages through the result with `$top` and `$skip`, and upserts every row. Report columns are defined in Accolade, so the connector declares only the table name and primary key and lets the columns be inferred from the data.

## Requirements
- [Supported Python versions](https://github.com/fivetran/community_connectors/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

To initialize a new Connector SDK project using this connector as a starting point, run:

```
fivetran init --template sopheon_accolade
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`. For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/connector-development-and-configuration/connector-sdk-commands#fivetraninit).

> Note: Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the required configuration parameters.

## Features
- Syncs any number of Accolade reports, one destination table per report (`get_reports`, `schema`).
- Authenticates with an OAuth2 refresh token (`authenticate`).
- Pages through large reports with `$top` and `$skip` and checkpoints after each page (`sync_report`).
- Accepts the different wrapper shapes a report response can have (`extract_rows`).
- Retries timeouts, connection errors, HTTP 429 and 5xx responses with exponential backoff (`get_page`).
- Supports a custom CA bundle for environments with private certificates.

## Configuration file
```
{
  "auth_url": "<YOUR_ACCOLADE_TOKEN_URL>",
  "base_url": "<YOUR_ACCOLADE_API_BASE_URL>",
  "refresh_token": "<YOUR_ACCOLADE_REFRESH_TOKEN>",
  "reports": "[{\"report_name\": \"<YOUR_REPORT_NAME>\", \"primary_key\": [\"<PRIMARY_KEY_COLUMN>\"]}]",
  "ca_bundle_path": "<OPTIONAL_CA_BUNDLE_PATH>",
  "page_size": "<OPTIONAL_PAGE_SIZE_DEFAULT_10000>",
  "request_timeout_seconds": "<OPTIONAL_REQUEST_TIMEOUT_SECONDS_DEFAULT_300>"
}
```

- `auth_url` - Token endpoint of your Accolade environment.
- `base_url` - API base URL of your Accolade environment.
- `refresh_token` - Refresh token issued to an Accolade API user.
- `reports` - JSON list of reports to sync. Each entry has a `report_name`, which is also the destination table name, and a `primary_key` list of column names that uniquely identify a row.
- `ca_bundle_path` - Optional path of a CA bundle file for private certificates. TLS verification is always on.
- `page_size`, `request_timeout_seconds` - Optional tuning values.

> Note: Optional keys keep an angle-bracket placeholder in `configuration.json`. A value that is empty or starts with `<` is treated as not set and the default is used.

> Note: When submitting connector code as a community connector in the open-source [Community Connector repository](https://github.com/fivetran/community_connectors/tree/main), ensure the `configuration.json` file has placeholder values. When adding the connector to your production repository, ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Authentication
The connector posts the refresh token to `auth_url` with the `refresh_token` grant and uses the returned access token as a bearer token for the whole sync. Ask your Accolade administrator to create an API user with access to the reports you want to sync and to issue a refresh token for it.

## Pagination
Each request sends `$top` set to `page_size` and `$skip` set to the number of rows already read. Paging stops when a page contains fewer rows than `page_size`.

## Data handling
Rows are upserted exactly as returned by the report. Every run reads the full report, so the primary key must be unique per row.

## Error handling
- `validate_configuration` fails fast on missing or invalid settings.
- `get_page` retries transient failures with exponential backoff and raises on non-retryable errors.
- A failing report fails the sync instead of being skipped, so a partial result is never reported as a success.

## Tables created
The connector creates one table per entry in `reports`, named after `report_name`. Columns come from the report and the primary key is the configured `primary_key`.

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
