---
name: Investran Report Wizard
description: "Run Investran Report Wizard reports through the REST API and load the CSV results"
category: "SaaS & APIs"
---

# Investran Report Wizard Connector Example

## Connector overview
This connector runs one or more saved Investran (FIS) Report Wizard reports through the Report Wizard REST API and loads the CSV result of each report into its own destination table. It suits teams that already maintain reports in Investran and want those results, such as journal entries or transaction data, replicated to a warehouse on a schedule.

For each report, the connector starts an execution, polls its status until it succeeds, streams the CSV to a temporary file, and upserts the rows. Because the report columns are defined in Investran, the connector infers them from the CSV header instead of hard-coding them.

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
fivetran init --template investran_report_wizard
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`. For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/connector-development-and-configuration/connector-sdk-commands#fivetraninit).

> Note: Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the required configuration parameters.

## Features
- Runs several reports in one sync, one destination table per report (`__table_for`).
- Authenticates with an OAuth2 refresh token. The identity provider rotates the refresh token on each exchange, so the connector stores the latest one in state and checkpoints it before any data is read (`get_access_token`).
- Waits for an already running execution of the same report instead of failing (`start_execution`).
- Streams large CSV results to disk and reads them row by row, so memory use stays flat (`download_result`, `load_report`).
- Retries timeouts, connection errors, HTTP 429 and 5xx responses with exponential backoff (`api_call`).
- Checkpoints every `checkpoint_rows` rows and after each report.

## Configuration file
```
{
  "token_url": "<YOUR_IDENTITY_PROVIDER_TOKEN_URL>",
  "client_id": "<YOUR_CLIENT_ID>",
  "redirect_uri": "<YOUR_REDIRECT_URI>",
  "refresh_token": "<YOUR_REFRESH_TOKEN>",
  "base_url": "<YOUR_INVESTRAN_REPORT_WIZARD_API_BASE_URL>",
  "report_ids": "<YOUR_COMMA_SEPARATED_REPORT_IDS>",
  "primary_keys": "<OPTIONAL_COMMA_SEPARATED_PRIMARY_KEY_COLUMNS>",
  "table_prefix": "<OPTIONAL_TABLE_PREFIX_DEFAULT_REPORT>",
  "poll_interval_seconds": "<OPTIONAL_POLL_INTERVAL_SECONDS_DEFAULT_5>",
  "poll_timeout_seconds": "<OPTIONAL_POLL_TIMEOUT_SECONDS_DEFAULT_900>",
  "checkpoint_rows": "<OPTIONAL_CHECKPOINT_ROWS_DEFAULT_2000>"
}
```

- `token_url`, `client_id`, `redirect_uri` - OAuth2 settings of the identity provider that fronts your Investran environment.
- `refresh_token` - A refresh token issued for that client. It is used as the starting point only, because the connector keeps the rotated token in state.
- `base_url` - Report Wizard API base URL of your Investran environment.
- `report_ids` - Comma-separated or semicolon-separated Report Wizard report ids.
- `primary_keys` - Optional comma-separated column names, after normalization, that uniquely identify a row. When empty, the connector adds a `row_hash` column built from every column of the row.
- `table_prefix` - Optional prefix of the destination tables. Defaults to `report`.
- `poll_interval_seconds`, `poll_timeout_seconds`, `checkpoint_rows` - Optional tuning values.

> Note: Optional keys keep an angle-bracket placeholder in `configuration.json`. A value that is empty or starts with `<` is treated as not set and the default is used.

> Note: When submitting connector code as a community connector in the open-source [Community Connector repository](https://github.com/fivetran/community_connectors/tree/main), ensure the `configuration.json` file has placeholder values. When adding the connector to your production repository, ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Authentication
The connector uses the OAuth2 refresh token grant. It exchanges the refresh token for an access token, reuses the access token until less than a day of its lifetime is left, and sends it as a bearer token. If the refresh token has been used elsewhere and is rejected, set a new `refresh_token` in the configuration. A newly configured token takes priority over the one stored in state.

## Data handling
Each CSV header is normalized to a lowercase column name with underscores, for example `Unrealized Asset - AVM` becomes `unrealized_asset_avm`. Empty values are stored as null. Every run re-executes the reports and upserts the full result, so choose `primary_keys` that are unique per row, and avoid columns that change on every run, such as a run date.

## Error handling
- `validate_configuration` fails fast on missing or invalid settings.
- `api_call` retries transient failures with exponential backoff. The execution POST is never retried after a timeout, because that could start a second execution.
- A failed or timed out execution raises an error and fails the sync, so state is not advanced past incomplete data.
- The temporary CSV file is always deleted.

## Tables created
The connector creates one table per report named `<table_prefix>_<report_id>`. Columns come from the report. The primary key is `primary_keys`, or `row_hash` when it is not set.

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
