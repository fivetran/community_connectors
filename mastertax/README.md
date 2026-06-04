# MasterTax Connector Example

## Connector overview

This connector fetches data extracts from the MasterTax API provided by [ADP](https://api.adp.com). It supports downloading ZIP files containing tab-delimited data extracts, which are parsed and streamed to Fivetran. The connector operates statelessly and dynamically handles certificate-based authentication for secure API access. It is designed for compatibility with Fivetran's Connector SDK and adheres to the standard schema-update model.

## Requirements

- [Supported Python versions](https://github.com/fivetran/community_connectors/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

To initialize a new Connector SDK project using this connector as a starting point, run:

```
fivetran init --template mastertax
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`. For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/connector-development-and-configuration/connector-sdk-commands#fivetraninit).

> Note: Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the required configuration parameters.

## Features

- Retrieves MasterTax data extracts defined in the `constants.py` file
- Handles certificate creation dynamically from config for each run
- Parses tab-delimited text files from downloaded ZIP archives
- Supports streaming upsert operations for Fivetran ingestion
- Configurable schema with primary key definition

## Configuration file

The connector reads configuration details from a `configuration.json` file. Required keys include:

```json
{
  "clientId": "<YOUR_CLIENT_ID>",
  "clientSecret": "<YOUR_CLIENT_SECRET>",
  "crtFile": "<certificate string>",
  "keyFile": "<key string>"
}
```

> Note: When submitting connector code as a [Community Connector](https://github.com/fivetran/community_connectors/tree/main) in the open-source [Connector SDK repository](https://github.com/fivetran/community_connectors/tree/main), ensure the `configuration.json` file has placeholder values. When adding the connector to your production repository, ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

Specify additional libraries needed by your connector in `requirements.txt` (if any).

> Note: [Some packages](https://fivetran.com/docs/connector-sdk/technical-reference#preinstalledpackages) are pre-installed in the Connector SDK runtime environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

Authentication is handled via OAuth2 client credentials. Certificates are generated dynamically at runtime from the config and used for mutual TLS (mTLS) authentication with ADP's API. An access token is requested and added to request headers for each session.

## Pagination

Pagination is not required. The connector fetches full data extracts and waits for the process to complete before downloading results.

## Data handling

- Each extract maps to a layout defined in the `data_extracts` dictionary from `constants.py`
- The connector reads tab-delimited files and uses column mappings defined in `column_names`
- Upserts are performed row-by-row into tables based on the layout name

## Error handling

- Retries on HTTP 400 errors with a wait time of 5 minutes (configurable via `RETRY_WAIT_SECONDS` and `MAX_RETRIES`)
- Exceptions are caught and logged with full stack trace to aid debugging
- Timeouts are raised if a process does not complete within 100 polling attempts

## Additional files

- **`constants.py`** – Holds layout-to-column mappings (`column_names`) and extract configurations (`data_extracts`). Required for extract validation and data mapping.

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
