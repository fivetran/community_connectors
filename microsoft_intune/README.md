# Microsoft Intune Connector Example

## Connector overview

This example demonstrates how to build a connector for [Microsoft Intune](https://www.microsoft.com/en-us/security/business/microsoft-intune) with Fivetran Connector SDK, using the [Microsoft Graph API](https://learn.microsoft.com/en-us/graph/use-the-api) to retrieve managed device data. The connector pulls data from the Intune managed devices endpoint and delivers it to your Fivetran destination in a single table called `MANAGED_DEVICES`.

## Requirements

- Microsoft Intune credentials: `tenant_id`, `client_id`, and `client_secret`
- [Supported Python versions](https://github.com/fivetran/community_connectors/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK setup guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

To initialize a new Connector SDK project using this connector as a starting point, run:

```
fivetran init --template microsoft_intune
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`. For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/connector-development-and-configuration/connector-sdk-commands#fivetraninit).

> Note: Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the required configuration parameters.

## Features

- Retrieves managed device data from Microsoft Intune using the Microsoft Graph API (see `update` function)
- Handles API pagination using the `@odata.nextLink` field (see `update` function)
- Converts list values in device records to JSON strings for compatibility (see `list_to_json` function)
- Delivers data to a single table, `MANAGED_DEVICES`
- Uses Fivetran Connector SDK logging for status and error reporting (see `log` usage)

## Configuration file

The connector expects a `configuration.json` file with the following structure:

```json
{
  "tenant_id": "<YOUR_TENANT_ID>",
  "client_id": "<YOUR_CLIENT_ID>",
  "client_secret": "<YOUR_CLIENT_SECRET>"
}
```

> Note: When submitting connector code as a [Community Connector](https://github.com/fivetran/community_connectors/tree/main) in the open-source [Connector SDK repository](https://github.com/fivetran/community_connectors/tree/main), ensure the `configuration.json` file has placeholder values. When adding the connector to your production repository, ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Authentication

The connector uses a OAuth2 client credentials flow to authenticate with Microsoft Graph API. You will need to provide your Azure tenant ID, client ID, and client secret in the `configuration.json` file. The connector retrieves an access token using these credentials (see `get_access_token` function in `test.py`).

## Pagination

The connector handles pagination using the `@odata.nextLink` field returned by the Microsoft Graph API. It continues to request additional pages until all managed devices are retrieved (see `update` function, lines ~38-81).

## Data handling

- Data is retrieved from the Microsoft Graph API using the `update` function (see lines ~38-81)
- List values in device records are converted to JSON strings using the `list_to_json` function (see lines ~84-89)
- Data is delivered to Fivetran using the `op.upsert` operation
- The schema is defined in the `schema` function (see lines ~13-18)

## Error handling

- Uses Fivetran Connector SDK logging for info and error messages (see `log` usage throughout)
- Raises exceptions for failed authentication or API errors (see `get_access_token` and `update` functions)

## Tables created

- `managed_devices` – Contains all managed device records retrieved from Microsoft Intune.

Sample data structure:

| id                                   | deviceName | operatingSystem | ... |
|--------------------------------------|------------|----------------|-----|
| 12345678-90ab-cdef-1234-567890abcdef | SurfacePro | Windows        | ... |

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
