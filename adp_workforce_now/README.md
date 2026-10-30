---
name: "ADP Workforce Now"
description: "Sync the departments validation table from the ADP Workforce Now HCM API using OAuth2 client credentials over mutual TLS"
category: "SaaS & APIs"
---

# ADP Workforce Now Connector Example

## Connector overview
This connector syncs the departments validation table from the ADP Workforce Now HCM API. Departments are an organizational reference table in ADP, used to classify employees and payroll records; this connector fetches the full list on every sync and upserts it to a single `departments` table.

## Requirements
- [Supported Python versions](https://github.com/fivetran/community_connectors/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connector-sdk/getting-started) to get started.

To initialize a new Connector SDK project using this connector as a starting point, run:

```
fivetran init --template adp_workforce_now
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`. For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/getting-started/connector-sdk-commands#fivetraninit).

> Note: Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the required configuration parameters.

## Features
- Syncs the ADP Workforce Now departments validation table, normalizing several possible ADP response shapes into a consistent row format.
- Authenticates with OAuth2 client credentials over mutual TLS, accepting either certificate/key file paths or inline PEM strings.
- Retries transient network errors and retryable HTTP status codes (429, 5xx) with exponential backoff, honoring a `Retry-After` header when ADP provides one.
- Checkpoints after every page of results; no offset is saved, so each sync still rereads the full table.
- Follows ADP's server-provided pagination link when present, falling back to OData `$skip`/`$top` paging otherwise.

## Configuration file
```json
{
  "client_id": "<YOUR_ADP_CLIENT_ID>",
  "client_secret": "<YOUR_ADP_CLIENT_SECRET>",
  "cert_string": "<YOUR_ADP_CLIENT_CERTIFICATE_PEM>",
  "key_string": "<YOUR_ADP_CLIENT_PRIVATE_KEY_PEM>"
}
```

- `client_id` (required) - OAuth2 client ID for your ADP API application.
- `client_secret` (required) - OAuth2 client secret for the same application.
- `cert_string` (required unless `cert_path` and `key_path` are provided) - the PEM-encoded client certificate content for mutual TLS, as a single string. Alternatively, provide `cert_path` with a filesystem path to the certificate file.
- `key_string` (required unless `cert_path` and `key_path` are provided) - the PEM-encoded private key content for mutual TLS, as a single string. Alternatively, provide `key_path` with a filesystem path to the key file.

The following keys are optional are also read from configuration, falling back to defaults if omitted:
- `token_url` (optional) - the OAuth2 token endpoint. Defaults to `https://api.adp.com/auth/oauth/v2/token`.
- `departments_url` (optional) - the departments validation-table endpoint. Defaults to `https://api.adp.com/hcm/v1/validation-tables/departments`.
- `start_skip` (optional) - the initial OData `$skip` value to start pagination from. Must be a non-negative integer. Defaults to `0`.
- `page_size` (optional) - the OData `$top` page size. Must be a positive integer. Defaults to `1000`.
- `request_timeout` (optional) - the per-request timeout in seconds. Must be a finite positive number; set to an empty value or null to disable the timeout. Defaults to `30`.

Numeric options are validated before any certificate files are created. Inline PEM strings may contain a full certificate chain; each block is normalized separately, and temporary files are removed even if setup fails.

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Authentication
This connector authenticates to ADP Workforce Now using OAuth2 client credentials combined with mutual TLS, as required by ADP's API. Refer to `def get_access_token` in `connector.py`.

To set up authentication:

1. Register an application in the ADP API Explorer or with your ADP account representative to obtain a `client_id` and `client_secret`, and a client certificate/private key pair for mutual TLS.
2. Provide the client ID and client secret as `client_id` and `client_secret` in `configuration.json`.
3. Provide the certificate and private key either as inline PEM strings (`cert_string`/`key_string`) or as filesystem paths (`cert_path`/`key_path`).

The connector requests an access token from `token_url` using the client credentials grant, presenting the configured certificate for mutual TLS on every request, including the token request itself.

## Pagination
Refer to `def fetch_departments` in `connector.py`. The connector first checks the response for a server-provided `next` link (ADP returns this as either a `links`/`_links` array or object) and follows it if present. If no link is provided, it falls back to OData-style `$skip`/`$top` paging, advancing `$skip` by `page_size` as long as a full page of results is returned. Once the connector starts following server-provided links, a response without a next link ends pagination. The connector checkpoints after each page without persisting an offset, so every sync is a full refresh.

## Data handling
Refer to `def _flatten_department` and `def _extract_departments` in `connector.py`. ADP's validation-table responses can wrap the department list under several different keys (`departments`, `items`, `content`, or nested under `data`) depending on account configuration; `_extract_departments` checks each of these in turn before falling back to treating the whole payload as a single record. Each raw record is then flattened into the `departments` table's columns, with the full original record also stored as JSON in `raw_json` so no source fields are lost even if they aren't explicitly mapped.

## Error handling
Refer to `def _request_with_backoff` in `connector.py`. Connection errors and timeouts are retried with exponential backoff. Retryable status codes (429 and 5xx) are retried with backoff, honoring a numeric `Retry-After` header when ADP provides one. A server-provided delay is honored as-is (the 60 second cap applies only to local exponential backoff), but if it exceeds 300 seconds the sync fails with a clear error instead of retrying early. Other non-2xx responses (for example, an invalid client certificate or credentials) raise an exception immediately, which fails the sync.

## Tables created
| Table | Primary key | Description |
| --- | --- | --- |
| `departments` | `department_code`, `payroll_group_code` | ADP Workforce Now departments validation table. |

The `departments` table has the following columns. Only the primary key columns are declared in `schema()`; Fivetran infers the types of the other columns.
- `department_code` (primary key) - the department code.
- `payroll_group_code` (primary key) - the payroll group code; an empty string when ADP provides none.
- `short_name` - the department short name.
- `long_name` - the department long name or description.
- `status` - the department status, `ACTIVE` or `INACTIVE` when ADP provides no explicit status.
- `updated_at` - the last update timestamp reported by ADP.
- `raw_json` - the full original record serialized as JSON.

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
