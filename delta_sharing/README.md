# Delta Sharing Connector Example

A Fivetran Connector SDK connector that syncs data from any [Delta Sharing](https://delta.io/sharing)-compatible server into your Fivetran destination.

## Connector overview

Delta Sharing is an open protocol for secure, real-time exchange of large datasets across platforms. This connector discovers all shares, schemas, and tables exposed by a Delta Sharing server and syncs them into a single Fivetran destination schema.

## Requirements

- [Supported Python versions](https://github.com/fivetran/community_connectors/blob/main/README.md#requirements)   
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)
- A Delta Sharing recipient profile (see [Authentication](#authentication))

## Getting Started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

To initialize a new Connector SDK project using this connector as a starting point, run:

```
fivetran init --template delta_sharing
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`. For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/connector-development-and-configuration/connector-sdk-commands#fivetraninit).

> Note: Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the required configuration parameters.


## Configuration

The connector reads credentials from `configuration.json`:

```json
{
  "endpoint": "<YOUR_DELTA_SHARING_ENDPOINT>",
  "bearer_token": "<YOUR_DELTA_SHARING_TOKEN>"
}
```

| Field | Required | Description |
|---|---|---|
| `endpoint` | Yes | Base URL of the Delta Sharing server (e.g. `https://sharing.example.com/delta-sharing`) |
| `bearer_token` | Yes | Bearer token used to authenticate all requests to the sharing server |


## Requirements File

`requirements.txt` lists Python packages installed into the connector runtime at deploy time:

```
delta-sharing   # Official Delta Sharing Python client — handles DeletionVectors, incremental reads, and all advanced Delta table features
pyarrow         # Columnar data processing for Parquet file reads
```

> **Why `delta-sharing` instead of raw HTTP?**  
> Tables that use Delta features such as `enableDeletionVectors` cannot be read via the standard parquet query API (the server returns HTTP 400). The `delta_sharing` library handles these transparently using the `responseformat=delta` protocol capability.

> Note: [Some packages](https://fivetran.com/docs/connector-sdk/technical-reference#preinstalledpackages) are pre-installed in the Connector SDK runtime environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`. 


## Authentication

Delta Sharing uses **bearer token authentication**. Both the endpoint URL and the bearer token are distributed to recipients via an **activation link** provided by the data provider.

### How to obtain credentials

1. The data provider shares a dataset with you as a **recipient** in their Delta Sharing platform (e.g. Databricks Unity Catalog).
2. They send you an **activation link** — a one-time URL that, when opened, downloads a **profile file** (`.share` or `.json`).
3. The profile file contains:
    
    ```json
    {
      "shareCredentialsVersion": 1,
      "bearerToken": "<your-bearer-token>",
      "endpoint": "<sharing-server-endpoint>",
      "expirationTime": "2027-01-01T00:00:00.000Z"
    }
    ```

4. Copy the `bearerToken` and `endpoint` values from the profile file into `configuration.json`.

> Note: The bearer token has an expiration time. When it expires, request a new activation link from the data provider and update `configuration.json` and redeploy.

## Pagination
This connector does not use API pagination for table rows. It discovers the table list first, then processes each table one by one, and performs record-level checkpointing every CHECKPOINT_INTERVAL rows plus a checkpoint after each table. This keeps progress durable and allows safe resume behavior across large sync runs.

## Data handling
Catalog metadata is synced into `shares`, `schemas`, and `tables`, while data tables are written as `{schema}__{table}`. On first sync, it loads a full snapshot; on later syncs, it loads only changes between versions and keeps insert/update-postimage records. Null-like pandas values are normalized to None, and state is advanced per table version only after successful writes.

## Error handling
Configuration is validated early with clear `ValueError` messages for missing or invalid fields. Per-table failures (version lookup or data load) are logged as warnings and skipped so other tables can continue syncing. A `finally` block always removes the temporary Delta Sharing profile file, ensuring cleanup even when errors occur.

## Tables created

Destination layout:

| Table | Description |
|---|---|
| `shares` | Names of all shares accessible to the recipient |
| `schemas` | Names of all schemas found across those shares |
| `tables` | Catalog of all tables (share, schema, name) |
| `{schema}__{table}` | Actual data — one table per shared table (e.g. `customers__account`) |


## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
