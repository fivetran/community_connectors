# Delta Sharing Connector for Fivetran

A Fivetran Connector SDK connector that syncs data from any [Delta Sharing](https://delta.io/sharing)-compatible server into your Fivetran destination.

---

## Overview

Delta Sharing is an open protocol for secure, real-time exchange of large datasets across platforms. This connector discovers all shares, schemas, and tables exposed by a Delta Sharing server and syncs them into a single Fivetran destination schema.

**Destination layout:**

| Table | Description |
|---|---|
| `shares` | Names of all shares accessible to the recipient |
| `schemas` | Names of all schemas found across those shares |
| `tables` | Catalog of all tables (share, schema, name) |
| `{schema}__{table}` | Actual data — one table per shared table (e.g. `customers__account`) |

Incremental syncs are supported: after the first full load, each subsequent sync fetches only rows changed since the last known Delta table version.

---

## Requirements

- Python 3.10+
- [Fivetran Connector SDK](https://pypi.org/project/fivetran-connector-sdk/) (`pip install fivetran-connector-sdk`)
- [fivetran-cli](https://pypi.org/project/fivetran-cli/) (`pip install fivetran-cli`)
- A Fivetran account with a configured destination
- A Delta Sharing recipient profile (see [Authentication](#authentication))

Python dependencies (declared in `requirements.txt`, installed automatically at deploy time):

```
delta-sharing
pyarrow
```

---

## Getting Started

1. **Clone or copy** this connector directory to your local machine.

2. **Fill in `configuration.json`** with your endpoint and bearer token (see [Configuration](#configuration)).

3. **Deploy** to your Fivetran destination:

```bash
fivetran deploy \
  --api-key <base64-encoded-fivetran-api-key> \
  --destination <destination-name> \
  --connection delta_sharing \
  --configuration configuration.json \
  --non-interactive
```

4. **Trigger a sync** from the Fivetran dashboard or API to start the initial full load.

---

## Configuration

The connector reads credentials from `configuration.json`:

```json
{
  "endpoint": "<ENDPOINT>",
  "bearer_token": "<TOKEN>"
}
```

| Field | Required | Description |
|---|---|---|
| `endpoint` | Yes | Base URL of the Delta Sharing server (e.g. `https://sharing.example.com/delta-sharing`) |
| `bearer_token` | Yes | Bearer token used to authenticate all requests to the sharing server |

---

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
---

## Requirements File

`requirements.txt` lists Python packages installed into the connector runtime at deploy time:

```
delta-sharing   # Official Delta Sharing Python client — handles DeletionVectors,
                # incremental reads, and all advanced Delta table features
pyarrow         # Columnar data processing for Parquet file reads
```

> **Why `delta-sharing` instead of raw HTTP?**  
> Tables that use Delta features such as `enableDeletionVectors` cannot be read via the standard parquet query API (the server returns HTTP 400). The `delta_sharing` library handles these transparently using the `responseformat=delta` protocol capability.
