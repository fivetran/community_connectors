# OData API Connector Example

## Connector overview

This connector demonstrates how to fetch data from OData APIs and sync it to a destination using the Fivetran Connector SDK. OData (Open Data Protocol) is a standardized protocol for building and consuming RESTful APIs. This example provides three sub-implementations covering different OData versions and client libraries:

- `odata_version_4` – Uses a custom `ODataClient` class built on top of `requests` to interact with OData version 4 services. Demonstrates querying with `select`, `filter`, `expand`, `orderby`, `top`, and `skip` options, incremental sync using state management, fetching multiple entities, and batch operations via the `$batch` endpoint.

- `odata_version_2_using_pyodata` – Uses the `pyodata` library to connect to OData version 2 services. Demonstrates select queries, expand, incremental sync, multiple entity fetching, and batch operations.

- `odata_version_4_using_python_odata` – Uses the `python-odata` library to connect to OData version 4 services. Demonstrates querying all records, filtering, and ordering data from an OData entity.

Each sub-implementation includes its own `connector.py`, `ODataClient.py` (where applicable), and `requirements.txt`.

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
fivetran init --template odata_api
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`. For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/connector-development-and-configuration/connector-sdk-commands#fivetraninit).

## Features

- Demonstrates three approaches to consuming OData APIs: raw `requests`-based client, `pyodata`, and `python-odata`
- Supports OData version 2 and version 4 services
- Implements query options including `select`, `filter`, `expand`, `orderby`, `top`, and `skip`
- Supports incremental sync using state management with `update_state` mappings
- Handles pagination automatically for all OData endpoints
- Supports batch operations via the `$batch` endpoint (OData v4)
- Demonstrates fetching data from multiple entities in a single sync

## Requirements file

Each sub-implementation specifies its own dependencies:

- `odata_version_4/requirements.txt`:

```
requests_toolbelt==1.0.0
```

- `odata_version_2_using_pyodata/requirements.txt`:

```
pyodata==1.11.1
```

- `odata_version_4_using_python_odata/requirements.txt`:

```
python-odata==0.6.3
```

> Note: [Some packages](https://fivetran.com/docs/connector-sdk/technical-reference#preinstalledpackages) are pre-installed in the Connector SDK runtime environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

The connector examples use a `requests.Session` object to manage connection state and headers. Authentication can be configured by setting headers or auth parameters on the session object before passing it to the OData client. The example implementations use public OData demo services that do not require authentication.

## Pagination

The `ODataClient` class (used in `odata_version_4` and `odata_version_2_using_pyodata`) handles pagination automatically. It follows the `@odata.nextLink` or `__next` URL in the API response to fetch subsequent pages until all data has been retrieved.

## Data handling

Each sub-implementation demonstrates the following data handling patterns:

- Basic queries with field selection using the `select` query option
- Incremental sync by applying a `filter` condition based on a state-tracked timestamp field and using `update_state` to update the state after each sync
- Expand queries to fetch related entities in a single request
- Multiple entity sync by iterating through a list of entity configurations
- Batch operations to reduce the number of API round trips (OData v4 only)

The connector upserts each fetched record to the destination table using `op.upsert()` and checkpoints state using `op.checkpoint()`.

## Error handling

The example implementations fail fast on HTTP and client-library errors instead of suppressing them. In `odata_version_4/connector.py`, the `update()` function relies on the underlying `requests`-based client to surface request failures, including HTTP errors and timeouts, so failed API calls stop the sync and make the issue visible during debugging. In `odata_version_2_using_pyodata/connector.py` and `odata_version_4_using_python_odata/connector.py`, the `update()` functions similarly propagate exceptions raised by `pyodata` and `python-odata` when requests fail or responses cannot be processed.

This example does not add custom retry logic in the top-level flow. If you extend these examples for production use, consider adding explicit timeout values, retry policies for transient failures, and logging around the request boundaries in each implementation's `update()` function.
## Tables created

### odata_version_4

| Table | Primary key | Description |
|-------|-------------|-------------|
| `People` | `UserName` | People data from the TripPin OData demo service |
| `Orders` | `OrderID` | Order data with expanded order details from Northwind |
| `Products` | `ID` | Product data with incremental sync based on `ReleaseDate` |
| `Customers_Multiple` | `CustomerID` | Customer data filtered by country from Northwind |
| `Products_Multiple` | `ProductID` | Product data with expanded category from Northwind |
| `Orders_batch` | `OrderID` | Order data fetched via batch operations |
| `Customers_batch` | `CustomerID` | Customer data fetched via batch operations |

### odata_version_2_using_pyodata

| Table | Primary key | Description |
|-------|-------------|-------------|
| `Products` | `ProductID` | Product data from Northwind OData v2 service |
| `Orders` | `OrderID` | Order data with expanded order details |
| `Orders_Inc` | `OrderID` | Order data with incremental sync |
| `Customers_Multiple` | `CustomerID` | Customer data from multiple entity sync |
| `Products_Multiple` | `ProductID` | Product data from multiple entity sync |
| `Orders_batch` | `OrderID` | Order data via batch operations |
| `Customers_batch` | `CustomerID` | Customer data via batch operations |

### odata_version_4_using_python_odata

| Table | Primary key | Description |
|-------|-------------|-------------|
| `Customers` | `CustomerID` | All customers from Northwind OData v4 service |
| `Filtered_Customers` | `CustomerID` | Customers whose names start with a specific letter |
| `Orders` | `OrderID` | Order data from Northwind OData v4 service |

## Additional files

- **`odata_version_4/ODataClient.py`** – Custom OData client class for OData version 4 services. Provides methods for `upsert_entity()`, `upsert_multiple_entity()`, `add_batch()`, and `upsert_batch()` with support for query options, state management, and pagination.
- **`odata_version_2_using_pyodata/ODataClient.py`** – Custom OData client class for OData version 2 services using the `pyodata` library. Provides the same interface as the version 4 client.

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
