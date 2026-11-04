---
name: CubeTally
description: Sync products, customers, jobs, invoices, estimates and stock history from CubeTally field service and inventory software
category: SaaS & APIs
---

# CubeTally Connector Example

## Connector overview

This connector moves one CubeTally organization's data into your Fivetran destination. It is read only: it sends GET requests to the CubeTally public REST API and nothing else. The API key decides the organization, so one connection only ever sees one organization.

## Accreditation

This example was contributed by [CubeTally](https://cubetally.com).

## Requirements

- [Supported Python versions](https://github.com/fivetran/community_connectors/blob/main/README.md#requirements)
- Operating system: Windows 10 or later (64-bit only), macOS 13 (Ventura) or later, or Linux distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later.
- A CubeTally plan with API access (Professional or higher).
- A Fivetran account of your own.

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connector-sdk/getting-started) to get started.

## Features

- 11 tables, 1 API key, no setup in CubeTally beyond the key.
- Incremental where the source allows it (see Data handling).
- Tables the key has no scope for are skipped with a warning.
- Resumes from the last saved page after a stopped sync.
- Honors `Retry-After` and stops cleanly at the monthly API allowance.

## Configuration file

```json
{
  "api_key": "<YOUR_CUBETALLY_API_KEY>",
  "tables": ""
}
```

- `api_key` (required): a `ct_test_` or `ct_live_` key. A `ct_test_` key reads real data and CubeTally blocks every write made with it, so it is the recommended choice.
- `tables` (optional): a comma separated list of table names. Empty means every table the key can read.
- `base_url` (optional, not in the form): only for testing against a local mock. It must be https unless the host is localhost.

Do not check `configuration.json` into version control.

## Requirements file

This connector has no extra dependencies. `requests` and `fivetran_connector_sdk` come with the Connector SDK runtime, so no `requirements.txt` is needed.

## Authentication

Create a key in CubeTally under Settings, Developer, and give it these read scopes: `products:read`, `customers:read`, `suppliers:read`, `warehouses:read`, `vehicles:read`, `purchaseOrders:read`, `workOrders:read`, `invoices:read`, `estimates:read`. The connector sends it as a bearer token.

## Pagination

Each list route returns `{data, nextCursor}` with up to 100 rows. See `sync_table` in `connector.py`. A deleted record can invalidate a saved page cursor. The connector then restarts that table's walk, which is safe because rows are upserted.

## Data handling

The API has no changed-since filter, so every table is read in full each sync and filtered locally. API request use therefore depends on total rows, not changed rows.

| Mode | Tables | Rows sent |
| --- | --- | --- |
| filtered | `products`, `purchase_orders`, `work_orders`, `invoices`, `estimates` | `updated_at` at or after the last sync, less 10 minutes |
| append | `stock_movements`, `transfers` | `created_at` at or after the last sync, less 10 minutes |
| snapshot | `customers`, `suppliers`, `warehouses`, `vehicles` | Every row, because these records have no reliable changed date |

A row with no usable date is always sent. Column names are snake_case. `line_items` and `inventory` are JSON columns. The last sync time moves only after a full walk of a table, to the time that walk began.

## Error handling

- 429 and 5xx responses and network errors are retried up to 6 attempts, using `Retry-After` when sent.
- An exhausted monthly allowance fails the sync with a clear message. Saved progress is kept and the next sync resumes.
- Bad keys and refused routes fail with a message that never contains the key.

## Tables created

`products`, `customers`, `suppliers`, `warehouses`, `vehicles`, `purchase_orders`, `work_orders`, `invoices`, `estimates`, `stock_movements`, `transfers`. Every table has primary key `id`.

## Additional considerations

- Deleted CubeTally records are not detected. They stay in the destination until a full re-sync. The API has no delete feed.
- A daily sync is recommended. Each sync uses about (total rows / 100) + 12 API requests against the monthly allowance. Check that against your plan before choosing a shorter frequency.
- The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.

## Testing

```bash
pip install fivetran-connector-sdk pytest
python -m pytest tests
python tests/mock_api.py 8931
fivetran debug --configuration tests/configuration.mock.json
```

`tests/mock_api.py` is a local stand-in with invented records. It answers on 127.0.0.1 only.
