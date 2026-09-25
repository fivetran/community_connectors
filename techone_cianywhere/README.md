# TechOne CiAnywhere Connector Example

## Connector overview
This connector syncs general ledger reference and transaction data from the TechOne CiAnywhere web services API. It syncs all ledgers, the AR and GL charts of accounts (including user-defined fields), per-ledger accounts, and transactions with their linked debits and credits.

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
fivetran init --template techone_cianywhere
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`. For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/connector-development-and-configuration/connector-sdk-commands#fivetraninit).

> Note: Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the required configuration parameters.

## Features
- Syncs ledgers, AR and GL chart of accounts (with user-defined fields), ledger accounts, transactions, and linked debits/credits for each transaction, syncing each account's transactions immediately after the account itself instead of holding the whole tenant's accounts in memory.
- Automatically refreshes the OAuth2 access token on a 401 response, using a retry cycle separate from the transient-error backoff so a refreshed token always gets a full retry attempt.
- Retries transient network errors and retryable HTTP status codes (429, 5xx) with backoff, honoring a `Retry-After` header in either delay-seconds or HTTP-date form.
- Periodically checkpoints during large account and transaction listings so upserts are flushed to the destination in bounded batches.

## Configuration file
```json
{
  "base_url": "<YOUR_TECHONE_CIANYWHERE_BASE_URL_EG_https://YOUR_TENANT.t1cloud.com/T1Default/CiAnywhere/Web/YOUR_ENV>",
  "client_id": "<YOUR_TECHONE_CLIENT_ID>",
  "client_secret": "<YOUR_TECHONE_CLIENT_SECRET>"
}
```

- `base_url` - your TechOne CiAnywhere web services base URL, including your tenant and environment path. Must start with `http://` or `https://`.
- `client_id` - OAuth2 client ID for a TechOne service account with access to the ledger and chart-of-accounts web services.
- `client_secret` - OAuth2 client secret for the same service account.

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Authentication
This connector authenticates to TechOne CiAnywhere using OAuth2 client credentials. Refer to `def _get_token` in `connector.py`.

To set up authentication:

1. Ask your TechOne administrator for a service account (client ID and client secret) with access to the ledger and chart-of-accounts web services.
2. Provide your tenant's CiAnywhere base URL as `base_url` in `configuration.json`.
3. Provide the service account's client ID and client secret as `client_id` and `client_secret` in `configuration.json`.

The connector requests an access token from `{base_url}/oauth2/access_token` using `client_id` and `client_secret`, and automatically requests a new token if a request returns HTTP 401.

## Pagination
Refer to `def _paged_post` in `connector.py`. List endpoints are paginated by requesting successive `PageNumber` values at a fixed `PageSize` of 100 until a page returns fewer rows than the page size.

## Data handling
Refer to `def update` and `def schema` in `connector.py`. The connector first syncs all ledgers, then AR and GL chart-of-accounts entries (and any populated user-defined fields) for the chart names found on those ledgers, then per-ledger accounts and, immediately after each account, its transactions and their linked debits/credits. If an account's user-defined fields become entirely empty on a later sync, its `glf_chart_acc_usf` row is deleted rather than left stale. Synthetic primary keys for transaction and linked-transaction rows are derived with a stable SHA-1 hash over their natural identifying fields, since the source API does not expose a single unique ID for them.

## Error handling
Refer to `def _request_with_backoff` and `def _request` in `connector.py`. Connection errors and timeouts are retried with backoff. Retryable status codes (429 and 5xx) are retried with backoff, honoring `Retry-After` when the source provides it. HTTP 401 triggers one token refresh, using its own retry attempt separate from the transient-error backoff budget. Other non-2xx responses raise an exception, which fails the sync.

## Tables created
- `glf_ldg_ctl` (primary key: `ldg_name`) - ledgers.
- `glf_chart_acct` (primary key: `chart_name`, `accnbri`) - AR and GL chart-of-accounts entries.
- `glf_chart_acc_usf` (primary key: `chart_name`, `accnbri`) - user-defined fields for chart-of-accounts entries that have at least one populated.
- `glf_ldg_acct` (primary key: `ldg_acct_rid`) - accounts by ledger.
- `glf_ldg_acc_trans` (primary key: `ldg_trans_rid`) - transactions by ledger account.
- `glf_ldg_acc_transd` (primary key: `ldg_transd_rid`) - linked debits and credits for a transaction.

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
