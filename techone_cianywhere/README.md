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

> Note: Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the required configuration parameters.

## Features
- Syncs ledgers, AR and GL chart of accounts (with user-defined fields), ledger accounts, transactions, and linked debits/credits for each transaction.
- Automatically refreshes the OAuth2 access token on a 401 response and retries the request once.
- Retries transient network errors and retryable HTTP status codes (429, 5xx) with backoff, honoring `Retry-After` when present.
- Periodically checkpoints during large account listings so upserts are flushed to the destination in bounded batches.

## Configuration file
```
{
  "base_url": "https://YOUR_TENANT.t1cloud.com/T1Default/CiAnywhere/Web/YOUR_ENV",
  "client_id": "YOUR_TECHONE_CLIENT_ID",
  "client_secret": "YOUR_TECHONE_CLIENT_SECRET"
}
```

- `base_url` - your TechOne CiAnywhere web services base URL, including your tenant and environment path.
- `client_id` - OAuth2 client ID for a TechOne service account with access to the ledger and chart-of-accounts web services.
- `client_secret` - OAuth2 client secret for the same service account.

> Note: When submitting connector code as a community connector in the open-source [Community Connector repository](https://github.com/fivetran/community_connectors/tree/main), ensure the `configuration.json` file has placeholder values. When adding the connector to your production repository, ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Authentication
Authentication uses OAuth2 client credentials. Refer to `_get_token()`. The connector requests an access token from `{base_url}/oauth2/access_token` using `client_id` and `client_secret`, and automatically requests a new token if a request returns HTTP 401.

## Pagination
Refer to `_paged_post()`. List endpoints are paginated by requesting successive `PageNumber` values at a fixed `PageSize` of 100 until a page returns fewer rows than the page size.

## Data handling
Refer to `update()` and `schema()`. The connector first syncs all ledgers, then AR and GL chart-of-accounts entries (and any populated user-defined fields) for the chart names found on those ledgers, then per-ledger accounts, and finally transactions and their linked debits/credits for each ledger account. Synthetic primary keys for transaction and linked-transaction rows are derived with a stable SHA-1 hash over their natural identifying fields, since the source API does not expose a single unique ID for them.

## Error handling
Refer to `_request()`. Connection errors and timeouts are retried with backoff. HTTP 401 triggers one token refresh and retry. Retryable status codes (429, 5xx) are retried with backoff, honoring `Retry-After` when the source provides it. Other non-2xx responses raise an exception, which fails the sync.

## Tables created
- `glf_ldg_ctl` (primary key: `ldg_name`) - ledgers.
- `glf_chart_acct` (primary key: `chart_name`, `accnbri`) - AR and GL chart-of-accounts entries.
- `glf_chart_acc_usf` (primary key: `chart_name`, `accnbri`) - user-defined fields for chart-of-accounts entries that have at least one populated.
- `glf_ldg_acct` (primary key: `ldg_acct_rid`) - accounts by ledger.
- `glf_ldg_acc_trans` (primary key: `ldg_trans_rid`) - transactions by ledger account.
- `glf_ldg_acc_transd` (primary key: `ldg_transd_rid`) - linked debits and credits for a transaction.

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
