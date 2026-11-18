---
name: "HTTP Proxy with requests"
description: "Demonstrates how to route API requests through an HTTP or HTTPS proxy using the requests library in a Connector SDK connector"
category: "SaaS & APIs"
---

# HTTP Proxy with requests Connector Example

## Connector overview

This connector demonstrates how to configure the Python `requests` library to route outbound HTTP and HTTPS traffic through a proxy server in a Fivetran Connector SDK connector.

Corporate environments often restrict direct outbound internet access, requiring all traffic to pass through a proxy. This example shows the recommended pattern for reading proxy settings from the connector configuration and applying them once to a `requests.Session`, so every subsequent API call through that session is automatically proxied.

The connector syncs posts and users from [JSONPlaceholder](https://jsonplaceholder.typicode.com), a free public REST API. You can swap in any API by replacing the base URL and the data-fetching logic. The proxy configuration pattern remains the same.

Key patterns demonstrated:

- Reading proxy URL and optional credentials from `configuration.json`.
- Creating a `requests.Session` with `session.proxies` set, then passing `proxies=session.proxies` explicitly to each `session.get` call so the configured proxy takes precedence over any `HTTP_PROXY` or `HTTPS_PROXY` environment variables.
- Supporting authenticated proxies by embedding credentials directly in the proxy URL.
- Validating proxy URL format at startup to fail fast on misconfiguration.
- Retrying `ProxyError`, `ConnectionError`, and `Timeout` with exponential backoff.
- Incremental sync using a post ID cursor persisted in connector state.


## Requirements

- [Supported Python versions](https://github.com/fivetran/community_connectors/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)
- An HTTP or HTTPS proxy server (optional — the connector works without a proxy when `proxy_url` is absent or left as its placeholder value)


## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connector-sdk/getting-started) to get started.

To initialize a new Connector SDK project using this connector as a starting point, run:

```
fivetran init --template http_proxy
```

> Note: Ensure you have updated the `configuration.json` file with your proxy details before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details.


## Features

- Optional HTTP or HTTPS proxy support via `requests.Session.proxies`.
- Proxy authentication by embedding credentials in the proxy URL.
- Incremental sync: only new posts (by ID) are fetched on subsequent syncs.
- Exponential backoff retry on proxy errors, connection errors, timeouts, and retryable HTTP errors (408, 429, 5xx).
- Fail-fast configuration validation rejects malformed proxy URLs before any network request is made.


## Configuration file

```json
{
  "proxy_url": "http://proxy.example.com:8080",
  "proxy_username": "<OPTIONAL_PROXY_USERNAME>",
  "proxy_password": "<OPTIONAL_PROXY_PASSWORD>"
}
```

| Key | Required | Description |
|---|---|---|
| `proxy_url` | No | The HTTP or HTTPS proxy URL, including scheme and port. Example: `http://proxy.example.com:8080`. When absent or left as the placeholder, the connector connects directly. |
| `proxy_username` | No | Proxy username for authenticated proxies. Must be paired with `proxy_password`. |
| `proxy_password` | No | Proxy password for authenticated proxies. Must be paired with `proxy_username`. |

> Note: When submitting connector code as a community connector in the open-source [Community Connector repository](https://github.com/fivetran/community_connectors/tree/main), ensure the `configuration.json` file has placeholder values. When adding the connector to your production repository, ensure that the `configuration.json` file is not checked into version control to protect sensitive information.


## Requirements file

This connector has no external dependencies beyond the packages pre-installed in the Connector SDK runtime environment. The `requirements.txt` file is not needed and is not included.

> Note: The `requests` library is pre-installed in the Connector SDK runtime environment. Do not add it to `requirements.txt`.


## Authentication

This connector requires no authentication for the JSONPlaceholder demo API.

When adapting this pattern to an authenticated API, add your API key or token to `configuration.json` and read it in the `update` function. The proxy authentication is separate and is handled via `proxy_username` and `proxy_password`.


## How the proxy works

The pattern spans two functions: `build_session` and `fetch_with_retry`.

`build_session` records the proxy on the session:

```python
session = requests.Session()
session.proxies = {
    "http":  "http://proxy.example.com:8080",
    "https": "http://proxy.example.com:8080",
}
```

`fetch_with_retry` then passes `proxies=session.proxies` explicitly to every request:

```python
session.get(url, proxies=session.proxies, timeout=...)
```

The explicit `proxies=` argument is required. Without it, `requests` merges `session.proxies` with any `HTTP_PROXY` or `HTTPS_PROXY` environment variables present in the runtime, and environment values can override the configured proxy. Passing `proxies=session.proxies` as a per-request argument makes the connector's configuration authoritative.

- HTTP requests are forwarded to the proxy, which fetches the resource on the connector's behalf.
- HTTPS requests use an HTTP CONNECT tunnel through the proxy, preserving end-to-end TLS encryption.

For authenticated proxies, credentials are percent-encoded and embedded in the proxy URL:

```python
session.proxies = {
    "http":  "http://alice:s3cret@proxy.example.com:8080",
    "https": "http://alice:s3cret@proxy.example.com:8080",
}
```

To adapt this to your own connector, replace `__BASE_URL` with your API's base URL. The `build_session` and `fetch_with_retry` functions do not need to change.


## Pagination

This connector fetches all records in a single request per endpoint (JSONPlaceholder returns at most 100 posts and 10 users). For paginated APIs, add a pagination loop inside the data-fetching functions and pass the cursor as a query parameter to `fetch_with_retry`.


## Error handling

`fetch_with_retry` handles the following error types with exponential backoff:

- `ProxyError` – the proxy server rejected or dropped the connection; retried up to `__MAX_RETRIES` times.
- `ConnectionError` – a transient network failure; retried with the same backoff.
- `Timeout` – the request exceeded `__API_TIMEOUT_SECONDS`; retried.
- HTTP 408, 429, 500, 502, 503, 504 – retryable server-side errors.
- All other `HTTPError` responses (e.g. 400, 404) are raised immediately without retrying.


## Tables created

### `posts`

Incremental sync, keyed on `id`. Only posts with an `id` greater than the last synced value are upserted on each run.

| Column | Type | Description |
|---|---|---|
| `id` | `INT` | Post identifier (primary key) |
| `user_id` | `INT` | ID of the author |
| `title` | `STRING` | Post title |
| `body` | `STRING` | Post body text |

### `users`

Full refresh on every sync. Nested `address`, `geo`, and `company` fields are flattened to scalar columns.

| Column | Type | Description |
|---|---|---|
| `id` | `INT` | User identifier (primary key) |
| `name` | `STRING` | Full name |
| `username` | `STRING` | Login username |
| `email` | `STRING` | Email address |
| `phone` | `STRING` | Phone number |
| `website` | `STRING` | Personal website |
| `street` | `STRING` | Street address |
| `suite` | `STRING` | Apartment or suite |
| `city` | `STRING` | City |
| `zipcode` | `STRING` | ZIP code |
| `geo_lat` | `STRING` | Latitude |
| `geo_lng` | `STRING` | Longitude |
| `company_name` | `STRING` | Company name |
| `company_catch_phrase` | `STRING` | Company catchphrase |
| `company_bs` | `STRING` | Company business summary |


## Additional files

- `tests/conftest.py` – Pytest fixture that patches the SDK logger, which is not initialized when the connector runs outside the Fivetran runtime.
- `tests/test_connector.py` – Unit tests covering proxy session setup, configuration validation, retry behaviour, user flattening, and sync logic.


## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
