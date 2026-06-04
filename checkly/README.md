# Checkly API Connector Example

## Connector overview

The [Checkly](https://www.checklyhq.com/) custom connector for Fivetran fetches monitoring check data and performance analytics from the Checkly API and syncs it to your destination. This connector supports multiple endpoints including check configurations and browser analytics.

The connector implements Bearer token authentication, handles pagination automatically, and separates aggregated and non-aggregated analytics data, following Fivetran best practices for reliability, security, and maintainability.

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
fivetran init --template checkly
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`. For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/connector-development-and-configuration/connector-sdk-commands#fivetraninit).

> Note: Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the required configuration parameters.

## Features

- Fetches all check configurations from Checkly API via `/v1/checks` endpoint
- Supports both API and Browser check types with full metadata
- Automatically fetches analytics data for Browser checks via `/v1/analytics/browser-checks/{check_id}` endpoint
- Separates analytics into aggregated and non-aggregated metrics for different analysis needs
- Flattens nested JSON objects into SQL-compatible flat structures for easy querying
- Handles pagination automatically for large datasets using page-based pagination
- Implements rate limiting with configurable delays to respect Checkly API quotas
- Comprehensive error handling with graceful degradation for failed analytics requests

## Configuration file

The connector requires the following configuration parameters in the configuration.json file. This configuration is uploaded to Fivetran and defines how the connector authenticates with and queries the Checkly API.

```json
{
  "api_key": "<YOUR_CHECKLY_API_KEY>",
  "account_id": "<YOUR_CHECKLY_ACCOUNT_ID>",
  "aggregation_interval": "<YOUR_AGGREGATION_INTERVAL_IN_MINUTES>",
  "quick_range": "<YOUR_QUICK_RANGE_OPTION>"
}
```

Required parameters:
- `api_key`: Your Checkly API key with read permissions for accessing checks and analytics data
- `account_id`: Your Checkly account identifier for API authentication

Optional parameters:
- `aggregation_interval`: Time interval for aggregating analytics data in minutes (default: 60). Must be a positive integer between 1 and 43200
- `quick_range`: Time range for analytics data collection (default: `last24Hours`)
  - Available options: `last24Hours`, `last7Days`, `last30Days`, `thisWeek`, `thisMonth`, `lastWeek`, `lastMonth`

> Note: When submitting connector code as a [Community Connector](https://github.com/fivetran/community_connectors/tree/main) in the open-source [Connector SDK repository](https://github.com/fivetran/community_connectors/tree/main), ensure the `configuration.json` file has placeholder values. When adding the connector to your production repository, ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

This connector example uses standard libraries provided by Python and does not require any additional packages.

> Note: [Some packages](https://fivetran.com/docs/connector-sdk/technical-reference#preinstalledpackages) are pre-installed in the Connector SDK runtime environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

The connector uses Bearer Token authentication with the Checkly API. Authentication is handled through the `validate_configuration` function which validates credentials before any API calls are made.

You'll need to obtain the following credentials:

1. API key: A Checkly API key with read permissions for checks and analytics endpoints.
2. Account ID: Your Checkly account identifier for proper API access.

### Steps to obtain credentials

1. Log into your Checkly dashboard at https://app.checklyhq.com/.
2. Navigate to **Account Settings > API Keys**.
3. Create a new API key with appropriate read permissions.
4. Copy your Account ID from the account settings page.
5. Add both values to your `configuration.json` file.

## Pagination

The connector implements page-based pagination using the Checkly API's `limit` and `page` parameters. Pagination is handled automatically in the `get_checks_data` function which:

- Fetches data in batches of 100 records per page (defined by `PAGE_SIZE` constant)
- Continues fetching pages until no more data is available (when `len(response_data) < PAGE_SIZE`)
- Optimizes performance while respecting API rate limits through built-in delays
- Handles large datasets without loading all data into memory at once

The pagination loop automatically increments the page number and constructs the appropriate API URL for each request until all check data is retrieved.

## Data handling

The connector processes Checkly data through several transformation steps implemented across multiple functions:

- Check data retrieval: Fetches check configurations from `/v1/checks` endpoint with automatic pagination
- Data flattening: Converts nested JSON objects into flat SQL-compatible structures
- Analytics processing: For browser checks, fetches analytics data from `/v1/analytics/browser-checks/{check_id}` endpoint
- Metrics separation: Separates analytics into aggregated and non-aggregated metrics tables using predefined metric constants (`AGGREGATED_METRICS` and `NON_AGGREGATED_METRICS`)
- Array handling: Converts string arrays to comma-separated values and complex arrays to JSON strings for optimal database storage
- Rate limiting: Implements configurable delays between API calls to respect Checkly's rate limits

## Error handling

The connector implements comprehensive error handling strategies across multiple functions:

- API rate limiting: Automatically handles HTTP 429 responses with retry delays
- Configuration validation: Validates all required configuration parameters and optional parameters with sensible defaults before any API calls
- Exception handling: Catches and logs specific `requests.exceptions.RequestException` while continuing processing where possible
- Graceful degradation: If analytics data fails for a specific check, the connector logs the error and continues processing other checks rather than failing the entire sync
- Request timeouts: Implements 30-second timeouts for all API requests to prevent hanging connections
- Retry logic: Built-in retry mechanism specifically for rate-limited requests (HTTP 429 status codes)
- Validation logic: Ensures `quick_range` values are within allowed options and `aggregation_interval` is a positive integer

All errors are logged appropriately using the Fivetran SDK logging system while maintaining sync reliability.

## Tables created

The connector creates three main tables in your data warehouse as defined in the `schema` function.

### checks

- Purpose: Contains all check configurations and metadata from the Checkly API
- Primary key: `id` (check identifier)
- Content: Check settings, URLs, schedules, alert configurations, and all flattened check properties
- Data source: `/v1/checks` endpoint with full pagination support

### browser_checks_analytics_aggregated

- Purpose: Contains aggregated analytics metrics for browser checks over specified time periods
- Primary key: `_fivetran_id` (hash of all the column values)
- Content: Statistical aggregations including averages, percentiles (p50, p90, p95, p99), min/max values, standard deviations for performance metrics
- Metrics: Response times, Core Web Vitals (FCP, LCP, CLS, TBT), TTFB, error counts (console, network, user script, document errors) with full statistical breakdowns
- Data source: `/v1/analytics/browser-checks/{check_id}` endpoint using `AGGREGATED_METRICS` constants

### browser_checks_analytics_non_aggregated

- Purpose: Contains raw analytics metrics for browser checks as individual data points
- Primary key: `_fivetran_id` (hash of all the column values)
- Content: Individual measurement data points for each check execution
- Metrics: Raw response times, Core Web Vitals measurements, TTFB values, error counts per execution
- Data source: `/v1/analytics/browser-checks/{check_id}` endpoint using `NON_AGGREGATED_METRICS` constants

All tables use the `op.upsert()` operation to insert or update data, ensuring data consistency across sync runs.

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
