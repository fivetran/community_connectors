# CallMiner Connector Example

## Connector overview

The CallMiner connector for Fivetran uses the CallMiner Bulk Export API to create export jobs, poll job status, download completed export archives, extract nested compressed CSV files, and sync the exported records to your destination.

The connector supports OAuth2 client credentials authentication, incremental date-window syncing, recent-window syncing with CallMiner's `LastNHours` export option, per-data-type state tracking, job timeout recovery, and parallel processing for nested export files.

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_csdk_connectors/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

To initialize a new Connector SDK project using this connector as a starting point, run:

```bash
fivetran init --template callminer
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`. For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/connector-development-and-configuration/connector-sdk-commands#fivetraninit).

> Note: Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the required configuration parameters.

## Features

- OAuth2 client credentials authentication with automatic token refresh.
- Bulk export job creation, polling, download, and cleanup.
- Incremental sync windows using a configurable `initial_start_date` and `increment_days`.
- Recent sync optimization using CallMiner's `LastNHours` export option when state is close to current time.
- Per-data-type state tracking through the `data_types` state object.
- Pending job recovery through the `pending_job` state object when an export does not finish within the configured polling window.
- Nested archive handling for outer ZIP files with inner `.gz` or `.zip` CSV payloads.
- Parallel nested file processing with a configurable `max_threads` value.
- Optional `max_records` and `test_job_id` settings for local testing.

## Configuration file

The connector requires the following configuration parameters in the `configuration.json` file. This configuration is uploaded to Fivetran and defines how the connector authenticates with CallMiner and selects Bulk Export data.

```json
{
  "client_id": "<YOUR_CLIENT_ID>",
  "client_secret": "<YOUR_CLIENT_SECRET>",
  "initial_start_date": "<YOUR_INITIAL_START_DATE>",
  "increment_days": "<YOUR_INCREMENT_DAYS>",
  "max_threads": "<YOUR_MAX_THREADS>",
  "max_polls": "<YOUR_MAX_POLLS>",
  "email_recipients": "<YOUR_EMAIL_RECIPIENTS>",
  "data_types": "<YOUR_DATA_TYPES>"
}
```

Required parameters:

- `client_id`: CallMiner OAuth2 client ID.
- `client_secret`: CallMiner OAuth2 client secret.
- `initial_start_date`: Starting timestamp for the first sync, formatted as `YYYY-MM-DDTHH:MM:SS.000Z`.
- `data_types`: Comma-separated list of CallMiner Bulk Export data types to request.

Optional parameters:

- `increment_days`: Number of days per initial or catch-up sync period. The default is `10`.
- `email_recipients`: Comma-separated email addresses for CallMiner export job notifications.
- `max_records`: Maximum records to process per file for local testing. Omit this value for full syncs.
- `max_threads`: Maximum number of parallel file processing threads. The default is `8`; valid values are `1` through `16`.
- `max_polls`: Maximum number of job polling attempts. The default is `60`, with one poll per minute in normal sync mode.
- `test_job_id`: Existing CallMiner export job ID for local testing. When set, the connector skips job creation and processes the specified job.

> Note: When submitting connector code as a [Community Connector](https://github.com/fivetran/fivetran_csdk_connectors/tree/main) in the open-source [Connector SDK repository](https://github.com/fivetran/fivetran_csdk_connectors/tree/main), ensure the `configuration.json` file has placeholder values. When adding the connector to your production repository, ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

This connector does not require a `requirements.txt` file. It uses Python standard libraries, the Fivetran Connector SDK, and the `requests` library, which are available in the Connector SDK runtime environment.

> Note: [Some packages](https://fivetran.com/docs/connector-sdk/technical-reference#preinstalledpackages) are pre-installed in the Connector SDK runtime environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

The connector uses OAuth2 client credentials flow through the CallMiner identity provider. The `get_access_token` function posts the configured `client_id` and `client_secret` to the token endpoint and stores the returned bearer token for API calls.

Tokens are refreshed before expiration by `refresh_token_if_needed`, using a five-minute buffer to avoid using an expired token during long-running export jobs.

## Bulk export workflow

The connector syncs CallMiner data through Bulk Export jobs rather than direct row pagination.

1. The `update` function validates and parses configuration.
2. The connector determines whether each configured data type should use an incremental date window or the `LastNHours` option.
3. Data types with the same sync strategy are grouped into one Bulk Export job.
4. The connector polls job history until the job is completed, failed, or times out.
5. Completed jobs are downloaded, extracted, and processed into destination tables.
6. The connector checkpoints state after each completed sync period.
7. Completed jobs are deleted after checkpointing.

If a job times out, the connector stores the job details in `pending_job` and checkpoints state before raising an error. The next sync resumes polling the same job before continuing normal sync work.

## Data handling

The connector downloads each completed CallMiner export as an outer ZIP file. The outer ZIP can contain metadata JSON and one or more nested `.gz` or `.zip` files with CSV data.

CSV files are processed as streams with `csv.DictReader`. Table names are derived from exported filenames by removing UUID prefixes and file extensions, then normalizing names to lowercase with underscores. Records are delivered with `op.upsert`.

Nested files are processed in parallel with `ThreadPoolExecutor`. Files are sorted by size before processing so larger files start earlier.

## Error handling

The connector implements targeted error handling across authentication, API requests, job polling, and file processing.

- API retries: The `retry_on_500_error` decorator retries HTTP 500-level errors with exponential backoff.
- Request errors: API request failures are logged and raised with the original `requests` exception.
- Job failures: Failed export jobs raise `ValueError` and are not deleted, allowing manual inspection in CallMiner.
- Job timeouts: Timed-out jobs are saved in state and resumed on the next sync.
- File processing errors: Gzip, ZIP, CSV, encoding, and unexpected file errors are tracked and logged with error statistics.

## Tables created

The connector defines primary keys for known CallMiner export tables in the `schema` function. Columns are inferred from the CSV headers returned by CallMiner.

| Table name | Primary key |
| ---------- | ----------- |
| `AI-SUMMARIES` | `contact_id` |
| `COMMENTS` | `comment_id` |
| `CONTACTS` | `id` |
| `CATEGORIES` | `contact_id`, `category_id`, `section_id` |
| `CATEGORY-COMPONENTS` | `contact_id`, `category_id`, `component_id`, `start_time` |
| `EVENTS-DELAY` | `contact_id`, `start_time`, `end_time` |
| `EVENTS-OVERTALK` | `contact_id`, `start_time`, `end_time` |
| `EVENTS-REDACTION` | `contact_id`, `start_time`, `end_time` |
| `EVENTS-SILENCE` | `contact_id`, `start_time`, `end_time` |
| `SCORES` | `contact_id`, `score_id` |
| `SCORE-INDICATORS` | `contact_id`, `score_id`, `score_component_id` |
| `TAGS` | `contact_id`, `tag_id` |
| `TRANSCRIPTS` | `contact_id`, `start_time` |

## Additional files

- `auth.py`: Handles OAuth2 token requests, token refresh, and retry behavior for 500-level API errors.
- `api_client.py`: Creates export jobs, retrieves job history, checks job status, and deletes completed jobs.
- `config.py`: Validates and parses connector configuration values.
- `file_processing.py`: Downloads export files, extracts nested archives, processes CSV streams, and tracks file processing errors.
- `state.py`: Reads and updates per-data-type sync state.
- `sync.py`: Determines sync strategy, orchestrates export jobs, polls job status, checkpoints progress, and resumes pending jobs.
- `configuration.json`: Provides placeholder local configuration values for `fivetran debug`.

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
