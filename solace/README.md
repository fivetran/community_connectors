# Solace Connector Example

## Connector overview

This connector demonstrates how to sync data from a Solace queue using the [Fivetran Connector SDK](https://fivetran.com/docs/connectors/connector-sdk). It fetches messages from a durable Solace queue using the Solace Messaging API, processes the events, and upserts them into a destination table. The connector supports incremental syncs using message timestamps and checkpointing to ensure continuity.

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
fivetran init --template solace
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`. For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/connector-development-and-configuration/connector-sdk-commands#fivetraninit).

> Note: Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the required configuration parameters.

## Features

- Connects to Solace PubSub+ using the [Solace PubSub+ Python API](https://solace.dev).
- Pulls events from a durable exclusive queue.
- Supports incremental data syncing using timestamp-based filtering.
- Deduplicates messages using an internally generated `event_id`.
- Graceful error handling and logging.
- Tracks sync state for resumable operations.
- Optionally supports publishing test messages for development.

## Configuration file

The connector expects the following configuration in the `configuration.json` file:

```json
{
    "solace_host": "<YOUR_SOLACE_HOST>",
    "solace_username": "<YOUR_SOLACE_USERNAME>",
    "solace_password": "<YOUR_SOLACE_PASSWORD>",
    "solace_vpn": "<YOUR_SOLACE_VPN>",
    "solace_queue": "<YOUR_SOLACE_QUEUE>"
}
```

> Note: When submitting connector code as a [Community Connector](https://github.com/fivetran/community_connectors/tree/main) in the open-source [Connector SDK repository](https://github.com/fivetran/community_connectors/tree/main), ensure the `configuration.json` file has placeholder values. When adding the connector to your production repository, ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

Add the following to `requirements.txt`:

```
pandas==2.3.0
solace-pubsubplus==1.10.0
```

> Note: [Some packages](https://fivetran.com/docs/connector-sdk/technical-reference#preinstalledpackages) are pre-installed in the Connector SDK runtime environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

The connector authenticates to Solace using basic credentials and VPN configuration:

- `solace_host`: Solace broker host
- `solace_username`: Username for broker authentication
- `solace_password`: Password for broker authentication
- `solace_vpn`: VPN name (default is `default`)

SSL validation is disabled in local development. Modify the `SolaceAuth` class to enable certificate validation if required.

## Pagination

The connector consumes messages in batches from a durable, exclusive queue until the configured batch size limit is reached.

Messages are retrieved using:
```python
message = receiver.receive_message(timeout=1000)
```

This allows the connector to stream messages efficiently with timeout-based pagination

## Data handling

The connector processes events from Solace as follows:

- Establishes a connection to the Solace broker using durable queue subscriptions.
- Receives messages one at a time using Solace's persistent message receiver.
- Parses each message payload (expects JSON structure).
- Extracts relevant metadata including timestamp, topic, and message ID.
- Skips and removes messages older than the last sync timestamp from queue.
- Constructs structured records and appends processing metadata.
- Deduplicates events using a combination of `event_id` and `timestamp`.
- Upserts cleaned records for inserting into the destination table.

Each event includes:

- `event_id`: A generated hash-based unique ID.
- `message_id`: Optional ID from the payload.
- `timestamp`: Event timestamp (from payload or receive time).
- `topic`: Source Solace topic or queue.
- `message_payload`: Raw message body.
- `message_type`: Type extracted from payload (default: `"raw"`).
- `details`: Optional field from payload.
- `processed_at`: Timestamp the message was processed.

## Error handling

This connector includes robust error handling at various stages:

- Connection errors: Fail fast with meaningful error messages if connection to the broker fails.
- Message processing errors: Malformed or unexpected payloads are logged and skipped without halting the sync.
- Upsert failures: Logged per record, allowing the connector to continue processing other events.
- Timeouts and retries: Configurable timeout ensures the sync completes even if the queue is empty or slow.

## Tables created

The connector creates a single table named `solace_events` (refer to the `schema()` function):

| Column | Type | Description |
|--------|------|-------------|
| `event_id` | STRING | Primary key – hash-based unique identifier derived from topic, timestamp, and payload |
| `timestamp` | STRING | Primary key – event timestamp |
| `message_id` | STRING | Optional message ID from the payload |
| `topic` | STRING | Source Solace topic or queue name |
| `message_payload` | STRING | Raw message body |
| `message_type` | STRING | Message type extracted from payload (default: `"raw"`) |
| `details` | STRING | Optional details field from payload |
| `processed_at` | STRING | Timestamp when the message was processed by the connector |

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
