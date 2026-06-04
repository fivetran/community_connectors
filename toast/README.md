# Toast Connector Example

## Connector overview

This is a custom [Fivetran connector](https://fivetran.com/docs/connectors/connector-sdk) implementation to extract and sync data from the [Toast POS API](https://doc.toasttab.com/) into a destination warehouse. Toast is a restaurant management platform providing point-of-sale, labor, menu, and operational data.

For full implementation details, see the [Toast connector example code](https://github.com/fivetran/community_connectors/tree/main/toast/).

## Requirements

- Toast API credentials: `clientId`, `clientSecret`, `userAccessType`
- Domain to connect to (e.g., `api.toasttab.com`)
- A Fernet key (`key`) for encrypting access tokens
- `initialSyncStart` ISO timestamp to define the start of sync window
- [Supported Python versions](https://github.com/fivetran/community_connectors/blob/main/README.md#requirements)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

To initialize a new Connector SDK project using this connector as a starting point, run:

```
fivetran init --template toast
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`. For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/connector-development-and-configuration/connector-sdk-commands#fivetraninit).

> Note: Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the required configuration parameters.

## Features

- Syncs data from multiple Toast endpoints, including orders, employees, shifts, menus, and more
- Automatically handles nested JSON structures and normalizes them into relational tables
- Includes incremental sync via time-based windowing and state checkpointing
- Graceful handling of rate limits, authentication, and API errors
- Supports voids, deletions, and nested child entities
- Uses Fernet encryption for token security in state

## Configuration file

Example `configuration.json`:

```json
{
  "clientId": "your_client_id",
  "clientSecret": "your_client_secret",
  "userAccessType": "TOAST_MACHINE_CLIENT",
  "domain": "ws-api.toasttab.com",
  "initialSyncStart": "2023-01-01T00:00:00.000Z",
  "key": "your_base64_encoded_fernet_key"
}
```

> Note: When submitting connector code as a [Community Connector](https://github.com/fivetran/community_connectors/tree/main) in the open-source [Connector SDK repository](https://github.com/fivetran/community_connectors/tree/main), ensure the `configuration.json` file has placeholder values. When adding the connector to your production repository, ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

The connector requires the `cryptography` library for Fernet-based token encryption. Add it to your `requirements.txt`:

```
cryptography
```

> Note: [Some packages](https://fivetran.com/docs/connector-sdk/technical-reference#preinstalledpackages) are pre-installed in the Connector SDK runtime environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Data handling

The connector performs the following actions for each key aspect:
- Authentication: Generates and caches a Toast token using the provided credentials
- Sync loop: Runs in 30-day time chunks, paginating through endpoints
- Data normalization: Flattens nested objects and lists using `flatten_dict`, `extract_fields`, and `stringify_lists`
- Upserts and deletes: Emits operations using `op.upsert()` and `op.delete()`
- Checkpointing: Updates state after each window to resume seamlessly

## Error handling

- Retries on 401 Unauthorized (max 3)
- Skips 403 Forbidden
- Backs off on 429 Too Many Requests
- Skips on 400 and 409 errors with logging

Uses Fivetran SDK logging levels (`info`, `debug`, `warning`, `error`) for detailed sync visibility.

## Tables created

The entity-relationship diagram (ERD) below shows how tables are linked in the Toast schema.

<img src="https://raw.githubusercontent.com/fivetran/connector_sdk/main/connectors/toast/Toast_ERD.png" alt="Fivetran Toast Connector ERD" width="100%">

### Core tables

- `restaurant`
- `job`, `employee`, `shift`, `break`, `time_entry`
- `orders`, `orders_check`, `payment`

### Configuration tables

- `menu`, `menu_item`, `menu_group`, `discounts`, `tables`, etc.

### Nested children

- `orders_check_payment`, `orders_check_selection`, `orders_check_selection_modifier`, etc.

### Cash management

- `cash_entry`, `cash_deposit`

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.

## Resources

- [Fivetran Connector SDK Docs](https://fivetran.com/docs/connectors/connector-sdk)
- [Toast API Reference](https://doc.toasttab.com/)
- [Fernet Encryption](https://cryptography.io/en/latest/fernet/)
