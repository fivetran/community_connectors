---
name: Native Library With Ctypes
description: "Load a compiled C shared library with ctypes and call it for every row during a sync"
category: "SaaS & APIs"
---

# Native Library With Ctypes Connector Example

## Connector overview
This example shows how a connector can load a compiled native C library with Python's `ctypes` module and call it for every row it syncs. This pattern applies when a vendor ships a proprietary shared library, for example for detokenization, decryption, or decoding of a binary format, that must run inside the connector process.

To keep the example self-contained, it uses a synthetic stub library (`lib_src/stub_token.c`) that reverses a string in place. The stub follows the common init, process, free API shape. The connector initializes the library once per sync, calls it for each generated row, and frees it when the sync ends. It also records the average per-call overhead in the logs, which makes it a useful smoke test before you wire in a real library.

## Requirements
- [Supported Python versions](https://github.com/fivetran/community_connectors/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)
- A C compiler such as `gcc` or `clang` to build the stub library.

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

To initialize a new Connector SDK project using this connector as a starting point, run:

```
fivetran init --template ctypes_native_library
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`. For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/connector-development-and-configuration/connector-sdk-commands#fivetraninit).

Build the stub library before running the connector:

```
gcc -shared -fPIC -o lib_src/libstubtoken.so lib_src/stub_token.c
```

> Note: Fivetran runs connectors on Linux (amd64). Build the library on, or cross-compile it for, Linux amd64 before running `fivetran deploy`, and place the resulting `.so` file in `lib_src/`. A library built on macOS only works for local `fivetran debug` runs.

> Note: Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the configuration parameters.

## Features
- Loads a shared library with `ctypes.CDLL` and declares the argument and return types of each function (`__load_library`).
- Initializes and frees the native library once per sync instead of once per row.
- Passes a mutable buffer to the native function for each row (`transform_value`).
- Checkpoints after every batch so an interrupted sync resumes from the last committed row.
- Logs the average per-call overhead of the native function.

## Configuration file
The configuration keys are all optional.

```
{
  "library_path": "lib_src/libstubtoken.so",
  "total_rows": "50000",
  "batch_size": "1000"
}
```

- `library_path` - Path of the shared library, relative to `connector.py`.
- `total_rows` - Number of synthetic rows to generate.
- `batch_size` - Number of rows to process between checkpoints.

> Note: When submitting connector code as a community connector in the open-source [Community Connector repository](https://github.com/fivetran/community_connectors/tree/main), ensure the `configuration.json` file has placeholder values. When adding the connector to your production repository, ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file
This connector has no third-party dependencies, so `requirements.txt` has no entries.

> Note: [Some packages](https://fivetran.com/docs/connector-sdk/technical-reference#preinstalledpackages) are pre-installed in the Connector SDK runtime environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Data handling
For each row id, the connector builds a token-like value (`TOK` followed by a 12-digit number), passes it through `stub_transform`, and upserts both the original and the transformed value. Rows are processed in batches of `batch_size`, and the cursor is stored in state as `last_row`. After the last batch the cursor resets to 0 so the next sync starts again from the first row.

## Error handling
- `validate_configuration` checks that numeric values are positive integers and that the shared library file exists, and raises a `ValueError` with a build hint if it does not.
- A non-zero return code from `stub_init` or `stub_transform` raises a `RuntimeError` and fails the sync.
- The native library is always freed in a `finally` block, even when the sync fails.

## Tables created
The connector creates the `transformed_rows` table:

```
{
  "table": "transformed_rows",
  "primary_key": ["id"],
  "columns": {
    "id": "INT",
    "raw_value": "STRING",
    "transformed_value": "STRING"
  }
}
```

## Additional files
- `lib_src/stub_token.c` - Source of the synthetic native library used to demonstrate the `ctypes` calls. Replace it with your own library and update the function names and signatures in `__load_library`.

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
