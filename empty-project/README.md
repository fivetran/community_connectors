# Empty Connector Example

## Connector overview

A blank starting point for building a Fivetran connector from scratch using the [Fivetran Connector SDK](https://fivetran.com/docs/connectors/connector-sdk).

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
fivetran init --template empty-project
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`. For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/connector-development-and-configuration/connector-sdk-commands#fivetraninit).



## Files

- `connector.py` – Entry point with empty `schema()` and `update()` stubs and required imports.
- `configuration.json` – Empty configuration file. Add your source credentials here.
- `requirements.txt` – Empty requirements file. Add any third-party libraries your connector needs.
