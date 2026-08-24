# AVEVA PI JDBC Connectivity Check

## Connector overview

This connector is a **proof of concept**, not a data sync. It validates that a
JRE and AVEVA's proprietary **PI SQL Client JDBC Driver** can be installed and
authenticated against from inside the Hosted Connector SDK's Linux (amd64)
container — the one blocker that ruled out AVEVA's ODBC driver, which is
Windows-only.

On each run it opens an authenticated JDBC connection to PI SQL Data Access
Server (DAS), reads standard JDBC driver/server metadata, and upserts a single
row recording that the connection succeeded. It does not sync any PI data
(elements, attributes, event frames, recorded values, etc.).

For a full data sync connector, see the REST-based [`aveva_pi`](../aveva_pi)
connector in this repo, which uses PI Web API instead of JDBC. A JDBC-based
version of that full sync is tracked separately (RD-1262718) and would build on
the connectivity layer proven here.

## Why JDBC instead of ODBC

AVEVA's PI SQL Client ODBC driver only ships for Windows. The Hosted Connector
SDK runs on Linux amd64, so ODBC cannot be used.

AVEVA's **PI JDBC Driver 2018 or later** is a pure-Java, platform-agnostic
driver — no native/COM-wrapper dependency (earlier, pre-2018 versions did
depend on a COM-wrapper library and are not platform-agnostic; do not use
those). Both drivers connect to the same backend, PI SQL DAS, over HTTPS or
net.tcp — the driver only changes what runs on the client side.

**Use PI JDBC Driver 2018 or later.** This connector has not been tested
against earlier versions.

## Obtaining the driver

AVEVA distributes `PIJDBCDriver.jar` only through the licensed AVEVA/OSIsoft
customer support portal (`techsupport.osisoft.com`), gated behind an active PI
System support entitlement. It is not published to Maven Central or any other
public repository, and this repo cannot bundle it.

Before running or deploying this connector:
1. Download `PIJDBCDriver.jar` from your organization's AVEVA/OSIsoft customer
   portal account.
2. Place it at `drivers/PIJDBCDriver.jar` in this connector's project
   directory.
3. Confirm with your AVEVA license terms whether the jar may be copied into a
   container image you don't directly control (such as Fivetran's Hosted
   Connector SDK runtime) — this is a licensing question this repo does not
   answer.

`drivers/installation.sh` fails fast with an explanatory error if the jar is
missing.

## Requirements

- [Supported Python versions](https://github.com/fivetran/community_connectors/blob/main/README.md#requirements)
- Operating system: Linux amd64 (Hosted Connector SDK runtime)
- `drivers/PIJDBCDriver.jar` — AVEVA PI JDBC Driver 2018 or later (see above)
- PI SQL Data Access Server (DAS), reachable over the network from the
  connector host
- A PI user account with read access to the target AF database, authenticated
  via **username/password** — not Integrated Security/SSPI. SSPI is a
  Windows-only OS API and is unavailable in this Linux container. Your DAS
  must be configured to accept non-SSPI authentication for this to work.

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

> Note: Ensure you have updated `configuration.json` and placed
> `drivers/PIJDBCDriver.jar` before running `fivetran debug`. See
> [Obtaining the driver](#obtaining-the-driver) and
> [Configuration file](#configuration-file).

## Configuration file

```json
{
  "das_host": "<PI_SQL_DAS_HOST>",
  "af_server": "<PI_AF_SERVER_NAME>",
  "af_database": "<PI_AF_DATABASE_NAME>",
  "username": "<PI_USERNAME>",
  "password": "<PI_PASSWORD>"
}
```

| Key | Required | Description |
|---|---|---|
| `das_host` | Yes | Hostname (and port, if non-default) of the PI SQL Data Access Server |
| `af_server` | Yes | Name of the PI AF Server to connect through |
| `af_database` | Yes | Name of the PI AF Database to connect to |
| `username` | Yes | PI user account with read access to the target AF database |
| `password` | Yes | Password for the PI user account |

> Note: When submitting connector code as a Community Connector, ensure `configuration.json` has placeholder values. When deploying, do not check this file into version control to protect credentials.

## Known unknowns — read before using against a real DAS

This connector was written and reviewed without access to AVEVA's gated
"Connection string format" documentation. Two things should be confirmed
against that doc (or AVEVA support) before pointing this at a real DAS:

- The exact connection-string property names for username/password
  authentication in `client.py`'s `build_jdbc_url()`. It currently assumes
  AVEVA's OLEDB-style provider grammar (`AF Server=...;AF Database=...;`)
  extends naturally to non-SSPI credentials, but the specific property names
  for that (equivalent to `Integrated Security=SSPI`) were not independently
  verified.
- Whether your DAS instance is configured to accept non-SSPI authentication at
  all — some PI environments may only allow Integrated Security.

## Authentication

The connector uses username/password authentication over JDBC, passed via
`jaydebeapi.connect()`'s auth parameter. See
[Known unknowns](#known-unknowns--read-before-using-against-a-real-das) above.

## Data handling

- Schema: a single diagnostic table, `jdbc_connection_check`.
- Each run records the JDBC driver name/version and PI SQL DAS product
  name/version (via standard `java.sql.DatabaseMetaData`, not a PI-specific
  query) as proof the connection authenticated successfully.

## Error handling

- Missing configuration values raise `ValueError` immediately (see
  `validate_configuration()` in `connector.py`).
- A missing `drivers/PIJDBCDriver.jar` raises `FileNotFoundError` with
  instructions (see `connect()` in `client.py`).
- Connection/authentication failures propagate from `jaydebeapi.connect()`
  uncaught — there is no retry logic, since this connector's only job is to
  prove connectivity once per run.

## Tables created

### JDBC connection check

| Column | Type | Primary key |
|---|---|---|
| `checked_at` | UTC_DATETIME | Yes |
| `connected` | BOOLEAN | |
| `driver_name` | STRING | |
| `driver_version` | STRING | |
| `database_product_name` | STRING | |
| `database_product_version` | STRING | |

## Additional files

- **`client.py`** — JDBC connection setup (`connect()`) and standard JDBC
  metadata lookup (`get_driver_and_server_info()`).
- **`drivers/installation.sh`** — installs a headless JRE and verifies the
  AVEVA-supplied driver jar is present, before this connector's Python
  dependencies are installed.

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
