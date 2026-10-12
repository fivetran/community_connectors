"""
Delta Sharing connector for Fivetran.
Single connection, all tables in one destination schema.
Destination layout:
  Catalog tables  : shares, schemas, tables
  Data tables     : {schema}__{table}  (e.g. customers__account)
Uses the delta_sharing Python library to handle DeletionVectors and other
advanced Delta table features unsupported by the raw parquet query API.
See: https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md

See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# Import standard libraries
import json
import os
import tempfile
import time

# Import libraries for Delta Sharing and HTTP requests
import delta_sharing
import requests

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like upsert(), update(), delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

CHECKPOINT_INTERVAL = 1000

# Supported authentication methods.
AUTH_BEARER_TOKEN = "bearer_token"
AUTH_OAUTH_CLIENT_CREDENTIALS = "oauth_client_credentials"
VALID_AUTH_TYPES = (AUTH_BEARER_TOKEN, AUTH_OAUTH_CLIENT_CREDENTIALS)

# Refresh OAuth access tokens this many seconds before they expire.
OAUTH_REFRESH_MARGIN_SECONDS = 60


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing or invalid.
    """
    endpoint = configuration.get("endpoint")

    if not endpoint:
        raise ValueError("Missing required configuration value: endpoint")
    if not isinstance(endpoint, str) or not endpoint.startswith(("http://", "https://")):
        raise ValueError(
            "Invalid configuration value for endpoint: must be a URL starting with http:// or https://"
        )

    auth_type = configuration.get("auth_type", AUTH_BEARER_TOKEN)
    if auth_type not in VALID_AUTH_TYPES:
        raise ValueError(
            f"Invalid configuration value for auth_type: must be one of {', '.join(VALID_AUTH_TYPES)}"
        )

    if auth_type == AUTH_BEARER_TOKEN:
        if not configuration.get("bearer_token"):
            raise ValueError("Missing required configuration value: bearer_token")
    else:
        required = ("token_endpoint", "client_id", "client_secret")
        missing = [key for key in required if not configuration.get(key)]
        if missing:
            raise ValueError(
                f"Missing required OAuth configuration value(s): {', '.join(missing)}"
            )


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-code/connector-sdk-methods#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {
            "table": "shares",
            "primary_key": ["name"],
            "columns": {"name": "STRING"},
        },
        {
            "table": "schemas",
            "primary_key": ["name"],
            "columns": {"name": "STRING"},
        },
        {
            "table": "tables",
            "primary_key": ["share", "schema_name", "name"],
            "columns": {
                "share": "STRING",
                "schema_name": "STRING",
                "name": "STRING",
            },
        },
    ]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_profile(configuration, endpoint):
    """Write a Delta Sharing recipient profile JSON to a temp file; return its path.

    Bearer-token auth uses a v1 profile. OAuth client credentials uses a v2
    profile so the delta_sharing library refreshes tokens on its own for the
    data reads (load_as_pandas / load_table_changes_as_pandas).
    """
    auth_type = configuration.get("auth_type", AUTH_BEARER_TOKEN)
    if auth_type == AUTH_OAUTH_CLIENT_CREDENTIALS:
        profile = {
            "shareCredentialsVersion": 2,
            "type": "oauth_client_credentials",
            "endpoint": endpoint,
            "tokenEndpoint": configuration["token_endpoint"],
            "clientId": configuration["client_id"],
            "clientSecret": configuration["client_secret"],
        }
        scope = configuration.get("scope")
        if scope:
            profile["scope"] = scope
    else:
        profile = {
            "shareCredentialsVersion": 1,
            "bearerToken": configuration["bearer_token"],
            "endpoint": endpoint,
        }
    f = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
    json.dump(profile, f)
    f.close()
    return f.name


def _fetch_oauth_token(configuration):
    """Run the OAuth 2.0 client credentials flow; return (access_token, expires_in)."""
    data = {
        "grant_type": "client_credentials",
        "client_id": configuration["client_id"],
        "client_secret": configuration["client_secret"],
    }
    scope = configuration.get("scope")
    if scope:
        data["scope"] = scope
    resp = requests.post(configuration["token_endpoint"], data=data, timeout=30)
    resp.raise_for_status()
    body = resp.json()
    return body["access_token"], int(body.get("expires_in", 3600))


def _make_token_getter(configuration):
    """Return a callable that yields a valid bearer token for raw HTTP requests.

    For bearer-token auth the static token is returned. For OAuth the token is
    fetched via client credentials and cached until shortly before it expires.
    """
    auth_type = configuration.get("auth_type", AUTH_BEARER_TOKEN)
    if auth_type != AUTH_OAUTH_CLIENT_CREDENTIALS:
        bearer_token = configuration["bearer_token"]
        return lambda: bearer_token

    cache = {"token": None, "expires_at": 0.0}

    def get_token():
        if cache["token"] is None or time.monotonic() >= cache["expires_at"]:
            token, expires_in = _fetch_oauth_token(configuration)
            cache["token"] = token
            refresh_window = min(OAUTH_REFRESH_MARGIN_SECONDS, expires_in / 2)
            cache["expires_at"] = time.monotonic() + expires_in - refresh_window
        return cache["token"]

    return get_token


def _get_table_version(endpoint, token_getter, share, schema_name, table_name):
    """GET .../version  →  current Delta-Table-Version as int."""
    url = f"{endpoint}/shares/{share}/schemas/{schema_name}" f"/tables/{table_name}/version"
    resp = requests.get(
        url,
        headers={"Authorization": f"Bearer {token_getter()}"},
        timeout=30,
    )
    resp.raise_for_status()
    return int(resp.headers["Delta-Table-Version"])


# ---------------------------------------------------------------------------
# Catalog sync
# ---------------------------------------------------------------------------


def _sync_catalog(client):
    """Upsert shares, schemas, and table names into the catalog tables."""
    seen_schemas = set()
    for share in client.list_shares():
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table="shares", data={"name": share.name})

        for schema_obj in client.list_schemas(share):
            if schema_obj.name not in seen_schemas:
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The first argument is the name of the destination table.
                # The second argument is a dictionary containing the record to be upserted.
                op.upsert("schemas", {"name": schema_obj.name})
                seen_schemas.add(schema_obj.name)

                for tbl in client.list_tables(schema_obj):
                    # The 'upsert' operation is used to insert or update data in the destination table.
                    # The first argument is the name of the destination table.
                    # The second argument is a dictionary containing the record to be upserted.
                    op.upsert(
                        "tables",
                        {
                            "share": share.name,
                            "schema_name": schema_obj.name,
                            "name": tbl.name,
                        },
                    )


# ---------------------------------------------------------------------------
# Data sync
# ---------------------------------------------------------------------------


def _sync_table(profile_path, endpoint, token_getter, share, schema_name, table_name, state):
    """
    Sync one Delta Sharing table into destination table {schema}__{table}.

    First sync  : load_as_pandas (full snapshot).
    Subsequent  : load_table_changes_as_pandas(starting_version=last+1).

    State key   : ver__{share}__{schema}__{table}  →  last synced version.
    """
    dest = f"{schema_name}__{table_name}".lower()
    state_key = f"ver__{share}__{schema_name}__{table_name}"
    last_version = state.get(state_key)
    table_url = f"{profile_path}#{share}.{schema_name}.{table_name}"

    try:
        current_version = _get_table_version(
            endpoint, token_getter, share, schema_name, table_name
        )
    except Exception as e:
        log.warning(f"Cannot get version for {dest}, skipping: {e}")
        return state

    if last_version is not None and last_version >= current_version:
        log.info(f"{dest}: no new data (version {current_version})")
        return state

    log.info(f"{dest}: syncing version {last_version} → {current_version}")

    try:
        if last_version is not None:
            df = delta_sharing.load_table_changes_as_pandas(
                table_url,
                starting_version=last_version + 1,
                ending_version=current_version,
            )
            if "_change_type" in df.columns:
                df = df[df["_change_type"].isin(["insert", "update_postimage"])]
                df = df.drop(
                    columns=[
                        c
                        for c in ["_change_type", "_commit_version", "_commit_timestamp"]
                        if c in df.columns
                    ]
                )
        else:
            df = delta_sharing.load_as_pandas(table_url)

    except Exception as e:
        log.warning(f"Skipping {dest}: {e}")
        return state

    count = 0
    for _, row in df.iterrows():
        record = {k: (None if str(v) == "nan" else v) for k, v in row.items()}
        op.upsert(dest, record)
        count += 1
        if count % CHECKPOINT_INTERVAL == 0:
            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
            # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
            op.checkpoint(state)
    state[state_key] = current_version
    log.info(f"{dest}: upserted {count} row(s) at version {current_version}")
    return state


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """
    log.warning("Example: Cloud Data Warehouses : Delta Sharing")

    validate_configuration(configuration)
    endpoint = configuration["endpoint"].rstrip("/")
    token_getter = _make_token_getter(configuration)

    profile_path = _write_profile(configuration, endpoint)
    try:
        client = delta_sharing.SharingClient(profile_path)

        log.info("Syncing catalog (shares, schemas, tables)")
        _sync_catalog(client)

        all_tables = [
            (share.name, schema_obj.name, tbl.name)
            for share in client.list_shares()
            for schema_obj in client.list_schemas(share)
            for tbl in client.list_tables(schema_obj)
        ]
        log.info(f"Discovered {len(all_tables)} table(s)")

        for share_name, schema_name, table_name in all_tables:
            state = _sync_table(
                profile_path, endpoint, token_getter, share_name, schema_name, table_name, state
            )

            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
            # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
            op.checkpoint(state)
        op.checkpoint(state)
    finally:
        os.unlink(profile_path)

    log.info("Sync complete")


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
#
# IMPORTANT: The recommended way to test your connector is using the Fivetran debug command:
#   fivetran debug
#
# This local testing block is provided as a convenience for quick debugging during development,
# such as using IDE debug tools (breakpoints, step-through debugging, etc.).
# Note: This method is not called by Fivetran when executing your connector in production.
# Always test using 'fivetran debug' prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
