from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op


def schema(configuration: dict):
    """Define the schema."""
    return [{"table": "demo", "primary_key": ["id"]}]


def update(configuration: dict, state: dict):
    """Sync data."""
    op.upsert(table="demo", data={"id": 1})
    op.checkpoint(state={})


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
