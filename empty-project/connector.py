from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log  # noqa: F401
from fivetran_connector_sdk import Operations as op  # noqa: F401


def schema(configuration: dict):
    return []


def update(configuration: dict, state: dict):
    pass


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
