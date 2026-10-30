"""Destination schema construction for the Deltek Vantagepoint connector.

Combines the Hub, General/Configuration, detail/child, and Billing table definitions from
table_registry.py into the flat list of {table, primary_key} entries the Fivetran Connector SDK
expects from schema().
"""

# Table/endpoint registry this connector syncs
from table_registry import __CONFIG_TABLES, __HUB_CHILDREN, __INVOICE_HEADER_TABLE, __TABLES


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-code/connector-sdk-methods#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    tables = [
        {"table": table_config["table"], "primary_key": table_config["primary_key"]}
        for table_config in __TABLES + __CONFIG_TABLES
    ]
    for hub_config in __HUB_CHILDREN.values():
        for child in hub_config["children"]:
            tables.append({"table": child["table"], "primary_key": child["primary_key"]})
    tables.append(
        {
            "table": __INVOICE_HEADER_TABLE["table"],
            "primary_key": __INVOICE_HEADER_TABLE["primary_key"],
        }
    )
    return tables
