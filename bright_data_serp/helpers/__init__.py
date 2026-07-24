"""Helper module exports for easy importing."""

# For exposing data transformation and upsert helpers to the connector
from .data_processing import (
    collect_all_fields,
    process_and_upsert_results,
    process_search_result,
)

# For exposing the Bright Data SERP search API client to the connector
from .search import perform_search

# For exposing configuration validation to the connector
from .validation import validate_configuration

__all__ = [
    "collect_all_fields",
    "perform_search",
    "process_and_upsert_results",
    "process_search_result",
    "validate_configuration",
]
