"""Data processing utilities for flattening and normalizing Bright Data results."""

# For parsing JSON payloads
import json

# For type hints
from typing import Any

# For enabling Logs and Operations in connector code

from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op


def flatten_dict(
    data: Any, parent_key: str = "", separator: str = "_", max_depth: int = 10
) -> dict:
    """
    Flatten a nested dictionary into a single-level dictionary.

    Converts nested structures like {"a": {"b": 1}} to {"a_b": 1}.
    Handles lists by converting them to JSON strings.
    Args:
        data: The dictionary to flatten.
        parent_key: The parent key to use for the flattened dictionary.
        separator: The separator to use for the flattened dictionary.
        max_depth: The maximum depth to flatten the dictionary to.
    Returns:
        A dictionary with the flattened dictionary.
    """
    if max_depth <= 0:
        return {parent_key: json.dumps(data) if data else None}

    items: list = []

    if isinstance(data, dict):
        for key, value in data.items():
            new_key = f"{parent_key}{separator}{key}" if parent_key else key
            if isinstance(value, dict):
                items.extend(flatten_dict(value, new_key, separator, max_depth - 1).items())
            elif isinstance(value, list):
                items.append((new_key, json.dumps(value) if value else "[]"))
            else:
                items.append((new_key, value))
    elif isinstance(data, list):
        return {parent_key: json.dumps(data) if data else "[]"}
    else:
        return {parent_key: data}

    return dict(items)


def collect_all_fields(results: list) -> set:
    """Collect all unique field names from a list of result dictionaries.
    Args:
        results: A list of result dictionaries.
    Returns:
        A set of all unique field names from the result dictionaries.
    """
    all_fields: set = set()
    for result in results:
        all_fields.update(result.keys())
    return all_fields


def process_unlocker_result(result: Any, requested_url: str, result_index: int) -> dict:
    """
    Process a single web unlocker result by flattening nested dictionaries.

    Primary key fields (requested_url, result_index) are always preserved and never
    overwritten by values from the flattened API response.
    Args:
        result: The result dictionary to process.
        requested_url: The URL that was requested.
        result_index: The index of the result.
    Returns:
        A dictionary with the processed result.
    """
    base_fields = {
        "requested_url": requested_url,
        "result_index": result_index,
        "position": result_index + 1,
    }

    if not isinstance(result, dict):
        base_fields["raw_data"] = str(result)
        return base_fields

    flattened = flatten_dict(result)

    for pk_field in ("requested_url", "result_index"):
        flattened.pop(pk_field, None)

    final_result = {**flattened, **base_fields}
    final_result["result_index"] = int(result_index)
    final_result["position"] = int(result_index + 1)

    return final_result


def process_and_upsert_results(processed_results: list, all_fields: set, table_name: str) -> None:
    """Validate primary keys and upsert processed unlocker result records.
    Args:
        processed_results: A list of processed unlocker result dictionaries.
        all_fields: A set of all field names from the processed results.
        table_name: The name of the table to upsert the results into.
    """
    primary_keys = {"requested_url": str, "result_index": int}
    primary_key_errors = []

    for result in processed_results:
        for pk, pk_type in primary_keys.items():
            if pk not in result:
                primary_key_errors.append(f"Primary key '{pk}' missing from result")
                result[pk] = pk_type() if pk_type == str else 0
            elif not isinstance(result[pk], pk_type):
                try:
                    if pk_type == str:
                        result[pk] = str(result[pk])
                    elif pk_type == int:
                        current_value = result[pk]
                        if isinstance(current_value, str):
                            cleaned = current_value.strip().strip("[]\"'")
                            result[pk] = int(cleaned) if cleaned.isdigit() else 0
                        else:
                            result[pk] = int(current_value)
                except (ValueError, TypeError):
                    primary_key_errors.append(
                        f"Could not convert primary key '{pk}' to {pk_type.__name__}"
                    )
                    result[pk] = pk_type() if pk_type == str else 0

        row = {field: result.get(field) for field in all_fields}
        op.upsert(table=table_name, data=row)

    if primary_key_errors:
        unique_errors = list(set(primary_key_errors))
        log.warning(
            f"Primary key validation issues: {', '.join(unique_errors[:3])}"
            f"{' (and more)' if len(unique_errors) > 3 else ''}"
        )
