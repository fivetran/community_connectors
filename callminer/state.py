"""
State management utilities for tracking sync progress.
"""

from typing import Dict, Any


def get_data_type_state(state: Dict[str, Any], data_type: str) -> Dict[str, Any]:
    """
    Get state for a specific data type.

    Args:
        state: Overall state dictionary
        data_type: Data type name (e.g., 'Contacts', 'Transcripts')

    Returns:
        State dictionary for the specific data type
    """
    if "data_types" not in state:
        state["data_types"] = {}

    if data_type not in state["data_types"]:
        state["data_types"][data_type] = {}

    return state["data_types"][data_type]


def update_data_type_state(state: Dict[str, Any], data_type: str, last_synced_date: str) -> None:
    """
    Update the last synced date for a specific data type.

    Args:
        state: Overall state dictionary
        data_type: Data type name
        last_synced_date: ISO format timestamp
    """
    dt_state = get_data_type_state(state, data_type)
    dt_state["last_synced_date"] = last_synced_date
