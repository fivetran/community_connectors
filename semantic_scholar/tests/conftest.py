"""Pytest setup for the Semantic Scholar connector.

The SDK log-level fixture and every universal connector rule live in the shared
harness at `connector_test_harness/`, so this file only wires the path.
"""

import sys
from pathlib import Path

# Allow test files to import connector.py from the parent directory and
# connector_test_harness from the repo root.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))

from connector_test_harness import sdk_log_level  # noqa: F401,E402  (autouse fixture)
