"""Shared test fixtures for the http_proxy connector tests.

Logging.LOG_LEVEL is None when the connector SDK is loaded outside the Fivetran
runner context. Patching the log module globally avoids TypeError on every test
that exercises code paths that call log.info or log.warning.
"""

from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def suppress_sdk_logging():
    """Patch connector.log so log.info/log.warning are no-ops in all tests."""
    with patch("connector.log"):
        yield
