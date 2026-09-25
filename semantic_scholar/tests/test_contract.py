"""Universal connector rules from the shared harness, applied to this connector.

Private to the development repository: it depends on connector_test_harness,
which is not shipped with the connector.
"""

import connector as c
from connector_test_harness import ConnectorContract
from test_connector import _paper


class TestSemanticScholarContract(ConnectorContract):
    """Every universal connector rule, inherited rather than re-written."""

    module = c
    flatten = staticmethod(lambda r: c.flatten_paper(r, enrichment=None))
    sparse_record = {"paperId": "abc123"}
    sample_record = _paper()
