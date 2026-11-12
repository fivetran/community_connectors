import sys
import types
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


sdk_module = types.ModuleType("fivetran_connector_sdk")


class _Connector:
    def __init__(self, update, schema):
        self.update = update
        self.schema = schema

    def debug(self, configuration):
        return self.update(configuration, {})


class _Logging:
    @staticmethod
    def warning(_message):
        return None

    @staticmethod
    def debug(_message):
        return None


class _Operations:
    @staticmethod
    def upsert(**_kwargs):
        return None

    @staticmethod
    def checkpoint(_state):
        return None


sdk_module.Connector = _Connector
sdk_module.Logging = _Logging
sdk_module.Operations = _Operations

sys.modules.setdefault("fivetran_connector_sdk", sdk_module)

redshift_module = types.ModuleType("redshift_connector")


def _connect(**_kwargs):
    return None


redshift_module.connect = _connect
sys.modules.setdefault("redshift_connector", redshift_module)
