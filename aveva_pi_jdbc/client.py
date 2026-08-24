# For locating the AVEVA-supplied driver jar relative to this file
import os

# Bridges Python to the JVM-hosted AVEVA PI SQL Client JDBC driver
import jaydebeapi

# For enabling logs in the connector
from fivetran_connector_sdk import Logging as log

# Driver class name shipped inside AVEVA's PIJDBCDriver.jar
# (PI JDBC Driver Administrator Guide, AVEVA/OSIsoft customer portal).
__DRIVER_CLASS = "com.osisoft.jdbc.Driver"

# drivers/installation.sh expects the AVEVA-supplied jar at this path; it cannot be
# committed to this repo because AVEVA distributes it only through a licensed,
# access-controlled customer portal. See the README's "Obtaining the driver" section.
__DRIVER_JAR_PATH = os.path.join(os.path.dirname(__file__), "drivers", "PIJDBCDriver.jar")


def build_jdbc_url(configuration: dict) -> str:
    """
    Build the PI SQL Client JDBC connection URL.

    NOTE: AVEVA's JDBC driver reuses the PI OLEDB-style provider connection-string
    grammar after the host segment (e.g. "AF Server=...;AF Database=...;"). This
    repo could not independently verify the exact property names against AVEVA's
    gated "Connection string format" documentation, since that page requires an
    active PI System customer portal login. Confirm this against that doc (or
    AVEVA support) before pointing this connector at a real PI SQL DAS instance.

    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        A jdbc:pisql:// connection URL string.
    """
    das_host = configuration["das_host"]
    af_server = configuration["af_server"]
    af_database = configuration["af_database"]
    return f"jdbc:pisql://{das_host}/AF Server={af_server};AF Database={af_database};"


def connect(configuration: dict):
    """
    Open an authenticated JDBC connection to PI SQL DAS.

    Uses username/password authentication rather than SSPI/Integrated Security.
    SSPI is a Windows-only OS API and is not available inside the Linux-based
    Hosted Connector SDK container, so a DAS that only accepts Integrated
    Security cannot be reached from here — it must be configured to also accept
    username/password auth.

    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        An open jaydebeapi.Connection.
    Raises:
        FileNotFoundError: if the AVEVA-supplied driver jar is missing from drivers/.
    """
    if not os.path.exists(__DRIVER_JAR_PATH):
        raise FileNotFoundError(
            f"AVEVA PI JDBC driver jar not found at '{__DRIVER_JAR_PATH}'. "
            "AVEVA distributes PIJDBCDriver.jar only through the licensed AVEVA/OSIsoft "
            "customer support portal, so it cannot be bundled with this connector. "
            "Download it from your portal account and place it at "
            "'drivers/PIJDBCDriver.jar' before running or deploying this connector. "
            "See the README's 'Obtaining the driver' section."
        )

    jdbc_url = build_jdbc_url(configuration)
    log.info(f"Connecting to PI SQL DAS at '{configuration['das_host']}'")
    return jaydebeapi.connect(
        __DRIVER_CLASS,
        jdbc_url,
        [configuration["username"], configuration["password"]],
        __DRIVER_JAR_PATH,
    )


def get_driver_and_server_info(conn) -> dict:
    """
    Read standard JDBC DatabaseMetaData from an open connection.

    Deliberately uses the generic JDBC metadata API rather than a PI-specific SQL
    query, since this repo has not independently verified PI SQL Client's query
    schema (table/view names). This is enough to prove the driver loaded and the
    connection authenticated successfully.

    Args:
        conn: an open jaydebeapi.Connection.
    Returns:
        A dict with driver_name, driver_version, database_product_name and
        database_product_version.
    """
    metadata = conn.jconn.getMetaData()
    return {
        "driver_name": metadata.getDriverName(),
        "driver_version": metadata.getDriverVersion(),
        "database_product_name": metadata.getDatabaseProductName(),
        "database_product_version": metadata.getDatabaseProductVersion(),
    }
