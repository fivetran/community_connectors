"""
MSSQLConnection and ConnectionPool classes for managing pyodbc connections to SQL Server.
This module abstracts connection string construction, connection lifecycle, and retry logic.
"""

# For queue operations
import queue

# For exponential backoff with jitter
import random

# For connection timeout tracking
import time

# Context manager for connection acquisition
from contextlib import contextmanager

# For timestamping connection open time and calculating elapsed time
from datetime import datetime

# ODBC driver for SQL Server
import pyodbc

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# constants for connection management and retry logic
from constants import (
    CONNECTION_TIMEOUT_HOURS,
    MAX_RETRIES,
    BASE_RETRY_DELAY,
    MAX_RETRY_DELAY,
    RETRYABLE_SQLSTATES,
    LOCK_TIMEOUT_NATIVE_ERROR,
)


def _is_retryable_error(exc: Exception) -> bool:
    """
    Return True when the exception represents a transient SQL Server error
    that is safe to retry — deadlocks, connection drops, and timeout conditions.
    Uses SQLSTATE codes (standardised, locale-independent) rather than
    substring-matching the human-readable error message.
    Args:
        exc: The exception to evaluate, expected to be a pyodbc.Error.
    """
    if not isinstance(exc, pyodbc.Error) or not exc.args:
        return False
    sqlstate = str(exc.args[0])
    if sqlstate in RETRYABLE_SQLSTATES:
        return True
    # SQL Server lock timeout returns SQLSTATE HY000 with native error 1222
    # embedded in the message string.
    if sqlstate == "HY000":
        message = str(exc.args[1]) if len(exc.args) > 1 else ""
        return LOCK_TIMEOUT_NATIVE_ERROR in message
    return False


class MSSQLConnection:
    """
    Wraps a single pyodbc connection to SQL Server.
    Builds an ODBC Driver 18 connection string with SNI support and
    sets READ UNCOMMITTED isolation level on connect.
    """

    def __init__(self, configuration: dict) -> None:
        """Store configuration and initialise connection tracking variables."""
        self._configuration = configuration
        self._connection = None
        self._connected_at = None

    def _build_connection_string(self) -> str:
        """Construct an ODBC connection string from the configuration dict."""
        server = self._configuration["mssql_server"].strip()
        port = self._configuration["mssql_port"].strip()
        certificate_server_name = self._configuration.get("mssql_cert_server", "").strip()
        database = self._configuration["mssql_database"].strip()
        user = self._configuration["mssql_user"].strip()
        password = self._configuration["mssql_password"]

        # TrustServerCertificate=yes when no explicit cert hostname is provided
        # (e.g., AWS RDS with self-signed certs).
        trust_server_certificate = "yes" if not certificate_server_name else "no"

        connection_string = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={server},{port};"
            f"DATABASE={database};"
            f"UID={user};"
            f"PWD={password};"
            f"Encrypt=yes;"
            f"TrustServerCertificate={trust_server_certificate};"
        )
        if certificate_server_name:
            connection_string += f"HostNameInCertificate={certificate_server_name};"
        return connection_string

    def _open(self) -> None:
        """Open a new connection using the built connection string and set isolation level."""
        connection_string = self._build_connection_string()
        # autocommit=True: no implicit transaction wraps our SELECTs, so no shared locks are held
        self._connection = pyodbc.connect(connection_string, autocommit=True)
        # Session-level command: tells SQL Server not to acquire shared locks on reads for this
        # connection, allowing SELECTs to run without blocking or being blocked by writers.
        # Cursor is closed immediately after since this command returns no rows.
        cursor = self._connection.cursor()
        cursor.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
        cursor.close()
        self._connected_at = datetime.now()
        log.fine(f"connection opened at {self._connected_at.isoformat()}")

    def _close(self) -> None:
        """Close the connection if it exists, and reset tracking variables."""
        if self._connection is not None:
            try:
                self._connection.close()
            except Exception as exc:
                log.warning(f"error on connection close: {exc}")
            finally:
                self._connection = None
                self._connected_at = None

    def _needs_reconnect(self) -> bool:
        """Return True if the connection has never been opened or has exceeded the max age."""
        # Never connected yet, or connection was reset after a failure
        if self._connected_at is None:
            return True
        elapsed = (datetime.now() - self._connected_at).total_seconds()
        return elapsed > CONNECTION_TIMEOUT_HOURS * 3600

    def ensure_open(self) -> None:
        """Open (or reopen) the connection if it is closed or has expired."""
        if self._connection is None or self._needs_reconnect():
            self._close()
            self._open()

    def execute_with_retry(self, sql: str, parameters=()) -> pyodbc.Cursor:
        """
        Execute SQL with exponential backoff on transient errors.
        On a retryable SQLSTATE the connection is closed, the thread sleeps
        with jitter, the connection is reopened, and the query retried.
        Non-retryable errors are re-raised immediately.
        """
        last_exception = None
        for attempt in range(MAX_RETRIES):
            try:
                self.ensure_open()
                cursor = self._connection.cursor()
                cursor.execute(sql, parameters)
                return cursor
            except Exception as exc:
                if not _is_retryable_error(exc):
                    log.severe(f"Non-retryable SQL error: {exc}")
                    self._close()
                    raise

                last_exception = exc
                delay = min(BASE_RETRY_DELAY * (2**attempt), MAX_RETRY_DELAY)
                sleep_time = delay + random.uniform(0, delay * 0.2)
                log.warning(
                    f"Retryable error (attempt {attempt + 1}/{MAX_RETRIES}): {exc}. "
                    f"Sleeping {sleep_time:.1f}s before retry."
                )
                self._close()
                time.sleep(sleep_time)

        log.severe(f"All {MAX_RETRIES} retry attempts exhausted. Last error: {last_exception}")
        raise last_exception

    def close(self) -> None:
        """Close the underlying pyodbc connection and reset tracking state."""
        self._close()


class ConnectionPool:
    """
    Fixed-size pool of MSSQLConnection instances backed by a thread-safe Queue.
    The pool calls ensure_open on every acquire so callers do not need to
    manage connection lifecycle.
    """

    def __init__(self, configuration: dict, size: int) -> None:
        """Open `size` connections upfront and place them in a thread-safe queue."""
        log.info(f"Initialising connection pool with {size} connection(s)")
        self._queue = queue.Queue(maxsize=size)
        self._all_connections: list = []
        for connection_index in range(size):
            try:
                # MSSQLConnection -> (configuration)
                connection = MSSQLConnection(configuration)
                connection.ensure_open()
                self._all_connections.append(connection)
                self._queue.put(connection)
            except Exception as exc:
                log.warning(
                    f"Failed to open a pool connection: {exc}. Continuing with fewer connections."
                )
        opened_connection_count = len(self._all_connections)
        if opened_connection_count == 0:
            raise RuntimeError(
                "Could not open any database connections. Check credentials and network connectivity."
            )
        if opened_connection_count < size:
            log.warning(
                f"Connection pool started with {opened_connection_count}/{size} "
                "connection(s); some connections failed."
            )
        log.info(f"Connection pool ready with {opened_connection_count} connection(s)")

    @contextmanager
    def acquire(self, timeout: float = 30.0):
        """
        Acquire a connection from the pool as a context manager.
        Always returns the connection to the pool in the finally block.
        """
        try:
            connection = self._queue.get(timeout=timeout)
        except queue.Empty:
            raise RuntimeError(f"No connection available from pool within {timeout}s timeout")
        try:
            connection.ensure_open()
            yield connection
        except Exception as exc:
            connection.close()  # reset so the next acquire gets a fresh connection
            log.warning(f"Connection error during acquire: {exc}")
            raise
        finally:
            self._queue.put(connection)

    def close_all(self) -> None:
        """Close every connection in the pool — called once when the sync finishes."""
        for connection in self._all_connections:
            connection.close()
        log.info("All pool connections closed")
