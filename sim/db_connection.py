"""
Database connection utilities for SQL Server.

Provides a consistent interface for creating SQL Server connections
using the mssql-python driver (high-performance, thread-safe).
"""

import re
from dataclasses import dataclass
from typing import Any, Optional
from contextlib import contextmanager


class DriverNotInstalledError(RuntimeError):
    """Raised when mssql-python is required but not installed."""


# Re-exported symbols are initialized to safe placeholders and rebound
# when the real driver module is first imported.
Error = DriverNotInstalledError
ProgrammingError = DriverNotInstalledError
Connection = Any

_mssql_module: Optional[Any] = None


def _get_mssql() -> Any:
    """Lazy-load mssql-python to avoid import-time hard dependency."""
    global _mssql_module, Error, ProgrammingError, Connection

    if _mssql_module is None:
        try:
            import mssql_python as mssql  # type: ignore[import-not-found]
        except ModuleNotFoundError as e:
            raise DriverNotInstalledError(
                "mssql-python is required for SQL Server connectivity. "
                "Install it with: pip install mssql-python"
            ) from e

        _mssql_module = mssql
        Error = mssql.Error
        ProgrammingError = mssql.ProgrammingError
        Connection = mssql.Connection

    return _mssql_module


@dataclass
class ConnectionConfig:
    """Configuration for SQL Server connection."""
    server: str
    port: int = 1433
    database: str = "master"
    user: str = "sa"
    password: str = ""
    trust_server_certificate: bool = True
    connection_timeout: int = 30
    
    @property
    def server_with_port(self) -> str:
        """Get server string with port (e.g., 'localhost,14333')."""
        return f"{self.server},{self.port}"
    
    def to_connection_string(self) -> str:
        """Convert to mssql-python connection string format."""
        return (
            f"SERVER={self.server},{self.port};"
            f"DATABASE={self.database};"
            f"UID={self.user};"
            f"PWD={self.password};"
            f"TrustServerCertificate={'yes' if self.trust_server_certificate else 'no'};"
        )
    
    def to_odbc_string(self) -> str:
        """Convert to legacy ODBC connection string format (for compatibility)."""
        return (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={self.server},{self.port};"
            f"DATABASE={self.database};"
            f"UID={self.user};"
            f"PWD={self.password};"
            f"TrustServerCertificate={'yes' if self.trust_server_certificate else 'no'};"
        )


def parse_odbc_connection_string(conn_str: str) -> ConnectionConfig:
    """
    Parse an ODBC connection string into ConnectionConfig.
    
    Handles connection strings like:
        DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost,14333;
        DATABASE=master;UID=sa;PWD=password;TrustServerCertificate=yes;
    
    Args:
        conn_str: ODBC-style connection string
        
    Returns:
        ConnectionConfig with parsed values
    """
    config = ConnectionConfig(server="localhost")
    
    # Parse key=value pairs (handle {bracketed} values)
    pattern = r'(\w+)=(?:\{([^}]*)\}|([^;]*))'
    
    for match in re.finditer(pattern, conn_str, re.IGNORECASE):
        key = match.group(1).upper()
        value = match.group(2) or match.group(3) or ""
        
        if key == "SERVER":
            # Handle SERVER=host,port format
            if "," in value:
                parts = value.split(",")
                config.server = parts[0]
                config.port = int(parts[1])
            else:
                config.server = value
        elif key == "DATABASE":
            config.database = value
        elif key in ("UID", "USER"):
            config.user = value
        elif key in ("PWD", "PASSWORD"):
            config.password = value
        elif key == "TRUSTSERVERCERTIFICATE":
            config.trust_server_certificate = value.lower() in ("yes", "true", "1")
        elif key in ("CONNECTION TIMEOUT", "TIMEOUT"):
            config.connection_timeout = int(value)
    
    return config


def connect(
    config: Optional[ConnectionConfig] = None,
    *,
    server: Optional[str] = None,
    port: int = 1433,
    database: str = "master",
    user: str = "sa",
    password: str = "",
    trust_server_certificate: bool = True,
    connection_timeout: int = 30,
    autocommit: bool = False,
    connection_string: Optional[str] = None,
) -> Any:
    """
    Create a SQL Server connection using mssql-python.
    
    Thread-safe and high-performance. Supports multiple call styles:
    
    1. Using ConnectionConfig:
        conn = connect(config=my_config)
    
    2. Using keyword arguments:
        conn = connect(server="localhost", port=14333, database="mydb", ...)
    
    3. Using legacy ODBC connection string (will be parsed):
        conn = connect(connection_string="DRIVER=...;SERVER=localhost,14333;...")
    
    Args:
        config: ConnectionConfig instance (takes precedence)
        server: Server hostname/IP
        port: Port number (default 1433)
        database: Database name
        user: Username
        password: Password
        trust_server_certificate: Trust server certificate (default True)
        connection_timeout: Connection timeout in seconds
        autocommit: Enable autocommit mode
        connection_string: Legacy ODBC connection string (will be parsed)
        
    Returns:
        mssql.Connection instance
    """
    # Parse legacy ODBC string if provided
    if connection_string:
        config = parse_odbc_connection_string(connection_string)
    
    # Use config if provided, otherwise build from kwargs
    if config is None:
        config = ConnectionConfig(
            server=server or "localhost",
            port=port,
            database=database,
            user=user,
            password=password,
            trust_server_certificate=trust_server_certificate,
            connection_timeout=connection_timeout,
        )
    
    # Build connection string for mssql-python
    conn_str = config.to_connection_string()

    # Create connection using mssql-python
    mssql = _get_mssql()
    conn = mssql.connect(
        connection_str=conn_str,
        autocommit=autocommit,
        timeout=config.connection_timeout,
    )
    
    return conn


@contextmanager
def connection_context(
    config: Optional[ConnectionConfig] = None,
    *,
    connection_string: Optional[str] = None,
    autocommit: bool = False,
    **kwargs
):
    """
    Context manager for SQL Server connections.
    
    Automatically closes the connection when exiting the context.
    
    Example:
        with connection_context(server="localhost", database="mydb") as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
    
    Args:
        config: ConnectionConfig instance
        connection_string: Legacy ODBC connection string
        autocommit: Enable autocommit mode
        **kwargs: Additional connection parameters
        
    Yields:
        SQL Server connection instance
    """
    conn = connect(
        config=config,
        connection_string=connection_string,
        autocommit=autocommit,
        **kwargs
    )
    try:
        yield conn
    finally:
        conn.close()
