"""
SQL Server RCA Assistant.

Stage-1 package scope:
- Web app for chat-based SQL Server troubleshooting
- AI-assisted RCA engine
- SQL Server diagnostics tooling (sp_Blitz integration)
- Optional ClickHouse monitoring backend

Quick Start:
    export SQLSERVER_HOST='your-sqlserver-host'
    export SQLSERVER_PASSWORD='your-password'
    python -m sim webapp start
"""

__version__ = "1.0.0"
__author__ = "SQL Server RCA Assistant Contributors"

# Core configuration
from sim.config import Config, get_config

# Exception hierarchy
from sim.exceptions import (
    SimError,
    ConfigError,
    ContainerError,
    ContainerRuntimeError,
    ContainerCommandError,
    DatabaseError,
    ConnectionError,
    QueryError,
    IncidentError,
    RCAError,
    LLMError,
    ToolError,
    SchemaError,
    CritiqueError,
)

# Logging utilities
from sim.logging_config import (
    configure_logging,
    get_logger,
    set_correlation_id,
    get_correlation_id,
)

__all__ = [
    # Version info
    "__version__",
    "__author__",
    # Configuration
    "Config",
    "get_config",
    # Exceptions
    "SimError",
    "ConfigError",
    "ContainerError",
    "ContainerRuntimeError",
    "ContainerCommandError",
    "DatabaseError",
    "ConnectionError",
    "QueryError",
    "IncidentError",
    "RCAError",
    "LLMError",
    "ToolError",
    "SchemaError",
    "CritiqueError",
    # Logging
    "configure_logging",
    "get_logger",
    "set_correlation_id",
    "get_correlation_id",
]
