"""
Custom exception hierarchy for the sim package.

This module defines a structured exception hierarchy for better error handling
and debugging throughout SQL Server RCA Assistant.

Exception Hierarchy:
    SimError (base)
    ├── ConfigError - Configuration issues
    ├── ContainerError - Container runtime issues
    │   ├── ContainerRuntimeError - No runtime available
    │   └── ContainerCommandError - Command execution failed
    ├── DatabaseError - Database connection/query issues
    │   ├── ConnectionError - Connection failures
    │   └── QueryError - Query execution failures
    ├── IncidentError - Incident management issues
    ├── RCAError - Root cause analysis issues
    │   ├── LLMError - LLM provider failures
    │   ├── ToolError - RCA tool execution failures
    │   └── SchemaError - Feature schema issues
    └── CritiqueError - Critique system issues

Usage:
    from sim.exceptions import SimError, ContainerError, LLMError

    try:
        # some operation
    except ContainerError as e:
        logger.error(f"Container operation failed: {e}")
    except SimError as e:
        logger.error(f"Application error: {e}")
"""

from typing import Optional, Any, Dict, Union


class SimError(Exception):
    """
    Base exception for all sim package errors.

    All custom exceptions in the sim package should inherit from this class
    to enable catching all sim-related errors with a single except clause.

    Attributes:
        message: Human-readable error message
        details: Optional additional context (dict, exception, etc.)
        suggestion: Optional suggestion for how to resolve the error
    """

    def __init__(
        self,
        message: str,
        details: Optional[Any] = None,
        suggestion: Optional[str] = None,
    ) -> None:
        self.message = message
        self.details = details
        self.suggestion = suggestion
        super().__init__(self._format_message())

    def _format_message(self) -> str:
        """Format the full error message."""
        parts = [self.message]
        if self.details:
            parts.append(f"\nDetails: {self.details}")
        if self.suggestion:
            parts.append(f"\nSuggestion: {self.suggestion}")
        return "".join(parts)


# =============================================================================
# Configuration Errors
# =============================================================================

class ConfigError(SimError):
    """
    Raised when there is a configuration issue.

    Examples:
        - Missing required configuration value
        - Invalid configuration value
        - Configuration file not found or malformed
    """
    pass


# =============================================================================
# Container Errors
# =============================================================================

class ContainerError(SimError):
    """
    Base class for container-related errors.

    Raised when there are issues with the container runtime or container operations.
    """
    pass


class ContainerRuntimeError(ContainerError):
    """
    Raised when no container runtime is available.

    This typically means neither Docker nor Podman is installed or accessible.
    """

    def __init__(self, message: Optional[str] = None) -> None:
        super().__init__(
            message or "No container runtime found",
            suggestion=(
                "Please install Docker or Podman:\n"
                "  macOS:  brew install --cask docker  (or brew install podman)\n"
                "  Linux:  sudo apt install docker.io  (or podman)\n"
                "  Windows: Install Docker Desktop from docker.com"
            ),
        )


class ContainerCommandError(ContainerError):
    """
    Raised when a container command fails.

    Attributes:
        command: The command that failed
        exit_code: Exit code from the command
        stderr: Standard error output
    """

    def __init__(
        self,
        message: str,
        command: Optional[str] = None,
        exit_code: Optional[int] = None,
        stderr: Optional[str] = None,
    ) -> None:
        self.command = command
        self.exit_code = exit_code
        self.stderr = stderr

        details: Dict[str, Any] = {}
        if command:
            details["command"] = command
        if exit_code is not None:
            details["exit_code"] = exit_code
        if stderr:
            details["stderr"] = stderr

        super().__init__(message, details=details if details else None)


# =============================================================================
# Database Errors
# =============================================================================

class DatabaseError(SimError):
    """
    Base class for database-related errors.

    Raised when there are issues connecting to or querying the database.
    """
    pass


class ConnectionError(DatabaseError):
    """
    Raised when a database connection cannot be established.

    Attributes:
        host: Database host
        port: Database port
    """

    def __init__(
        self,
        message: str,
        host: Optional[str] = None,
        port: Optional[int] = None,
        details: Optional[Any] = None,
    ) -> None:
        self.host = host
        self.port = port

        conn_details: Dict[str, Any] = {}
        if host:
            conn_details["host"] = host
        if port:
            conn_details["port"] = port
        if details:
            if isinstance(details, dict):
                conn_details.update(details)
            else:
                conn_details["original_error"] = str(details)

        super().__init__(
            message,
            details=conn_details if conn_details else None,
            suggestion="Ensure SQL Server container is running: python -m sim status",
        )


class QueryError(DatabaseError):
    """
    Raised when a database query fails.

    Attributes:
        query: The SQL query that failed (may be truncated for security)
        sql_state: SQL state code if available
    """

    def __init__(
        self,
        message: str,
        query: Optional[str] = None,
        sql_state: Optional[str] = None,
        details: Optional[Any] = None,
    ) -> None:
        self.query = query
        self.sql_state = sql_state

        query_details = {}
        if query:
            # Truncate very long queries for readability
            truncated = query[:500] + "..." if len(query) > 500 else query
            query_details["query"] = truncated
        if sql_state:
            query_details["sql_state"] = sql_state
        if details:
            if isinstance(details, dict):
                query_details.update(details)
            else:
                query_details["original_error"] = str(details)

        super().__init__(message, details=query_details if query_details else None)


# =============================================================================
# Incident Errors
# =============================================================================

class IncidentError(SimError):
    """
    Raised when there is an issue with incident management.

    Examples:
        - Unknown incident template
        - Failed to start/stop incident
        - Incident verification failure

    Attributes:
        incident_name: Name of the incident template
    """

    def __init__(
        self,
        message: str,
        incident_name: Optional[str] = None,
        details: Optional[Any] = None,
    ) -> None:
        self.incident_name = incident_name

        incident_details = {}
        if incident_name:
            incident_details["incident_name"] = incident_name
        if details:
            if isinstance(details, dict):
                incident_details.update(details)
            else:
                incident_details["original_error"] = str(details)

        super().__init__(
            message,
            details=incident_details if incident_details else None,
            suggestion=(
                "Stage 1 does not ship built-in incident templates. "
                "Use the web app or monitoring tools against your own SQL Server target."
            ),
        )


# =============================================================================
# RCA Errors
# =============================================================================

class RCAError(SimError):
    """
    Base class for Root Cause Analysis errors.

    Raised when there are issues with the RCA engine.
    """
    pass


class LLMError(RCAError):
    """
    Raised when there is an issue with the LLM provider.

    Examples:
        - API key missing or invalid
        - Rate limit exceeded
        - Model not available
        - Response parsing failed

    Attributes:
        provider: LLM provider name (anthropic)
        model: Model name if applicable
        status_code: HTTP status code if applicable
    """

    def __init__(
        self,
        message: str,
        provider: Optional[str] = None,
        model: Optional[str] = None,
        status_code: Optional[int] = None,
        details: Optional[Any] = None,
    ) -> None:
        self.provider = provider
        self.model = model
        self.status_code = status_code

        llm_details: Dict[str, Any] = {}
        if provider:
            llm_details["provider"] = provider
        if model:
            llm_details["model"] = model
        if status_code:
            llm_details["status_code"] = status_code
        if details:
            if isinstance(details, dict):
                llm_details.update(details)
            else:
                llm_details["original_error"] = str(details)

        suggestion = None
        if "api_key" in message.lower() or "authentication" in message.lower():
            suggestion = (
                "Set the appropriate API key environment variable:\n"
                "  Anthropic: export ANTHROPIC_API_KEY=sk-ant-..."
            )

        super().__init__(
            message,
            details=llm_details if llm_details else None,
            suggestion=suggestion,
        )


class ToolError(RCAError):
    """
    Raised when an RCA tool fails to execute.

    Attributes:
        tool_name: Name of the tool that failed
        tool_params: Parameters passed to the tool
    """

    def __init__(
        self,
        message: str,
        tool_name: Optional[str] = None,
        tool_params: Optional[dict] = None,
        details: Optional[Any] = None,
    ) -> None:
        self.tool_name = tool_name
        self.tool_params = tool_params

        tool_details: Dict[str, Any] = {}
        if tool_name:
            tool_details["tool_name"] = tool_name
        if tool_params:
            tool_details["tool_params"] = tool_params
        if details:
            if isinstance(details, dict):
                tool_details.update(details)
            else:
                tool_details["original_error"] = str(details)

        super().__init__(message, details=tool_details if tool_details else None)


class SchemaError(RCAError):
    """
    Raised when there is an issue with the Feature Schema.

    Examples:
        - Invalid analytics data format
        - Missing required fields
        - Schema validation failure

    Attributes:
        field: Field that caused the error
    """

    def __init__(
        self,
        message: str,
        field: Optional[str] = None,
        details: Optional[Any] = None,
    ) -> None:
        self.field = field

        schema_details = {}
        if field:
            schema_details["field"] = field
        if details:
            if isinstance(details, dict):
                schema_details.update(details)
            else:
                schema_details["original_error"] = str(details)

        super().__init__(message, details=schema_details if schema_details else None)


# =============================================================================
# Critique Errors
# =============================================================================

class CritiqueError(SimError):
    """
    Raised when there is an issue with the critique system.

    Examples:
        - Unknown incident for ground truth
        - Failed to parse RCA output
        - LLM critique failed
    """
    pass


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Base
    "SimError",
    # Config
    "ConfigError",
    # Container
    "ContainerError",
    "ContainerRuntimeError",
    "ContainerCommandError",
    # Database
    "DatabaseError",
    "ConnectionError",
    "QueryError",
    # Incident
    "IncidentError",
    # RCA
    "RCAError",
    "LLMError",
    "ToolError",
    "SchemaError",
    # Critique
    "CritiqueError",
]
