"""Tests for sim.exceptions module."""

import pytest


class TestSimError:
    """Tests for the base SimError class."""

    def test_basic_error(self):
        """Test basic error creation."""
        from sim.exceptions import SimError

        error = SimError("Something went wrong")

        assert str(error) == "Something went wrong"
        assert error.message == "Something went wrong"
        assert error.details is None
        assert error.suggestion is None

    def test_error_with_details(self):
        """Test error with details."""
        from sim.exceptions import SimError

        error = SimError(
            "Operation failed",
            details={"operation": "connect", "attempts": 3},
        )

        assert "Operation failed" in str(error)
        assert "Details:" in str(error)
        assert error.details == {"operation": "connect", "attempts": 3}

    def test_error_with_suggestion(self):
        """Test error with suggestion."""
        from sim.exceptions import SimError

        error = SimError(
            "Connection failed",
            suggestion="Check if the server is running",
        )

        assert "Connection failed" in str(error)
        assert "Suggestion:" in str(error)
        assert "Check if the server is running" in str(error)

    def test_error_inheritance(self):
        """Test that SimError is an Exception."""
        from sim.exceptions import SimError

        error = SimError("Test error")

        assert isinstance(error, Exception)

        with pytest.raises(SimError):
            raise error


class TestContainerErrors:
    """Tests for container-related errors."""

    def test_container_runtime_error_default(self):
        """Test ContainerRuntimeError with default message."""
        from sim.exceptions import ContainerRuntimeError, ContainerError

        error = ContainerRuntimeError()

        assert "No container runtime found" in str(error)
        assert error.suggestion is not None
        assert "Docker" in error.suggestion
        assert "Podman" in error.suggestion
        assert isinstance(error, ContainerError)

    def test_container_runtime_error_custom(self):
        """Test ContainerRuntimeError with custom message."""
        from sim.exceptions import ContainerRuntimeError

        error = ContainerRuntimeError("Custom runtime error")

        assert "Custom runtime error" in str(error)

    def test_container_command_error(self):
        """Test ContainerCommandError with details."""
        from sim.exceptions import ContainerCommandError, ContainerError

        error = ContainerCommandError(
            "Command failed",
            command="docker run test",
            exit_code=1,
            stderr="Error: image not found",
        )

        assert error.command == "docker run test"
        assert error.exit_code == 1
        assert error.stderr == "Error: image not found"
        assert isinstance(error, ContainerError)


class TestDatabaseErrors:
    """Tests for database-related errors."""

    def test_connection_error(self):
        """Test ConnectionError with host/port."""
        from sim.exceptions import ConnectionError, DatabaseError

        error = ConnectionError(
            "Failed to connect",
            host="localhost",
            port=14333,
        )

        assert error.host == "localhost"
        assert error.port == 14333
        assert isinstance(error, DatabaseError)
        assert error.suggestion is not None

    def test_query_error(self):
        """Test QueryError with query details."""
        from sim.exceptions import QueryError, DatabaseError

        error = QueryError(
            "Query execution failed",
            query="SELECT * FROM NonExistentTable",
            sql_state="42S02",
        )

        assert error.query is not None
        assert error.sql_state == "42S02"
        assert isinstance(error, DatabaseError)

    def test_query_error_truncates_long_query(self):
        """Test that very long queries are truncated."""
        from sim.exceptions import QueryError

        long_query = "SELECT " + ", ".join([f"col{i}" for i in range(1000)])
        error = QueryError("Query failed", query=long_query)

        # The details should contain a truncated version
        assert len(error.details["query"]) <= 503  # 500 + "..."


class TestRCAErrors:
    """Tests for RCA-related errors."""

    def test_llm_error_basic(self):
        """Test LLMError with provider info."""
        from sim.exceptions import LLMError, RCAError

        error = LLMError(
            "API call failed",
            provider="anthropic",
            model="claude-opus-4-6",
            status_code=429,
        )

        assert error.provider == "anthropic"
        assert error.model == "claude-opus-4-6"
        assert error.status_code == 429
        assert isinstance(error, RCAError)

    def test_llm_error_api_key_suggestion(self):
        """Test LLMError suggests API key fix for auth errors."""
        from sim.exceptions import LLMError

        error = LLMError("Authentication failed: Invalid API key")

        assert error.suggestion is not None
        assert "API key" in error.suggestion

    def test_tool_error(self):
        """Test ToolError with tool details."""
        from sim.exceptions import ToolError, RCAError

        error = ToolError(
            "Tool execution failed",
            tool_name="get_query_plan",
            tool_params={"query_hash": "0xABC123"},
        )

        assert error.tool_name == "get_query_plan"
        assert error.tool_params == {"query_hash": "0xABC123"}
        assert isinstance(error, RCAError)

    def test_schema_error(self):
        """Test SchemaError with field info."""
        from sim.exceptions import SchemaError, RCAError

        error = SchemaError(
            "Missing required field",
            field="meta.incident_id",
        )

        assert error.field == "meta.incident_id"
        assert isinstance(error, RCAError)


class TestIncidentError:
    """Tests for IncidentError."""

    def test_incident_error_with_name(self):
        """Test IncidentError with incident name."""
        from sim.exceptions import IncidentError

        error = IncidentError(
            "Unknown incident template",
            incident_name="nonexistent_incident",
        )

        assert error.incident_name == "nonexistent_incident"
        assert error.suggestion is not None
        assert "does not ship built-in incident templates" in error.suggestion


class TestExceptionHierarchy:
    """Tests for the exception hierarchy."""

    def test_all_errors_inherit_from_sim_error(self):
        """Test that all custom exceptions inherit from SimError."""
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

        exceptions = [
            ConfigError("test"),
            ContainerError("test"),
            ContainerRuntimeError(),
            ContainerCommandError("test"),
            DatabaseError("test"),
            ConnectionError("test"),
            QueryError("test"),
            IncidentError("test"),
            RCAError("test"),
            LLMError("test"),
            ToolError("test"),
            SchemaError("test"),
            CritiqueError("test"),
        ]

        for exc in exceptions:
            assert isinstance(exc, SimError), f"{type(exc).__name__} should inherit from SimError"

    def test_can_catch_by_base_class(self):
        """Test that exceptions can be caught by base class."""
        from sim.exceptions import SimError, LLMError

        with pytest.raises(SimError):
            raise LLMError("API error")

        with pytest.raises(Exception):
            raise LLMError("API error")
