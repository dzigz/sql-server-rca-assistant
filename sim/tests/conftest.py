"""
Shared pytest fixtures for sim package tests.

This module provides fixtures for:
- Mock SQL Server responses
- Mock LLM responses
- Test configuration
"""

import pytest
from pathlib import Path
from typing import Any, Generator
from unittest.mock import MagicMock, patch


# =============================================================================
# Configuration Fixtures
# =============================================================================

@pytest.fixture
def mock_env_vars(monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    """Set up mock environment variables for testing."""
    env_vars = {
        "SIM_SA_PASSWORD": "TestPassword123!",
        "SIM_HOST_PORT": "14334",
        "SIM_CONTAINER_NAME": "test-sqlserver",
    }
    for key, value in env_vars.items():
        monkeypatch.setenv(key, value)
    return env_vars


@pytest.fixture
def test_config():
    """Create a test configuration with isolated settings."""
    from sim.config import Config
    return Config(
        container_name="test-sqlserver",
        host_port=14334,
        sa_password="TestPassword123!",
        cpu_limit="1.0",
        memory_limit="2g",
    )


# =============================================================================
# Container Manager Fixtures
# =============================================================================

@pytest.fixture
def mock_container_manager():
    """Create a mock container manager that doesn't require Docker/Podman."""
    from sim.container_manager import ContainerManager

    with patch.object(ContainerManager, "_detect_runtime", return_value="docker"):
        with patch.object(ContainerManager, "_run_cmd") as mock_run:
            mock_run.return_value = MagicMock(
                stdout="container_id_123\n",
                stderr="",
                returncode=0,
            )
            manager = ContainerManager()
            manager._mock_run = mock_run
            yield manager


@pytest.fixture
def mock_running_container(mock_container_manager):
    """Mock a running container for tests that need it."""
    mock_container_manager.is_running = MagicMock(return_value=True)
    mock_container_manager.exists = MagicMock(return_value=True)
    return mock_container_manager


# =============================================================================
# SQL Server / Database Fixtures
# =============================================================================

@pytest.fixture
def mock_db_connection():
    """Create a mock database connection."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchall.return_value = []
    mock_cursor.fetchone.return_value = None
    return mock_conn


@pytest.fixture
def sample_wait_stats() -> list[dict[str, Any]]:
    """Sample wait statistics data."""
    return [
        {
            "wait_type": "LCK_M_X",
            "waiting_tasks_count": 15,
            "wait_time_ms": 45000,
            "signal_wait_time_ms": 100,
        },
        {
            "wait_type": "PAGEIOLATCH_SH",
            "waiting_tasks_count": 50,
            "wait_time_ms": 12000,
            "signal_wait_time_ms": 200,
        },
        {
            "wait_type": "ASYNC_NETWORK_IO",
            "waiting_tasks_count": 100,
            "wait_time_ms": 5000,
            "signal_wait_time_ms": 50,
        },
    ]


@pytest.fixture
def sample_blocking_chain() -> list[dict[str, Any]]:
    """Sample blocking chain data."""
    return [
        {
            "blocker_session_id": 55,
            "blocked_session_id": 56,
            "wait_type": "LCK_M_X",
            "wait_time_ms": 15000,
            "blocking_sql": "UPDATE dbo.SimHotTable SET Counter = Counter + 1 WHERE Id = 1",
            "blocked_sql": "SELECT * FROM dbo.SimHotTable WHERE Id = 1",
        },
        {
            "blocker_session_id": 55,
            "blocked_session_id": 57,
            "wait_type": "LCK_M_S",
            "wait_time_ms": 14500,
            "blocking_sql": "UPDATE dbo.SimHotTable SET Counter = Counter + 1 WHERE Id = 1",
            "blocked_sql": "SELECT * FROM dbo.SimHotTable",
        },
    ]


@pytest.fixture
def sample_query_stats() -> list[dict[str, Any]]:
    """Sample query statistics data."""
    return [
        {
            "query_hash": "0xABC123",
            "execution_count": 1000,
            "total_elapsed_time_ms": 50000,
            "total_logical_reads": 500000,
            "avg_elapsed_time_ms": 50.0,
            "avg_logical_reads": 500.0,
            "sql_text": "SELECT * FROM dbo.SimOrders WHERE CustomerId = @p0",
        },
        {
            "query_hash": "0xDEF456",
            "execution_count": 500,
            "total_elapsed_time_ms": 25000,
            "total_logical_reads": 100000,
            "avg_elapsed_time_ms": 50.0,
            "avg_logical_reads": 200.0,
            "sql_text": "UPDATE dbo.SimHotTable SET Counter = Counter + 1",
        },
    ]


# =============================================================================
# RCA / LLM Fixtures
# =============================================================================

@pytest.fixture
def mock_llm_response():
    """Sample LLM response for RCA testing."""
    return {
        "root_cause": "blocking_chain",
        "confidence": 0.95,
        "summary": "Long-running transaction holding exclusive locks on SimHotTable",
        "evidence": [
            "5 sessions blocked on LCK_M_X waits",
            "Session 55 holding locks for 15+ seconds",
            "Blocking query: UPDATE dbo.SimHotTable",
        ],
        "recommendations": [
            "Reduce transaction scope",
            "Add NOLOCK hints for read queries",
            "Consider optimistic concurrency",
        ],
    }


@pytest.fixture
def mock_openai_client():
    """Create a mock OpenAI client."""
    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.choices = [
        MagicMock(
            message=MagicMock(
                content='{"root_cause": "blocking", "confidence": 0.9}',
                tool_calls=None,
            ),
            finish_reason="stop",
        )
    ]
    mock_response.usage = MagicMock(
        prompt_tokens=100,
        completion_tokens=50,
        total_tokens=150,
    )
    mock_client.chat.completions.create.return_value = mock_response
    return mock_client


@pytest.fixture
def mock_anthropic_client():
    """Create a mock Anthropic client."""
    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.content = [
        MagicMock(
            type="text",
            text='{"root_cause": "blocking", "confidence": 0.9}',
        )
    ]
    mock_response.stop_reason = "end_turn"
    mock_response.usage = MagicMock(
        input_tokens=100,
        output_tokens=50,
    )
    mock_client.messages.create.return_value = mock_response
    return mock_client


# =============================================================================
# Incident Fixtures
# =============================================================================

@pytest.fixture
def sample_incident_templates() -> list[dict[str, Any]]:
    """Sample incident templates data."""
    return [
        {
            "name": "blocking_chain",
            "description": "Simulate blocking chain with long-running transaction",
            "start_proc": "Incidents.usp_start_blocking_chain",
            "stop_proc": "Incidents.usp_stop_blocking_chain",
        },
        {
            "name": "missing_index",
            "description": "Scenario 1",
            "start_proc": "Incidents.usp_start_scenario_1",
            "stop_proc": "Incidents.usp_stop_scenario_1",
        },
        {
            "name": "parameter_sniffing",
            "description": "Scenario 2",
            "start_proc": "Incidents.usp_start_scenario_2",
            "stop_proc": "Incidents.usp_stop_scenario_2",
        },
    ]


# =============================================================================
# Ground Truth Fixtures
# =============================================================================

@pytest.fixture
def sample_ground_truth() -> dict[str, Any]:
    """Sample ground truth for critique testing."""
    return {
        "blocking_chain": {
            "expected_root_cause": "blocking",
            "expected_table": "SimHotTable",
            "expected_column": None,
            "severity": "high",
        },
        "missing_index": {
            "expected_root_cause": "missing_index",
            "expected_table": "SimOrders",
            "expected_column": "CustomerId",
            "severity": "medium",
        },
        "parameter_sniffing": {
            "expected_root_cause": "parameter_sniffing",
            "expected_table": "SimProducts",
            "expected_column": "CategoryId",
            "severity": "medium",
        },
    }


# =============================================================================
# Utility Fixtures
# =============================================================================

@pytest.fixture
def capture_logs(caplog: pytest.LogCaptureFixture) -> Generator[pytest.LogCaptureFixture, None, None]:
    """Capture log output for testing."""
    import logging
    caplog.set_level(logging.DEBUG)
    yield caplog


@pytest.fixture
def temp_output_dir(tmp_path: Path) -> Path:
    """Create a temporary output directory."""
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    return output_dir
