"""Tests for sim.config module."""

import os
import pytest
from pathlib import Path


class TestConfig:
    """Tests for the Config dataclass."""

    def test_config_defaults(self):
        """Test that Config has sensible defaults."""
        from sim.config import Config, CONTAINER_NAME, DEFAULT_HOST_PORT

        config = Config()

        assert config.container_name == CONTAINER_NAME
        assert config.host_port == DEFAULT_HOST_PORT
        assert config.cpu_limit == "2.0"
        assert config.memory_limit == "4g"
        assert config.connection_timeout == 30
        assert config.command_timeout == 300

    def test_config_override(self):
        """Test that Config accepts overrides."""
        from sim.config import Config

        config = Config(
            container_name="custom-container",
            host_port=15000,
            sa_password="CustomPass123!",
        )

        assert config.container_name == "custom-container"
        assert config.host_port == 15000
        assert config.sa_password == "CustomPass123!"

    def test_config_connection_string(self):
        """Test connection string generation."""
        from sim.config import Config

        config = Config(
            host_port=14333,
            sa_password="TestPass123!",
            connection_timeout=60,
        )

        conn_str = config.connection_string
        assert "SERVER=localhost,14333" in conn_str
        assert "PWD=TestPass123!" in conn_str
        assert "Connection Timeout=60" in conn_str
        assert "TrustServerCertificate=yes" in conn_str

    def test_config_database_connection_string(self):
        """Test per-database connection string includes the requested database."""
        from sim.config import Config

        config = Config(host_port=14333, sa_password="TestPass123!")

        conn_str = config.database_connection_string("tempdb")
        assert "DATABASE=tempdb" in conn_str
        assert "SERVER=localhost,14333" in conn_str

    def test_config_to_dict(self):
        """Test config serialization to dict."""
        from sim.config import Config

        config = Config(
            container_name="test-container",
            sa_password="SecretPass123!",
        )

        config_dict = config.to_dict()

        assert config_dict["container_name"] == "test-container"
        assert config_dict["sa_password"] == "********"  # Password should be masked
        assert "assets_dir" in config_dict
        assert "sql_dir" in config_dict


class TestGetConfig:
    """Tests for get_config function."""

    def test_get_config_no_args(self):
        """Test get_config with no arguments."""
        from sim.config import get_config, Config

        config = get_config()

        assert isinstance(config, Config)

    def test_get_config_with_overrides(self):
        """Test get_config with keyword overrides."""
        from sim.config import get_config

        config = get_config(
            host_port=15000,
            memory_limit="8g",
        )

        assert config.host_port == 15000
        assert config.memory_limit == "8g"


class TestEnvironmentOverrides:
    """Tests for environment variable overrides."""

    def test_env_override_sa_password(self, monkeypatch):
        """Test SIM_SA_PASSWORD environment variable."""
        from sim.config import reload_config, get_config

        monkeypatch.setenv("SIM_SA_PASSWORD", "EnvPassword123!")
        reload_config()

        config = get_config()
        assert config.sa_password == "EnvPassword123!"

    def test_env_override_host_port(self, monkeypatch):
        """Test SIM_HOST_PORT environment variable."""
        from sim.config import reload_config, get_config

        monkeypatch.setenv("SIM_HOST_PORT", "15555")
        reload_config()

        config = get_config()
        assert config.host_port == 15555

    def test_env_override_invalid_int(self, monkeypatch):
        """Test that invalid int values are ignored."""
        from sim.config import reload_config, get_config, DEFAULT_HOST_PORT

        monkeypatch.setenv("SIM_HOST_PORT", "not_a_number")
        reload_config()

        config = get_config()
        # Should fall back to default when conversion fails
        assert isinstance(config.host_port, int)


class TestShowConfig:
    """Tests for show_config function."""

    def test_show_config_output(self):
        """Test show_config returns formatted string."""
        from sim.config import show_config

        output = show_config()

        assert "Current Configuration:" in output
        assert "container_name:" in output
        assert "host_port:" in output
        assert "sa_password: ********" in output  # Password should be masked
        assert "Config Sources:" in output


class TestConfigConstants:
    """Tests for configuration constants."""

    def test_paths_exist(self):
        """Test that path constants are valid."""
        from sim.config import SIM_ROOT, ASSETS_DIR, SQL_DIR

        assert SIM_ROOT.exists()
        assert SIM_ROOT.is_dir()
        # ASSETS_DIR and SQL_DIR may not exist initially

    def test_constants_are_correct_types(self):
        """Test that constants have correct types."""
        from sim.config import (
            DEFAULT_HOST_PORT,
            CONNECTION_TIMEOUT,
            COMMAND_TIMEOUT,
            READINESS_RETRY_ATTEMPTS,
            READINESS_RETRY_DELAY,
        )

        assert isinstance(DEFAULT_HOST_PORT, int)
        assert isinstance(CONNECTION_TIMEOUT, int)
        assert isinstance(COMMAND_TIMEOUT, int)
        assert isinstance(READINESS_RETRY_ATTEMPTS, int)
        assert isinstance(READINESS_RETRY_DELAY, int)

    def test_url_constant(self):
        """Test default SQL Server image points at Microsoft SQL Server."""
        from sim.config import SQL_SERVER_IMAGE

        assert SQL_SERVER_IMAGE.startswith("mcr.microsoft.com/")
        assert "mssql" in SQL_SERVER_IMAGE
