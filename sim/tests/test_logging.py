"""Tests for sim.logging_config module."""

import json
import logging
import pytest


class TestConfigureLogging:
    """Tests for configure_logging function."""

    def test_configure_logging_default(self):
        """Test default logging configuration."""
        from sim.logging_config import configure_logging, get_logger

        configure_logging()
        logger = get_logger("test")

        # Should not raise
        logger.info("Test message")

    def test_configure_logging_verbose(self):
        """Test verbose logging sets DEBUG level."""
        from sim.logging_config import configure_logging

        configure_logging(verbose=True)

        logger = logging.getLogger("sim")
        assert logger.level == logging.DEBUG

    def test_configure_logging_info_default(self):
        """Test default logging is INFO level."""
        from sim.logging_config import configure_logging

        configure_logging(verbose=False)

        logger = logging.getLogger("sim")
        assert logger.level == logging.INFO


class TestGetLogger:
    """Tests for get_logger function."""

    def test_get_logger_returns_adapter(self):
        """Test get_logger returns a SimLoggerAdapter."""
        from sim.logging_config import get_logger, SimLoggerAdapter

        logger = get_logger(__name__)

        assert isinstance(logger, SimLoggerAdapter)

    def test_get_logger_with_extra(self):
        """Test get_logger with extra context."""
        from sim.logging_config import get_logger

        logger = get_logger(__name__, component="test_component")

        assert logger.extra == {"component": "test_component"}


class TestCorrelationId:
    """Tests for correlation ID functionality."""

    def test_set_and_get_correlation_id(self):
        """Test setting and getting correlation ID."""
        from sim.logging_config import (
            set_correlation_id,
            get_correlation_id,
            clear_correlation_id,
        )

        # Initially None
        clear_correlation_id()
        assert get_correlation_id() is None

        # Set and get
        set_correlation_id("test-123")
        assert get_correlation_id() == "test-123"

        # Clear
        clear_correlation_id()
        assert get_correlation_id() is None

    def test_correlation_id_context_isolation(self):
        """Test that correlation IDs are context-isolated."""
        from sim.logging_config import (
            set_correlation_id,
            get_correlation_id,
            clear_correlation_id,
        )

        clear_correlation_id()
        set_correlation_id("main-context")

        # In the same context, should be the same
        assert get_correlation_id() == "main-context"

        clear_correlation_id()


class TestJSONFormatter:
    """Tests for JSONFormatter."""

    def test_json_formatter_output(self):
        """Test JSONFormatter produces valid JSON."""
        from sim.logging_config import JSONFormatter

        formatter = JSONFormatter()

        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=42,
            msg="Test message",
            args=(),
            exc_info=None,
        )

        output = formatter.format(record)

        # Should be valid JSON
        data = json.loads(output)

        assert data["level"] == "INFO"
        assert data["logger"] == "test.logger"
        assert data["message"] == "Test message"
        assert "timestamp" in data
        assert "location" in data
        assert data["location"]["line"] == 42

    def test_json_formatter_with_extra(self):
        """Test JSONFormatter includes extra fields."""
        from sim.logging_config import JSONFormatter

        formatter = JSONFormatter()

        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=42,
            msg="Test message",
            args=(),
            exc_info=None,
        )
        record.custom_field = "custom_value"

        output = formatter.format(record)
        data = json.loads(output)

        assert "extra" in data
        assert data["extra"]["custom_field"] == "custom_value"


class TestColoredFormatter:
    """Tests for ColoredFormatter."""

    def test_colored_formatter_with_color(self):
        """Test ColoredFormatter with colors enabled."""
        from sim.logging_config import ColoredFormatter

        formatter = ColoredFormatter(use_color=True)

        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=42,
            msg="Test message",
            args=(),
            exc_info=None,
        )

        output = formatter.format(record)

        # Should contain ANSI codes
        assert "\033[" in output
        assert "Test message" in output

    def test_colored_formatter_without_color(self):
        """Test ColoredFormatter without colors."""
        from sim.logging_config import ColoredFormatter

        formatter = ColoredFormatter(use_color=False)

        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=42,
            msg="Test message",
            args=(),
            exc_info=None,
        )

        output = formatter.format(record)

        # Should not contain ANSI codes
        assert "\033[" not in output
        assert "Test message" in output
        assert "INFO" in output


class TestLogException:
    """Tests for log_exception function."""

    def test_log_exception_basic(self, caplog):
        """Test log_exception with basic exception."""
        from sim.logging_config import log_exception, get_logger, configure_logging

        configure_logging(verbose=True)
        # Enable propagation so caplog can capture logs
        sim_logger = logging.getLogger("sim")
        sim_logger.propagate = True

        logger = get_logger(__name__)

        with caplog.at_level(logging.ERROR):
            try:
                raise ValueError("Test error")
            except Exception as e:
                log_exception(logger, "Operation failed", e)

        assert "Operation failed" in caplog.text

    def test_log_exception_with_sim_error(self, caplog):
        """Test log_exception with SimError."""
        from sim.logging_config import log_exception, get_logger, configure_logging
        from sim.exceptions import SimError

        configure_logging(verbose=True)
        # Enable propagation so caplog can capture logs
        sim_logger = logging.getLogger("sim")
        sim_logger.propagate = True

        logger = get_logger(__name__)

        with caplog.at_level(logging.ERROR):
            try:
                raise SimError(
                    "Custom error",
                    details={"key": "value"},
                    suggestion="Try this fix",
                )
            except Exception as e:
                log_exception(logger, "Sim operation failed", e)

        assert "Sim operation failed" in caplog.text
