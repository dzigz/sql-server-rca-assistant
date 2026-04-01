"""
Logging configuration for the sim package.

This module provides structured logging with support for:
- Console output with optional color (via Rich)
- JSON-formatted logs for machine parsing
- Configurable verbosity levels
- Correlation IDs for request tracing

Usage:
    from sim.logging_config import configure_logging, get_logger

    # Configure logging at startup
    configure_logging(verbose=True, json_format=False)

    # Get a logger for your module
    logger = get_logger(__name__)

    # Log with structured data
    logger.info("Processing started", extra={"request_id": "123", "user": "admin"})
"""

from __future__ import annotations

import json
import logging
import sys
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any, Dict, MutableMapping, Optional, Tuple

# Context variable for correlation ID (thread-safe)
_correlation_id: ContextVar[Optional[str]] = ContextVar("correlation_id", default=None)


def set_correlation_id(correlation_id: str) -> None:
    """Set the correlation ID for the current context."""
    _correlation_id.set(correlation_id)


def get_correlation_id() -> Optional[str]:
    """Get the correlation ID for the current context."""
    return _correlation_id.get()


def clear_correlation_id() -> None:
    """Clear the correlation ID for the current context."""
    _correlation_id.set(None)


class JSONFormatter(logging.Formatter):
    """
    JSON log formatter for structured logging.

    Produces JSON-formatted log entries suitable for log aggregation
    systems like ELK, Splunk, or CloudWatch.
    """

    def format(self, record: logging.LogRecord) -> str:
        """Format a log record as JSON."""
        log_entry: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Add correlation ID if present
        correlation_id = get_correlation_id()
        if correlation_id:
            log_entry["correlation_id"] = correlation_id

        # Add location info
        log_entry["location"] = {
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add any extra fields from the record
        if hasattr(record, "__dict__"):
            extra_fields = {
                k: v
                for k, v in record.__dict__.items()
                if k
                not in {
                    "name",
                    "msg",
                    "args",
                    "created",
                    "filename",
                    "funcName",
                    "levelname",
                    "levelno",
                    "lineno",
                    "module",
                    "msecs",
                    "pathname",
                    "process",
                    "processName",
                    "relativeCreated",
                    "stack_info",
                    "exc_info",
                    "exc_text",
                    "thread",
                    "threadName",
                    "message",
                    "taskName",
                }
                and not k.startswith("_")
            }
            if extra_fields:
                log_entry["extra"] = extra_fields

        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry, default=str)


class ColoredFormatter(logging.Formatter):
    """
    Colored console formatter using ANSI codes.

    Provides colored output without requiring Rich for simple console logging.
    """

    # ANSI color codes
    COLORS = {
        "DEBUG": "\033[36m",  # Cyan
        "INFO": "\033[32m",  # Green
        "WARNING": "\033[33m",  # Yellow
        "ERROR": "\033[31m",  # Red
        "CRITICAL": "\033[35m",  # Magenta
    }
    RESET = "\033[0m"
    DIM = "\033[2m"

    def __init__(self, use_color: bool = True) -> None:
        super().__init__()
        self.use_color = use_color

    def format(self, record: logging.LogRecord) -> str:
        """Format a log record with optional colors."""
        timestamp = datetime.now().strftime("%H:%M:%S")

        if self.use_color:
            level_color = self.COLORS.get(record.levelname, "")
            level_str = f"{level_color}{record.levelname:<8}{self.RESET}"
            time_str = f"{self.DIM}{timestamp}{self.RESET}"
            name_str = f"{self.DIM}{record.name}{self.RESET}"
        else:
            level_str = f"{record.levelname:<8}"
            time_str = timestamp
            name_str = record.name

        message = record.getMessage()

        # Build the log line
        parts = [time_str, level_str, name_str, message]
        log_line = " | ".join(parts)

        # Add correlation ID if present
        correlation_id = get_correlation_id()
        if correlation_id:
            if self.use_color:
                log_line = f"{self.DIM}[{correlation_id}]{self.RESET} {log_line}"
            else:
                log_line = f"[{correlation_id}] {log_line}"

        # Add exception info if present
        if record.exc_info:
            log_line += "\n" + self.formatException(record.exc_info)

        return log_line


class SimLoggerAdapter(logging.LoggerAdapter):
    """
    Logger adapter that automatically includes correlation ID.

    Provides additional context and convenience methods for logging.
    """

    def process(
        self, msg: str, kwargs: MutableMapping[str, Any]
    ) -> Tuple[str, MutableMapping[str, Any]]:
        """Process log message, adding correlation ID if present."""
        extra = kwargs.get("extra", {})

        # Add correlation ID
        correlation_id = get_correlation_id()
        if correlation_id and "correlation_id" not in extra:
            extra["correlation_id"] = correlation_id

        # Add any adapter-level extra data
        if self.extra:
            extra.update(self.extra)

        kwargs["extra"] = extra
        return msg, kwargs


def get_logger(name: str, **extra: Any) -> SimLoggerAdapter:
    """
    Get a logger for the given module name.

    Args:
        name: Logger name (typically __name__)
        **extra: Extra context to include in all log messages

    Returns:
        Logger adapter with sim-specific functionality

    Example:
        logger = get_logger(__name__, component="workload_runner")
        logger.info("Starting workload", extra={"workers": 10})
    """
    base_logger = logging.getLogger(name)
    return SimLoggerAdapter(base_logger, extra)


def configure_logging(
    verbose: bool = False,
    json_format: bool = False,
    log_file: Optional[str] = None,
    use_color: bool = True,
) -> None:
    """
    Configure logging for the sim package.

    Args:
        verbose: If True, set log level to DEBUG; otherwise INFO
        json_format: If True, output logs in JSON format
        log_file: Optional file path to write logs to
        use_color: If True and not using JSON, colorize console output

    Example:
        # Development mode with verbose output
        configure_logging(verbose=True)

        # Production mode with JSON logs
        configure_logging(json_format=True, log_file="/var/log/sim.json")
    """
    # Determine log level
    level = logging.DEBUG if verbose else logging.INFO

    # Get root logger for sim package
    root_logger = logging.getLogger("sim")
    root_logger.setLevel(level)

    # Remove existing handlers
    root_logger.handlers.clear()

    # Create console handler
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(level)

    if json_format:
        console_handler.setFormatter(JSONFormatter())
    else:
        console_handler.setFormatter(ColoredFormatter(use_color=use_color))

    root_logger.addHandler(console_handler)

    # Add file handler if specified
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(level)
        # Always use JSON for file output
        file_handler.setFormatter(JSONFormatter())
        root_logger.addHandler(file_handler)

    # Prevent propagation to root logger
    root_logger.propagate = False

    # Also configure third-party loggers to be less noisy
    for name in ["httpx", "httpcore", "urllib3", "openai", "anthropic"]:
        third_party = logging.getLogger(name)
        third_party.setLevel(logging.WARNING)


def log_exception(
    logger: logging.Logger | SimLoggerAdapter,
    message: str,
    exc: Exception,
    **extra: Any,
) -> None:
    """
    Log an exception with full context.

    Args:
        logger: Logger instance to use
        message: Message describing what failed
        exc: The exception that occurred
        **extra: Additional context to include

    Example:
        try:
            do_something()
        except Exception as e:
            log_exception(logger, "Operation failed", e, operation="do_something")
    """
    extra["exception_type"] = type(exc).__name__
    extra["exception_message"] = str(exc)

    # Include sim-specific exception attributes
    if hasattr(exc, "details") and exc.details:
        extra["error_details"] = exc.details
    if hasattr(exc, "suggestion") and exc.suggestion:
        extra["error_suggestion"] = exc.suggestion

    logger.error(message, extra=extra, exc_info=True)


__all__ = [
    "configure_logging",
    "get_logger",
    "set_correlation_id",
    "get_correlation_id",
    "clear_correlation_id",
    "log_exception",
    "JSONFormatter",
    "ColoredFormatter",
    "SimLoggerAdapter",
]
