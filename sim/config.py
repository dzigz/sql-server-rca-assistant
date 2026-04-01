"""
Shared configuration helpers for SQL Server RCA Assistant.

This module mainly exists to centralize common defaults used by the package:
- password resolution guards
- optional local SQL Server sidecar defaults
- connection timeout settings

The web app and diagnostics flow target external SQL Server instances by
default. The container-related settings here are optional compatibility
settings for local development helpers.
"""

from pathlib import Path
from dataclasses import dataclass, field
from typing import Any, Optional
import os

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False

# Load .env file if python-dotenv is available
try:
    from dotenv import load_dotenv
    # Load from sim/.env first, then project root .env
    _sim_env = Path(__file__).parent / ".env"
    _root_env = Path(__file__).parent.parent / ".env"
    if _sim_env.exists():
        load_dotenv(_sim_env)
    elif _root_env.exists():
        load_dotenv(_root_env)
    DOTENV_AVAILABLE = True
except ImportError:
    DOTENV_AVAILABLE = False


# =============================================================================
# Base Paths
# =============================================================================

SIM_ROOT = Path(__file__).parent
ASSETS_DIR = SIM_ROOT / "assets"
SQL_DIR = SIM_ROOT / "sql"
DEFAULT_CONFIG_PATH = SIM_ROOT / "config.yaml"
USER_CONFIG_PATH = Path.home() / ".sim" / "config.yaml"

# =============================================================================
# Default Values (used when YAML not available or as fallbacks)
# =============================================================================

# Optional local SQL Server sidecar configuration
SQL_SERVER_IMAGE = "mcr.microsoft.com/mssql/server:2022-latest"
CONTAINER_NAME = "sim-sqlserver"
DEFAULT_HOST_PORT = 14333
CONTAINER_PORT = 1433

# SQL Server credentials
# Secure default: require explicit password unless insecure defaults are opted in.
DEFAULT_SA_PASSWORD = "__SIM_SA_PASSWORD_REQUIRED__"
INSECURE_DEFAULT_SA_PASSWORD = "YourSecurePassword123!"

# Resource limits for container
DEFAULT_CPU_LIMIT = "2.0"
DEFAULT_MEMORY_LIMIT = "4g"

# Connection settings
CONNECTION_TIMEOUT = 30
COMMAND_TIMEOUT = 300

# Retry settings for local SQL Server readiness
READINESS_RETRY_ATTEMPTS = 45
READINESS_RETRY_DELAY = 3  # seconds


def _load_yaml_file(path: Path) -> dict[str, Any]:
    """Load a YAML configuration file."""
    if not YAML_AVAILABLE:
        return {}
    if not path.exists():
        return {}
    try:
        with open(path, "r") as f:
            data = yaml.safe_load(f)
            return data if data else {}
    except Exception:
        return {}


def _get_nested(data: dict, *keys: str, default: Any = None) -> Any:
    """Get a nested value from a dict."""
    for key in keys:
        if not isinstance(data, dict):
            return default
        data = data.get(key, {})
    return data if data != {} else default


def _load_config_values() -> dict[str, Any]:
    """
    Load configuration values from all sources.

    Priority (highest to lowest):
    1. Environment variables
    2. User config file (~/.sim/config.yaml)
    3. Default config file (package config.yaml)
    4. Hardcoded defaults
    """
    # Start with empty config
    config: dict[str, Any] = {}

    # Load default config from package
    default_config = _load_yaml_file(DEFAULT_CONFIG_PATH)

    # Load user config (overrides defaults)
    user_config = _load_yaml_file(USER_CONFIG_PATH)

    # Merge configs (user overrides default)
    def merge_dicts(base: dict, override: dict) -> dict:
        result = base.copy()
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = merge_dicts(result[key], value)
            else:
                result[key] = value
        return result

    merged = merge_dicts(default_config, user_config)

    # Map YAML config to flat config dict
    config["sql_image"] = _get_nested(merged, "sqlserver", "image", default=SQL_SERVER_IMAGE)
    config["container_name"] = _get_nested(merged, "sqlserver", "container_name", default=CONTAINER_NAME)
    config["host_port"] = _get_nested(merged, "sqlserver", "host_port", default=DEFAULT_HOST_PORT)
    config["cpu_limit"] = _get_nested(merged, "sqlserver", "cpu_limit", default=DEFAULT_CPU_LIMIT)
    config["memory_limit"] = _get_nested(merged, "sqlserver", "memory_limit", default=DEFAULT_MEMORY_LIMIT)

    config["connection_timeout"] = _get_nested(merged, "connection", "timeout", default=CONNECTION_TIMEOUT)
    config["command_timeout"] = _get_nested(merged, "connection", "command_timeout", default=COMMAND_TIMEOUT)

    # Apply environment variable overrides
    env_mappings = {
        "SIM_SA_PASSWORD": ("sa_password", str),
        "SIM_HOST_PORT": ("host_port", int),
        "SIM_MEMORY_LIMIT": ("memory_limit", str),
        "SIM_CPU_LIMIT": ("cpu_limit", str),
        "SIM_CONTAINER_NAME": ("container_name", str),
        "SIM_SQL_IMAGE": ("sql_image", str),
        "SIM_CONNECTION_TIMEOUT": ("connection_timeout", int),
        "SIM_COMMAND_TIMEOUT": ("command_timeout", int),
    }

    for env_var, (config_key, type_fn) in env_mappings.items():
        value = os.environ.get(env_var)
        if value is not None:
            try:
                config[config_key] = type_fn(value)
            except (ValueError, TypeError):
                pass  # Keep existing value if conversion fails

    # SA password: prefer explicit env var, allow insecure fallback only when opted in.
    if "sa_password" not in config:
        explicit_password = os.environ.get("SIM_SA_PASSWORD") or os.environ.get("SA_PASSWORD")
        allow_insecure_defaults = os.environ.get("SIM_ALLOW_INSECURE_DEFAULTS", "").lower() in ("1", "true", "yes")
        if explicit_password:
            config["sa_password"] = explicit_password
        elif allow_insecure_defaults:
            config["sa_password"] = INSECURE_DEFAULT_SA_PASSWORD
        else:
            config["sa_password"] = DEFAULT_SA_PASSWORD

    return config


# Cache for loaded config values
_config_cache: Optional[dict[str, Any]] = None


def _get_config_values() -> dict[str, Any]:
    """Get cached config values, loading if necessary."""
    global _config_cache
    if _config_cache is None:
        _config_cache = _load_config_values()
    return _config_cache


def reload_config() -> None:
    """Reload configuration from files and environment."""
    global _config_cache
    _config_cache = None


@dataclass
class Config:
    """
    Shared runtime configuration.

    Attributes:
        container_name: Optional local SQL Server container name
        sql_image: Optional local SQL Server container image
        host_port: Optional local SQL Server host port
        cpu_limit: Optional local sidecar CPU limit
        memory_limit: Optional local sidecar memory limit
        sa_password: SQL Server SA password
        assets_dir: Repository assets directory
        sql_dir: Repository SQL directory
        connection_timeout: Timeout for database connections (seconds)
        command_timeout: Timeout for SQL commands (seconds)
    """

    # Container settings
    container_name: str = field(default_factory=lambda: _get_config_values().get("container_name", CONTAINER_NAME))
    sql_image: str = field(default_factory=lambda: _get_config_values().get("sql_image", SQL_SERVER_IMAGE))
    host_port: int = field(default_factory=lambda: _get_config_values().get("host_port", DEFAULT_HOST_PORT))
    cpu_limit: str = field(default_factory=lambda: _get_config_values().get("cpu_limit", DEFAULT_CPU_LIMIT))
    memory_limit: str = field(default_factory=lambda: _get_config_values().get("memory_limit", DEFAULT_MEMORY_LIMIT))

    # Credentials
    sa_password: str = field(default_factory=lambda: _get_config_values().get("sa_password", DEFAULT_SA_PASSWORD))

    # Paths
    assets_dir: Path = field(default_factory=lambda: ASSETS_DIR)
    sql_dir: Path = field(default_factory=lambda: SQL_DIR)

    # Connection
    connection_timeout: int = field(default_factory=lambda: _get_config_values().get("connection_timeout", CONNECTION_TIMEOUT))
    command_timeout: int = field(default_factory=lambda: _get_config_values().get("command_timeout", COMMAND_TIMEOUT))

    @property
    def connection_string(self) -> str:
        """ODBC connection string for a local SQL Server on the configured host port."""
        return (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER=localhost,{self.host_port};"
            f"UID=sa;"
            f"PWD={self.sa_password};"
            f"TrustServerCertificate=yes;"
            f"Connection Timeout={self.connection_timeout};"
        )

    def database_connection_string(self, database: str = "master") -> str:
        """ODBC connection string for a specific database on the local SQL Server."""
        return (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER=localhost,{self.host_port};"
            f"DATABASE={database};"
            f"UID=sa;"
            f"PWD={self.sa_password};"
            f"TrustServerCertificate=yes;"
            f"Connection Timeout={self.connection_timeout};"
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert config to dictionary for display/logging."""
        return {
            "container_name": self.container_name,
            "sql_image": self.sql_image,
            "host_port": self.host_port,
            "cpu_limit": self.cpu_limit,
            "memory_limit": self.memory_limit,
            "sa_password": "********",  # Don't expose password
            "assets_dir": str(self.assets_dir),
            "sql_dir": str(self.sql_dir),
            "connection_timeout": self.connection_timeout,
            "command_timeout": self.command_timeout,
        }


def get_config(**overrides: Any) -> Config:
    """
    Create a Config instance with optional overrides.

    Args:
        **overrides: Config field values to override

    Returns:
        Config instance with merged values

    Example:
        # Get config with defaults
        config = get_config()

        # Get config with custom local port
        config = get_config(host_port=14334)

        # Get config with multiple overrides
        config = get_config(
            host_port=14334,
            memory_limit="8g",
            sa_password="MySecurePass123!"
        )
    """
    return Config(**overrides)


def show_config() -> str:
    """
    Get a formatted string showing current configuration.

    Returns:
        Multi-line string with configuration values
    """
    config = get_config()
    lines = [
        "Current Configuration:",
        "=" * 40,
    ]
    for key, value in config.to_dict().items():
        lines.append(f"  {key}: {value}")

    lines.append("")
    lines.append("Config Sources:")
    lines.append(f"  Default: {DEFAULT_CONFIG_PATH}")
    lines.append(f"  User:    {USER_CONFIG_PATH} {'(exists)' if USER_CONFIG_PATH.exists() else '(not found)'}")
    lines.append(f"  YAML:    {'available' if YAML_AVAILABLE else 'not installed (pip install pyyaml)'}")
    lines.append(f"  dotenv:  {'available' if DOTENV_AVAILABLE else 'not installed (pip install python-dotenv)'}")

    return "\n".join(lines)


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Main interface
    "Config",
    "get_config",
    "reload_config",
    "show_config",
    # Constants (for backwards compatibility)
    "SIM_ROOT",
    "ASSETS_DIR",
    "SQL_DIR",
    "SQL_SERVER_IMAGE",
    "CONTAINER_NAME",
    "DEFAULT_HOST_PORT",
    "CONTAINER_PORT",
    "DEFAULT_SA_PASSWORD",
    "INSECURE_DEFAULT_SA_PASSWORD",
    "DEFAULT_CPU_LIMIT",
    "DEFAULT_MEMORY_LIMIT",
    "CONNECTION_TIMEOUT",
    "COMMAND_TIMEOUT",
    "READINESS_RETRY_ATTEMPTS",
    "READINESS_RETRY_DELAY",
]
