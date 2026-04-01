"""Configuration for DMV Collector."""

from pydantic_settings import BaseSettings


class CollectorConfig(BaseSettings):
    """Configuration loaded from environment variables."""

    # SQL Server connection
    sqlserver_host: str = "sqlserver"
    sqlserver_port: int = 1433
    sqlserver_user: str = "sa"
    sqlserver_password: str = ""
    sqlserver_database: str = "master"

    # ClickHouse connection
    clickhouse_host: str = "clickhouse"
    clickhouse_port: int = 8123
    clickhouse_database: str = "rca_metrics"
    clickhouse_user: str = "default"
    clickhouse_password: str = ""

    # Collection settings
    collection_interval: int = 5  # seconds
    query_timeout: int = 30  # seconds

    class Config:
        env_prefix = ""
        case_sensitive = False


def get_config() -> CollectorConfig:
    """Get the collector configuration."""
    return CollectorConfig()
