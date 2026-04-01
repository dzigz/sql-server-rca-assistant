"""
Tool registry factory.

Creates and configures the tool registry with ClickHouse-based tools,
optional code analysis tools (if claude-agent-sdk is available),
and optional on-demand Blitz diagnostic tools.
"""

from datetime import datetime
from typing import TYPE_CHECKING, Optional
from sim.rca.tools.base import ToolRegistry
from sim.rca.tools.clickhouse_tools import create_clickhouse_tools
from sim.rca.tools.code_analysis_tools import (
    create_code_analysis_tools,
    CLAUDE_AGENT_SDK_AVAILABLE,
)
from sim.logging_config import get_logger

if TYPE_CHECKING:
    from sim.rca.datasources import ClickHouseDataSource

logger = get_logger(__name__)


def create_clickhouse_tool_registry(
    data_source: "ClickHouseDataSource",
    incident_id: Optional[str] = None,
    include_code_analysis: bool = True,
    include_blitz_tools: bool = False,
    sqlserver_config: Optional[dict] = None,
) -> ToolRegistry:
    """
    Create a tool registry with ClickHouse-based tools.

    This is the primary way to create tools for the RCA engine,
    using ClickHouse as the data source.

    Args:
        data_source: ClickHouse data source instance
        incident_id: The incident ID to analyze (optional for time-based analysis)
        include_code_analysis: Whether to include code analysis tools
            (requires claude-agent-sdk to be installed)
        include_blitz_tools: Whether to include on-demand Blitz diagnostic tools
            (requires sqlserver_config to be provided)
        sqlserver_config: SQL Server connection config for Blitz tools:
            - sqlserver_host: SQL Server hostname
            - sqlserver_port: SQL Server port (default 1433)
            - sqlserver_user: SQL Server username (default "sa")
            - sqlserver_password: SQL Server password
            - sqlserver_database: Target database name (default "master")

    Returns:
        ToolRegistry with ClickHouse tools registered
    """
    registry = ToolRegistry()

    # Generate effective incident ID for time-based analysis
    effective_id = incident_id or f"adhoc_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # Register ClickHouse tools
    for tool in create_clickhouse_tools(data_source, effective_id):
        registry.register(tool)

    # Register on-demand Blitz diagnostic tools if requested
    if include_blitz_tools:
        if sqlserver_config:
            from sim.rca.tools.blitz_tools import create_blitz_diagnostic_tool
            blitz_tool = create_blitz_diagnostic_tool(
                sqlserver_host=sqlserver_config.get("sqlserver_host", ""),
                sqlserver_port=sqlserver_config.get("sqlserver_port", 1433),
                sqlserver_user=sqlserver_config.get("sqlserver_user", "sa"),
                sqlserver_password=sqlserver_config.get("sqlserver_password", ""),
                sqlserver_database=sqlserver_config.get("sqlserver_database", "master"),
                data_source=data_source,
            )
            registry.register(blitz_tool)
            logger.debug("Registered on-demand Blitz diagnostics tool")
        else:
            logger.warning(
                "Blitz tools requested but sqlserver_config not provided - skipping"
            )

    # Register code analysis tools if available and requested
    if include_code_analysis:
        if CLAUDE_AGENT_SDK_AVAILABLE:
            for tool in create_code_analysis_tools():
                registry.register(tool)
                logger.debug("Registered code analysis tool: %s", tool.name)
        else:
            logger.debug(
                "Code analysis tools not registered: claude-agent-sdk not installed"
            )

    return registry
