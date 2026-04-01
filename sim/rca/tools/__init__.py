"""
RCA Tooling Layer.

Provides read-only, safe tools for the AI agent to retrieve
additional evidence during investigation. All tools:

- Support SQL Server diagnostics in stage 1
- Return deterministic, structured results
- Operate in read-only mode
- Have built-in safety limits (timeouts, row limits)

Available Tools (ClickHouse-based):
- compare_baseline: Compare baseline vs incident metrics
- query_clickhouse: Query ClickHouse tables directly
- get_query_details: Get details for a specific query

Blitz Diagnostic Tools (require SQL Server connection):
- run_blitz_diagnostics: Run First Responder Kit diagnostics on-demand

Code Analysis Tools (require repo_path):
- analyze_code_impact: Find code paths affected by slow queries
- correlate_incident: Correlate incidents with recent changes
- find_query_origin: Find where a query originates in code
- analyze_orm_patterns: Detect ORM anti-patterns
"""

from sim.rca.tools.base import (
    RCATool,
    ToolRegistry,
    ToolResult,
    DatabaseContext,
    create_db_context_from_connection_string,
)
from sim.rca.tools.registry import create_clickhouse_tool_registry
from sim.rca.tools.clickhouse_tools import create_clickhouse_tools
from sim.rca.tools.code_analysis_tools import (
    AnalyzeCodeImpactTool,
    CorrelateIncidentTool,
    FindQueryOriginTool,
    AnalyzeORMPatternsTool,
    create_code_analysis_tools,
    CLAUDE_AGENT_SDK_AVAILABLE,
)

__all__ = [
    "RCATool",
    "ToolRegistry",
    "ToolResult",
    "DatabaseContext",
    "create_db_context_from_connection_string",
    "create_clickhouse_tool_registry",
    "create_clickhouse_tools",
    # Code analysis tools
    "AnalyzeCodeImpactTool",
    "CorrelateIncidentTool",
    "FindQueryOriginTool",
    "AnalyzeORMPatternsTool",
    "create_code_analysis_tools",
    "CLAUDE_AGENT_SDK_AVAILABLE",
]
