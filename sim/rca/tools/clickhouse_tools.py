"""
ClickHouse-based tools for RCA agent.

These tools allow the agent to query ClickHouse for additional
evidence during incident investigation.
"""

from typing import Any, Dict, List, Optional

from sim.rca.tools.base import RCATool, ToolResult
from sim.rca.datasources import ClickHouseDataSource


class CompareBaselineTool(RCATool):
    """Compare baseline vs incident metrics."""

    name = "compare_baseline"
    description = """Compare metrics between baseline and incident periods.

Returns deltas for:
- Wait statistics (which waits increased)
- Blocking comparison (blocking events before/during)
- Memory grants (waiting, spilled, ok counts)
- Top new waits that appeared during incident

Use this tool FIRST to understand what changed from normal operation.

Optional parameters allow comparing specific time ranges (useful for ad-hoc analysis):
- recent_minutes: How many minutes of recent data to analyze (default: 10)
- baseline_minutes: How many minutes of baseline data to compare against (default: 30)"""

    def __init__(self, data_source: ClickHouseDataSource, incident_id: str):
        self.data_source = data_source
        self.incident_id = incident_id

    @property
    def parameters_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "recent_minutes": {
                    "type": "integer",
                    "description": "Minutes of recent data to analyze (default: 10)",
                    "default": 10,
                },
                "baseline_minutes": {
                    "type": "integer",
                    "description": "Minutes of baseline data before recent period (default: 30)",
                    "default": 30,
                },
            },
            "required": [],
        }

    def execute(self, recent_minutes: int = 10, baseline_minutes: int = 30, **kwargs) -> ToolResult:
        """Execute baseline comparison."""
        from datetime import datetime, timedelta, timezone

        try:
            # First try incident-based comparison if we have a real incident
            incident_window, baseline_window = self.data_source.get_incident_window(self.incident_id)
            if incident_window and baseline_window:
                result = self.data_source.compare_baseline_incident(self.incident_id)
                return ToolResult(success=True, data=result)
        except Exception:
            pass  # Fall through to time-based comparison

        # Fall back to time-based comparison (for web app / ad-hoc analysis)
        try:
            now = datetime.now(timezone.utc)
            incident_start = now - timedelta(minutes=recent_minutes)
            incident_end = now
            baseline_start = incident_start - timedelta(minutes=baseline_minutes)
            baseline_end = incident_start

            result = self.data_source.compare_time_ranges(
                incident_start=incident_start,
                incident_end=incident_end,
                baseline_start=baseline_start,
                baseline_end=baseline_end,
            )
            return ToolResult(success=True, data=result)
        except Exception as e:
            return ToolResult(success=False, error=str(e))


class QueryClickHouseTool(RCATool):
    """Query any ClickHouse table with filters."""

    name = "query_clickhouse"
    description = """Query ClickHouse metrics tables directly.

Available tables:
- wait_stats: Wait type statistics (wait_type, wait_time_ms, waiting_tasks_count)
- blocking_chains: Blocking relationships (session_id, blocking_session_id, wait_type, sql_text)
- memory_grants: Memory grant status (grant_status, requested_mb, granted_mb, max_used_mb)
- query_stats: Query performance (query_hash, total_worker_time_us, total_elapsed_time_us, total_logical_reads, execution_count)
- schedulers: CPU scheduler health (scheduler_id, runnable_tasks_count)
- file_stats: I/O statistics (database_name, io_stall_read_ms, io_stall_write_ms)
- missing_indexes: Index recommendations (table_name, equality_columns, impact_score)

Use order_by to sort results (e.g., "total_worker_time_us DESC" for CPU, "wait_time_ms DESC" for waits)."""

    def __init__(self, data_source: ClickHouseDataSource, incident_id: str):
        self.data_source = data_source
        self.incident_id = incident_id

    @property
    def parameters_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "table": {
                    "type": "string",
                    "description": "Table name to query",
                    "enum": [
                        "wait_stats",
                        "blocking_chains",
                        "memory_grants",
                        "query_stats",
                        "schedulers",
                        "file_stats",
                        "missing_indexes",
                    ],
                },
                "filters": {
                    "type": "object",
                    "description": "Column filters as key-value pairs",
                },
                "order_by": {
                    "type": "string",
                    "description": "ORDER BY clause (e.g., 'wait_time_ms DESC')",
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum rows to return (default 20)",
                    "default": 20,
                },
            },
            "required": ["table"],
        }

    def execute(
        self,
        table: str,
        filters: Optional[Dict[str, Any]] = None,
        order_by: Optional[str] = None,
        limit: int = 20,
        **kwargs,
    ) -> ToolResult:
        """Execute ClickHouse query."""
        try:
            result = self.data_source.query_table(
                table=table,
                incident_id=self.incident_id,
                filters=filters,
                order_by=order_by,
                limit=limit,
            )
            return ToolResult(success=True, data=result)
        except Exception as e:
            return ToolResult(success=False, error=str(e))


class GetQueryDetailsTool(RCATool):
    """Get detailed information about a specific query."""

    name = "get_query_details"
    description = """Get full details for a specific query by query_hash.

Returns execution stats, wait breakdown, and plan info if available."""

    def __init__(self, data_source: ClickHouseDataSource, incident_id: str):
        self.data_source = data_source
        self.incident_id = incident_id

    @property
    def parameters_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "query_hash": {
                    "type": "string",
                    "description": "Query hash to look up",
                },
            },
            "required": ["query_hash"],
        }

    def execute(self, query_hash: str, **kwargs) -> ToolResult:
        """Get query details."""
        try:
            result = self.data_source.query_table(
                table="query_stats",
                incident_id=self.incident_id,
                filters={"query_hash": query_hash},
                limit=10,
            )
            if result:
                return ToolResult(success=True, data=result[0])
            else:
                return ToolResult(
                    success=False,
                    error=f"No query found with hash: {query_hash}",
                )
        except Exception as e:
            return ToolResult(success=False, error=str(e))


def create_clickhouse_tools(
    data_source: ClickHouseDataSource,
    incident_id: str,
) -> List[RCATool]:
    """Create all ClickHouse tools for an incident."""
    return [
        CompareBaselineTool(data_source, incident_id),
        QueryClickHouseTool(data_source, incident_id),
        GetQueryDetailsTool(data_source, incident_id),
    ]
