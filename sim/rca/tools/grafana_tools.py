"""
Grafana tools for the RCA agent.

Provides tools for:
- Embedding existing charts from pre-built dashboards
- Creating new charts from natural language prompts
"""

import os
import json
import httpx
from typing import Optional

from sim.rca.tools.base import RCATool, ToolResult, register_tool
from sim.rca.config import RCAConfig
from sim.rca.llm.factory import create_llm_client
from sim.logging_config import get_logger

logger = get_logger(__name__)

# Configuration
GRAFANA_URL = os.getenv("GRAFANA_URL", "http://localhost:3001")
GRAFANA_USER = os.getenv("GRAFANA_USER", "admin")
GRAFANA_PASSWORD = os.getenv("GRAFANA_PASSWORD", "admin123!")

# Pre-built dashboards and panels
AVAILABLE_CHARTS = {
    "wait_time_trend": {
        "dashboard_uid": "sql-server-overview",
        "panel_id": 1,
        "title": "Wait Time by Type",
        "description": "Time series showing wait time trends by wait type"
    },
    "blocked_sessions": {
        "dashboard_uid": "sql-server-overview",
        "panel_id": 2,
        "title": "Blocked Sessions",
        "description": "Current count of blocked sessions"
    },
    "active_requests": {
        "dashboard_uid": "sql-server-overview",
        "panel_id": 3,
        "title": "Active Requests",
        "description": "Current count of active requests"
    },
    "memory_grants": {
        "dashboard_uid": "sql-server-overview",
        "panel_id": 4,
        "title": "Memory Grant Status",
        "description": "Distribution of memory grant statuses"
    },
    "top_waits": {
        "dashboard_uid": "sql-server-overview",
        "panel_id": 5,
        "title": "Top 10 Wait Types",
        "description": "Bar chart of top wait types by total wait time"
    },
    "wait_stats_trend": {
        "dashboard_uid": "wait-stats",
        "panel_id": 1,
        "title": "Wait Time Trend by Type",
        "description": "Detailed wait statistics over time with stacking"
    },
    "wait_by_category": {
        "dashboard_uid": "wait-stats",
        "panel_id": 2,
        "title": "Wait Time by Category",
        "description": "Wait times grouped by category (Lock, I/O, Memory, etc.)"
    },
    "top_queries_cpu": {
        "dashboard_uid": "query-performance",
        "panel_id": 1,
        "title": "Top 10 Queries by CPU Time",
        "description": "Bar chart of queries consuming most CPU"
    },
    "top_queries_reads": {
        "dashboard_uid": "query-performance",
        "panel_id": 2,
        "title": "Top 10 Queries by Logical Reads",
        "description": "Bar chart of queries with most logical reads"
    },
    "query_details": {
        "dashboard_uid": "query-performance",
        "panel_id": 3,
        "title": "Query Details Table",
        "description": "Detailed table of query statistics"
    },
}

# ClickHouse schema for SQL generation
CLICKHOUSE_SCHEMA = """
Available tables in rca_metrics database:

1. wait_stats - SQL Server wait statistics
   - collected_at: DateTime64(3)
   - wait_type: String
   - waiting_tasks_count: UInt64
   - wait_time_ms: UInt64
   - max_wait_time_ms: UInt64
   - signal_wait_time_ms: UInt64

2. query_stats - Query execution statistics
   - collected_at: DateTime64(3)
   - query_hash: String
   - execution_count: UInt64
   - total_worker_time_us: UInt64 (CPU time)
   - total_elapsed_time_us: UInt64
   - total_logical_reads: UInt64
   - sql_text: String

3. blocking_chains - Blocking session information
   - collected_at: DateTime64(3)
   - blocking_level: UInt8
   - session_id: UInt32
   - blocking_session_id: UInt32
   - wait_type: String
   - wait_time_ms: UInt64
   - sql_text: String

4. memory_grants - Memory grant information
   - collected_at: DateTime64(3)
   - session_id: UInt32
   - requested_memory_mb: Float64
   - granted_memory_mb: Float64
   - used_memory_mb: Float64
   - grant_status: String ('OK', 'WAITING', 'SPILL_LIKELY', 'SPILLED')

5. file_stats - File I/O statistics
   - collected_at: DateTime64(3)
   - database_name: String
   - file_name: String
   - file_type: String
   - io_stall_read_ms: UInt64
   - io_stall_write_ms: UInt64

Time filtering: Use $__fromTime and $__toTime for Grafana time range.
For timeseries, alias time column as 'time' using toStartOfMinute(collected_at).
"""


@register_tool(name="embed_chart", category="visualization")
class EmbedChartTool(RCATool):
    """Tool to embed an existing chart in the response."""

    @property
    def name(self) -> str:
        return "embed_chart"

    @property
    def description(self) -> str:
        return """Embed a pre-built Grafana chart in your response to visualize data.

Available charts:
- wait_time_trend: Wait time trends by type (timeseries)
- blocked_sessions: Count of blocked sessions (stat)
- active_requests: Count of active requests (stat)
- memory_grants: Memory grant status distribution (pie)
- top_waits: Top 10 wait types (bar chart)
- wait_stats_trend: Detailed wait statistics (stacked timeseries)
- wait_by_category: Waits grouped by category (bar chart)
- top_queries_cpu: Top queries by CPU time (bar chart)
- top_queries_reads: Top queries by logical reads (bar chart)
- query_details: Query statistics table

Use this when you want to show the user a visual representation of the data you're discussing."""

    @property
    def parameters_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "chart_name": {
                    "type": "string",
                    "description": "Name of the chart to embed",
                    "enum": list(AVAILABLE_CHARTS.keys())
                },
                "time_range": {
                    "type": "string",
                    "description": "Time range for the chart",
                    "enum": ["15m", "1h", "6h", "24h", "7d"],
                    "default": "1h"
                }
            },
            "required": ["chart_name"]
        }

    def execute(self, chart_name: str, time_range: str = "1h") -> ToolResult:
        """Execute the embed_chart tool."""
        if chart_name not in AVAILABLE_CHARTS:
            return ToolResult.fail(
                f"Unknown chart: {chart_name}. Available: {list(AVAILABLE_CHARTS.keys())}"
            )

        chart = AVAILABLE_CHARTS[chart_name]
        time_from = f"now-{time_range}"

        embed_url = (
            f"{GRAFANA_URL}/d-solo/{chart['dashboard_uid']}/{chart['dashboard_uid']}"
            f"?orgId=1&panelId={chart['panel_id']}&from={time_from}&to=now&theme=dark"
        )

        return ToolResult.ok({
            "type": "chart_embed",
            "chart_name": chart_name,
            "title": chart["title"],
            "description": chart["description"],
            "embed_url": embed_url,
            "dashboard_uid": chart["dashboard_uid"],
            "panel_id": chart["panel_id"],
            "time_range": time_range,
        })


@register_tool(name="create_chart", category="visualization")
class CreateChartTool(RCATool):
    """Tool to create a new chart from a natural language prompt."""

    @property
    def name(self) -> str:
        return "create_chart"

    @property
    def description(self) -> str:
        return """Create a custom Grafana chart from a natural language description.

Use this when the pre-built charts don't show exactly what you need, or when you want
to create a specific visualization based on the user's question.

Examples:
- "Show wait times grouped by category for the last hour"
- "Display blocking session count over time"
- "Create a pie chart of memory grant statuses"
- "Top 5 queries by elapsed time"

The tool will generate appropriate ClickHouse SQL and create a Grafana panel."""

    @property
    def parameters_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "prompt": {
                    "type": "string",
                    "description": "Natural language description of the chart to create"
                },
                "chart_type": {
                    "type": "string",
                    "description": "Type of chart to create",
                    "enum": ["timeseries", "barchart", "piechart", "table"],
                    "default": "timeseries"
                },
                "time_range": {
                    "type": "string",
                    "description": "Time range for the chart",
                    "enum": ["15m", "1h", "6h", "24h", "7d"],
                    "default": "1h"
                }
            },
            "required": ["prompt"]
        }

    def execute(
        self,
        prompt: str,
        chart_type: str = "timeseries",
        time_range: str = "1h"
    ) -> ToolResult:
        """Execute the create_chart tool."""
        try:
            # Generate SQL from prompt
            sql_query = self._generate_sql(prompt, chart_type)

            # Validate SQL
            is_valid, error = self._validate_sql(sql_query)
            if not is_valid:
                return ToolResult.fail(f"Generated SQL failed validation: {error}")

            # Create panel in Grafana
            result = self._create_grafana_panel(sql_query, prompt, chart_type)

            return ToolResult.ok({
                "type": "chart_embed",
                "title": prompt[:50] + ("..." if len(prompt) > 50 else ""),
                "embed_url": result["embed_url"],
                "sql_query": sql_query,
                "time_range": time_range,
                "panel_id": result["panel_id"],
                "dashboard_uid": result["dashboard_uid"],
            })

        except Exception as e:
            logger.exception("Failed to create chart")
            return ToolResult.fail(f"Failed to create chart: {str(e)}")

    def _generate_sql(self, prompt: str, chart_type: str) -> str:
        """Generate SQL from natural language prompt."""
        config = RCAConfig()
        llm = create_llm_client(config)

        full_prompt = f"""You are a SQL expert for ClickHouse databases. Generate a valid ClickHouse SQL query.

{CLICKHOUSE_SCHEMA}

Rules:
1. Only SELECT queries (no INSERT, UPDATE, DELETE, DROP)
2. Include time filtering: WHERE collected_at >= $__fromTime AND collected_at <= $__toTime
3. For timeseries: include toStartOfMinute(collected_at) as time
4. Use aggregations (sum, avg, max, count) appropriately
5. Limit to 100 rows max for tables
6. Return ONLY the SQL query

User request: {prompt}
Chart type: {chart_type}

SQL query:"""

        response = llm.chat(
            messages=[{"role": "user", "content": full_prompt}],
            max_tokens=1000,
        )

        sql = (response.content or "").strip()

        # Clean up markdown
        if sql.startswith("```sql"):
            sql = sql[6:]
        if sql.startswith("```"):
            sql = sql[3:]
        if sql.endswith("```"):
            sql = sql[:-3]

        return sql.strip()

    def _validate_sql(self, sql: str) -> tuple[bool, str]:
        """Validate SQL is safe to execute."""
        sql_upper = sql.upper().strip()

        dangerous = ["INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE"]
        for keyword in dangerous:
            if keyword in sql_upper.split():
                return False, f"Forbidden keyword: {keyword}"

        if not (sql_upper.startswith("SELECT") or sql_upper.startswith("WITH")):
            return False, "Must start with SELECT or WITH"

        return True, ""

    def _create_grafana_panel(
        self,
        sql_query: str,
        title: str,
        chart_type: str
    ) -> dict:
        """Create a Grafana panel via API."""
        dashboard_uid = "generated-charts"

        # Get or create dashboard
        try:
            response = httpx.get(
                f"{GRAFANA_URL}/api/dashboards/uid/{dashboard_uid}",
                auth=(GRAFANA_USER, GRAFANA_PASSWORD),
                timeout=10.0,
            )

            if response.status_code == 200:
                data = response.json()
                dashboard = data["dashboard"]
                version = dashboard.get("version", 1)
            else:
                dashboard = {
                    "uid": dashboard_uid,
                    "title": "Generated Charts",
                    "panels": [],
                    "schemaVersion": 39,
                    "tags": ["generated"],
                }
                version = None
        except Exception:
            dashboard = {
                "uid": dashboard_uid,
                "title": "Generated Charts",
                "panels": [],
                "schemaVersion": 39,
                "tags": ["generated"],
            }
            version = None

        # Calculate panel ID
        panels = dashboard.get("panels", [])
        panel_id = max([p.get("id", 0) for p in panels], default=0) + 1

        # Create panel
        query_format = 0 if chart_type == "timeseries" else 1
        new_panel = {
            "id": panel_id,
            "title": title[:50],
            "type": chart_type,
            "datasource": {
                "type": "grafana-clickhouse-datasource",
                "uid": "clickhouse-rca"
            },
            "gridPos": {
                "h": 8,
                "w": 12,
                "x": (len(panels) % 2) * 12,
                "y": (len(panels) // 2) * 8,
            },
            "targets": [{
                "datasource": {
                    "type": "grafana-clickhouse-datasource",
                    "uid": "clickhouse-rca"
                },
                "editorType": "sql",
                "format": query_format,
                "queryType": "timeseries" if chart_type == "timeseries" else "table",
                "rawSql": sql_query,
                "refId": "A"
            }],
            "fieldConfig": {
                "defaults": {
                    "color": {"mode": "palette-classic"},
                    "mappings": [],
                    "thresholds": {
                        "mode": "absolute",
                        "steps": [{"color": "green", "value": None}]
                    }
                },
                "overrides": []
            },
        }

        panels.append(new_panel)
        dashboard["panels"] = panels

        # Save dashboard
        payload = {
            "dashboard": dashboard,
            "overwrite": True,
            "message": f"Added panel: {title[:50]}"
        }
        if version:
            payload["dashboard"]["version"] = version

        response = httpx.post(
            f"{GRAFANA_URL}/api/dashboards/db",
            json=payload,
            auth=(GRAFANA_USER, GRAFANA_PASSWORD),
            timeout=10.0,
        )

        if response.status_code not in (200, 201):
            raise Exception(f"Failed to save dashboard: {response.text}")

        embed_url = f"{GRAFANA_URL}/d-solo/{dashboard_uid}/generated-charts?orgId=1&panelId={panel_id}&theme=dark"

        return {
            "dashboard_uid": dashboard_uid,
            "panel_id": panel_id,
            "embed_url": embed_url,
        }


def create_grafana_tool_registry():
    """Create a registry with Grafana visualization tools."""
    from sim.rca.tools.base import ToolRegistry

    registry = ToolRegistry()
    registry.register(EmbedChartTool())
    registry.register(CreateChartTool())

    return registry
