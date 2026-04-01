"""
Grafana API endpoints for chart generation and embedding.

Provides endpoints for:
- Generating charts from natural language prompts
- Listing available panels
- Getting embed URLs for panels
"""

import os
import json
import httpx
from typing import Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from sim.rca.config import RCAConfig
from sim.rca.llm.factory import create_llm_client

router = APIRouter()

# Grafana configuration
GRAFANA_URL = os.getenv("GRAFANA_URL", "http://localhost:3001")
GRAFANA_USER = os.getenv("GRAFANA_USER", "admin")
GRAFANA_PASSWORD = os.getenv("GRAFANA_PASSWORD", "admin123!")

# ClickHouse schema for LLM context
CLICKHOUSE_SCHEMA = """
Available tables in rca_metrics database:

1. wait_stats - SQL Server wait statistics
   - collected_at: DateTime64(3) - collection timestamp
   - wait_type: String - type of wait (e.g., LCK_M_X, PAGEIOLATCH_SH)
   - waiting_tasks_count: UInt64 - number of tasks waiting
   - wait_time_ms: UInt64 - total wait time in milliseconds
   - max_wait_time_ms: UInt64 - maximum wait time
   - signal_wait_time_ms: UInt64 - signal wait time

2. query_stats - Query execution statistics
   - collected_at: DateTime64(3)
   - query_hash: String - unique query identifier
   - execution_count: UInt64
   - total_worker_time_us: UInt64 - CPU time in microseconds
   - total_elapsed_time_us: UInt64 - total duration
   - total_logical_reads: UInt64
   - total_logical_writes: UInt64
   - total_physical_reads: UInt64
   - sql_text: String - query text

3. blocking_chains - Blocking session information
   - collected_at: DateTime64(3)
   - blocking_level: UInt8 - depth in blocking tree
   - session_id: UInt32
   - blocking_session_id: UInt32 - session causing the block
   - wait_type: String
   - wait_time_ms: UInt64
   - sql_text: String

4. memory_grants - Memory grant information
   - collected_at: DateTime64(3)
   - session_id: UInt32
   - requested_memory_mb: Float64
   - granted_memory_mb: Float64
   - used_memory_mb: Float64
   - grant_status: String - 'OK', 'WAITING', 'SPILL_LIKELY', 'SPILLED'

5. file_stats - File I/O statistics
   - collected_at: DateTime64(3)
   - database_name: String
   - file_name: String
   - file_type: String - 'ROWS' or 'LOG'
   - num_of_reads: UInt64
   - io_stall_read_ms: UInt64
   - num_of_writes: UInt64
   - io_stall_write_ms: UInt64

6. schedulers - CPU scheduler statistics
   - collected_at: DateTime64(3)
   - scheduler_id: UInt16
   - cpu_id: UInt16
   - current_tasks_count: UInt32
   - runnable_tasks_count: UInt32

7. perf_counters - SQL Server performance counters
   - collected_at: DateTime64(3)
   - object_name: String
   - counter_name: String
   - instance_name: String
   - counter_value: Int64

Time-based filtering:
- Use $__fromTime and $__toTime for Grafana time range variables
- Example: WHERE collected_at >= $__fromTime AND collected_at <= $__toTime
"""

SQL_GENERATION_PROMPT = """You are a SQL expert for ClickHouse databases. Generate a valid ClickHouse SQL query based on the user's request.

{schema}

Rules:
1. Only generate SELECT queries (no INSERT, UPDATE, DELETE, DROP, etc.)
2. Always include time filtering using $__fromTime and $__toTime Grafana variables
3. For timeseries charts, include a time column aliased as 'time' using toStartOfMinute(collected_at) or similar
4. Use aggregations (sum, avg, max, count) appropriately
5. Limit results to prevent performance issues (LIMIT 100 max for tables, no limit for timeseries)
6. Return ONLY the SQL query, no explanation

User request: {prompt}

SQL query:"""


class GenerateChartRequest(BaseModel):
    """Request to generate a chart from a prompt."""
    prompt: str
    chart_type: str = "timeseries"  # timeseries, barchart, piechart, table
    time_range: str = "1h"  # 1h, 6h, 24h, 7d


class GenerateChartResponse(BaseModel):
    """Response with generated chart details."""
    success: bool
    panel_id: Optional[int] = None
    dashboard_uid: Optional[str] = None
    embed_url: Optional[str] = None
    sql_query: Optional[str] = None
    error: Optional[str] = None


class PanelInfo(BaseModel):
    """Information about a Grafana panel."""
    panel_id: int
    title: str
    dashboard_uid: str
    dashboard_title: str
    embed_url: str


def validate_sql_query(sql: str) -> tuple[bool, str]:
    """
    Validate that the SQL query is safe to execute.

    Returns:
        Tuple of (is_valid, error_message)
    """
    sql_upper = sql.upper().strip()

    # Check for dangerous operations
    dangerous_keywords = [
        "INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER",
        "TRUNCATE", "GRANT", "REVOKE", "EXEC", "EXECUTE"
    ]

    for keyword in dangerous_keywords:
        if keyword in sql_upper.split():
            return False, f"Query contains forbidden keyword: {keyword}"

    # Must start with SELECT or WITH
    if not (sql_upper.startswith("SELECT") or sql_upper.startswith("WITH")):
        return False, "Query must start with SELECT or WITH"

    return True, ""


async def generate_sql_from_prompt(prompt: str) -> str:
    """Use LLM to generate SQL from natural language prompt."""
    config = RCAConfig()
    llm = create_llm_client(config)

    full_prompt = SQL_GENERATION_PROMPT.format(
        schema=CLICKHOUSE_SCHEMA,
        prompt=prompt
    )

    response = llm.chat(
        messages=[{"role": "user", "content": full_prompt}],
        max_tokens=1000,
    )

    # Extract SQL from response
    sql = (response.content or "").strip()

    # Remove markdown code blocks if present
    if sql.startswith("```sql"):
        sql = sql[6:]
    if sql.startswith("```"):
        sql = sql[3:]
    if sql.endswith("```"):
        sql = sql[:-3]

    return sql.strip()


async def create_grafana_panel(
    sql_query: str,
    title: str,
    chart_type: str = "timeseries",
) -> dict:
    """
    Create a new panel in Grafana's ad-hoc dashboard.

    Returns dict with dashboard_uid, panel_id, and embed_url.
    """
    # Dashboard for generated charts
    dashboard_uid = "generated-charts"
    dashboard_title = "Generated Charts"

    async with httpx.AsyncClient() as client:
        # First, try to get existing dashboard
        try:
            response = await client.get(
                f"{GRAFANA_URL}/api/dashboards/uid/{dashboard_uid}",
                auth=(GRAFANA_USER, GRAFANA_PASSWORD),
            )

            if response.status_code == 200:
                dashboard_data = response.json()
                dashboard = dashboard_data["dashboard"]
                version = dashboard.get("version", 1)
            else:
                # Create new dashboard
                dashboard = {
                    "uid": dashboard_uid,
                    "title": dashboard_title,
                    "panels": [],
                    "schemaVersion": 39,
                    "tags": ["generated"],
                }
                version = None
        except Exception:
            dashboard = {
                "uid": dashboard_uid,
                "title": dashboard_title,
                "panels": [],
                "schemaVersion": 39,
                "tags": ["generated"],
            }
            version = None

        # Calculate next panel ID
        existing_panels = dashboard.get("panels", [])
        next_panel_id = max([p.get("id", 0) for p in existing_panels], default=0) + 1

        # Determine format based on chart type
        query_format = 0 if chart_type == "timeseries" else 1  # 0=timeseries, 1=table

        # Create new panel
        new_panel = {
            "id": next_panel_id,
            "title": title,
            "type": chart_type,
            "datasource": {
                "type": "grafana-clickhouse-datasource",
                "uid": "clickhouse-rca"
            },
            "gridPos": {
                "h": 8,
                "w": 12,
                "x": (len(existing_panels) % 2) * 12,
                "y": (len(existing_panels) // 2) * 8,
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
                    "custom": {
                        "axisBorderShow": False,
                        "axisCenteredZero": False,
                        "axisColorMode": "text",
                        "axisLabel": "",
                        "axisPlacement": "auto",
                        "barAlignment": 0,
                        "drawStyle": "line",
                        "fillOpacity": 20,
                        "gradientMode": "none",
                        "lineInterpolation": "smooth",
                        "lineWidth": 2,
                        "pointSize": 5,
                        "showPoints": "never",
                        "spanNulls": False,
                        "stacking": {"group": "A", "mode": "none"},
                        "thresholdsStyle": {"mode": "off"}
                    },
                    "mappings": [],
                    "thresholds": {
                        "mode": "absolute",
                        "steps": [{"color": "green", "value": None}]
                    }
                },
                "overrides": []
            },
            "options": {
                "legend": {
                    "calcs": ["mean", "max"],
                    "displayMode": "table",
                    "placement": "right",
                    "showLegend": True
                },
                "tooltip": {"mode": "multi", "sort": "desc"}
            }
        }

        # Add panel to dashboard
        existing_panels.append(new_panel)
        dashboard["panels"] = existing_panels

        # Save dashboard
        payload = {
            "dashboard": dashboard,
            "overwrite": True,
            "message": f"Added panel: {title}"
        }

        if version:
            payload["dashboard"]["version"] = version

        response = await client.post(
            f"{GRAFANA_URL}/api/dashboards/db",
            json=payload,
            auth=(GRAFANA_USER, GRAFANA_PASSWORD),
        )

        if response.status_code not in (200, 201):
            raise Exception(f"Failed to save dashboard: {response.text}")

        # Build embed URL
        embed_url = f"{GRAFANA_URL}/d-solo/{dashboard_uid}/generated-charts?orgId=1&panelId={next_panel_id}&theme=dark"

        return {
            "dashboard_uid": dashboard_uid,
            "panel_id": next_panel_id,
            "embed_url": embed_url,
        }


@router.post("/generate", response_model=GenerateChartResponse)
async def generate_chart(request: GenerateChartRequest):
    """
    Generate a Grafana chart from a natural language prompt.

    1. Uses LLM to convert prompt to ClickHouse SQL
    2. Validates the SQL is safe (SELECT only)
    3. Creates a panel in Grafana
    4. Returns the embed URL
    """
    try:
        # Generate SQL from prompt
        sql_query = await generate_sql_from_prompt(request.prompt)

        # Validate SQL
        is_valid, error = validate_sql_query(sql_query)
        if not is_valid:
            return GenerateChartResponse(
                success=False,
                sql_query=sql_query,
                error=f"Generated SQL failed validation: {error}"
            )

        # Create panel in Grafana
        result = await create_grafana_panel(
            sql_query=sql_query,
            title=request.prompt[:50] + ("..." if len(request.prompt) > 50 else ""),
            chart_type=request.chart_type,
        )

        return GenerateChartResponse(
            success=True,
            panel_id=result["panel_id"],
            dashboard_uid=result["dashboard_uid"],
            embed_url=result["embed_url"],
            sql_query=sql_query,
        )

    except Exception as e:
        return GenerateChartResponse(
            success=False,
            error=str(e)
        )


@router.get("/panels", response_model=list[PanelInfo])
async def list_panels():
    """List all available panels from pre-built dashboards."""
    panels = []

    # Pre-built dashboards
    dashboards = [
        ("sql-server-overview", "SQL Server Overview"),
        ("wait-stats", "Wait Statistics"),
        ("query-performance", "Query Performance"),
    ]

    async with httpx.AsyncClient() as client:
        for dashboard_uid, dashboard_title in dashboards:
            try:
                response = await client.get(
                    f"{GRAFANA_URL}/api/dashboards/uid/{dashboard_uid}",
                    auth=(GRAFANA_USER, GRAFANA_PASSWORD),
                )

                if response.status_code == 200:
                    data = response.json()
                    for panel in data["dashboard"].get("panels", []):
                        embed_url = f"{GRAFANA_URL}/d-solo/{dashboard_uid}/{dashboard_uid}?orgId=1&panelId={panel['id']}&theme=dark"
                        panels.append(PanelInfo(
                            panel_id=panel["id"],
                            title=panel.get("title", "Untitled"),
                            dashboard_uid=dashboard_uid,
                            dashboard_title=dashboard_title,
                            embed_url=embed_url,
                        ))
            except Exception:
                continue

    return panels


@router.get("/embed-url/{dashboard_uid}/{panel_id}")
async def get_embed_url(dashboard_uid: str, panel_id: int, time_range: str = "1h"):
    """Get the embed URL for a specific panel."""
    time_from = {
        "1h": "now-1h",
        "6h": "now-6h",
        "24h": "now-24h",
        "7d": "now-7d",
    }.get(time_range, "now-1h")

    embed_url = f"{GRAFANA_URL}/d-solo/{dashboard_uid}/{dashboard_uid}?orgId=1&panelId={panel_id}&from={time_from}&to=now&theme=dark"

    return {"embed_url": embed_url}
