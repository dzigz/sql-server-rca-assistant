# RCA Tools

Investigation tools available to the LLM during Root Cause Analysis.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          Tool Registry                                   │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ClickHouse Tools (clickhouse_tools.py)                                 │
│  ├─► compare_baseline      - Compare baseline vs incident metrics        │
│  ├─► query_clickhouse      - Query any metrics table with filters        │
│  └─► get_query_details     - Get details for a specific query            │
│                                                                          │
│  Blitz Diagnostic Tools (blitz_tools.py)                                │
│  └─► run_blitz_diagnostics - Run FRK scripts on-demand (live/cached)    │
│                                                                          │
│  Health Tools (health_tools.py)                                         │
│  └─► run_sp_blitz          - Run sp_Blitz server health check            │
│                                                                          │
│  Code Analysis Tools (code_analysis_tools.py)                           │
│  ├─► analyze_code_impact   - Find app features affected by slow query    │
│  ├─► correlate_incident    - Correlate incident with code changes        │
│  ├─► find_query_origin     - Find where SQL originates in code           │
│  └─► analyze_orm_patterns  - Detect ORM anti-patterns                    │
│                                                                          │
│  Grafana Tools (grafana_tools.py)                                       │
│  ├─► embed_chart           - Embed existing Grafana panel                │
│  └─► create_chart          - Generate custom chart from prompt           │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

## ClickHouse Tools

### compare_baseline

**Compares baseline period metrics against incident period to identify what changed.** This is typically the first tool the agent calls to understand deviations from normal operation.

Returns deltas for:
- Wait statistics (which waits increased and by how much)
- Blocking comparison (blocking events before vs during incident)
- Memory grants (waiting, spilled, ok counts)
- Top new waits that appeared during the incident

```python
result = tools.execute("compare_baseline", {})
# Returns: {"wait_deltas": [...], "blocking_comparison": {...}, "memory_grants": {...}}
```

### query_clickhouse

**Queries any ClickHouse metrics table directly with optional filters and sorting.** Use this for targeted investigation when you know which metric category to examine.

Available tables:
| Table | Contents |
|-------|----------|
| `wait_stats` | Wait type statistics (wait_type, wait_time_ms, waiting_tasks_count) |
| `blocking_chains` | Active blocking relationships (session_id, blocking_session_id, wait_type, sql_text) |
| `memory_grants` | Memory grant status and sizes (grant_status, requested_mb, granted_mb, max_used_mb) |
| `query_stats` | Query performance metrics (query_hash, total_worker_time_us, total_elapsed_time_us, total_logical_reads, execution_count) |
| `schedulers` | CPU scheduler health (scheduler_id, runnable_tasks_count, current_tasks_count) |
| `file_stats` | I/O statistics by file (database_name, io_stall_read_ms, io_stall_write_ms) |
| `missing_indexes` | Index recommendations (table_name, equality_columns, inequality_columns, impact_score) |

```python
result = tools.execute("query_clickhouse", {
    "table": "wait_stats",
    "filters": {"wait_type": "LCK_M_X"},
    "order_by": "wait_time_ms DESC",
    "limit": 10
})
```

## Blitz Diagnostic Tools

### run_blitz_diagnostics

**Run First Responder Kit diagnostic scripts on-demand.** This unified tool can run any or all Blitz scripts and automatically handles fallback to cached results.

**Use this tool primarily during ACTIVE INCIDENTS** - it captures real-time server state.

Available scripts:
| Script | Purpose |
|--------|---------|
| `first` | sp_BlitzFirst - Real-time wait stats delta (what's happening NOW) |
| `cache` | sp_BlitzCache - Current query plan cache analysis |
| `who` | sp_BlitzWho - Active sessions, blocking chains RIGHT NOW |
| `index` | sp_BlitzIndex - Missing/unused index recommendations |
| `lock` | sp_BlitzLock - Recent deadlock analysis from system_health |
| `all` | Run all scripts (default) |

Priority levels: 1-10 (critical), 11-50 (high), 51-100 (medium)

**Data Source Indicators** (check the response message):
- "Live diagnostics" = Fresh data from SQL Server (best)
- "Using cached Blitz findings (X min old)" = Stored results, recent enough
- "WARNING: STALE cached findings (X min old)" = Data >30 min old
- "DIAGNOSTIC: TIMED OUT" = Server is overloaded - this is itself a finding!

```python
# Run all Blitz diagnostics (default)
result = tools.execute("run_blitz_diagnostics", {})

# Run specific script
result = tools.execute("run_blitz_diagnostics", {
    "script": "first",  # Run only sp_BlitzFirst
    "seconds": 5        # Sampling period
})

# Run BlitzCache for top queries
result = tools.execute("run_blitz_diagnostics", {
    "script": "cache",
    "top": 20           # Number of top queries
})
```

### get_query_details

**Gets detailed execution statistics for a specific query by its query_hash.** Use this when you've identified a problematic query and need full details.

Returns: execution count, total CPU time, logical reads, elapsed time, and plan info if available.

```python
result = tools.execute("get_query_details", {
    "query_hash": "0x1234ABCD..."
})
```

## Health Tools

Health assessment tools for proactive server configuration review.

### run_sp_blitz

**Run sp_Blitz server health check.** Use this for HEALTH ASSESSMENTS (proactive checks when no incident is detected). For ACTIVE INCIDENT investigation, use `run_blitz_diagnostics()` instead.

Returns prioritized findings about server configuration, security, and performance issues:
- Priority 1-10: Critical (security vulnerabilities, corruption risks)
- Priority 11-50: High (performance issues, misconfigurations)
- Priority 51-100: Medium (best practice violations)

```python
# Run full health check
result = tools.execute("run_sp_blitz", {
    "priority_threshold": 100,  # Only show findings up to this priority
    "check_server_info": True   # Include server information
})
```

## Code Analysis Tools

Correlate database issues with application code. Requires `--repo-path` to be specified.

### analyze_code_impact

**Finds which application features are affected by a slow query.**

```python
result = tools.execute("analyze_code_impact", {
    "slow_query": "SELECT * FROM Orders WHERE CustomerId = @p0",
    "table_name": "Orders",
    "repo_path": "/path/to/app"
})
```

### correlate_incident

**Correlates an incident with recent code or schema changes.**

```python
result = tools.execute("correlate_incident", {
    "incident_time": "2024-01-05T14:30:00Z",
    "affected_table": "Orders",
    "repo_path": "/path/to/app"
})
```

### find_query_origin

**Finds where a SQL query originates in the application code.**

```python
result = tools.execute("find_query_origin", {
    "sql_pattern": "SELECT.*FROM Orders",
    "repo_path": "/path/to/app"
})
```

### analyze_orm_patterns

**Detects ORM anti-patterns that could cause performance issues.**

```python
result = tools.execute("analyze_orm_patterns", {
    "repo_path": "/path/to/app"
})
# Detects: N+1 queries, missing eager loading, cartesian products
```

## Grafana Tools

Generate and embed visualizations in chat responses.

### embed_chart

**Embeds an existing Grafana panel in the response.**

```python
result = tools.execute("embed_chart", {
    "chart_name": "wait_time_trend",  # Predefined chart name
    "time_range": "1h"                # 15m, 1h, 6h, 24h, 7d
})
```

Available charts: `wait_time_trend`, `blocked_sessions`, `active_requests`, `memory_grants`, `top_waits`, `wait_stats_trend`, `wait_by_category`, `top_queries_cpu`, `top_queries_reads`

### create_chart

**Creates a custom chart based on a natural language prompt.**

```python
result = tools.execute("create_chart", {
    "prompt": "Show me CPU usage over the last hour",
    "chart_type": "line"  # line, bar, gauge, table
})
```

## Usage

### Creating a Tool Registry

```python
from sim.rca.tools import create_clickhouse_tool_registry
from sim.rca.tools.health_tools import create_health_tool_registry
from sim.rca.datasources import ClickHouseDataSource

# Connect to ClickHouse
data_source = ClickHouseDataSource(host="localhost", port=8123)

# Create ClickHouse tools
tools = create_clickhouse_tool_registry(data_source, incident_id="abc123")

# Add health tools (optional)
health_tools = create_health_tool_registry(
    sqlserver_container="sqlserver",
    sqlserver_password="YourPassword123!"
)
for tool in health_tools._tools.values():
    tools.register(tool)

# Use with AgentRCAEngine
engine = AgentRCAEngine(config=config, tools=tools, feature_schema=schema)
```

### Tool Schemas

Each tool has a JSON Schema definition for LLM tool calling:

```python
{
    "name": "query_clickhouse",
    "description": "Query ClickHouse metrics tables directly...",
    "input_schema": {
        "type": "object",
        "properties": {
            "table": {
                "type": "string",
                "enum": ["wait_stats", "blocking_chains", ...]
            },
            "filters": {
                "type": "object",
                "description": "Column filters as key-value pairs"
            },
            "order_by": {"type": "string"},
            "limit": {"type": "integer", "default": 20}
        },
        "required": ["table"]
    }
}
```

## Custom Tools

Create custom tools by extending `RCATool`:

```python
from sim.rca.tools.base import RCATool, ToolResult

class MyCustomTool(RCATool):
    name = "my_custom_tool"
    description = "Does something custom"

    @property
    def parameters_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "param1": {"type": "string"}
            }
        }

    def execute(self, param1: str, **kwargs) -> ToolResult:
        # Implementation
        return ToolResult(success=True, data={"result": "..."})

# Register
registry.register(MyCustomTool())
```

## File Structure

```
sim/rca/tools/
├── README.md               # This file
├── __init__.py             # Package exports
├── base.py                 # RCATool, ToolRegistry, ToolResult base classes
├── registry.py             # Tool registry factory functions
├── clickhouse_tools.py     # ClickHouse query tools
├── health_tools.py         # sp_Blitz health check tools
├── code_analysis_tools.py  # Code correlation tools
├── grafana_tools.py        # Visualization tools
├── plan_tools.py           # Execution plan analysis
└── sql_tools.py            # SQL text retrieval
```

## Package Exports

```python
from sim.rca.tools import (
    # Registry
    ToolRegistry,
    create_clickhouse_tool_registry,

    # Base classes
    RCATool,
    ToolResult,
)

from sim.rca.tools.clickhouse_tools import (
    CompareBaselineTool,
    QueryClickHouseTool,
    GetQueryDetailsTool,
)

from sim.rca.tools.blitz_tools import (
    RunBlitzDiagnosticsTool,
    create_blitz_diagnostic_tool,
)

from sim.rca.tools.health_tools import (
    create_health_tool_registry,
    RunSpBlitzTool,
)

from sim.rca.tools.grafana_tools import (
    EmbedChartTool,
    CreateChartTool,
)
```
