# RCA (Root Cause Analysis)

AI-powered Root Cause Analysis for SQL Server performance incidents using Claude with extended thinking.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                            sim/rca/                                      │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │  engine/                                                         │    │
│  │  └─► AgentRCAEngine: Main analysis engine with Claude            │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                              │                                           │
│                              ▼                                           │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │  llm/                                                            │    │
│  │  ├─► AnthropicClient: Claude API with extended thinking          │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                              │                                           │
│                              ▼                                           │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │  tools/                                                          │    │
│  │  ├─► ClickHouse tools: Query metrics tables                      │    │
│  │  └─► Agent tools: Analytics.json access (legacy)                 │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                              │                                           │
│                              ▼                                           │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │  datasources/                                                    │    │
│  │  ├─► ClickHouseDataSource: Connect and query ClickHouse          │    │
│  │  └─► ClickHouseVerifier: Threshold-based incident detection      │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

## Quick Start

```python
from sim.rca import (
    AgentRCAEngine,
    RCAConfig,
    ClickHouseDataSource,
    ClickHouseVerifier,
    create_clickhouse_tool_registry,
)

# Connect to ClickHouse
data_source = ClickHouseDataSource(
    host="localhost",
    port=8123,
    database="rca_metrics"
)

# Build Feature Schema from metrics
feature_schema = data_source.build_feature_schema(
    incident_id="blocking_chain_20240105",
    baseline_minutes=5
)

# Create tools and engine
tools = create_clickhouse_tool_registry(data_source)
config = RCAConfig()
engine = AgentRCAEngine(
    config=config,
    tools=tools,
    feature_schema=feature_schema
)

# Run analysis with streaming
report = engine.analyze(stream=True)

print(f"Root Cause: {report.root_cause['summary']}")
```

## Module Overview

### engine/
The main analysis engine using Claude with extended thinking.
- See [engine/README.md](engine/README.md) for details

### llm/
LLM client abstraction for Anthropic/Claude.
- See [llm/README.md](llm/README.md) for details

### tools/
Investigation tools callable by the LLM during analysis.
- See [tools/README.md](tools/README.md) for details

### datasources/
Data source adapters and verification logic.
- `ClickHouseDataSource`: Query ClickHouse metrics
- `ClickHouseVerifier`: Threshold-based incident detection

### config.py
Configuration management via environment variables.

### feature_schema.py
Feature Schema definition for normalized metrics format.

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `ANTHROPIC_API_KEY` | - | Required for Claude |
| `RCA_MODEL` | (latest Claude) | Model to use |
| `RCA_THINKING_BUDGET` | `10000` | Extended thinking tokens |
| `RCA_MAX_TOOL_ITERATIONS` | `10` | Max tool calls |
| `RCA_TEMPERATURE` | `1.0` | Required for thinking |

## Package Exports

```python
from sim.rca import (
    # Engine
    AgentRCAEngine,
    AgentRCAReport,

    # Configuration
    RCAConfig,

    # Data Sources
    ClickHouseDataSource,
    ClickHouseVerifier,

    # Tools
    create_clickhouse_tool_registry,

    # LLM
    AnthropicClient,
    StreamEvent,
    StreamEventType,
)
```

## File Structure

```
sim/rca/
├── README.md           # This file
├── __init__.py         # Package exports
├── cli.py              # CLI commands (sim rca ...)
├── config.py           # RCAConfig
├── feature_schema.py   # Feature Schema builder
├── engine/             # Analysis engine
│   └── agent_engine.py
├── llm/                # LLM clients
│   ├── anthropic_client.py
│   └── factory.py
├── tools/              # Investigation tools
│   ├── clickhouse_tools.py
│   └── base.py
└── datasources/        # Data adapters
    └── clickhouse.py
```
