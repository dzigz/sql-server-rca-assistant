# RCA Engine

The Root Cause Analysis engine uses Claude with extended thinking to analyze SQL Server performance incidents stored in ClickHouse.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        AgentRCAEngine                                │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │              System Prompt + Feature Schema                     │ │
│  │              (Baseline vs Incident metrics from ClickHouse)     │ │
│  └────────────────────────────────────────────────────────────────┘ │
│                              ↓                                       │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │            Extended Thinking Reasoning Loop                     │ │
│  │  ┌──────────────────────────────────────────────────────────┐  │ │
│  │  │  Think → Call Tools → Think → Call Tools → Conclude      │  │ │
│  │  │                                                           │  │ │
│  │  │  ClickHouse Tools:                                        │  │ │
│  │  │  • query_wait_stats()      - Wait type analysis           │  │ │
│  │  │  • query_blocking_chains() - Lock contention details      │  │ │
│  │  │  • query_query_stats()     - Query performance metrics    │  │ │
│  │  │  • query_memory_grants()   - Memory grant queuing         │  │ │
│  │  │  • query_file_stats()      - I/O latency analysis         │  │ │
│  │  │  • query_blitz_results()   - sp_Blitz findings            │  │ │
│  │  │  • compare_metrics()       - Baseline vs incident delta   │  │ │
│  │  └──────────────────────────────────────────────────────────┘  │ │
│  └────────────────────────────────────────────────────────────────┘ │
│                              ↓                                       │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │                  Structured JSON Report                         │ │
│  │  • root_cause, causal_chain, evidence, mitigation, prevention   │ │
│  └────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
```

## Usage

### Basic Analysis

```python
from sim.rca import AgentRCAEngine, RCAConfig, ClickHouseDataSource, create_clickhouse_tool_registry

# Connect to ClickHouse
data_source = ClickHouseDataSource(
    host="localhost",
    port=8123,
    database="rca_metrics"
)

# Build Feature Schema (aggregated metrics)
feature_schema = data_source.build_feature_schema(
    incident_id="blocking_chain_20240105",
    baseline_minutes=5
)

# Create tools and engine
tools = create_clickhouse_tool_registry(data_source)
config = RCAConfig()
engine = AgentRCAEngine(config=config, tools=tools, feature_schema=feature_schema)

# Run analysis
report = engine.analyze()

print(f"Root Cause: {report.root_cause['summary']}")
print(f"Confidence: {report.root_cause['confidence']}")
```

### Streaming Output

```python
# Enable real-time streaming of thinking and response
report = engine.analyze(stream=True)

# Or with custom handler
from sim.rca.llm import StreamEvent, StreamEventType

def handler(event: StreamEvent):
    if event.type == StreamEventType.THINKING_DELTA:
        print(event.content, end="", flush=True)
    elif event.type == StreamEventType.TOOL_USE_START:
        print(f"\n[Tool: {event.content}]")

report = engine.analyze(stream=True, on_stream=handler)
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `ANTHROPIC_API_KEY` | - | Required for Claude API |
| `RCA_MODEL` | (latest Claude) | Claude model to use |
| `RCA_THINKING_BUDGET` | `10000` | Token budget for extended thinking |
| `RCA_MAX_TOOL_ITERATIONS` | `10` | Maximum tool-calling iterations |
| `RCA_TEMPERATURE` | `1.0` | Required for extended thinking |

## Investigation Methodology

The system prompt guides Claude through a 6-step methodology:

1. **Review Feature Schema** - Understand baseline vs incident context
2. **Identify What Changed** - Focus on deltas, not absolute values
3. **Form Hypotheses** - Rank 2-3 potential root causes
4. **Investigate** - Use tools to gather targeted evidence
5. **Validate** - Check for alternative explanations
6. **Form Causal Chain** - Connect trigger → effect → symptom

## Output Format

```json
{
  "root_cause": {
    "category": "blocking|missing_index|plan_regression|memory_pressure|...",
    "summary": "One sentence description",
    "confidence": 0.85,
    "entity": "Schema.Table or object name"
  },
  "causal_chain": [
    {"event": "Trigger", "description": "Long-running UPDATE holds X lock"},
    {"event": "Effect", "description": "Concurrent SELECTs blocked on LCK_M_S"},
    {"event": "Symptom", "description": "Response time spike, timeouts"}
  ],
  "evidence": [
    {"source": "query_blocking_chains", "finding": "Session 55 blocked 3 others for 45s"}
  ],
  "mitigation": ["Kill blocking session 55", "Rollback uncommitted transaction"],
  "prevention": ["Add lock timeout", "Optimize transaction scope"]
}
```

## File Structure

```
sim/rca/engine/
├── README.md           # This file
├── __init__.py         # Package exports (AgentRCAEngine, AgentRCAReport)
└── agent_engine.py     # Main engine implementation

sim/rca/tools/
├── clickhouse_tools.py # ClickHouse query tools (primary)
├── agent_tools.py      # Analytics.json tools (legacy)
├── base.py             # RCATool, ToolRegistry base classes
└── registry.py         # Tool registry factory functions

sim/rca/datasources/
├── __init__.py
└── clickhouse.py       # ClickHouseDataSource, ClickHouseVerifier
```

## Stream Event Types

| Event Type | Description |
|------------|-------------|
| `THINKING_START` | Extended thinking block started |
| `THINKING_DELTA` | Chunk of thinking content |
| `THINKING_END` | Extended thinking block ended |
| `TEXT_START` | Response text block started |
| `TEXT_DELTA` | Chunk of response text |
| `TEXT_END` | Response text block ended |
| `TOOL_USE_START` | Tool call initiated |
| `TOOL_USE_END` | Tool call completed (includes tool_call details) |
| `TOOL_RESULT` | Tool execution result (includes tool_call_id, tool_result) |
| `MESSAGE_END` | Full message completed |

## Integration Example

The engine is typically invoked by the web app session layer or another caller
that prepares the feature schema and tool registry:

```python
from sim.rca import AgentRCAEngine, RCAConfig, create_clickhouse_tool_registry

feature_schema = data_source.build_feature_schema(incident_id, baseline_minutes)
tools = create_clickhouse_tool_registry(data_source, incident_id)
engine = AgentRCAEngine(config=config, tools=tools, feature_schema=feature_schema)
report = engine.analyze(stream=stream_rca)
```
