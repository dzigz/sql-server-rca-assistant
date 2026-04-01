# LLM Clients

LLM abstraction for Claude (Anthropic) with streaming and tool calling.

## Supported Providers

| Provider | Client | Features |
|----------|--------|----------|
| Anthropic | `AnthropicClient` | Extended thinking, streaming, tools |

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          LLMClient (base.py)                             │
├─────────────────────────────────────────────────────────────────────────┤
│  Abstract interface:                                                     │
│  ├─► chat() - Synchronous completion                                     │
│  ├─► chat_stream() - Streaming completion with events                    │
│  └─► chat_with_tools() - Tool-enabled completion                         │
└─────────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
                    ┌─────────────────────────┐
                    │   AnthropicClient       │
                    │   (anthropic_client.py) │
                    ├─────────────────────────┤
                    │  • Extended thinking    │
                    │  • Claude models        │
                    │  • Streaming            │
                    │  • Tool calling         │
                    └─────────────────────────┘
```

## Usage

### Basic Chat

```python
from sim.rca.llm import AnthropicClient

client = AnthropicClient(api_key="your-key")
response = client.chat(
    messages=[{"role": "user", "content": "Analyze this error..."}],
    system="You are a SQL Server DBA."
)
print(response.content)
```

### Streaming with Extended Thinking

```python
from sim.rca.llm import AnthropicClient, StreamEvent, StreamEventType

client = AnthropicClient(api_key="your-key")

def handler(event: StreamEvent):
    if event.type == StreamEventType.THINKING_DELTA:
        print(f"[Thinking] {event.content}", end="")
    elif event.type == StreamEventType.TEXT_DELTA:
        print(event.content, end="")

response = client.chat_stream(
    messages=[{"role": "user", "content": "Analyze..."}],
    system="...",
    thinking_budget=10000,  # Enable extended thinking
    on_stream=handler
)
```

### Tool Calling

```python
from sim.rca.llm import AnthropicClient
from sim.rca.tools import ToolRegistry

client = AnthropicClient(api_key="your-key")
tools = ToolRegistry()

# Register tools
tools.register("query_wait_stats", query_wait_stats_func, schema={...})

# Chat with tools
response = client.chat_with_tools(
    messages=[{"role": "user", "content": "What are the top wait types?"}],
    tools=tools,
    max_iterations=10
)

# Tool calls are automatically executed and results fed back
print(response.content)
print(f"Tool calls made: {len(response.tool_calls)}")
```

## Stream Event Types

```python
from sim.rca.llm import StreamEventType

class StreamEventType(Enum):
    THINKING_START = "thinking_start"     # Extended thinking started
    THINKING_DELTA = "thinking_delta"     # Thinking content chunk
    THINKING_END = "thinking_end"         # Extended thinking ended
    TEXT_START = "text_start"             # Response text started
    TEXT_DELTA = "text_delta"             # Response text chunk
    TEXT_END = "text_end"                 # Response text ended
    TOOL_USE_START = "tool_use_start"     # Tool call initiated
    TOOL_USE_END = "tool_use_end"         # Tool call completed (includes tool_call)
    TOOL_RESULT = "tool_result"           # Tool execution result (includes tool_call_id, tool_result)
    MESSAGE_END = "message_end"           # Full message done
```

## Configuration

### Anthropic

| Variable | Default | Description |
|----------|---------|-------------|
| `ANTHROPIC_API_KEY` | - | Required |
| `RCA_MODEL` | (latest Claude) | Model name |
| `RCA_THINKING_BUDGET` | `10000` | Extended thinking tokens |
| `RCA_TEMPERATURE` | `1.0` | Required for thinking |
| `RCA_MAX_TOKENS` | `16000` | Max response tokens |

## Extended Thinking

Claude's extended thinking enables complex multi-step reasoning:

```python
# Extended thinking is automatically enabled when thinking_budget > 0
response = client.chat_stream(
    messages=[...],
    thinking_budget=10000,  # Token budget for thinking
    temperature=1.0,        # Required for thinking
)
```

The thinking process appears in `THINKING_DELTA` stream events before the final response.

## File Structure

```
sim/rca/llm/
├── README.md             # This file
├── __init__.py           # Package exports
├── base.py               # LLMClient abstract base
├── anthropic_client.py   # Anthropic/Claude implementation
└── factory.py            # Client factory
```

## Package Exports

```python
from sim.rca.llm import (
    # Clients
    AnthropicClient,

    # Streaming
    StreamEvent,
    StreamEventType,

    # Types
    LLMResponse,
    ToolCall,
)
```
