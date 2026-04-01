"""
LLM client abstraction layer.

Provides a unified interface for interacting with different LLM providers
(Anthropic) with support for tool/function calling and streaming.

Usage:
    from sim.rca.llm import create_llm_client
    from sim.rca.config import RCAConfig

    config = RCAConfig()
    client = create_llm_client(config)
    response = client.chat([{"role": "user", "content": "Hello"}])

    # Streaming with callback
    def on_event(event):
        if event.type == StreamEventType.THINKING_DELTA:
            print(event.content, end="", flush=True)

    response = client.chat_stream(messages, on_event=on_event)
"""

from sim.rca.llm.base import LLMClient, LLMResponse, ToolCall
from sim.rca.llm.factory import create_llm_client
from sim.rca.llm.anthropic_client import StreamEvent, StreamEventType

__all__ = [
    "LLMClient",
    "LLMResponse",
    "ToolCall",
    "create_llm_client",
    "StreamEvent",
    "StreamEventType",
]
