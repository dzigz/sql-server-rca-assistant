"""
Base classes for LLM client abstraction.

Defines the interface that all LLM provider implementations must follow,
enabling seamless switching between providers.

Features:
- Synchronous and asynchronous API support
- Tool/function calling abstraction
- JSON output mode
- Provider-agnostic interface
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Optional


@dataclass
class ToolCall:
    """
    Represents a tool/function call requested by the LLM.
    
    Attributes:
        id: Unique identifier for this tool call
        name: Name of the tool to call
        arguments: Arguments for the tool (as parsed dict)
    """
    id: str
    name: str
    arguments: dict


@dataclass
class LLMResponse:
    """
    Response from an LLM call.
    
    Attributes:
        content: Text content of the response (may be None if tool calls)
        tool_calls: List of tool calls requested by the LLM
        finish_reason: Reason the generation stopped ('stop', 'tool_calls', etc.)
        usage: Token usage statistics
        raw_response: Raw response from the provider (for debugging)
    """
    content: Optional[str] = None
    tool_calls: list[ToolCall] = field(default_factory=list)
    finish_reason: str = "stop"
    usage: dict = field(default_factory=dict)
    raw_response: Any = None
    
    @property
    def has_tool_calls(self) -> bool:
        """Check if the response contains tool calls."""
        return len(self.tool_calls) > 0


@dataclass
class ToolDefinition:
    """
    Definition of a tool that can be called by the LLM.
    
    Attributes:
        name: Name of the tool
        description: Description of what the tool does
        parameters: JSON Schema for the tool's parameters
    """
    name: str
    description: str
    parameters: dict

    def to_anthropic_format(self) -> dict:
        """Convert to Anthropic tool use format."""
        return {
            "name": self.name,
            "description": self.description,
            "input_schema": self.parameters,
        }


class LLMClient(ABC):
    """
    Abstract base class for LLM clients.
    
    Implementations should handle provider-specific API calls while
    presenting a unified interface for the RCA engine.
    """
    
    @abstractmethod
    def chat(
        self,
        messages: list[dict],
        tools: Optional[list[ToolDefinition]] = None,
        tool_choice: Optional[str] = None,
    ) -> LLMResponse:
        """
        Send a chat completion request.
        
        Args:
            messages: List of message dicts with 'role' and 'content'
            tools: Optional list of tools the LLM can call
            tool_choice: How to handle tool selection ('auto', 'none', or specific tool)
        
        Returns:
            LLMResponse with content and/or tool calls
        """
        pass
    
    @abstractmethod
    def chat_with_json_output(
        self,
        messages: list[dict],
        schema: Optional[dict] = None,
    ) -> dict:
        """
        Send a chat request expecting structured JSON output.

        Args:
            messages: List of message dicts
            schema: Optional JSON schema for the expected output

        Returns:
            Parsed JSON response as dict
        """
        pass

    # =========================================================================
    # Async Methods (optional - have default implementations that raise)
    # =========================================================================

    async def chat_async(
        self,
        messages: list[dict],
        tools: Optional[list[ToolDefinition]] = None,
        tool_choice: Optional[str] = None,
    ) -> LLMResponse:
        """
        Async version of chat().

        Args:
            messages: List of message dicts with 'role' and 'content'
            tools: Optional list of tools the LLM can call
            tool_choice: How to handle tool selection

        Returns:
            LLMResponse with content and/or tool calls
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support async operations. "
            "Use chat() instead or implement chat_async()."
        )

    async def chat_with_json_output_async(
        self,
        messages: list[dict],
        schema: Optional[dict] = None,
    ) -> dict:
        """
        Async version of chat_with_json_output().

        Args:
            messages: List of message dicts
            schema: Optional JSON schema for the expected output

        Returns:
            Parsed JSON response as dict
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support async operations. "
            "Use chat_with_json_output() instead or implement chat_with_json_output_async()."
        )

    async def stream_chat_async(
        self,
        messages: list[dict],
        tools: Optional[list[ToolDefinition]] = None,
    ) -> AsyncIterator[str]:
        """
        Stream chat response asynchronously.

        Args:
            messages: List of message dicts
            tools: Optional list of tools

        Yields:
            Text chunks as they are generated
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support streaming. "
            "Use chat() or implement stream_chat_async()."
        )
        # Required to make this an async generator
        yield ""  # type: ignore

    def format_tool_result(
        self,
        tool_call_id: str,
        result: Any,
    ) -> dict:
        """
        Format a tool result for inclusion in messages.
        
        Args:
            tool_call_id: ID of the tool call this is responding to
            result: Result from the tool execution
        
        Returns:
            Message dict formatted for the provider
        """
        # Default implementation - subclasses may override
        import json
        content = json.dumps(result) if not isinstance(result, str) else result
        return {
            "role": "tool",
            "tool_call_id": tool_call_id,
            "content": content,
        }
