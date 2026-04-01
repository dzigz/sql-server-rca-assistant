"""
Anthropic LLM client implementation.

Provides LLM capabilities using Anthropic's Claude API with support for
chat completions, tool use, structured output, and streaming.
"""

import json
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Generator, Optional

from sim.rca.config import RCAConfig
from sim.rca.llm.base import LLMClient, LLMResponse, ToolCall, ToolDefinition


class StreamEventType(Enum):
    """Types of streaming events."""
    THINKING_START = "thinking_start"
    THINKING_DELTA = "thinking_delta"
    THINKING_END = "thinking_end"
    TEXT_START = "text_start"
    TEXT_DELTA = "text_delta"
    TEXT_END = "text_end"
    TOOL_USE_START = "tool_use_start"
    TOOL_USE_END = "tool_use_end"
    TOOL_RESULT = "tool_result"  # Tool execution result
    MESSAGE_END = "message_end"


@dataclass
class StreamEvent:
    """A streaming event from the LLM."""
    type: StreamEventType
    content: str = ""
    tool_call: Optional[ToolCall] = None
    tool_result: Optional[str] = None  # Tool execution result (JSON string)
    tool_call_id: Optional[str] = None  # ID of the tool call this result belongs to
    usage: Optional[dict] = None


class AnthropicClient(LLMClient):
    """
    Anthropic Claude API client for the RCA system.
    
    Supports:
    - Chat completions with Claude models
    - Tool use for agent-based investigation
    - Structured JSON responses
    """
    
    def __init__(self, config: RCAConfig):
        """
        Initialize the Anthropic client.
        
        Args:
            config: RCA configuration with API key and model settings
        """
        self.config = config
        self._client = None
    
    @property
    def client(self):
        """Lazy initialization of Anthropic client."""
        if self._client is None:
            try:
                from anthropic import Anthropic
            except ImportError:
                raise ImportError(
                    "Anthropic package not installed. Install with: pip install anthropic>=0.18.0"
                )
            # Use stable API surface for Claude 4.6 models.
            self._client = Anthropic(api_key=self.config.api_key)
        return self._client

    def _uses_adaptive_thinking(self) -> bool:
        """Return True when the configured model should use adaptive thinking."""
        model = (self.config.model or "").strip().lower()
        return model.startswith("claude-opus-4-6")

    def _effort_for_budget(self, thinking_budget: int) -> str:
        """Map legacy thinking budgets to Anthropic's effort levels."""
        if thinking_budget >= 24000:
            return "max"
        if thinking_budget >= 12000:
            return "high"
        if thinking_budget >= 4000:
            return "medium"
        return "low"

    def _apply_thinking_config(
        self,
        kwargs: dict[str, Any],
        *,
        extended_thinking: bool,
        thinking_budget: int,
    ) -> None:
        """Apply Anthropic thinking settings for the configured model."""
        if not extended_thinking:
            return

        if self._uses_adaptive_thinking():
            kwargs["thinking"] = {"type": "adaptive"}
            kwargs["output_config"] = {"effort": self._effort_for_budget(thinking_budget)}
        else:
            kwargs["thinking"] = {
                "type": "enabled",
                "budget_tokens": thinking_budget,
            }

        # Keep a larger output budget when extended thinking is enabled.
        if kwargs["max_tokens"] < thinking_budget + 4000:
            kwargs["max_tokens"] = thinking_budget + 8000
    
    def chat(
        self,
        messages: list[dict],
        tools: Optional[list[ToolDefinition]] = None,
        tool_choice: Optional[str] = None,
        extended_thinking: bool = False,
        thinking_budget: int = 10000,
        max_tokens: Optional[int] = None,
    ) -> LLMResponse:
        """
        Send a chat completion request to Anthropic.

        Args:
            messages: List of message dicts with 'role' and 'content'
            tools: Optional list of tools the LLM can call
            tool_choice: How to handle tool selection
            extended_thinking: Enable extended thinking for complex reasoning
            thinking_budget: Token budget for thinking (default 10000)
            max_tokens: Override max_tokens from config

        Returns:
            LLMResponse with content and/or tool calls
        """
        # Extract system message if present
        system = None
        chat_messages = []

        for msg in messages:
            if msg["role"] == "system":
                system = msg["content"]
            else:
                chat_messages.append(self._convert_message(msg))

        kwargs = {
            "model": self.config.model,
            "messages": chat_messages,
            "max_tokens": max_tokens or self.config.max_tokens,
        }

        if system:
            kwargs["system"] = system

        self._apply_thinking_config(
            kwargs,
            extended_thinking=extended_thinking,
            thinking_budget=thinking_budget,
        )

        if tools:
            kwargs["tools"] = [t.to_anthropic_format() for t in tools]
            if tool_choice:
                if tool_choice == "auto":
                    kwargs["tool_choice"] = {"type": "auto"}
                elif tool_choice == "none":
                    # Don't include tools at all
                    del kwargs["tools"]
                elif tool_choice == "required":
                    kwargs["tool_choice"] = {"type": "any"}
                else:
                    # Specific tool name
                    kwargs["tool_choice"] = {"type": "tool", "name": tool_choice}

        response = self.client.messages.create(**kwargs)

        return self._parse_response(response, extended_thinking=extended_thinking)

    def chat_stream(
        self,
        messages: list[dict],
        tools: Optional[list[ToolDefinition]] = None,
        tool_choice: Optional[str] = None,
        extended_thinking: bool = False,
        thinking_budget: int = 10000,
        max_tokens: Optional[int] = None,
        on_event: Optional[Callable[[StreamEvent], None]] = None,
    ) -> LLMResponse:
        """
        Send a streaming chat completion request to Anthropic.

        Streams thinking and text content in real-time via the on_event callback.

        Args:
            messages: List of message dicts with 'role' and 'content'
            tools: Optional list of tools the LLM can call
            tool_choice: How to handle tool selection
            extended_thinking: Enable extended thinking for complex reasoning
            thinking_budget: Token budget for thinking (default 10000)
            max_tokens: Override max_tokens from config
            on_event: Callback for streaming events

        Returns:
            LLMResponse with complete content and/or tool calls
        """
        # Extract system message if present
        system = None
        chat_messages = []

        for msg in messages:
            if msg["role"] == "system":
                system = msg["content"]
            else:
                chat_messages.append(self._convert_message(msg))

        kwargs = {
            "model": self.config.model,
            "messages": chat_messages,
            "max_tokens": max_tokens or self.config.max_tokens,
        }

        if system:
            kwargs["system"] = system

        self._apply_thinking_config(
            kwargs,
            extended_thinking=extended_thinking,
            thinking_budget=thinking_budget,
        )

        if tools:
            kwargs["tools"] = [t.to_anthropic_format() for t in tools]
            if tool_choice:
                if tool_choice == "auto":
                    kwargs["tool_choice"] = {"type": "auto"}
                elif tool_choice == "none":
                    del kwargs["tools"]
                elif tool_choice == "required":
                    kwargs["tool_choice"] = {"type": "any"}
                else:
                    kwargs["tool_choice"] = {"type": "tool", "name": tool_choice}

        # Stream the response
        content_parts = []
        tool_calls = []
        thinking_content = []
        current_tool_input = {}
        current_tool_id = None
        current_tool_name = None
        usage = {}
        in_thinking_block = False
        in_text_block = False

        with self.client.messages.stream(**kwargs) as stream:
            for event in stream:
                # Get event type as string (handles both string and enum types)
                event_type = str(getattr(event, 'type', ''))

                if event_type == 'content_block_start':
                    block = getattr(event, 'content_block', None)
                    if block:
                        block_type = str(getattr(block, 'type', ''))
                        if block_type == 'thinking':
                            in_thinking_block = True
                            if on_event:
                                on_event(StreamEvent(type=StreamEventType.THINKING_START))
                        elif block_type == 'text':
                            in_text_block = True
                            if on_event:
                                on_event(StreamEvent(type=StreamEventType.TEXT_START))
                        elif block_type == 'tool_use':
                            current_tool_id = getattr(block, 'id', None)
                            current_tool_name = getattr(block, 'name', None)
                            current_tool_input = {}
                            if on_event:
                                on_event(StreamEvent(
                                    type=StreamEventType.TOOL_USE_START,
                                    content=current_tool_name or ''
                                ))

                elif event_type == 'content_block_delta':
                    delta = getattr(event, 'delta', None)
                    if delta:
                        delta_type = str(getattr(delta, 'type', ''))
                        if delta_type == 'thinking_delta':
                            thinking_text = getattr(delta, 'thinking', '')
                            if thinking_text:
                                thinking_content.append(thinking_text)
                                if on_event:
                                    on_event(StreamEvent(
                                        type=StreamEventType.THINKING_DELTA,
                                        content=thinking_text
                                    ))
                        elif delta_type == 'text_delta':
                            text = getattr(delta, 'text', '')
                            if text:
                                content_parts.append(text)
                                if on_event:
                                    on_event(StreamEvent(
                                        type=StreamEventType.TEXT_DELTA,
                                        content=text
                                    ))
                        elif delta_type == 'input_json_delta':
                            # Accumulate tool input JSON
                            pass  # We'll get the full input from final message

                elif event_type == 'content_block_stop':
                    # Emit end events based on what block type was active
                    if in_thinking_block:
                        in_thinking_block = False
                        if on_event:
                            on_event(StreamEvent(type=StreamEventType.THINKING_END))
                    if in_text_block:
                        in_text_block = False
                        if on_event:
                            on_event(StreamEvent(type=StreamEventType.TEXT_END))

                elif event_type == 'message_stop':
                    if on_event:
                        on_event(StreamEvent(type=StreamEventType.MESSAGE_END))

                elif event_type == 'message_delta':
                    # Get usage info
                    event_usage = getattr(event, 'usage', None)
                    if event_usage:
                        usage["completion_tokens"] = getattr(event_usage, 'output_tokens', 0)

            # Get the final message
            final_message = stream.get_final_message()

        # Parse tool calls and thinking blocks from final message
        thinking_blocks = []
        for block in final_message.content:
            if block.type == "tool_use":
                tool_calls.append(ToolCall(
                    id=block.id,
                    name=block.name,
                    arguments=block.input if isinstance(block.input, dict) else {},
                ))
                if on_event:
                    on_event(StreamEvent(
                        type=StreamEventType.TOOL_USE_END,
                        tool_call=tool_calls[-1]
                    ))
            elif block.type == "thinking":
                # Capture full thinking block with signature
                thinking_blocks.append({
                    "type": "thinking",
                    "thinking": block.thinking,
                    "signature": getattr(block, 'signature', None),
                })

        content = "".join(content_parts) if content_parts else None
        thinking = "".join(thinking_content) if thinking_content else None

        # Build usage dict
        usage = {
            "prompt_tokens": final_message.usage.input_tokens,
            "completion_tokens": final_message.usage.output_tokens,
            "total_tokens": final_message.usage.input_tokens + final_message.usage.output_tokens,
        }

        thinking_tokens = 0
        if hasattr(final_message.usage, 'thinking_tokens'):
            thinking_tokens = final_message.usage.thinking_tokens
            usage["thinking_tokens"] = thinking_tokens

        # Determine finish reason
        finish_reason = "stop"
        if final_message.stop_reason == "tool_use":
            finish_reason = "tool_calls"
        elif final_message.stop_reason:
            finish_reason = final_message.stop_reason

        llm_response = LLMResponse(
            content=content,
            tool_calls=tool_calls,
            finish_reason=finish_reason,
            usage=usage,
            raw_response=final_message,
        )

        llm_response.thinking_tokens = thinking_tokens
        if thinking:
            llm_response.thinking_content = thinking
        # Attach full thinking blocks with signatures (needed for multi-turn conversations)
        if thinking_blocks:
            llm_response.thinking_blocks = thinking_blocks

        return llm_response

    def chat_with_json_output(
        self,
        messages: list[dict],
        schema: Optional[dict] = None,
    ) -> dict:
        """
        Send a chat request expecting JSON output.
        
        Args:
            messages: List of message dicts
            schema: Optional JSON schema
        
        Returns:
            Parsed JSON response
        """
        # Add JSON instruction
        json_instruction = "You must respond with valid JSON only, no other text."
        if schema:
            json_instruction += f"\n\nExpected schema:\n```json\n{json.dumps(schema, indent=2)}\n```"
        
        messages = messages.copy()
        if messages and messages[0]["role"] == "system":
            messages[0] = {
                **messages[0],
                "content": messages[0]["content"] + "\n\n" + json_instruction
            }
        else:
            messages.insert(0, {
                "role": "system",
                "content": json_instruction
            })
        
        # Add a prompt to encourage JSON output
        if messages and messages[-1]["role"] == "user":
            messages[-1] = {
                **messages[-1],
                "content": messages[-1]["content"] + "\n\nRespond with JSON only."
            }
        
        response = self.chat(messages)
        
        # Parse JSON from response
        content = response.content or ""
        
        # Try to extract JSON from the response
        content = content.strip()
        if content.startswith("```json"):
            content = content[7:]
        if content.startswith("```"):
            content = content[3:]
        if content.endswith("```"):
            content = content[:-3]
        content = content.strip()
        
        return json.loads(content)
    
    def _convert_message(self, msg: dict) -> dict:
        """Convert message to Anthropic format."""
        role = msg["role"]
        
        if role == "tool":
            # Tool result message
            return {
                "role": "user",
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": msg.get("tool_call_id", msg.get("tool_use_id")),
                        "content": msg["content"],
                    }
                ]
            }
        elif role == "assistant" and "tool_calls" in msg:
            # Assistant message with tool calls
            content = []
            if msg.get("content"):
                content.append({"type": "text", "text": msg["content"]})
            for tc in msg["tool_calls"]:
                content.append({
                    "type": "tool_use",
                    "id": tc["id"],
                    "name": tc["name"],
                    "input": tc["arguments"] if isinstance(tc["arguments"], dict) else json.loads(tc["arguments"]),
                })
            return {"role": "assistant", "content": content}
        else:
            return {"role": role, "content": msg["content"]}
    
    def _parse_response(self, response, extended_thinking: bool = False) -> LLMResponse:
        """Parse Anthropic response into LLMResponse."""
        content_parts = []
        tool_calls = []
        thinking_content = []
        thinking_blocks = []  # Full thinking blocks with signatures

        for block in response.content:
            if block.type == "text":
                content_parts.append(block.text)
            elif block.type == "tool_use":
                tool_calls.append(ToolCall(
                    id=block.id,
                    name=block.name,
                    arguments=block.input if isinstance(block.input, dict) else {},
                ))
            elif block.type == "thinking":
                # Extended thinking block - capture full block with signature
                thinking_content.append(block.thinking)
                thinking_blocks.append({
                    "type": "thinking",
                    "thinking": block.thinking,
                    "signature": getattr(block, 'signature', None),
                })

        content = "\n".join(content_parts) if content_parts else None

        # Determine finish reason
        finish_reason = "stop"
        if response.stop_reason == "tool_use":
            finish_reason = "tool_calls"
        elif response.stop_reason:
            finish_reason = response.stop_reason

        usage = {
            "prompt_tokens": response.usage.input_tokens,
            "completion_tokens": response.usage.output_tokens,
            "total_tokens": response.usage.input_tokens + response.usage.output_tokens,
        }

        # Track thinking tokens if available
        thinking_tokens = 0
        if hasattr(response.usage, 'thinking_tokens'):
            thinking_tokens = response.usage.thinking_tokens
            usage["thinking_tokens"] = thinking_tokens

        llm_response = LLMResponse(
            content=content,
            tool_calls=tool_calls,
            finish_reason=finish_reason,
            usage=usage,
            raw_response=response,
        )

        # Attach thinking tokens as attribute
        llm_response.thinking_tokens = thinking_tokens
        if thinking_content:
            llm_response.thinking_content = "\n".join(thinking_content)
        # Attach full thinking blocks with signatures (needed for multi-turn conversations)
        if thinking_blocks:
            llm_response.thinking_blocks = thinking_blocks

        return llm_response
    
    def format_tool_result(
        self,
        tool_call_id: str,
        result: Any,
    ) -> dict:
        """Format tool result for Anthropic message format."""
        content = json.dumps(result) if not isinstance(result, str) else result
        return {
            "role": "tool",
            "tool_use_id": tool_call_id,
            "content": content,
        }
    
    def format_assistant_tool_calls(
        self,
        tool_calls: list[ToolCall],
        content: Optional[str] = None,
    ) -> dict:
        """
        Format assistant message with tool calls for message history.
        """
        return {
            "role": "assistant",
            "content": content,
            "tool_calls": [
                {
                    "id": tc.id,
                    "name": tc.name,
                    "arguments": tc.arguments,
                }
                for tc in tool_calls
            ]
        }
