"""
Chat API endpoints with SSE streaming.
"""

import json
import asyncio
import base64
from typing import AsyncGenerator, Optional, List
from fastapi import APIRouter, HTTPException, Query, Form, UploadFile, File
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from sim.webapp.backend.session_manager import session_manager
from sim.rca.llm import StreamEvent, StreamEventType

router = APIRouter()


# Processed file content for AI
class ProcessedFile:
    def __init__(self, name: str, file_type: str, media_type: str, data: str):
        self.name = name
        self.file_type = file_type  # 'csv' or 'image'
        self.media_type = media_type
        self.data = data  # text for CSV, base64 for images


class ChatRequest(BaseModel):
    """Request body for chat."""
    message: str


class ChatResponse(BaseModel):
    """Response for non-streaming chat."""
    response: str
    session_id: str


def stream_event_to_dict(event: StreamEvent) -> dict:
    """Convert StreamEvent to dictionary for JSON serialization."""
    data = {
        "type": event.type.value,
        "content": event.content or "",
    }
    if event.tool_call:
        data["tool_call"] = {
            "id": event.tool_call.id,
            "name": event.tool_call.name,
            "arguments": event.tool_call.arguments,
        }
    # Include tool result for TOOL_RESULT events
    if event.tool_call_id:
        data["tool_call_id"] = event.tool_call_id
    if event.tool_result:
        data["tool_result"] = event.tool_result
    return data


async def generate_chat_stream(
    session_id: str,
    message: str,
    is_first_message: bool = False,
    processed_files: List[ProcessedFile] = None,
) -> AsyncGenerator[str, None]:
    """
    Generate SSE events for chat streaming.

    Yields SSE-formatted events for:
    - thinking_start/delta/end
    - text_start/delta/end
    - tool_use_start/end
    - message_end
    """
    session = session_manager.get_session(session_id)
    if not session or not session.engine:
        yield f"data: {json.dumps({'type': 'error', 'content': 'Session not found'})}\n\n"
        return

    engine = session.engine
    repo_path = session.metadata.get("repo_path")

    # Track events to yield
    events_queue: asyncio.Queue = asyncio.Queue()
    response_content = ""
    is_complete = False

    def on_stream(event: StreamEvent):
        """Callback for stream events."""
        nonlocal response_content
        if event.type == StreamEventType.TEXT_DELTA:
            response_content += event.content or ""
        events_queue.put_nowait(event)

    # Run chat in background thread since it's blocking
    async def run_chat():
        nonlocal is_complete, response_content
        try:
            if is_first_message:
                # First message - need to start analysis
                # Build initial data for the engine
                data_source = engine.data_source
                if data_source:
                    # Fetch current metrics for autonomous assessment
                    from datetime import datetime, timedelta, timezone
                    now = datetime.now(timezone.utc)
                    baseline_start = now - timedelta(minutes=30)
                    recent_start = now - timedelta(minutes=10)

                    # Get data - the engine will use tools to assess
                    initial_data = data_source.get_data_for_time_range(
                        incident_start=recent_start,
                        incident_end=now,
                        baseline_start=baseline_start,
                        baseline_end=recent_start,
                    )
                    # Add user message context
                    initial_data["user_message"] = message

                    # Build initial message
                    initial_message = build_initial_message(message, initial_data)

                    # Initialize engine message history with user's question
                    engine._message_history = [
                        {"role": "system", "content": engine._system_prompt},
                        {"role": "user", "content": initial_message},
                    ]

                    # Get tool definitions
                    tool_definitions = None
                    if engine.tools:
                        tool_definitions = engine.tools.get_tool_definitions()

                    # Run the analysis loop
                    response = engine._call_llm(
                        messages=engine._message_history,
                        tool_definitions=tool_definitions,
                    )

                    # Process tool calls
                    iteration = 0
                    max_iterations = engine.config.max_tool_iterations

                    while response.has_tool_calls and iteration < max_iterations:
                        iteration += 1
                        engine._current_iteration = iteration

                        assistant_message = engine._build_assistant_tool_message(response)
                        engine._message_history.append(assistant_message)

                        tool_messages = engine._execute_tools(response.tool_calls)
                        engine._message_history.extend(tool_messages)

                        response = engine._call_llm(
                            messages=engine._message_history,
                            tool_definitions=tool_definitions,
                        )

                    response_content = response.content or ""
            else:
                # Follow-up message
                response_content = engine.chat(message, stream=True, on_stream=on_stream)
        except Exception as e:
            events_queue.put_nowait(StreamEvent(
                type=StreamEventType.MESSAGE_END,
                content=f"Error: {str(e)}"
            ))
        finally:
            is_complete = True
            events_queue.put_nowait(None)  # Signal completion

    # Start chat task
    chat_task = asyncio.create_task(asyncio.to_thread(run_chat_sync, engine, message, is_first_message, events_queue, repo_path, processed_files))

    # Yield events as they arrive
    try:
        while True:
            try:
                event = await asyncio.wait_for(events_queue.get(), timeout=0.1)
                if event is None:
                    break
                yield f"data: {json.dumps(stream_event_to_dict(event))}\n\n"
            except asyncio.TimeoutError:
                if is_complete:
                    break
                continue
    except Exception as e:
        yield f"data: {json.dumps({'type': 'error', 'content': str(e)})}\n\n"

    # Wait for chat task to complete and get results
    result = await chat_task
    if isinstance(result, tuple):
        response_content, blocks = result
    else:
        response_content = result
        blocks = []

    # Store assistant response in session history with structured blocks
    if response_content:
        session_manager.add_message(session_id, "assistant", response_content, blocks=blocks)

    # Send final done event
    yield f"data: {json.dumps({'type': 'done', 'content': response_content})}\n\n"


def run_chat_sync(engine, message: str, is_first_message: bool, events_queue: asyncio.Queue, repo_path: str = None, processed_files: List[ProcessedFile] = None):
    """
    Synchronous wrapper for chat that puts events in queue.

    Returns:
        Tuple of (response_content, blocks) where blocks is a list of structured
        blocks for storage (thinking, tool calls, text).
    """
    response_content = ""
    blocks = []  # Structured blocks for storage
    current_thinking = ""
    current_text = ""
    current_tool = None

    def on_stream(event: StreamEvent):
        nonlocal response_content, current_thinking, current_text, current_tool, blocks

        # Put event in queue (thread-safe)
        try:
            events_queue.put_nowait(event)
        except Exception:
            pass

        # Capture blocks for storage
        if event.type == StreamEventType.THINKING_START:
            current_thinking = ""
        elif event.type == StreamEventType.THINKING_DELTA:
            current_thinking += event.content or ""
        elif event.type == StreamEventType.THINKING_END:
            if current_thinking:
                blocks.append({
                    "type": "thinking",
                    "content": current_thinking,
                })
            current_thinking = ""
        elif event.type == StreamEventType.TEXT_START:
            current_text = ""
        elif event.type == StreamEventType.TEXT_DELTA:
            response_content += event.content or ""
            current_text += event.content or ""
        elif event.type == StreamEventType.TEXT_END:
            if current_text:
                blocks.append({
                    "type": "text",
                    "content": current_text,
                })
            current_text = ""
        elif event.type == StreamEventType.TOOL_USE_START:
            current_tool = {"name": event.content}
        elif event.type == StreamEventType.TOOL_USE_END:
            if event.tool_call:
                tool_block = {
                    "type": "tool",
                    "id": event.tool_call.id,
                    "name": event.tool_call.name,
                    "arguments": event.tool_call.arguments,
                }
                blocks.append(tool_block)
                current_tool = tool_block
        elif event.type == StreamEventType.TOOL_RESULT:
            # Find and update the matching tool block with result
            if event.tool_call_id and event.tool_result:
                for block in blocks:
                    if block.get("type") == "tool" and block.get("id") == event.tool_call_id:
                        block["result"] = event.tool_result
                        break

    try:
        if is_first_message:
            # First message - start session with available context
            from datetime import datetime, timedelta, timezone
            data_source = engine.data_source
            now = datetime.now(timezone.utc)

            if data_source:
                baseline_start = now - timedelta(minutes=30)
                recent_start = now - timedelta(minutes=10)
                initial_data = data_source.get_data_for_time_range(
                    incident_start=recent_start,
                    incident_end=now,
                    baseline_start=baseline_start,
                    baseline_end=recent_start,
                )
                initial_data["monitoring_available"] = True
            else:
                initial_data = {
                    "monitoring_available": False,
                    "collected_at": now.isoformat(),
                    "note": (
                        "No ClickHouse monitoring backend is configured for this session. "
                        "Use direct SQL Server diagnostic tools (run_sp_blitz, get_server_config) "
                        "and ask targeted follow-up questions."
                    ),
                }

            initial_data["user_message"] = message
            initial_message = build_initial_message(message, initial_data, repo_path, processed_files)

            # Extract images for multimodal API
            images = None
            if processed_files:
                image_files = [f for f in processed_files if f.file_type == 'image']
                if image_files:
                    images = [{"media_type": f.media_type, "data": f.data} for f in image_files]

            response_content = engine.start_session(
                initial_message=initial_message,
                stream=True,
                on_stream=on_stream,
                images=images,
            )

            # Add final text block if not captured via streaming
            if response_content and (not blocks or blocks[-1].get("type") != "text"):
                blocks.append({
                    "type": "text",
                    "content": response_content,
                })
        else:
            # Follow-up message - include any uploaded files
            enhanced_message = message
            images = None
            if processed_files:
                csv_files = [f for f in processed_files if f.file_type == 'csv']
                if csv_files:
                    enhanced_message += "\n\n## Uploaded CSV Data:\n"
                    for csv_file in csv_files:
                        enhanced_message += f"\n### {csv_file.name}\n```csv\n{csv_file.data}\n```\n"

                image_files = [f for f in processed_files if f.file_type == 'image']
                if image_files:
                    enhanced_message += "\n\n## Uploaded Chart Images:\nI've attached chart images for your analysis. Please describe what you observe.\n"
                    images = [{"media_type": f.media_type, "data": f.data} for f in image_files]

            response_content = engine.chat(enhanced_message, stream=True, on_stream=on_stream, images=images)

        return response_content, blocks
    except Exception as e:
        events_queue.put_nowait(StreamEvent(
            type=StreamEventType.MESSAGE_END,
            content=f"Error: {str(e)}"
        ))
        return f"Error: {str(e)}", blocks
    finally:
        events_queue.put_nowait(None)


def build_initial_message(user_message: str, data: dict, repo_path: str = None, processed_files: List[ProcessedFile] = None) -> str:
    """Build initial message for the engine with user's question and data context."""
    # Sanitize data - remove scenario/name hints
    sanitized = {k: v for k, v in data.items() if k not in ('scenario', 'name', 'user_message')}
    data_json = json.dumps(sanitized, indent=2, default=str)
    monitoring_available = bool(sanitized.get("monitoring_available", False))
    monitoring_line = (
        "Monitoring backend: enabled (time-window and baseline comparison tools are available)."
        if monitoring_available
        else "Monitoring backend: disabled (use direct SQL Server diagnostics tools only)."
    )

    # Build uploaded files section
    uploaded_files_section = ""
    if processed_files:
        csv_files = [f for f in processed_files if f.file_type == 'csv']
        image_files = [f for f in processed_files if f.file_type == 'image']

        if csv_files:
            uploaded_files_section += "\n\n## Uploaded CSV Data\n\nThe user has provided the following CSV files for analysis:\n"
            for csv_file in csv_files:
                uploaded_files_section += f"\n### {csv_file.name}\n```csv\n{csv_file.data}\n```\n"

        if image_files:
            uploaded_files_section += "\n\n## Uploaded Chart Images\n\nThe user has provided chart images. These images have been included in this message for your visual analysis. Please describe what you observe in these charts and incorporate your observations into your analysis.\n"

    # Add code analysis context if repo_path is available
    code_analysis_section = ""
    if repo_path:
        code_analysis_section = f"""

## Code Analysis Available

An application repository is available for code analysis at: `{repo_path}`

You have access to code analysis tools that can help correlate database issues with application code:
- `analyze_code_impact(slow_query, table_name, repo_path)` - Find which application features are affected by a slow query
- `correlate_incident(incident_time, affected_table, repo_path)` - Correlate incident with recent code/schema changes
- `find_query_origin(sql_pattern, repo_path)` - Find where a SQL query originates in the code
- `analyze_orm_patterns(repo_path)` - Detect ORM anti-patterns causing performance issues

When investigating queries, consider using these tools to understand the application-side impact and potential code-level causes.
"""

    return f"""The user has come to you with a database performance concern:

**User's Question:** {user_message}
{uploaded_files_section}
---

## Current Database Metrics

Here is the current diagnostic context for this session.
{monitoring_line}

```json
{data_json}
```
{code_analysis_section}
---

## Your Task

1. **Assess the situation first** using `compare_baseline()` to determine if this is an active incident or a chronic health issue.

2. If the user's question is vague, ask clarifying questions:
   - When did they first notice the issue?
   - Is it affecting specific queries or everything?
   - Did anything change recently?

3. Investigate using the appropriate tools based on your assessment.

4. Provide actionable recommendations.

Start by assessing whether there's been a recent change in database performance.
"""


@router.post("/stream")
async def chat_stream(
    session_id: str = Form(..., description="Session ID"),
    message: str = Form(..., description="User message"),
    files: Optional[List[UploadFile]] = File(None),
):
    """
    Stream chat response via SSE.

    Accepts multipart form data with:
    - session_id: The session ID
    - message: User message text
    - files: Optional uploaded files (CSV or images)

    Returns Server-Sent Events with:
    - thinking_start/delta/end: Agent's thinking process
    - tool_use_start/end: Tool calls being made
    - text_start/delta/end: Response text
    - done: Final response
    """
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    # Process uploaded files
    processed_files: List[ProcessedFile] = []
    for file in (files or []):
        if file.filename:
            content = await file.read()
            if file.content_type and file.content_type.startswith('image/'):
                # Encode image as base64 for Claude's vision
                processed_files.append(ProcessedFile(
                    name=file.filename,
                    file_type='image',
                    media_type=file.content_type,
                    data=base64.b64encode(content).decode('utf-8'),
                ))
            elif file.filename.endswith('.csv'):
                # Parse CSV as text
                processed_files.append(ProcessedFile(
                    name=file.filename,
                    file_type='csv',
                    media_type='text/csv',
                    data=content.decode('utf-8'),
                ))

    # Check if this is the first message
    is_first = len(session.message_history) == 0

    # Add user message to history
    session_manager.add_message(session_id, "user", message)

    return StreamingResponse(
        generate_chat_stream(session_id, message, is_first, processed_files),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.post("/{session_id}")
async def chat_sync(session_id: str, request: ChatRequest):
    """
    Synchronous chat endpoint (non-streaming).

    Use /stream for streaming responses.
    """
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    engine = session_manager.get_engine(session_id)
    if not engine:
        raise HTTPException(status_code=500, detail="Engine not available")

    is_first = len(session.message_history) == 0
    session_manager.add_message(session_id, "user", request.message)

    try:
        if is_first:
            # For first message, we need to do the full analysis setup
            # This is simplified - use streaming endpoint for full functionality
            response = "Please use the streaming endpoint (/api/chat/stream) for the initial analysis."
        else:
            response = engine.chat(request.message, stream=False)

        session_manager.add_message(session_id, "assistant", response)

        return ChatResponse(
            response=response,
            session_id=session_id,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
