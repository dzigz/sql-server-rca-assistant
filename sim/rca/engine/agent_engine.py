"""
Agent-style RCA Engine with Extended Thinking.

This module implements a single-loop reasoning engine that operates like
a coding agent (similar to Claude Code) but for database performance analysis.

Key differences from the legacy pipeline:
- Single continuous conversation instead of 4 separate LLM calls
- Extended thinking enabled for complex reasoning
- Tools available throughout reasoning (not just in investigation phase)
- Agent decides when to investigate and when to conclude
- Self-correcting with evidence validation
- Streaming support for real-time thinking and response output
"""

import json
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional, Union

from sim.rca.config import RCAConfig
from sim.rca.llm import create_llm_client, LLMClient, StreamEvent, StreamEventType
from sim.rca.datasources import ClickHouseDataSource
from sim.rca.tools import ToolRegistry


# Type alias for stream callback
StreamCallback = Callable[[StreamEvent], None]


# =============================================================================
# System Prompt
# =============================================================================

AGENT_SYSTEM_PROMPT = """You are an expert SQL Server performance analyst. You diagnose database
performance incidents with the same methodology as a senior DBA.

Your diagnostic data comes from ClickHouse, a time-series database that continuously
collects SQL Server metrics via OpenTelemetry and custom DMV collectors.

## Data Source: ClickHouse Time-Series Database

### Available Data Tables

1. **wait_stats** - Wait type deltas collected every 5-10 seconds
   - Key columns: wait_type, wait_time_ms, waiting_tasks_count
   - Use for: Identifying bottleneck wait types

2. **blocking_chains** - Blocking relationships captured in real-time
   - Shows complete blocking tree with head blockers
   - Key columns: blocking_level, session_id, blocking_session_id, wait_type, sql_text
   - Use for: Lock contention analysis

3. **memory_grants** - Memory grant status for active queries
   - Detects: WAITING (RESOURCE_SEMAPHORE), SPILLED, SPILL_LIKELY
   - Key columns: grant_status, requested_mb, granted_mb, max_used_mb
   - Use for: Memory pressure and tempdb spill detection

4. **query_stats** - Top queries by resource consumption
   - Captured from dm_exec_query_stats
   - Key columns: query_hash, execution_count, total_worker_time_us, total_logical_reads
   - Use for: Identifying expensive queries

5. **blitz_results** - First Responder Kit findings
   - All 5 Blitz scripts (BlitzFirst, BlitzCache, BlitzWho, BlitzIndex, BlitzLock) run during incidents
   - Key columns: blitz_type, priority, finding, details
   - Use for: Industry-standard diagnostics

6. **schedulers** - CPU scheduler health
   - Detects CPU pressure via runnable_tasks_count > 0
   - Key columns: scheduler_id, runnable_tasks_count, current_tasks_count
   - Use for: CPU saturation detection

7. **file_stats** - I/O latency and throughput
   - Key columns: database_name, io_stall_read_ms, io_stall_write_ms
   - Use for: Storage performance analysis

8. **missing_indexes** - Missing index recommendations
   - Key columns: table_name, equality_columns, impact_score
   - Use for: Index optimization

## Your Investigation Approach

1. **START WITH COMPARE_BASELINE**
   Use `compare_baseline()` to see what changed from baseline to incident period.
   Performance issues are caused by CHANGES - focus on deltas.

2. **CHECK BLITZ FINDINGS**
   Review `blitz_results` for priority findings from all 5 Blitz scripts.
   Priority 1-10 are critical, 11-50 are high-impact.

3. **FORM HYPOTHESES**
   Based on evidence, form 2-3 ranked hypotheses with confidence levels.
   Consider: blocking, missing index, plan regression, resource exhaustion.

4. **DRILL INTO SPECIFIC METRICS**
   Use `query_clickhouse()` to examine wait_stats, blocking_chains, memory_grants.
   Cross-reference: match wait types to blocking/memory issues.

5. **VALIDATE BEFORE CONCLUDING**
   Check if evidence supports the hypothesis. Look for alternative explanations.
   Verify causation, not just correlation.

6. **FORM A CAUSAL CHAIN**
   Connect trigger → intermediate effects → symptoms.

## Investigation Tools

### Baseline Comparison
- `compare_baseline()` - Shows what's different between baseline and incident.
  Returns wait deltas, blocking comparison, memory grant changes.

### ClickHouse Query
- `query_clickhouse(table, filters, order_by, limit)` - Query any ClickHouse table.
  Tables: wait_stats, blocking_chains, memory_grants, query_stats, schedulers,
  file_stats, missing_indexes, blitz_results

### Blitz Diagnostics (for active incidents)
- `run_blitz_diagnostics(script)` - Run First Responder Kit diagnostics on-demand.
  Scripts: first, cache, who, index, lock, or all (default)
  Use this during active incidents to capture real-time server state.

### Query Investigation
- `get_query_details(query_hash)` - Full details for a specific query including
  execution stats, plan info, and wait breakdown.

### Code Analysis Tools (ONLY if repo_path is provided)

The following tools are available ONLY when the user provides an application repository path.
**DO NOT attempt to use these tools if repo_path is not in the incident data.**

- `analyze_code_impact(slow_query, table_name, repo_path)` - Find which application code
  paths are affected by a slow query. Returns affected files, endpoints, ORM mappings.

- `correlate_incident(incident_time, affected_table, repo_path, lookback_days)` - Correlate
  the incident with recent code commits, schema migrations, and config changes.

- `find_query_origin(query_hash, sql_pattern, repo_path)` - Find where a query originates
  in the application code (useful when you identify a problematic query in query_stats).

- `analyze_orm_patterns(table_name, repo_path)` - Detect N+1 queries and other ORM
  anti-patterns that could cause performance issues.

**Important**: Check for `repo_path` in the incident data before using these tools.
If repo_path is not available and code analysis would be helpful, inform the user:
"To analyze application code impact, I would need the path to your application repository.
You can provide this via --repo-path when running the analysis, or tell me the path now."

## Common Root Cause Patterns

- **Missing Index**: High improvement_score in missing indexes, table scans
  in blocking query plans
- **Statistics Stale**: Query plan regression after data growth, outdated
  modification counters
- **Blocking Chain**: LCK_M_* waits spiking, clear head blocker identified
- **Memory Pressure**: RESOURCE_SEMAPHORE waits, memory clerk growth
- **I/O Bottleneck**: PAGEIOLATCH_* waits, high read/write latency
- **CPU Saturation**: SOS_SCHEDULER_YIELD, high runnable queue counts
- **Plan Regression**: Query store shows plan change, before/after performance
  differs significantly

## Output Format

After investigation, provide a structured analysis as JSON:

```json
{
  "root_cause": {
    "category": "missing_index|blocking|plan_regression|statistics_stale|memory_pressure|io_bottleneck|cpu_saturation",
    "summary": "One sentence description",
    "confidence": 0.0-1.0,
    "entity": "Table/query/resource affected"
  },
  "causal_chain": [
    {"event": "Trigger", "description": "What initiated the issue"},
    {"event": "Effect", "description": "What happened as a result"},
    {"event": "Symptom", "description": "What the user observed"}
  ],
  "evidence": [
    {"source": "tool_name", "finding": "What you found"}
  ],
  "mitigation": [
    "Immediate step to resolve",
    "Follow-up actions"
  ],
  "prevention": [
    "How to prevent recurrence"
  ]
}
```

## Important Guidelines

- **Start broad, then narrow**: Don't fixate on the first anomaly. Survey the
  landscape before diving deep.
- **Evidence over intuition**: Every conclusion should be backed by data from
  tools.
- **Consider the timeline**: When did the issue start? What changed around
  that time?
- **Check your assumptions**: If evidence contradicts your hypothesis, revise
  the hypothesis.
- **Be specific**: Reference actual query hashes, table names, wait types -
  not generic descriptions.

## False Signals to Avoid

- **"Plan Cache Erased Recently"**: If SQL Server was recently restarted or DBCC FREEPROCCACHE
  was run, all plans will appear "new" and compilations will spike. This does NOT indicate
  parameter sniffing or plan regression. Before concluding plan regression, verify that:
  1. The plan change correlates with the incident start time (not just SQL Server startup)
  2. Query Store shows an actual plan_id change for the affected query
  3. There is measurable performance difference between the old and new plan

- **"Statistics Updated Recently"**: Statistics updates are normal maintenance. Only suspect
  stats-related issues if you can show the update caused a plan change AND performance degraded.

## Memory Grant Interpretation

The `memory_grants` section shows query memory allocation status from dm_exec_query_memory_grants.
This is critical for detecting two types of issues:

### Grant Status Values
- **WAITING**: Query is blocked waiting for a memory grant (causes RESOURCE_SEMAPHORE waits)
- **SPILL_LIKELY**: Query received grant but `granted < required` - will likely spill to tempdb
- **SPILLED**: Query actually spilled (`max_used > granted`)
- **OK**: Query has sufficient memory grant

### Detection Patterns

**RESOURCE_SEMAPHORE (memory_grant_queue incident)**:
- Look for multiple queries with `grant_status = 'WAITING'`
- High `wait_time_ms` values indicate queries blocked waiting for memory
- Often accompanied by RESOURCE_SEMAPHORE in wait stats

**Tempdb Spills (tempdb_spill incident)**:
- Look for queries with `grant_status = 'SPILL_LIKELY'` or `'SPILLED'`
- `memory_deficit_mb > 0` shows how much memory was underestimated
- Often accompanied by PAGEIOLATCH waits on tempdb files

### Key Metrics
- `requested_mb`: What the optimizer asked for
- `granted_mb`: What was actually allocated
- `required_mb`: Minimum needed to avoid spills
- `max_used_mb`: Actual peak usage (if > granted, query spilled)
- `memory_deficit_mb`: `required - granted` (positive = spill risk)

## Wait Type Interpretation

When analyzing wait statistics, apply these principles:

1. **Focus on waits that changed** - Compare incident vs baseline. Background waits
   that are constant across both periods are usually not the root cause.

2. **Ignore infrastructure waits** - These are typically irrelevant to workload issues:
   - XE_* (Extended Events internals)
   - BROKER_* (Service Broker background, unless using queuing)
   - SLEEP_* (Background sleep timers)
   - QDS_* (Query Store maintenance)
   - HADR_* (Always On replication, unless investigating AG issues)

3. **Map waits to root cause categories**:
   | Wait Pattern | Likely Issue |
   |--------------|--------------|
   | LCK_M_* | Lock contention, blocking |
   | PAGEIOLATCH_* | I/O from disk reads - check for missing indexes |
   | ASYNC_NETWORK_IO | Large result sets - often indicates table scans |
   | CXPACKET | Parallelism issues - check for parameter sniffing |
   | SOS_SCHEDULER_YIELD | CPU pressure |
   | RESOURCE_SEMAPHORE | Memory grant waits |

4. **Validate with evidence** - Don't conclude based on wait types alone. Cross-reference
   with blocking_info, missing_indexes, and query patterns.

## Blitz Script Analysis (First Responder Kit)

The Feature Schema may include data from Brent Ozar's First Responder Kit (sp_BlitzFirst,
sp_BlitzCache, sp_BlitzWho, sp_BlitzIndex, sp_BlitzLock). This data provides industry-standard
diagnostics that complement the raw DMV telemetry.

### Delta Wait Stats (blitz_wait_stats_delta)
- These are REAL-TIME waits sampled over 5 seconds during the incident
- Unlike cumulative DMV waits, these show what's happening NOW
- **Use these as the PRIMARY wait signal** for RCA analysis
- High signal_wait_time_ms relative to wait_time_ms indicates CPU/scheduler pressure
- Low signal_wait_time_ms indicates resource waits (I/O, locks, memory)

### Query Plan Warnings (blitz_query_plan_warnings)
- `has_implicit_conversions`: Data type mismatch forcing index scans instead of seeks
  - Common cause: NVARCHAR parameter vs VARCHAR column, or INT vs BIGINT
  - Fix: Match parameter types to column types
- `has_spills`: Query used more memory than granted, spilled to tempdb
  - Indicates memory grant underestimation or parameter sniffing
  - Check total_spills count - higher = worse performance
- `min_max_cpu_ratio > 10`: Strong indicator of PARAMETER SNIFFING
  - Same query executes with vastly different CPU times
  - Cached plan optimized for atypical parameter value

### Index Analysis (blitz_index_analysis)
- `finding_type="missing_index"`: Includes ready-to-use CREATE INDEX statement
  - More reliable than dm_db_missing_index_details (considers actual usage patterns)
- `finding_type="unused_index"`: Index has writes but no reads
  - Candidate for removal to reduce write overhead
- `finding_type="duplicate_index"`: Multiple indexes on same columns

### Blitz Findings (blitz_findings)
- Priority 1-10: Critical issues requiring immediate attention
- Priority 11-50: High-impact issues
- Priority 51-100: Medium issues
- Use findings_group to categorize (e.g., "Wait Stats", "Query Plans", "Indexes")

### Active Sessions (blitz_active_sessions)
- Point-in-time snapshot of running queries
- open_transaction_count > 0 with long wait = potential blocker
- Cross-reference blocking_session_id with blocking_info

### Investigation Priority with Blitz Data
1. Check blitz_wait_stats_delta FIRST (real-time signal)
2. If LCK_M_* waits high → check blitz_active_sessions for blockers
3. If PAGEIOLATCH_* waits high → check blitz_file_stats for slow I/O
4. For slow queries → check blitz_query_plan_warnings for plan issues
5. Use blitz_index_analysis for missing/unused index recommendations with CREATE statements

## Blitz Data Availability and Fallbacks

IMPORTANT: For resource-intensive incidents (CPU pressure, memory exhaustion), Blitz diagnostic
queries may time out because they compete for the same resources as the incident workload.

### When Blitz Times Out
If you see `"fallback_used": true` in the data, this means:
1. Standard Blitz diagnostics couldn't complete (resource contention)
2. This itself is STRONG EVIDENCE of severe resource exhaustion
3. Lightweight DMV data was collected instead

### Interpreting Fallback Data

When fallback is used, you'll have:

**scheduler_status** (CPU pressure indicator):
- `runnable_tasks_count > 0` on multiple schedulers = CPU saturation
- High `current_tasks_count` = active query overload

**memory_grants** (RESOURCE_SEMAPHORE indicator):
- `pending > 0` = queries waiting for memory grants
- `requested_mb >> granted_mb` = memory pressure

**wait_stats_snapshot** (point-in-time waits):
- These are cumulative, compare to baseline deltas for actual incident waits
- Key waits: RESOURCE_SEMAPHORE, SOS_SCHEDULER_YIELD, LCK_M_*, PAGEIOLATCH_*

### Investigation Priority When Blitz Unavailable
1. The timeout itself confirms resource exhaustion - start with that hypothesis
2. Use scheduler_status to confirm CPU saturation
3. Use memory_grants to confirm memory pressure
4. Cross-reference with DMV-based wait deltas (always available in feature schema)
5. If blocking suspected, check blocking_info in feature schema (DMV-based, not Blitz)
"""


# =============================================================================
# Health Check System Prompt
# =============================================================================

HEALTH_CHECK_SYSTEM_PROMPT = """You are an expert SQL Server DBA performing a comprehensive health check.

Your goal is to assess the overall health of the database server and provide actionable
recommendations. Unlike incident analysis, there is no specific problem to diagnose -
you're doing a proactive health assessment.

## Data Sources

### sp_Blitz Results
sp_Blitz is a comprehensive server health check script that identifies configuration issues,
security concerns, and performance problems. Findings are prioritized:
- Priority 1-10: Critical (security vulnerabilities, corruption risks)
- Priority 11-50: High (performance issues, misconfigurations)
- Priority 51-100: Medium (best practice violations)
- Priority 100+: Informational

### ClickHouse Baseline Metrics
The DMV collector continuously captures baseline metrics:
- `wait_stats` - Wait type patterns during normal operation
- `query_stats` - Top queries by resource consumption
- `file_stats` - I/O latency and throughput
- `schedulers` - CPU health indicators
- `memory_grants` - Memory pressure indicators
- `missing_indexes` - Index recommendations

## Investigation Tools

### sp_Blitz Execution
- `run_sp_blitz(priority_threshold)` - Execute sp_Blitz and get health findings

### ClickHouse Query
- `query_clickhouse(table, filters, order_by, limit)` - Query baseline metrics
  Tables: wait_stats, query_stats, file_stats, schedulers, memory_grants, missing_indexes

### Code Analysis Tools (ONLY if repo_path is provided)

The following tools are available ONLY when the user provides an application repository path.
**DO NOT attempt to use these tools if repo_path is not in the health check data.**

- `analyze_code_impact(slow_query, table_name, repo_path)` - Find which application code
  paths are affected by a slow query.

- `analyze_orm_patterns(table_name, repo_path)` - Detect N+1 queries and other ORM
  anti-patterns proactively.

**Important**: Check for `repo_path` in the health check data before using these tools.
If repo_path is not available and code analysis would be helpful, inform the user:
"To analyze application code, I would need the path to your application repository.
You can provide this via --repo-path when running the health check, or tell me the path now."

## Areas to Evaluate

1. **Server Configuration**
   - Memory settings (max/min server memory)
   - Parallelism (MAXDOP, cost threshold for parallelism)
   - TempDB configuration (number of files, autogrowth)
   - Trace flags and startup parameters

2. **Security Concerns**
   - SA account status
   - Database compatibility levels
   - Deprecated features in use
   - Orphaned users and logins

3. **Performance Baseline**
   - Normal wait patterns (what's typical for this workload)
   - Query performance trends
   - I/O latency patterns
   - CPU utilization patterns

4. **Index Health**
   - Missing indexes with high impact
   - Unused indexes (write overhead)
   - Fragmentation levels
   - Index maintenance status

5. **Capacity & Growth**
   - Database file sizes and growth settings
   - Disk space availability
   - Log file management

6. **Best Practices**
   - Database options (auto-close, auto-shrink - should be OFF)
   - Recovery model alignment with backup strategy
   - Statistics maintenance

## Output Format

Provide a health assessment as JSON:

```json
{
  "health_score": 0-100,
  "summary": "Overall assessment in 1-2 sentences",
  "critical_findings": [
    {"finding": "Description", "impact": "Why it matters", "remediation": "How to fix"}
  ],
  "warnings": [
    {"finding": "Description", "impact": "Why it matters", "remediation": "How to fix"}
  ],
  "recommendations": [
    {"finding": "Description", "impact": "Benefit of fixing", "remediation": "How to fix"}
  ],
  "healthy_areas": ["Area 1 is well configured", "Area 2 looks good"]
}
```

## Guidelines

- Start with sp_Blitz findings - they provide expert-level diagnostics
- Cross-reference with baseline metrics for context
- Prioritize findings by business impact
- Provide specific, actionable remediation steps
- Acknowledge what's working well, not just problems
- Consider the workload context when making recommendations
"""


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class AgentRCAReport:
    """Report generated by the agent RCA engine."""
    root_cause: dict
    causal_chain: list[dict]
    evidence: list[dict]
    mitigation: list[str]
    prevention: list[str]

    # Metadata
    analysis_duration_seconds: float
    tool_calls_made: list[dict]
    tool_failure_summary: dict
    thinking_tokens_used: int
    model_used: str

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        # Derive severity from confidence level
        confidence = self.root_cause.get("confidence", 0)
        if isinstance(confidence, float):
            if confidence >= 0.8:
                severity = "high"
            elif confidence >= 0.5:
                severity = "medium"
            elif confidence > 0:
                severity = "low"
            else:
                severity = "unknown"
        else:
            severity = "unknown"

        return {
            "root_cause": self.root_cause,
            # Convenience fields for CLI display
            "primary_root_cause": self.root_cause.get("summary", "Unknown"),
            "severity": severity,
            "causal_chain": self.causal_chain,
            "evidence": self.evidence,
            "mitigation": self.mitigation,
            "prevention": self.prevention,
            "metadata": {
                "analysis_duration_seconds": self.analysis_duration_seconds,
                "tool_calls_made": self.tool_calls_made,
                "tool_failure_summary": self.tool_failure_summary,
                "thinking_tokens_used": self.thinking_tokens_used,
                "model_used": self.model_used,
            }
        }

    def to_json(self, indent: int = 2) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=indent, default=str)

    def save(self, path: Path) -> None:
        """Save report to file."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            f.write(self.to_json())


# =============================================================================
# Agent Engine
# =============================================================================

class AgentRCAEngine:
    """
    Single-loop agent-style RCA engine with extended thinking.

    This engine operates like a coding agent:
    - Single system prompt with clear methodology
    - Extended thinking for complex reasoning
    - Tools available throughout the conversation
    - Self-correcting with evidence validation
    - Streaming support for real-time output

    Data comes from ClickHouse time-series database via ClickHouseDataSource.
    """

    def __init__(
        self,
        config: RCAConfig,
        tools: Optional[ToolRegistry] = None,
        data_source: Optional[ClickHouseDataSource] = None,
        llm_client: Optional[LLMClient] = None,
        system_prompt: Optional[str] = None,
        initial_message_builder: Optional[Callable[[dict], str]] = None,
        analysis_mode: str = "incident",
    ):
        """
        Initialize the agent engine.

        Args:
            config: RCA configuration
            tools: Tool registry for investigation tools
            data_source: ClickHouse data source for fetching incident data
            llm_client: Optional LLM client (created from config if not provided)
            system_prompt: Custom system prompt (uses AGENT_SYSTEM_PROMPT if not provided)
            initial_message_builder: Optional function to build initial user message from data
            analysis_mode: "incident" for RCA or "health_check" for health assessment
        """
        self.config = config
        self.tools = tools
        self.data_source = data_source
        self.llm = llm_client or create_llm_client(config)
        self._system_prompt = system_prompt or AGENT_SYSTEM_PROMPT
        self._initial_message_builder = initial_message_builder
        self._analysis_mode = analysis_mode

        # Track tool calls for reporting
        self._tool_calls: list[dict] = []
        self._thinking_tokens: int = 0

        # Streaming state
        self._stream_callback: Optional[StreamCallback] = None
        self._in_thinking_block: bool = False
        self._in_text_block: bool = False
        self._current_iteration: int = 0
        self._suppress_final_json: bool = True

        # Chat mode state - persists message history for follow-up questions
        self._message_history: list[dict] = []

    def analyze(
        self,
        incident_id: Optional[str] = None,
        incident_data: Optional[dict] = None,
        stream: bool = False,
        on_stream: Optional[StreamCallback] = None,
    ) -> AgentRCAReport:
        """
        Run the agent loop to analyze an incident.

        Args:
            incident_id: Incident ID to fetch from ClickHouse (if data_source provided)
            incident_data: Pre-fetched incident data dict (alternative to incident_id)
            stream: Enable streaming output of thinking and response
            on_stream: Callback for streaming events (uses default printer if None and stream=True)

        Returns:
            AgentRCAReport with analysis results
        """
        start_time = datetime.now()
        self._tool_calls = []
        self._thinking_tokens = 0
        self._current_iteration = 0

        # Set up streaming callback
        if stream:
            self._stream_callback = on_stream or self._default_stream_handler
        else:
            self._stream_callback = None

        # Get incident data from ClickHouse or use provided data
        if incident_data:
            data = incident_data
        elif incident_id and self.data_source:
            data = self.data_source.get_incident_data(incident_id)
        else:
            raise ValueError("Either incident_id (with data_source) or incident_data must be provided")

        # Build initial message with incident data (use custom builder if provided)
        if self._initial_message_builder:
            initial_message = self._initial_message_builder(data)
        else:
            initial_message = self._build_initial_message(data)

        # Initialize message history (persisted for chat mode)
        self._message_history = [
            {"role": "system", "content": self._system_prompt},
            {"role": "user", "content": initial_message}
        ]

        # Get tool definitions if tools available
        tool_definitions = None
        if self.tools:
            tool_definitions = self.tools.get_tool_definitions()

        if self.config.debug and not stream:
            print("Agent RCA: Starting analysis with extended thinking...")

        # Show streaming indicator
        if stream and self._stream_callback == self._default_stream_handler:
            print("\n\033[1m🔍 Agent RCA Analysis\033[0m", flush=True)
            sys.stdout.flush()

        # Agent loop with extended thinking (streaming or non-streaming)
        response = self._call_llm(
            messages=self._message_history,
            tool_definitions=tool_definitions,
        )

        # Track thinking tokens
        if hasattr(response, 'thinking_tokens'):
            self._thinking_tokens += response.thinking_tokens

        # Process tool calls in loop until agent concludes
        iteration = 0
        max_iterations = self.config.max_tool_iterations

        while response.has_tool_calls and iteration < max_iterations:
            iteration += 1
            self._current_iteration = iteration

            if self.config.debug and not stream:
                print(f"  Iteration {iteration}: Processing {len(response.tool_calls)} tool calls...")

            # Add the assistant's message with tool_use blocks to history
            assistant_message = self._build_assistant_tool_message(response)
            self._message_history.append(assistant_message)

            # Execute tools and build result messages
            tool_messages = self._execute_tools(response.tool_calls)
            self._message_history.extend(tool_messages)

            # Show progress indicator before next LLM call
            if stream and self._stream_callback == self._default_stream_handler:
                sys.stdout.write(f"\n\033[2m📊 Processing tool results...\033[0m\n")
                sys.stdout.flush()

            # Continue reasoning
            response = self._call_llm(
                messages=self._message_history,
                tool_definitions=tool_definitions,
            )

            if hasattr(response, 'thinking_tokens'):
                self._thinking_tokens += response.thinking_tokens

        if self.config.debug and not stream:
            print(f"  Completed after {iteration} iterations, {len(self._tool_calls)} tool calls")

        # Add final assistant response to message history (for chat mode continuity)
        if response.content:
            self._message_history.append({
                "role": "assistant",
                "content": response.content
            })

        # Parse final report from response
        report = self._parse_report(
            response.content,
            start_time=start_time,
        )

        # Print formatted summary when streaming with default handler
        if stream and self._stream_callback == self._default_stream_handler:
            self._print_summary(report)

        return report

    def chat(
        self,
        user_question: str,
        stream: bool = False,
        on_stream: Optional[StreamCallback] = None,
        images: Optional[list[dict]] = None,
    ) -> str:
        """
        Continue investigation with a follow-up question.

        Requires analyze() to have been called first.
        Tools remain available for investigation.

        Args:
            user_question: The follow-up question from the user
            stream: Enable streaming output
            on_stream: Callback for streaming events
            images: Optional list of images for multimodal input.
                    Each image dict should have 'media_type' and 'data' (base64).

        Returns:
            Text response from the agent
        """
        if not self._message_history:
            raise RuntimeError("Must call analyze() before chat()")

        # Set up streaming
        if stream:
            self._stream_callback = on_stream or self._default_stream_handler
        else:
            self._stream_callback = None

        # Get tool definitions
        tool_definitions = None
        if self.tools:
            tool_definitions = self.tools.get_tool_definitions()

        # Build user message content (multimodal if images provided)
        if images:
            user_content = [{"type": "text", "text": user_question}]
            for img in images:
                user_content.append({
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": img["media_type"],
                        "data": img["data"],
                    }
                })
        else:
            user_content = user_question

        # Add user question to history
        self._message_history.append({
            "role": "user",
            "content": user_content
        })

        # Show streaming indicator
        if stream and self._stream_callback == self._default_stream_handler:
            print("\n\033[1m🔍 Follow-up Analysis\033[0m", flush=True)
            sys.stdout.flush()

        # Call LLM with full history
        response = self._call_llm(
            messages=self._message_history,
            tool_definitions=tool_definitions,
        )

        # Track thinking tokens
        if hasattr(response, 'thinking_tokens'):
            self._thinking_tokens += response.thinking_tokens

        # Agent loop - process tool calls if any
        iteration = 0
        max_iterations = self.config.max_tool_iterations

        while response.has_tool_calls and iteration < max_iterations:
            iteration += 1
            self._current_iteration = iteration

            # Add assistant message with tool calls
            assistant_message = self._build_assistant_tool_message(response)
            self._message_history.append(assistant_message)

            # Execute tools
            tool_messages = self._execute_tools(response.tool_calls)
            self._message_history.extend(tool_messages)

            # Show progress indicator
            if stream and self._stream_callback == self._default_stream_handler:
                sys.stdout.write(f"\n\033[2m📊 Processing tool results...\033[0m\n")
                sys.stdout.flush()

            # Continue reasoning
            response = self._call_llm(
                messages=self._message_history,
                tool_definitions=tool_definitions,
            )

            if hasattr(response, 'thinking_tokens'):
                self._thinking_tokens += response.thinking_tokens

        # Add final assistant response to history
        if response.content:
            self._message_history.append({
                "role": "assistant",
                "content": response.content
            })

            # Stream the response text if using default handler
            if stream and self._stream_callback == self._default_stream_handler:
                sys.stdout.write(f"\n\033[1mResponse:\033[0m\n{response.content}\n")
                sys.stdout.flush()

        return response.content or ""

    def start_session(
        self,
        initial_message: str,
        stream: bool = False,
        on_stream: Optional[StreamCallback] = None,
        images: Optional[list[dict]] = None,
    ) -> str:
        """
        Start a new chat session with a prepared initial message.

        This is intended for interactive clients that build a custom
        prompt with context before the first LLM call.

        Args:
            initial_message: The text message to start the session
            stream: Whether to stream the response
            on_stream: Callback for streaming events
            images: Optional list of images for multimodal input.
                    Each image dict should have 'media_type' and 'data' (base64).
        """
        self._tool_calls = []
        self._thinking_tokens = 0
        self._current_iteration = 0

        if stream:
            self._stream_callback = on_stream or self._default_stream_handler
        else:
            self._stream_callback = None

        # Build user message content (multimodal if images provided)
        if images:
            user_content = [{"type": "text", "text": initial_message}]
            for img in images:
                user_content.append({
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": img["media_type"],
                        "data": img["data"],
                    }
                })
        else:
            user_content = initial_message

        self._message_history = [
            {"role": "system", "content": self._system_prompt},
            {"role": "user", "content": user_content},
        ]

        tool_definitions = None
        if self.tools:
            tool_definitions = self.tools.get_tool_definitions()

        response = self._call_llm(
            messages=self._message_history,
            tool_definitions=tool_definitions,
        )

        iteration = 0
        max_iterations = self.config.max_tool_iterations

        while response.has_tool_calls and iteration < max_iterations:
            iteration += 1
            self._current_iteration = iteration

            assistant_message = self._build_assistant_tool_message(response)
            self._message_history.append(assistant_message)

            tool_messages = self._execute_tools(response.tool_calls)
            self._message_history.extend(tool_messages)

            response = self._call_llm(
                messages=self._message_history,
                tool_definitions=tool_definitions,
            )

        if response.content:
            self._message_history.append({
                "role": "assistant",
                "content": response.content,
            })

        return response.content or ""

    def _call_llm(
        self,
        messages: list[dict],
        tool_definitions: Optional[list] = None,
    ):
        """
        Call the LLM with or without streaming based on configuration.
        """
        if self._stream_callback:
            # Use streaming
            return self.llm.chat_stream(
                messages=messages,
                tools=tool_definitions,
                tool_choice="auto" if tool_definitions else None,
                extended_thinking=True,
                thinking_budget=self.config.thinking_budget,
                max_tokens=self.config.max_tokens,
                on_event=self._handle_stream_event,
            )
        else:
            # Non-streaming
            return self.llm.chat(
                messages=messages,
                tools=tool_definitions,
                tool_choice="auto" if tool_definitions else None,
                extended_thinking=True,
                thinking_budget=self.config.thinking_budget,
                max_tokens=self.config.max_tokens,
            )

    def _handle_stream_event(self, event: StreamEvent) -> None:
        """
        Handle a streaming event and forward to user callback.
        """
        if self._stream_callback:
            self._stream_callback(event)

    def _default_stream_handler(self, event: StreamEvent) -> None:
        """
        Default handler that prints streaming output to terminal with formatting.

        Shows thinking at each step with iteration numbers.
        Suppresses JSON output in the final response (summary is shown separately).
        """
        if event.type == StreamEventType.THINKING_START:
            self._in_thinking_block = True
            step_label = f"Step {self._current_iteration + 1}" if self._current_iteration > 0 else "Initial Analysis"
            sys.stdout.write(f"\n\033[2m\033[36m━━━ Thinking ({step_label}) ━━━\033[0m\n")
            sys.stdout.flush()

        elif event.type == StreamEventType.THINKING_DELTA:
            # Print thinking content in dim cyan
            if event.content:
                sys.stdout.write(f"\033[2m\033[36m{event.content}\033[0m")
                sys.stdout.flush()

        elif event.type == StreamEventType.THINKING_END:
            self._in_thinking_block = False
            sys.stdout.write("\n\033[2m\033[36m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\033[0m\n\n")
            sys.stdout.flush()

        elif event.type == StreamEventType.TEXT_START:
            self._in_text_block = True
            # Don't show "Response" header - we'll show summary at the end

        elif event.type == StreamEventType.TEXT_DELTA:
            # Suppress JSON output - summary will be shown at the end
            pass

        elif event.type == StreamEventType.TEXT_END:
            self._in_text_block = False

        elif event.type == StreamEventType.TOOL_USE_START:
            # Print tool call in yellow
            sys.stdout.write(f"\n\033[33m⚙ Calling tool: {event.content}\033[0m\n")
            sys.stdout.flush()

        elif event.type == StreamEventType.TOOL_USE_END:
            if event.tool_call:
                args_str = json.dumps(event.tool_call.arguments, indent=2)
                sys.stdout.write(f"\033[33m  Args: {args_str}\033[0m\n")
                sys.stdout.flush()

        elif event.type == StreamEventType.MESSAGE_END:
            sys.stdout.flush()  # Ensure all output is flushed

    def _print_tool_result(self, tool_name: str, data: Any) -> None:
        """
        Print a brief summary of a tool result.

        Extracts key information based on the tool type and shows a concise summary.
        """
        if not data:
            sys.stdout.write(f"\033[2m  ✓ No data returned\033[0m\n")
            sys.stdout.flush()
            return

        sys.stdout.write(f"\033[2m  ✓ Result:\033[0m\n")

        if tool_name == "compare_baseline":
            # Show key deltas
            if isinstance(data, dict):
                if "wait_deltas" in data and data["wait_deltas"]:
                    top_waits = data["wait_deltas"][:3]
                    for w in top_waits:
                        wait_type = w.get("wait_type", "?")
                        delta = w.get("delta_ms", 0)
                        sys.stdout.write(f"\033[2m    • {wait_type}: +{delta:.0f}ms\033[0m\n")
                if "blocking_comparison" in data:
                    bc = data["blocking_comparison"]
                    if bc.get("incident_max_blocked", 0) > 0:
                        sys.stdout.write(f"\033[2m    • Blocking detected: {bc.get('incident_max_blocked')} sessions\033[0m\n")

        elif tool_name == "query_clickhouse":
            # Show row count and sample
            if isinstance(data, dict) and "rows" in data:
                rows = data["rows"]
                sys.stdout.write(f"\033[2m    • {len(rows)} rows returned\033[0m\n")
            elif isinstance(data, list):
                sys.stdout.write(f"\033[2m    • {len(data)} rows returned\033[0m\n")

        elif tool_name == "run_blitz_diagnostics":
            # Show Blitz diagnostics summary
            if isinstance(data, dict):
                total_findings = sum(
                    len(v) if isinstance(v, list) else 0
                    for v in data.values()
                )
                scripts_run = [k.replace("blitz_", "") for k in data.keys() if k.startswith("blitz_")]
                sys.stdout.write(f"\033[2m    • {total_findings} findings from {', '.join(scripts_run) or 'scripts'}\033[0m\n")
            elif isinstance(data, list):
                sys.stdout.write(f"\033[2m    • {len(data)} findings\033[0m\n")

        elif tool_name == "get_query_details":
            # Show query info
            if isinstance(data, dict):
                exec_count = data.get("execution_count", "?")
                sys.stdout.write(f"\033[2m    • Executions: {exec_count}\033[0m\n")

        else:
            # Generic summary
            if isinstance(data, dict):
                sys.stdout.write(f"\033[2m    • {len(data)} fields\033[0m\n")
            elif isinstance(data, list):
                sys.stdout.write(f"\033[2m    • {len(data)} items\033[0m\n")

        sys.stdout.flush()

    def _stream_line(self, text: str, delay: float = 0.02) -> None:
        """
        Write a line to stdout with streaming effect.

        Args:
            text: Text to write
            delay: Delay in seconds after writing (default 20ms)
        """
        sys.stdout.write(text)
        sys.stdout.flush()
        if delay > 0:
            time.sleep(delay)

    def _print_summary(self, report: "AgentRCAReport") -> None:
        """
        Print a formatted summary of the report to the terminal.

        This replaces the raw JSON output with a human-readable summary.
        Streams each section progressively for better visual feedback.
        """
        if self._analysis_mode == "health_check":
            self._print_health_check_summary(report)
        else:
            self._print_rca_summary(report)

    def _print_health_check_summary(self, report: "AgentRCAReport") -> None:
        """Print health check summary with health-specific formatting."""
        root_cause = report.root_cause

        # Header
        self._stream_line("\n\033[1m\033[36m" + "═" * 60 + "\033[0m\n", delay=0.05)
        self._stream_line("\033[1m\033[36m  Health Check Complete\033[0m\n", delay=0.05)
        self._stream_line("\033[1m\033[36m" + "═" * 60 + "\033[0m\n\n", delay=0.1)

        # Health Score (if available in root_cause)
        health_score = root_cause.get("health_score") or root_cause.get("score")
        if health_score:
            score_color = "\033[32m" if health_score >= 70 else "\033[33m" if health_score >= 50 else "\033[31m"
            self._stream_line(f"\033[1mHealth Score:\033[0m {score_color}{health_score}/100\033[0m\n")

        # Summary
        summary = root_cause.get("summary", root_cause.get("description", "Health assessment complete"))
        self._stream_line(f"\n\033[1mSummary:\033[0m\n", delay=0.03)
        self._stream_line(f"  {summary}\n", delay=0.05)

        # Critical Findings (from causal_chain or evidence with high severity)
        critical = [e for e in report.evidence if e.get("severity") == "critical" or e.get("priority", 100) <= 10]
        if critical:
            time.sleep(0.1)
            self._stream_line(f"\n\033[1m\033[31mCritical Findings:\033[0m\n", delay=0.03)
            for finding in critical[:5]:
                f_text = finding.get("finding", finding.get("description", ""))
                self._stream_line(f"  \033[31m✗\033[0m {f_text}\n", delay=0.02)

        # Warnings
        warnings = [e for e in report.evidence if e.get("severity") == "warning" or (10 < e.get("priority", 100) <= 50)]
        if warnings:
            time.sleep(0.1)
            self._stream_line(f"\n\033[1m\033[33mWarnings:\033[0m\n", delay=0.03)
            for warning in warnings[:5]:
                w_text = warning.get("finding", warning.get("description", ""))
                self._stream_line(f"  \033[33m⚠\033[0m {w_text}\n", delay=0.02)

        # Recommendations (from mitigation/prevention)
        recommendations = report.mitigation + report.prevention
        if recommendations:
            time.sleep(0.1)
            self._stream_line(f"\n\033[1mRecommendations:\033[0m\n", delay=0.03)
            for rec in recommendations[:8]:
                self._stream_line(f"  → {rec}\n", delay=0.03)

        # Healthy Areas (from evidence with good status)
        healthy = [e for e in report.evidence if e.get("severity") == "healthy" or e.get("status") == "ok"]
        if healthy:
            time.sleep(0.1)
            self._stream_line(f"\n\033[1m\033[32mHealthy Areas:\033[0m\n", delay=0.03)
            for h in healthy[:5]:
                h_text = h.get("finding", h.get("description", ""))
                self._stream_line(f"  \033[32m✓\033[0m {h_text}\n", delay=0.02)

        # Metadata
        time.sleep(0.1)
        self._stream_line(f"\n\033[2m─────────────────────────────────────────────────────────────\033[0m\n", delay=0.02)
        self._stream_line(f"\033[2mAnalysis time: {report.analysis_duration_seconds:.1f}s | ", delay=0.02)
        self._stream_line(f"Tool calls: {len(report.tool_calls_made)} | ", delay=0.02)
        self._stream_line(f"Thinking tokens: {report.thinking_tokens_used}\033[0m\n", delay=0)

    def _print_rca_summary(self, report: "AgentRCAReport") -> None:
        """Print RCA summary with incident-specific formatting."""
        root_cause = report.root_cause

        # Header - slower reveal for emphasis
        self._stream_line("\n\033[1m\033[32m" + "═" * 60 + "\033[0m\n", delay=0.05)
        self._stream_line("\033[1m\033[32m  RCA Analysis Complete\033[0m\n", delay=0.05)
        self._stream_line("\033[1m\033[32m" + "═" * 60 + "\033[0m\n\n", delay=0.1)

        # Root Cause
        category = root_cause.get("category", "unknown")
        confidence = root_cause.get("confidence", 0)
        confidence_pct = int(confidence * 100) if isinstance(confidence, float) else confidence
        summary = root_cause.get("summary", "Unknown")
        entity = root_cause.get("entity", "")

        self._stream_line(f"\033[1mRoot Cause:\033[0m {category.upper()}\n")
        self._stream_line(f"\033[1mConfidence:\033[0m {confidence_pct}%\n")
        if entity:
            self._stream_line(f"\033[1mAffected:\033[0m {entity}\n")
        self._stream_line(f"\n\033[1mSummary:\033[0m\n", delay=0.03)
        self._stream_line(f"  {summary}\n", delay=0.05)

        # Causal Chain
        if report.causal_chain:
            time.sleep(0.1)  # Pause between sections
            self._stream_line(f"\n\033[1mCausal Chain:\033[0m\n", delay=0.03)
            for i, step in enumerate(report.causal_chain, 1):
                event = step.get("event", "")
                desc = step.get("description", "")
                self._stream_line(f"  {i}. \033[33m{event}\033[0m: {desc}\n", delay=0.03)

        # Key Evidence
        if report.evidence:
            time.sleep(0.1)
            self._stream_line(f"\n\033[1mKey Evidence:\033[0m\n", delay=0.03)
            for ev in report.evidence[:5]:  # Show top 5
                source = ev.get("source", "")
                finding = ev.get("finding", "")
                self._stream_line(f"  • [{source}] {finding}\n", delay=0.02)

        # Mitigation
        if report.mitigation:
            time.sleep(0.1)
            self._stream_line(f"\n\033[1mMitigation:\033[0m\n", delay=0.03)
            for m in report.mitigation:
                self._stream_line(f"  → {m}\n", delay=0.03)

        # Prevention
        if report.prevention:
            time.sleep(0.1)
            self._stream_line(f"\n\033[1mPrevention:\033[0m\n", delay=0.03)
            for p in report.prevention:
                self._stream_line(f"  → {p}\n", delay=0.03)

        # Metadata
        time.sleep(0.1)
        self._stream_line(f"\n\033[2m─────────────────────────────────────────────────────────────\033[0m\n", delay=0.02)
        self._stream_line(f"\033[2mAnalysis time: {report.analysis_duration_seconds:.1f}s | ", delay=0.02)
        self._stream_line(f"Tool calls: {len(report.tool_calls_made)} | ", delay=0.02)
        self._stream_line(f"Thinking tokens: {report.thinking_tokens_used}\033[0m\n", delay=0)

    def _build_assistant_tool_message(self, response) -> dict:
        """
        Build the assistant's message with tool_use blocks for the message history.

        This is required by Anthropic's API - tool_result blocks must have
        corresponding tool_use blocks in the previous assistant message.

        When extended thinking is enabled, the message must start with thinking blocks
        that include the signature field.

        Args:
            response: LLM response with tool_calls

        Returns:
            Assistant message dict with thinking and tool_use content blocks
        """
        content = []

        # Add thinking blocks first (required when extended thinking is enabled)
        # Must include the signature field for multi-turn conversations
        thinking_blocks = getattr(response, 'thinking_blocks', None)
        if thinking_blocks:
            for block in thinking_blocks:
                content.append(block)

        # Add text content if present
        if response.content:
            content.append({"type": "text", "text": response.content})

        # Add tool_use blocks
        for tc in response.tool_calls:
            content.append({
                "type": "tool_use",
                "id": tc.id,
                "name": tc.name,
                "input": tc.arguments if isinstance(tc.arguments, dict) else {},
            })

        return {"role": "assistant", "content": content}

    def _sanitize_incident_data(self, incident_data: dict) -> dict:
        """
        Remove any identifying information that could hint at the incident type.

        This ensures the RCA engine operates blindly, just like in production
        where it would be triggered by anomaly detection without context.
        """
        sanitized = {}

        for key, value in incident_data.items():
            if key == "incident":
                # Strip scenario, name - only keep time info
                status = value.get("status")
                # For ongoing incidents, set ended_at to null
                # Only completed/post-mortem incidents have an actual end time
                is_ongoing = status not in ("completed", "resolved")
                sanitized["incident"] = {
                    "started_at": value.get("started_at"),
                    "ended_at": None if is_ongoing else value.get("ended_at"),
                    "status": "ongoing" if is_ongoing else "completed",
                }
            else:
                sanitized[key] = value

        return sanitized

    def _build_initial_message(self, incident_data: dict) -> str:
        """Build the initial user message with incident data from ClickHouse."""
        # Sanitize data to remove any hints about incident type
        sanitized_data = self._sanitize_incident_data(incident_data)
        data_json = json.dumps(sanitized_data, indent=2, default=str)

        return f"""A database performance incident has been detected. Here is the incident data
collected from ClickHouse:

## Incident Data

```json
{data_json}
```

**Note on incident status:**
- `status: "ongoing"` with `ended_at: null` means the incident is still active and you are performing real-time analysis
- `status: "completed"` with an `ended_at` timestamp means this is a post-mortem analysis of a resolved incident

---

Please investigate this incident and determine the root cause.

Review the incident data above, then use the available tools:
- `compare_baseline()` - Compare baseline vs incident metrics
- `query_clickhouse(table, filters, order_by, limit)` - Query specific tables
- `run_blitz_diagnostics(script)` - Run Blitz diagnostics on-demand (for active incidents)

Form hypotheses, investigate to confirm or refute them, and provide a structured analysis.
"""

    def _execute_tools(self, tool_calls: list) -> list[dict]:
        """
        Execute tool calls and return result messages.

        Args:
            tool_calls: List of tool call objects from LLM response

        Returns:
            List of message dicts to add to conversation
        """
        result_messages = []

        for tc in tool_calls:
            tool_name = tc.name
            tool_args = tc.arguments

            # Track the call with enhanced structure
            self._tool_calls.append({
                "tool": tool_name,
                "arguments": tool_args,
                "success": None,
                "error": None,
                "error_type": None,
                "execution_time_ms": None,
            })

            # Execute the tool
            if self.tools:
                result = self.tools.execute(tool_name, **tool_args)

                # Update tracking with result
                self._tool_calls[-1]["success"] = result.success
                self._tool_calls[-1]["execution_time_ms"] = result.metadata.get("execution_time_ms") if result.metadata else None

                if not result.success:
                    error_msg = result.error or "Tool execution failed"
                    self._tool_calls[-1]["error"] = error_msg
                    self._tool_calls[-1]["error_type"] = self._classify_error(error_msg)

                # Build result content
                if result.success:
                    result_content = json.dumps(result.data, indent=2, default=str)
                    # Show brief result summary when streaming
                    if self._stream_callback == self._default_stream_handler:
                        self._print_tool_result(tool_name, result.data)
                else:
                    result_content = json.dumps({
                        "error": result.error or "Tool execution failed",
                        "status": "error"
                    })
                    if self._stream_callback == self._default_stream_handler:
                        sys.stdout.write(f"\033[31m  ✗ Error: {result.error}\033[0m\n")
                        sys.stdout.flush()
            else:
                # No tools available - return unavailable message
                error_msg = f"Tool '{tool_name}' is not available in this context"
                result_content = json.dumps({
                    "status": "unavailable",
                    "message": error_msg
                })
                self._tool_calls[-1]["success"] = False
                self._tool_calls[-1]["error"] = error_msg
                self._tool_calls[-1]["error_type"] = "unavailable"
                if self._stream_callback == self._default_stream_handler:
                    sys.stdout.write(f"\033[31m  ✗ {error_msg}\033[0m\n")
                    sys.stdout.flush()

            # Emit TOOL_RESULT event for streaming
            if self._stream_callback and self._stream_callback != self._default_stream_handler:
                self._stream_callback(StreamEvent(
                    type=StreamEventType.TOOL_RESULT,
                    tool_call_id=tc.id,
                    tool_result=result_content,
                ))

            # Add tool result message
            result_messages.append({
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": tc.id,
                    "content": result_content,
                }]
            })

        return result_messages

    def _classify_error(self, error: str) -> str:
        """
        Classify tool error for pattern analysis.

        Args:
            error: Error message from tool execution

        Returns:
            Error category: "validation", "unavailable", "connection", or "execution"
        """
        if not error:
            return "unknown"
        error_lower = error.lower()
        if "required" in error_lower or "invalid" in error_lower or "missing" in error_lower:
            return "validation"
        if "not available" in error_lower or "unavailable" in error_lower:
            return "unavailable"
        if "timeout" in error_lower or "connection" in error_lower:
            return "connection"
        return "execution"

    def _compute_tool_failure_summary(self) -> dict:
        """
        Compute summary of tool call failures for reporting.

        Returns:
            Dict with total_calls, failed_calls, success_rate, and failures_by_tool
        """
        total = len(self._tool_calls)
        failures = [tc for tc in self._tool_calls if not tc.get("success")]

        # Group failures by tool
        failures_by_tool = {}
        for tc in failures:
            tool = tc["tool"]
            if tool not in failures_by_tool:
                failures_by_tool[tool] = []
            failures_by_tool[tool].append({
                "error": tc.get("error"),
                "error_type": tc.get("error_type"),
                "arguments": tc.get("arguments"),
            })

        return {
            "total_calls": total,
            "failed_calls": len(failures),
            "success_rate": round((total - len(failures)) / total * 100, 1) if total > 0 else 100.0,
            "failures_by_tool": failures_by_tool,
        }

    def log_tool_failures(self, incident_id: str, output_dir: Union[str, Path]) -> None:
        """
        Append tool failures to aggregate log file for cross-incident analysis.

        Call this after analyze() to persist failures for later aggregation.
        The log file (tool_failures.jsonl) is created in the parent of output_dir.

        Args:
            incident_id: Identifier for the current incident
            output_dir: Directory where RCA report is saved
        """
        output_dir = Path(output_dir)
        failures = [tc for tc in self._tool_calls if not tc.get("success")]
        if not failures:
            return  # No failures to log

        log_file = output_dir / "tool_failures.jsonl"

        record = {
            "timestamp": datetime.utcnow().isoformat(),
            "incident_id": incident_id,
            "total_calls": len(self._tool_calls),
            "failures": failures,
        }

        try:
            with open(log_file, "a") as f:
                f.write(json.dumps(record, default=str) + "\n")
        except Exception as e:
            if self.config.debug:
                print(f"Warning: Could not write to tool failures log: {e}")

    def _parse_report(
        self,
        content: str,
        start_time: datetime,
    ) -> AgentRCAReport:
        """
        Parse the agent's final response into a structured report.

        Args:
            content: Raw response content from LLM
            start_time: When analysis started

        Returns:
            AgentRCAReport
        """
        duration = (datetime.now() - start_time).total_seconds()

        # Try to extract JSON from the response
        report_data = self._extract_json(content)

        # Compute failure summary for all cases
        failure_summary = self._compute_tool_failure_summary()

        if report_data:
            # Handle health check mode differently
            if self._analysis_mode == "health_check":
                # Map health check fields to report structure
                health_score = report_data.get("health_score") or report_data.get("score")
                summary = report_data.get("summary", "Health check completed")

                # Build evidence from findings
                evidence = []
                for finding in report_data.get("critical_findings", []):
                    if isinstance(finding, dict):
                        evidence.append({
                            "source": "sp_Blitz",
                            "finding": finding.get("finding", finding.get("description", str(finding))),
                            "severity": "critical",
                            "priority": 1,
                        })
                    else:
                        evidence.append({"source": "sp_Blitz", "finding": str(finding), "severity": "critical", "priority": 1})

                for warning in report_data.get("warnings", []):
                    if isinstance(warning, dict):
                        evidence.append({
                            "source": "baseline",
                            "finding": warning.get("finding", warning.get("description", str(warning))),
                            "severity": "warning",
                            "priority": 25,
                        })
                    else:
                        evidence.append({"source": "baseline", "finding": str(warning), "severity": "warning", "priority": 25})

                for item in report_data.get("healthy_areas", []):
                    if isinstance(item, str):
                        evidence.append({"source": "health_check", "finding": item, "severity": "healthy", "status": "ok"})
                    elif isinstance(item, dict):
                        evidence.append({
                            "source": "health_check",
                            "finding": item.get("finding", item.get("description", str(item))),
                            "severity": "healthy",
                            "status": "ok",
                        })

                # Build recommendations from recommendations field
                recommendations = []
                for rec in report_data.get("recommendations", []):
                    if isinstance(rec, dict):
                        recommendations.append(rec.get("remediation", rec.get("finding", str(rec))))
                    else:
                        recommendations.append(str(rec))

                return AgentRCAReport(
                    root_cause={
                        "category": "health_check",
                        "summary": summary,
                        "health_score": health_score,
                        "confidence": 1.0,
                        "entity": "server",
                    },
                    causal_chain=[],
                    evidence=evidence,
                    mitigation=recommendations,
                    prevention=[],
                    analysis_duration_seconds=duration,
                    tool_calls_made=self._tool_calls,
                    tool_failure_summary=failure_summary,
                    thinking_tokens_used=self._thinking_tokens,
                    model_used=self.config.model,
                )

            # Standard RCA mode
            return AgentRCAReport(
                root_cause=report_data.get("root_cause", {
                    "category": "unknown",
                    "summary": "Unable to determine root cause",
                    "confidence": 0.0,
                    "entity": "unknown"
                }),
                causal_chain=report_data.get("causal_chain", []),
                evidence=report_data.get("evidence", []),
                mitigation=report_data.get("mitigation", []),
                prevention=report_data.get("prevention", []),
                analysis_duration_seconds=duration,
                tool_calls_made=self._tool_calls,
                tool_failure_summary=failure_summary,
                thinking_tokens_used=self._thinking_tokens,
                model_used=self.config.model,
            )
        else:
            # Fallback: create report from raw content
            if self._analysis_mode == "health_check":
                return AgentRCAReport(
                    root_cause={
                        "category": "health_check",
                        "summary": content[:500] if content else "Health check completed",
                        "health_score": None,
                        "confidence": 0.5,
                        "entity": "server",
                    },
                    causal_chain=[],
                    evidence=[{"source": "agent_analysis", "finding": content, "severity": "info"}],
                    mitigation=["Review health check analysis for recommendations"],
                    prevention=[],
                    analysis_duration_seconds=duration,
                    tool_calls_made=self._tool_calls,
                    tool_failure_summary=failure_summary,
                    thinking_tokens_used=self._thinking_tokens,
                    model_used=self.config.model,
                )

            return AgentRCAReport(
                root_cause={
                    "category": "analysis_complete",
                    "summary": content[:500] if content else "Analysis completed",
                    "confidence": 0.5,
                    "entity": "unknown"
                },
                causal_chain=[],
                evidence=[{"source": "agent_analysis", "finding": content}],
                mitigation=["Review agent analysis for recommendations"],
                prevention=["Review agent analysis for prevention steps"],
                analysis_duration_seconds=duration,
                tool_calls_made=self._tool_calls,
                tool_failure_summary=failure_summary,
                thinking_tokens_used=self._thinking_tokens,
                model_used=self.config.model,
            )

    def _extract_json(self, content: str) -> Optional[dict]:
        """
        Extract JSON from response content.

        Handles:
        - Raw JSON
        - JSON in markdown code blocks
        - JSON embedded in text
        """
        if not content:
            return None

        # Try direct parse first
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            pass

        # Try to find JSON in markdown code blocks
        import re
        json_pattern = r'```(?:json)?\s*(\{[\s\S]*?\})\s*```'
        matches = re.findall(json_pattern, content)

        for match in matches:
            try:
                return json.loads(match)
            except json.JSONDecodeError:
                continue

        # Try to find bare JSON object
        brace_start = content.find('{')
        if brace_start >= 0:
            # Find matching closing brace
            depth = 0
            for i, char in enumerate(content[brace_start:]):
                if char == '{':
                    depth += 1
                elif char == '}':
                    depth -= 1
                    if depth == 0:
                        try:
                            return json.loads(content[brace_start:brace_start + i + 1])
                        except json.JSONDecodeError:
                            break

        return None
