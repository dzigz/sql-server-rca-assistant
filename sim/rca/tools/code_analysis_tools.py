"""
Code analysis tools using Claude Agent SDK.

Provides tools for analyzing application code impact and correlating
incidents with code changes using a sub-agent approach.

These tools spawn Claude Agent SDK sub-agents to analyze application codebases,
finding code paths affected by slow queries and correlating incidents with
recent code/schema/config changes.

IMPORTANT: These tools require a `repo_path` parameter pointing to an
application repository. The RCA agent should only use these tools when
repo_path is available in the incident data.
"""

import asyncio
import json
from pathlib import Path
from typing import Optional

from sim.rca.tools.base import RCATool, ToolResult
from sim.logging_config import get_logger

logger = get_logger(__name__)

# Check if claude-agent-sdk is available
try:
    from claude_agent_sdk import query, ClaudeAgentOptions
    CLAUDE_AGENT_SDK_AVAILABLE = True
except ImportError:
    CLAUDE_AGENT_SDK_AVAILABLE = False
    logger.warning("claude-agent-sdk not installed. Code analysis tools will not be available.")


# Default timeout for sub-agent queries (seconds)
DEFAULT_TIMEOUT = 120


# System prompts for sub-agents

CODE_IMPACT_SYSTEM_PROMPT = """You are a code analyst specializing in database performance impact assessment.

Your task is to analyze a codebase to find:
1. All code paths that execute the slow query or similar queries
2. Which application features/endpoints are affected
3. The business impact (user-facing vs background jobs)
4. Upstream and downstream dependencies

Use Grep to find SQL patterns, Glob to locate relevant files, and Read to understand context.
Use Bash for git commands (git log, git blame) to understand history.

After your investigation, provide a structured JSON response with:
- affected_files: List of {path, line, context} for files referencing the query/table
- affected_endpoints: List of API endpoints or features affected
- orm_mappings: List of ORM model files and classes
- criticality: "high", "medium", or "low"
- summary: Brief description of impact"""


INCIDENT_CORRELATION_SYSTEM_PROMPT = """You are an expert at correlating database incidents with code changes.

Investigate:
1. Recent code commits affecting the relevant tables/queries (use git log, git blame)
2. Recent schema migrations (look in migrations/, alembic/, flyway/, db/migrate/)
3. Recent configuration changes (*.yaml, *.json, .env files)
4. ORM model changes

Build a timeline of changes and identify the most likely cause.

After your investigation, provide a structured JSON response with:
- timeline: List of {time, type, description, likelihood} for each change
- most_likely_cause: {type, description, file} for the most likely culprit
- summary: Brief description of findings"""


QUERY_ORIGIN_SYSTEM_PROMPT = """You are an expert at tracing SQL queries back to their origin in application code.

Your task is to find where a specific SQL query is generated in the codebase:
1. Search for the SQL pattern or fragments
2. Identify ORM methods or raw SQL calls that generate this query
3. Trace back to the calling functions and endpoints
4. Understand the query parameters and how they're populated

After your investigation, provide a structured JSON response with:
- origin_files: List of {path, line, function, context} where query originates
- call_chain: List of functions that lead to this query
- parameters: Description of how query parameters are populated
- summary: Brief description of query origin"""


ORM_PATTERNS_SYSTEM_PROMPT = """You are an expert at detecting ORM anti-patterns that cause database performance issues.

Analyze the codebase for common ORM problems:
1. N+1 query patterns (loops that trigger individual queries)
2. Missing eager loading / select_related / prefetch_related
3. Inefficient query patterns (loading entire objects when only IDs needed)
4. Missing database indexes based on query patterns
5. Cartesian product joins from incorrect relationship definitions

After your investigation, provide a structured JSON response with:
- issues: List of {severity, pattern, file, line, description, recommendation}
- summary: Brief overview of ORM health
- recommendations: List of top improvements to make"""


class AnalyzeCodeImpactTool(RCATool):
    """
    Analyze which application code paths are affected by a slow query.

    Uses Claude Agent SDK to spawn a sub-agent that searches the codebase
    for code locations executing the query, affected endpoints, ORM mappings,
    and assesses business criticality.
    """

    @property
    def name(self) -> str:
        return "analyze_code_impact"

    @property
    def description(self) -> str:
        return """Analyze application code to find which features are affected by a slow query.

Uses Claude Agent SDK to search the codebase for:
- Code locations executing the query
- Affected API endpoints and features
- ORM models and repository patterns
- Business criticality assessment

REQUIRES: repo_path pointing to the application repository.
DO NOT use this tool if repo_path is not available in the incident data."""

    @property
    def parameters_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "slow_query": {
                    "type": "string",
                    "description": "The SQL query text to analyze"
                },
                "table_name": {
                    "type": "string",
                    "description": "Primary table affected by the query"
                },
                "repo_path": {
                    "type": "string",
                    "description": "Path to the application repository"
                }
            },
            "required": ["slow_query", "table_name", "repo_path"]
        }

    def execute(
        self,
        slow_query: str,
        table_name: str,
        repo_path: str,
        **kwargs
    ) -> ToolResult:
        """Execute code impact analysis using Claude Agent SDK."""
        if not CLAUDE_AGENT_SDK_AVAILABLE:
            return ToolResult.fail(
                "claude-agent-sdk is not installed. "
                "Install with: pip install claude-agent-sdk"
            )

        try:
            # Validate repo path exists
            repo = Path(repo_path)
            if not repo.exists():
                return ToolResult.fail(f"Repository path does not exist: {repo_path}")
            if not repo.is_dir():
                return ToolResult.fail(f"Repository path is not a directory: {repo_path}")

            # Run async query in sync context
            result = asyncio.run(
                self._analyze_async(slow_query, table_name, repo_path)
            )
            return ToolResult.ok(result)

        except asyncio.TimeoutError:
            return ToolResult.fail(
                f"Code impact analysis timed out after {DEFAULT_TIMEOUT} seconds. "
                "The repository may be too large or complex."
            )
        except Exception as e:
            logger.exception("Code impact analysis failed")
            return ToolResult.fail(f"Analysis failed: {str(e)}")

    async def _analyze_async(
        self,
        slow_query: str,
        table_name: str,
        repo_path: str
    ) -> dict:
        """Async implementation using Claude Agent SDK."""
        options = ClaudeAgentOptions(
            system_prompt=CODE_IMPACT_SYSTEM_PROMPT,
            allowed_tools=["Read", "Glob", "Grep", "Bash"],
            cwd=repo_path,
            permission_mode="bypassPermissions"
        )

        prompt = f"""Analyze the impact of this slow query on the codebase:

**Slow Query:**
```sql
{slow_query}
```

**Affected Table:** {table_name}

Please:
1. Find all code locations that reference this table or similar queries
2. Identify which API endpoints, services, or jobs use this query
3. Trace the call stack to understand user-facing impact
4. Check for any ORM mappings or repository patterns
5. Look at recent git changes to this area (git log -5 -- "*{table_name}*")
6. Assess criticality (user-facing, background, batch job)

Return your findings as JSON with: affected_files, affected_endpoints, orm_mappings, criticality, summary"""

        results = []
        async for message in query(prompt=prompt, options=options):
            if hasattr(message, "result"):
                results.append(str(message.result))

        # Try to parse JSON from results
        raw_output = "\n".join(results)
        return self._parse_analysis_result(raw_output)

    def _parse_analysis_result(self, raw_output: str) -> dict:
        """Parse the sub-agent's output into structured result."""
        # Try to extract JSON from the output
        try:
            # Look for JSON block in the output
            if "```json" in raw_output:
                json_start = raw_output.find("```json") + 7
                json_end = raw_output.find("```", json_start)
                if json_end > json_start:
                    json_str = raw_output[json_start:json_end].strip()
                    return json.loads(json_str)

            # Try direct JSON parse
            return json.loads(raw_output)
        except json.JSONDecodeError:
            # Return raw output if JSON parsing fails
            return {
                "raw_analysis": raw_output,
                "parse_error": "Could not parse structured JSON from sub-agent response"
            }


class CorrelateIncidentTool(RCATool):
    """
    Correlate an incident with recent code, schema, or config changes.

    Uses Claude Agent SDK to analyze git history, migration files, and
    configuration changes to identify what change most likely caused
    the incident.
    """

    @property
    def name(self) -> str:
        return "correlate_incident"

    @property
    def description(self) -> str:
        return """Correlate a database incident with recent code/schema/config changes.

Uses Claude Agent SDK to investigate:
- Recent git commits affecting relevant tables/queries
- Schema migrations (alembic, flyway, rails migrations)
- Configuration changes
- ORM model changes

Returns a timeline of changes with likelihood assessment.

REQUIRES: repo_path pointing to the application repository.
DO NOT use this tool if repo_path is not available in the incident data."""

    @property
    def parameters_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "incident_time": {
                    "type": "string",
                    "description": "When the incident started (ISO format, e.g., 2026-01-07T10:30:00Z)"
                },
                "affected_table": {
                    "type": "string",
                    "description": "Primary table involved in the incident"
                },
                "repo_path": {
                    "type": "string",
                    "description": "Path to the application repository"
                },
                "lookback_days": {
                    "type": "integer",
                    "description": "Days to search back for changes (default: 7)",
                    "default": 7
                }
            },
            "required": ["incident_time", "affected_table", "repo_path"]
        }

    def execute(
        self,
        incident_time: str,
        affected_table: str,
        repo_path: str,
        lookback_days: int = 7,
        **kwargs
    ) -> ToolResult:
        """Execute incident correlation using Claude Agent SDK."""
        if not CLAUDE_AGENT_SDK_AVAILABLE:
            return ToolResult.fail(
                "claude-agent-sdk is not installed. "
                "Install with: pip install claude-agent-sdk"
            )

        try:
            repo = Path(repo_path)
            if not repo.exists():
                return ToolResult.fail(f"Repository path does not exist: {repo_path}")
            if not repo.is_dir():
                return ToolResult.fail(f"Repository path is not a directory: {repo_path}")

            result = asyncio.run(
                self._correlate_async(incident_time, affected_table, repo_path, lookback_days)
            )
            return ToolResult.ok(result)

        except asyncio.TimeoutError:
            return ToolResult.fail(
                f"Incident correlation timed out after {DEFAULT_TIMEOUT} seconds."
            )
        except Exception as e:
            logger.exception("Incident correlation failed")
            return ToolResult.fail(f"Correlation failed: {str(e)}")

    async def _correlate_async(
        self,
        incident_time: str,
        affected_table: str,
        repo_path: str,
        lookback_days: int
    ) -> dict:
        """Async implementation using Claude Agent SDK."""
        options = ClaudeAgentOptions(
            system_prompt=INCIDENT_CORRELATION_SYSTEM_PROMPT,
            allowed_tools=["Read", "Glob", "Grep", "Bash"],
            cwd=repo_path,
            permission_mode="bypassPermissions"
        )

        prompt = f"""Correlate this database incident with recent changes:

**Incident Time:** {incident_time}
**Affected Table:** {affected_table}
**Lookback Period:** {lookback_days} days

Investigation steps:
1. Run: git log --since="{lookback_days} days ago" --oneline -- "*{affected_table}*"
2. Search for recent migrations: find . -name "*migration*" -newer (based on dates)
3. Look for schema changes to this table in migrations/, alembic/, flyway/, db/migrate/
4. Check for ORM model changes: grep -r "class.*{affected_table}" --include="*.py"
5. Look for configuration changes: git log --since="{lookback_days} days ago" -- "*.yaml" "*.json" ".env*"

Build a timeline of changes and identify the most likely cause.

Return your findings as JSON with: timeline, most_likely_cause, summary"""

        results = []
        async for message in query(prompt=prompt, options=options):
            if hasattr(message, "result"):
                results.append(str(message.result))

        raw_output = "\n".join(results)
        return self._parse_correlation_result(raw_output)

    def _parse_correlation_result(self, raw_output: str) -> dict:
        """Parse the sub-agent's output into structured result."""
        try:
            if "```json" in raw_output:
                json_start = raw_output.find("```json") + 7
                json_end = raw_output.find("```", json_start)
                if json_end > json_start:
                    json_str = raw_output[json_start:json_end].strip()
                    return json.loads(json_str)
            return json.loads(raw_output)
        except json.JSONDecodeError:
            return {
                "raw_analysis": raw_output,
                "parse_error": "Could not parse structured JSON from sub-agent response"
            }


class FindQueryOriginTool(RCATool):
    """
    Find where in application code a query is generated.

    Uses Claude Agent SDK to trace a SQL query back to its origin
    in the application codebase.
    """

    @property
    def name(self) -> str:
        return "find_query_origin"

    @property
    def description(self) -> str:
        return """Find where a SQL query originates in the application code.

Uses Claude Agent SDK to:
- Search for SQL patterns matching the query
- Identify ORM methods or raw SQL calls
- Trace back to calling functions and endpoints

REQUIRES: repo_path pointing to the application repository.
DO NOT use this tool if repo_path is not available in the incident data."""

    @property
    def parameters_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "sql_pattern": {
                    "type": "string",
                    "description": "SQL query text or pattern to search for"
                },
                "repo_path": {
                    "type": "string",
                    "description": "Path to the application repository"
                },
                "query_hash": {
                    "type": "string",
                    "description": "Optional query hash from DMV stats for reference"
                }
            },
            "required": ["sql_pattern", "repo_path"]
        }

    def execute(
        self,
        sql_pattern: str,
        repo_path: str,
        query_hash: Optional[str] = None,
        **kwargs
    ) -> ToolResult:
        """Execute query origin search using Claude Agent SDK."""
        if not CLAUDE_AGENT_SDK_AVAILABLE:
            return ToolResult.fail(
                "claude-agent-sdk is not installed. "
                "Install with: pip install claude-agent-sdk"
            )

        try:
            repo = Path(repo_path)
            if not repo.exists():
                return ToolResult.fail(f"Repository path does not exist: {repo_path}")

            result = asyncio.run(
                self._find_origin_async(sql_pattern, repo_path, query_hash)
            )
            return ToolResult.ok(result)

        except asyncio.TimeoutError:
            return ToolResult.fail(
                f"Query origin search timed out after {DEFAULT_TIMEOUT} seconds."
            )
        except Exception as e:
            logger.exception("Query origin search failed")
            return ToolResult.fail(f"Search failed: {str(e)}")

    async def _find_origin_async(
        self,
        sql_pattern: str,
        repo_path: str,
        query_hash: Optional[str]
    ) -> dict:
        """Async implementation using Claude Agent SDK."""
        options = ClaudeAgentOptions(
            system_prompt=QUERY_ORIGIN_SYSTEM_PROMPT,
            allowed_tools=["Read", "Glob", "Grep", "Bash"],
            cwd=repo_path,
            permission_mode="bypassPermissions"
        )

        # Extract key parts of the query for searching
        hash_context = f"\n**Query Hash:** {query_hash}" if query_hash else ""

        prompt = f"""Find where this SQL query originates in the codebase:

**SQL Pattern:**
```sql
{sql_pattern}
```
{hash_context}

Please:
1. Search for this SQL pattern or key fragments (table names, column names)
2. Look for ORM methods that could generate this query
3. Check for raw SQL execution (execute(), cursor.execute(), etc.)
4. Trace back to the functions/methods that call this code
5. Identify which API endpoints or jobs trigger this query

Return your findings as JSON with: origin_files, call_chain, parameters, summary"""

        results = []
        async for message in query(prompt=prompt, options=options):
            if hasattr(message, "result"):
                results.append(str(message.result))

        raw_output = "\n".join(results)
        return self._parse_origin_result(raw_output)

    def _parse_origin_result(self, raw_output: str) -> dict:
        """Parse the sub-agent's output."""
        try:
            if "```json" in raw_output:
                json_start = raw_output.find("```json") + 7
                json_end = raw_output.find("```", json_start)
                if json_end > json_start:
                    json_str = raw_output[json_start:json_end].strip()
                    return json.loads(json_str)
            return json.loads(raw_output)
        except json.JSONDecodeError:
            return {
                "raw_analysis": raw_output,
                "parse_error": "Could not parse structured JSON from sub-agent response"
            }


class AnalyzeORMPatternsTool(RCATool):
    """
    Analyze ORM patterns for potential N+1 or inefficient query generation.

    Uses Claude Agent SDK to detect common ORM anti-patterns that cause
    database performance issues.
    """

    @property
    def name(self) -> str:
        return "analyze_orm_patterns"

    @property
    def description(self) -> str:
        return """Analyze ORM patterns for potential performance anti-patterns.

Uses Claude Agent SDK to detect:
- N+1 query patterns
- Missing eager loading
- Inefficient query patterns
- Missing database indexes

REQUIRES: repo_path pointing to the application repository.
DO NOT use this tool if repo_path is not available in the incident data."""

    @property
    def parameters_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Table name to focus analysis on (optional)"
                },
                "repo_path": {
                    "type": "string",
                    "description": "Path to the application repository"
                }
            },
            "required": ["repo_path"]
        }

    def execute(
        self,
        repo_path: str,
        table_name: Optional[str] = None,
        **kwargs
    ) -> ToolResult:
        """Execute ORM pattern analysis using Claude Agent SDK."""
        if not CLAUDE_AGENT_SDK_AVAILABLE:
            return ToolResult.fail(
                "claude-agent-sdk is not installed. "
                "Install with: pip install claude-agent-sdk"
            )

        try:
            repo = Path(repo_path)
            if not repo.exists():
                return ToolResult.fail(f"Repository path does not exist: {repo_path}")

            result = asyncio.run(
                self._analyze_orm_async(repo_path, table_name)
            )
            return ToolResult.ok(result)

        except asyncio.TimeoutError:
            return ToolResult.fail(
                f"ORM analysis timed out after {DEFAULT_TIMEOUT} seconds."
            )
        except Exception as e:
            logger.exception("ORM pattern analysis failed")
            return ToolResult.fail(f"Analysis failed: {str(e)}")

    async def _analyze_orm_async(
        self,
        repo_path: str,
        table_name: Optional[str]
    ) -> dict:
        """Async implementation using Claude Agent SDK."""
        options = ClaudeAgentOptions(
            system_prompt=ORM_PATTERNS_SYSTEM_PROMPT,
            allowed_tools=["Read", "Glob", "Grep", "Bash"],
            cwd=repo_path,
            permission_mode="bypassPermissions"
        )

        table_context = f"\n**Focus Table:** {table_name}" if table_name else ""

        prompt = f"""Analyze this codebase for ORM anti-patterns that cause performance issues:
{table_context}

Please investigate:
1. Find model definitions (look for class definitions, SQLAlchemy, Django models, etc.)
2. Search for N+1 patterns: loops that access related objects without prefetching
3. Look for missing select_related/prefetch_related/joinedload
4. Identify queries that load full objects when only specific fields are needed
5. Check for raw SQL that could indicate ORM limitations

Common patterns to search for:
- "for .* in .*:" followed by attribute access
- ".all()" or ".filter()" in loops
- Missing eager loading decorators/options

Return your findings as JSON with: issues, summary, recommendations"""

        results = []
        async for message in query(prompt=prompt, options=options):
            if hasattr(message, "result"):
                results.append(str(message.result))

        raw_output = "\n".join(results)
        return self._parse_orm_result(raw_output)

    def _parse_orm_result(self, raw_output: str) -> dict:
        """Parse the sub-agent's output."""
        try:
            if "```json" in raw_output:
                json_start = raw_output.find("```json") + 7
                json_end = raw_output.find("```", json_start)
                if json_end > json_start:
                    json_str = raw_output[json_start:json_end].strip()
                    return json.loads(json_str)
            return json.loads(raw_output)
        except json.JSONDecodeError:
            return {
                "raw_analysis": raw_output,
                "parse_error": "Could not parse structured JSON from sub-agent response"
            }


def create_code_analysis_tools() -> list[RCATool]:
    """
    Create all code analysis tools.

    Returns empty list if claude-agent-sdk is not available.
    """
    if not CLAUDE_AGENT_SDK_AVAILABLE:
        logger.warning(
            "Code analysis tools not available: claude-agent-sdk not installed"
        )
        return []

    return [
        AnalyzeCodeImpactTool(),
        CorrelateIncidentTool(),
        FindQueryOriginTool(),
        AnalyzeORMPatternsTool(),
    ]
