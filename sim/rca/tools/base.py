"""
Base classes for RCA tools.

Defines the interface that all RCA tools must implement,
providing a consistent API for the AI agent to use.

Features:
- Abstract base class for all RCA tools
- Tool registry with decorator-based registration
- Result caching for expensive tool calls
- Database context abstraction
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from functools import wraps
from typing import Any, Dict, Optional, Callable, Type, TypeVar
import hashlib
import json
import time

from sim.logging_config import get_logger

logger = get_logger(__name__)

# Type variable for tool classes
T = TypeVar("T", bound="RCATool")

# Global tool registry for decorator-based registration
_tool_registry: dict[str, Type["RCATool"]] = {}
_tool_categories: dict[str, list[str]] = {}


@dataclass
class ToolResult:
    """
    Result from a tool execution.
    
    Attributes:
        success: Whether the tool executed successfully
        data: The result data (structured)
        error: Error message if failed
        metadata: Additional metadata about the execution
    """
    success: bool
    data: Any = None
    error: Optional[str] = None
    metadata: dict = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result: Dict[str, Any] = {
            "success": self.success,
        }
        if self.success:
            result["data"] = self.data
        else:
            result["error"] = self.error
        if self.metadata:
            result["metadata"] = self.metadata
        return result
    
    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), default=str)
    
    @classmethod
    def ok(cls, data: Any, **metadata) -> "ToolResult":
        """Create a successful result."""
        return cls(success=True, data=data, metadata=metadata)
    
    @classmethod
    def fail(cls, error: str, **metadata) -> "ToolResult":
        """Create a failed result."""
        return cls(success=False, error=error, metadata=metadata)


def register_tool(
    name: Optional[str] = None,
    category: str = "general",
) -> Callable[[Type[T]], Type[T]]:
    """
    Decorator to register a tool class in the global registry.

    Usage:
        @register_tool(name="fetch_plan", category="investigation")
        class FetchPlanTool(RCATool):
            ...

    Args:
        name: Tool name (defaults to class name in snake_case)
        category: Tool category for organization

    Returns:
        Decorated class
    """

    def decorator(cls: Type[T]) -> Type[T]:
        tool_name = name or _to_snake_case(cls.__name__.replace("Tool", ""))

        # Register the tool class
        _tool_registry[tool_name] = cls

        # Add to category
        if category not in _tool_categories:
            _tool_categories[category] = []
        _tool_categories[category].append(tool_name)

        # Store metadata on the class
        cls._registered_name = tool_name  # type: ignore[attr-defined]
        cls._category = category  # type: ignore[attr-defined]

        logger.debug("Registered tool: %s (category: %s)", tool_name, category)

        return cls

    return decorator


def _to_snake_case(name: str) -> str:
    """Convert CamelCase to snake_case."""
    import re
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def get_registered_tools() -> dict[str, Type["RCATool"]]:
    """Get all registered tool classes."""
    return _tool_registry.copy()


def get_tools_by_category(category: str) -> list[str]:
    """Get tool names in a category."""
    return _tool_categories.get(category, [])


def get_all_categories() -> list[str]:
    """Get all tool categories."""
    return list(_tool_categories.keys())


class ToolCache:
    """
    Cache for tool execution results.

    Caches results based on tool name and parameter hash.
    Supports TTL-based expiration.
    """

    def __init__(self, ttl_seconds: int = 300, max_size: int = 100):
        """
        Initialize the cache.

        Args:
            ttl_seconds: Time-to-live for cache entries (default 5 minutes)
            max_size: Maximum number of cached entries
        """
        self._cache: dict[str, tuple[ToolResult, float]] = {}
        self._ttl = ttl_seconds
        self._max_size = max_size

    def _make_key(self, tool_name: str, params: dict) -> str:
        """Create a cache key from tool name and parameters."""
        param_str = json.dumps(params, sort_keys=True, default=str)
        param_hash = hashlib.md5(param_str.encode()).hexdigest()[:12]
        return f"{tool_name}:{param_hash}"

    def get(self, tool_name: str, params: dict) -> Optional[ToolResult]:
        """
        Get a cached result if available and not expired.

        Args:
            tool_name: Name of the tool
            params: Tool parameters

        Returns:
            Cached ToolResult or None
        """
        key = self._make_key(tool_name, params)

        if key not in self._cache:
            return None

        result, timestamp = self._cache[key]

        # Check if expired
        if time.time() - timestamp > self._ttl:
            del self._cache[key]
            return None

        logger.debug("Cache hit for tool: %s", tool_name)
        return result

    def set(self, tool_name: str, params: dict, result: ToolResult) -> None:
        """
        Cache a tool result.

        Args:
            tool_name: Name of the tool
            params: Tool parameters
            result: Result to cache
        """
        # Evict oldest entries if at capacity
        if len(self._cache) >= self._max_size:
            oldest_key = min(self._cache.keys(), key=lambda k: self._cache[k][1])
            del self._cache[oldest_key]

        key = self._make_key(tool_name, params)
        self._cache[key] = (result, time.time())
        logger.debug("Cached result for tool: %s", tool_name)

    def clear(self) -> None:
        """Clear all cached entries."""
        self._cache.clear()

    def invalidate(self, tool_name: str) -> int:
        """
        Invalidate all cache entries for a tool.

        Args:
            tool_name: Name of the tool

        Returns:
            Number of entries invalidated
        """
        keys_to_remove = [k for k in self._cache if k.startswith(f"{tool_name}:")]
        for key in keys_to_remove:
            del self._cache[key]
        return len(keys_to_remove)

    def stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        return {
            "size": len(self._cache),
            "max_size": self._max_size,
            "ttl_seconds": self._ttl,
        }


class RCATool(ABC):
    """
    Abstract base class for RCA investigation tools.
    
    All tools must:
    - Be read-only (no modifications to database)
    - Return structured, deterministic results
    - Handle errors gracefully
    - Respect timeout and row limits
    """
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Unique name of the tool."""
        pass
    
    @property
    @abstractmethod
    def description(self) -> str:
        """Description of what the tool does."""
        pass
    
    @property
    @abstractmethod
    def parameters_schema(self) -> dict:
        """JSON Schema for the tool's parameters."""
        pass
    
    @abstractmethod
    def execute(self, **kwargs) -> ToolResult:
        """
        Execute the tool with the given parameters.
        
        Args:
            **kwargs: Tool-specific parameters
        
        Returns:
            ToolResult with data or error
        """
        pass
    
    def to_tool_definition(self):
        """Convert to LLM ToolDefinition format."""
        from sim.rca.llm.base import ToolDefinition
        return ToolDefinition(
            name=self.name,
            description=self.description,
            parameters=self.parameters_schema,
        )


class ToolRegistry:
    """
    Registry for RCA tools.

    Manages available tools, provides lookup by name, and supports
    optional result caching for expensive tool calls.
    """

    def __init__(self, cache: Optional[ToolCache] = None):
        """
        Initialize the registry.

        Args:
            cache: Optional ToolCache for caching results
        """
        self._tools: dict[str, RCATool] = {}
        self._cache = cache

    def register(self, tool: RCATool) -> None:
        """
        Register a tool instance.

        Args:
            tool: Tool instance to register
        """
        self._tools[tool.name] = tool
        logger.debug("Registered tool instance: %s", tool.name)

    def register_class(
        self,
        tool_class: Type[RCATool],
        db_context: Optional["DatabaseContext"] = None,
    ) -> None:
        """
        Register a tool class by instantiating it.

        Args:
            tool_class: Tool class to instantiate and register
            db_context: Database context to pass to the tool
        """
        # Check if the tool accepts db_context
        import inspect
        sig = inspect.signature(tool_class.__init__)
        params = list(sig.parameters.keys())

        if "db_context" in params:
            tool = tool_class(db_context)  # type: ignore[call-arg]
        else:
            tool = tool_class()

        self.register(tool)

    def get(self, name: str) -> Optional[RCATool]:
        """
        Get a tool by name.

        Args:
            name: Tool name

        Returns:
            Tool instance or None
        """
        return self._tools.get(name)

    def execute(self, name: str, use_cache: bool = True, **kwargs) -> ToolResult:
        """
        Execute a tool by name.

        Args:
            name: Tool name
            use_cache: Whether to use cache (default True)
            **kwargs: Tool parameters

        Returns:
            ToolResult
        """
        tool = self.get(name)
        if tool is None:
            return ToolResult.fail(f"Unknown tool: {name}")

        # Check cache first
        if use_cache and self._cache:
            cached = self._cache.get(name, kwargs)
            if cached is not None:
                return cached

        # Execute tool
        try:
            start_time = time.time()
            result = tool.execute(**kwargs)
            elapsed = time.time() - start_time

            # Add timing metadata
            result.metadata["execution_time_ms"] = round(elapsed * 1000, 2)

            # Cache successful results
            if use_cache and self._cache and result.success:
                self._cache.set(name, kwargs, result)

            return result

        except Exception as e:
            logger.exception("Tool execution failed: %s", name)
            return ToolResult.fail(f"Tool execution error: {str(e)}")

    def enable_caching(self, ttl_seconds: int = 300, max_size: int = 100) -> None:
        """
        Enable caching for tool results.

        Args:
            ttl_seconds: Cache TTL in seconds
            max_size: Maximum cache size
        """
        self._cache = ToolCache(ttl_seconds=ttl_seconds, max_size=max_size)

    def disable_caching(self) -> None:
        """Disable caching."""
        self._cache = None

    def clear_cache(self) -> None:
        """Clear the cache."""
        if self._cache:
            self._cache.clear()

    def cache_stats(self) -> Optional[dict[str, Any]]:
        """Get cache statistics."""
        return self._cache.stats() if self._cache else None

    def list_tools(self) -> list[str]:
        """Get list of registered tool names."""
        return list(self._tools.keys())

    def get_all_tools(self) -> list[RCATool]:
        """Get all registered tools."""
        return list(self._tools.values())

    def get_tool_definitions(self):
        """Get all tools as LLM ToolDefinition objects."""
        return [t.to_tool_definition() for t in self._tools.values()]

    def __contains__(self, name: str) -> bool:
        """Check if a tool is registered."""
        return name in self._tools

    def __len__(self) -> int:
        """Get number of registered tools."""
        return len(self._tools)


class DatabaseContext:
    """
    Database context for tools that need database access.
    
    This is a database-agnostic interface that tools can use
    to execute queries against the database.
    """
    
    def __init__(
        self,
        execute_func: Callable[[str, Optional[tuple]], list[dict]],
        database_type: str = "sqlserver",
    ):
        """
        Initialize database context.
        
        Args:
            execute_func: Function to execute SQL and return results as list of dicts
            database_type: Type of database ('sqlserver', 'postgres', etc.)
        """
        self.execute = execute_func
        self.database_type = database_type
    
    def query(self, sql: str, params: Optional[tuple] = None) -> list[dict]:
        """
        Execute a query and return results.
        
        Args:
            sql: SQL query
            params: Query parameters
        
        Returns:
            List of row dictionaries
        """
        return self.execute(sql, params)


def create_db_context_from_connection_string(conn_str: str) -> DatabaseContext:
    """
    Create a DatabaseContext from an ODBC connection string.
    
    This enables live RCA analysis by providing tools with a real
    database connection to query DMVs and other system views.
    
    Uses mssql-python for thread-safe, high-performance connections.
    
    Args:
        conn_str: ODBC-style connection string for SQL Server
    
    Returns:
        DatabaseContext with live database access
    
    Example:
        conn_str = "DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost,14333;..."
        db_context = create_db_context_from_connection_string(conn_str)
        # Use db_context for direct SQL Server queries if needed
    """
    from decimal import Decimal
    from datetime import datetime, date, time, timedelta
    from sim.db_connection import connect as db_connect
    
    # Create thread-safe connection using mssql-python
    conn = db_connect(
        connection_string=conn_str,
        connection_timeout=30,
        autocommit=True,  # Read-only queries don't need transactions
    )
    
    def _convert_value(val):
        """Convert SQL types to JSON-serializable Python types."""
        if val is None:
            return None
        if isinstance(val, Decimal):
            return float(val)
        if isinstance(val, (datetime, date, time)):
            return val.isoformat()
        if isinstance(val, timedelta):
            return val.total_seconds()
        if isinstance(val, bytes):
            return val.hex()
        return val
    
    def execute_query(sql: str, params: Optional[tuple] = None) -> list[dict]:
        """Execute SQL and return results as list of dicts."""
        cursor = conn.cursor()
        try:
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            
            # Return empty list if no results (e.g., for non-SELECT statements)
            if cursor.description is None:
                return []
            
            columns = [col[0] for col in cursor.description]
            rows = []
            for row in cursor.fetchall():
                rows.append({col: _convert_value(val) for col, val in zip(columns, row)})
            return rows
        finally:
            cursor.close()
    
    return DatabaseContext(execute_query, database_type="sqlserver")
