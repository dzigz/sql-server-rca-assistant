"""
AI-powered Root Cause Analysis (RCA) package.

Heavy runtime components (datasources, engine, tool registry) are loaded lazily
to keep imports lightweight for modules that only need configuration/types.
"""

from sim.rca.config import RCAConfig, LLMProvider

__version__ = "2.0.0"

__all__ = [
    "AgentRCAEngine",
    "AgentRCAReport",
    "RCAConfig",
    "ClickHouseDataSource",
    "TimeWindow",
    "LLMProvider",
    "create_clickhouse_tool_registry",
]


def __getattr__(name: str):
    """Lazy-load runtime modules to avoid optional dependency import failures."""
    if name in {"AgentRCAEngine", "AgentRCAReport"}:
        from sim.rca.engine import AgentRCAEngine, AgentRCAReport

        return {"AgentRCAEngine": AgentRCAEngine, "AgentRCAReport": AgentRCAReport}[name]

    if name in {
        "ClickHouseDataSource",
        "TimeWindow",
    }:
        from sim.rca.datasources import ClickHouseDataSource, TimeWindow

        return {
            "ClickHouseDataSource": ClickHouseDataSource,
            "TimeWindow": TimeWindow,
        }[name]

    if name == "create_clickhouse_tool_registry":
        from sim.rca.tools import create_clickhouse_tool_registry

        return create_clickhouse_tool_registry

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
