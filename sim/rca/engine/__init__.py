"""
AI RCA Engine using AgentRCAEngine.

Single-loop agent reasoning with extended thinking for root cause analysis.

Usage:
    from sim.rca.engine import AgentRCAEngine
    from sim.rca.config import RCAConfig

    config = RCAConfig()
    engine = AgentRCAEngine(config)
    report = engine.analyze(data_source, incident_id)
"""

from sim.rca.engine.agent_engine import AgentRCAEngine, AgentRCAReport

__all__ = [
    "AgentRCAEngine",
    "AgentRCAReport",
]
