"""
LLM client factory.

Creates the appropriate LLM client based on configuration.
"""

from sim.rca.config import RCAConfig, LLMProvider
from sim.rca.llm.base import LLMClient


def create_llm_client(config: RCAConfig) -> LLMClient:
    """
    Create an LLM client based on the configuration.
    
    Args:
        config: RCA configuration specifying provider and settings
    
    Returns:
        LLMClient instance for the configured provider
    
    Raises:
        ValueError: If the provider is not supported
        ImportError: If the required package is not installed
    """
    if config.provider == LLMProvider.ANTHROPIC:
        from sim.rca.llm.anthropic_client import AnthropicClient
        return AnthropicClient(config)
    raise ValueError(f"Unsupported LLM provider: {config.provider}")
