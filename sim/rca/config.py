"""Configuration for the AI RCA system.

All sensitive configuration (API keys) is loaded from environment variables.
No hardcoded secrets in the codebase.

Environment Variables:
    RCA_LLM_PROVIDER: LLM provider to use (only 'anthropic' is supported)
    ANTHROPIC_API_KEY: API key for Anthropic
    RCA_MODEL: Model name to use (defaults to provider-specific model)
    RCA_MAX_TOKENS: Maximum tokens for LLM responses (default: 4096)
    RCA_TEMPERATURE: Temperature for LLM responses (default: 0.1)
    RCA_MAX_TOOL_ITERATIONS: Maximum tool-calling iterations (default: 10)
"""

import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class LLMProvider(Enum):
    """Supported LLM providers."""
    ANTHROPIC = "anthropic"


# Default models for each provider
DEFAULT_MODELS = {
    LLMProvider.ANTHROPIC: "claude-opus-4-6",
}


@dataclass
class RCAConfig:
    """
    Configuration for the AI RCA system.

    All API keys and sensitive settings are loaded from environment variables.
    This ensures no secrets are stored in code.

    Attributes:
        provider: LLM provider to use (anthropic)
        model: Model name (defaults to provider-specific default)
        max_tokens: Maximum tokens for LLM responses
        temperature: Temperature for LLM generation (lower = more deterministic)
        max_tool_iterations: Maximum number of tool-calling iterations
        top_k_queries: Number of top queries to analyze
        top_k_waits: Number of top wait types to analyze
        include_sql_text: Whether to include SQL text in analysis
        debug: Enable debug output
        use_agent: Use new agent-style engine (default True)
        thinking_budget: Token budget for extended thinking (agent mode)
    """

    # LLM settings (loaded from env vars)
    provider: LLMProvider = field(
        default_factory=lambda: LLMProvider(
            os.environ.get("RCA_LLM_PROVIDER", "anthropic").lower()
        )
    )
    model: Optional[str] = field(
        default_factory=lambda: os.environ.get("RCA_MODEL")
    )
    max_tokens: int = field(
        default_factory=lambda: int(os.environ.get("RCA_MAX_TOKENS", "4096"))
    )
    temperature: float = field(
        default_factory=lambda: float(os.environ.get("RCA_TEMPERATURE", "0.1"))
    )
    max_tool_iterations: int = field(
        default_factory=lambda: int(os.environ.get("RCA_MAX_TOOL_ITERATIONS", "30"))
    )

    # Analysis settings
    top_k_queries: int = 10
    top_k_waits: int = 10
    include_sql_text: bool = True

    # Agent mode settings
    use_agent: bool = field(
        default_factory=lambda: os.environ.get("RCA_USE_AGENT", "true").lower() in ("1", "true", "yes")
    )
    thinking_budget: int = field(
        default_factory=lambda: int(os.environ.get("RCA_THINKING_BUDGET", "20000"))
    )

    # Debug settings
    debug: bool = field(
        default_factory=lambda: os.environ.get("RCA_DEBUG", "").lower() in ("1", "true", "yes")
    )
    
    def __post_init__(self):
        """Set default model based on provider if not specified."""
        if self.model is None:
            self.model = DEFAULT_MODELS.get(self.provider)
    
    @property
    def api_key(self) -> str:
        """
        Get the API key for the configured provider.
        
        Raises:
            ValueError: If the required API key is not set
        """
        if self.provider == LLMProvider.ANTHROPIC:
            key = os.environ.get("ANTHROPIC_API_KEY")
            if not key:
                raise ValueError(
                    "ANTHROPIC_API_KEY environment variable is required when using Anthropic provider. "
                    "Set it with: export ANTHROPIC_API_KEY='your-key-here'"
                )
            return key
        raise ValueError(f"Unsupported provider: {self.provider}")
    
    def validate(self) -> None:
        """
        Validate the configuration.
        
        Raises:
            ValueError: If configuration is invalid
        """
        # Check API key is available
        _ = self.api_key
        
        # Validate numeric ranges
        if self.max_tokens < 100:
            raise ValueError("max_tokens must be at least 100")
        if not (0.0 <= self.temperature <= 2.0):
            raise ValueError("temperature must be between 0.0 and 2.0")
        if self.max_tool_iterations < 1:
            raise ValueError("max_tool_iterations must be at least 1")
        if self.top_k_queries < 1:
            raise ValueError("top_k_queries must be at least 1")
        if self.top_k_waits < 1:
            raise ValueError("top_k_waits must be at least 1")
    
    @classmethod
    def from_env(cls) -> "RCAConfig":
        """
        Create configuration from environment variables.
        
        This is a convenience method that creates a config instance
        with all settings loaded from environment variables.
        
        Returns:
            RCAConfig instance
        """
        return cls()
    
    def to_dict(self) -> dict:
        """
        Convert configuration to dictionary (without sensitive data).

        Returns:
            Dictionary representation (API keys are masked)
        """
        return {
            "provider": self.provider.value,
            "model": self.model,
            "max_tokens": self.max_tokens,
            "temperature": self.temperature,
            "max_tool_iterations": self.max_tool_iterations,
            "top_k_queries": self.top_k_queries,
            "top_k_waits": self.top_k_waits,
            "include_sql_text": self.include_sql_text,
            "use_agent": self.use_agent,
            "thinking_budget": self.thinking_budget,
            "debug": self.debug,
            "api_key": "***" if self._has_api_key() else "(not set)",
        }
    
    def _has_api_key(self) -> bool:
        """Check if API key is available without raising."""
        try:
            _ = self.api_key
            return True
        except ValueError:
            return False
