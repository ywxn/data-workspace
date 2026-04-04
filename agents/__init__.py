"""Agent system package — AI-powered data analysis agents."""

from agents.agent import AIAgent

# Re-export symbols used by tests and external callers so that
# patch("agents.XYZ") continues to work after the package split.
from agents.agent import ConfigManager, DatabaseConnector

# Re-export third-party names that tests patch on this namespace
try:
    from anthropic import Anthropic
except ImportError:
    pass

try:
    from openai import AsyncOpenAI
except ImportError:
    pass

__all__ = ["AIAgent"]
