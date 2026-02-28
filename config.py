"""Configuration management for API keys and settings."""

import os
import json
import logging
from typing import Dict, Any, Optional, Tuple
from pathlib import Path
import sys
import re


class ConfigManager:
    """
    Manages application configuration including API keys and settings.

    Configuration is stored in a JSON file that persists between sessions.
    """

    CONFIG_FILE = "config.json"
    _logger = logging.getLogger(__name__)

    @staticmethod
    def get_config_path() -> str:
        """
        Get the full path to config.json in the root directory.

        Returns:
            Absolute path to config file
        """
        return os.path.join(
            os.path.dirname(os.path.abspath(__file__)), ConfigManager.CONFIG_FILE
        )

    @staticmethod
    def config_exists() -> bool:
        """
        Check if config.json exists.

        Returns:
            True if config file exists, False otherwise
        """
        return os.path.isfile(ConfigManager.get_config_path())

    @staticmethod
    def load_config() -> Dict[str, Any]:
        """
        Load configuration from config.json.

        Returns:
            Configuration dictionary, empty dict if file doesn't exist
        """
        config_path = ConfigManager.get_config_path()

        if not os.path.isfile(config_path):
            ConfigManager._logger.debug(f"Config file not found at {config_path}")
            return {}

        try:
            with open(config_path, "r") as f:
                config = json.load(f)
                ConfigManager._logger.info(f"Configuration loaded from {config_path}")
                return config
        except json.JSONDecodeError as e:
            ConfigManager._logger.error(f"Failed to parse config.json: {e}")
            return {}
        except Exception as e:
            ConfigManager._logger.error(f"Error loading config: {e}")
            return {}

    @staticmethod
    def save_config(config: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Save configuration to config.json.

        Args:
            config: Configuration dictionary to save

        Returns:
            Tuple of (success: bool, message: str)
        """
        try:
            config_path = ConfigManager.get_config_path()
            with open(config_path, "w") as f:
                json.dump(config, f, indent=2)
            ConfigManager._logger.info(f"Configuration saved to {config_path}")
            return True, "Configuration saved successfully."
        except Exception as e:
            msg = f"Failed to save configuration: {str(e)}"
            ConfigManager._logger.error(msg)
            return False, msg

    @staticmethod
    def get_api_key(provider: str) -> Optional[str]:
        """
        Get API key for a specific provider (openai or claude).

        Args:
            provider: API provider name ('openai' or 'claude')

        Returns:
            API key string or None if not configured
        """
        config = ConfigManager.load_config()

        if "api_keys" not in config:
            return None

        return config["api_keys"].get(provider.lower())

    @staticmethod
    def _validate_api_key_format(provider: str, api_key: str) -> Tuple[bool, str]:
        """
        Basic provider-specific API key format validation to catch obvious typos.

        This is intentionally conservative: it prevents blatantly invalid values
        (like short strings or missing expected prefixes) while avoiding
        over-strict checks that would break future key formats.
        """
        if not api_key or not isinstance(api_key, str):
            return False, "API key must be a non-empty string."

        p = provider.lower()

        if p == "openai":
            # OpenAI keys commonly start with 'sk-' or 'openai-'
            if re.match(r"^(sk-|openai-)[A-Za-z0-9\-_]{20,}$", api_key):
                return True, ""
            return False, (
                "OpenAI API key appears invalid — it should start with 'sk-' or 'openai-' "
                "and be at least ~20 characters."
            )

        if p == "claude":
            # Anthropic/Claude keys vary; accept common prefixes and reasonable length
            if re.match(r"^(api-|claude-|sk-)[A-Za-z0-9\-_]{20,}$", api_key):
                return True, ""
            return False, (
                "Claude API key appears invalid — it should start with 'api-' or 'claude-' "
                "(or 'sk-') and be at least ~20 characters."
            )

        # Generic fallback: require reasonable length
        if len(api_key) < 20:
            return False, "API key is too short to be valid."

        return True, ""

    @staticmethod
    def set_api_key(provider: str, api_key: str) -> Tuple[bool, str]:
        """
        Set API key for a specific provider.

        Args:
            provider: API provider name ('openai' or 'claude')
            api_key: The API key to store

        Returns:
            Tuple of (success: bool, message: str)
        """
        # Validate format before persisting
        ok, msg = ConfigManager._validate_api_key_format(provider, api_key)
        if not ok:
            ConfigManager._logger.warning(f"API key format validation failed: {msg}")
            return False, msg

        config = ConfigManager.load_config()

        if "api_keys" not in config:
            config["api_keys"] = {}

        config["api_keys"][provider.lower()] = api_key
        ConfigManager._logger.info(f"API key set for provider: {provider}")

        success, msg = ConfigManager.save_config(config)

        # If the agents module is already loaded, update its in-memory API key
        if success:
            try:
                agents_mod = sys.modules.get("agents")
                if agents_mod:
                    if provider.lower() == "openai":
                        setattr(agents_mod, "OPENAI_API_KEY", api_key)
                    elif provider.lower() == "claude":
                        setattr(agents_mod, "CLAUDE_API_KEY", api_key)
            except Exception:
                ConfigManager._logger.exception(
                    "Failed to update agents module with new API key"
                )

        return success, msg

    @staticmethod
    def get_default_api() -> str:
        """
        Get the default API provider (openai or claude).

        Returns:
            Default provider name
        """
        config = ConfigManager.load_config()
        return config.get("default_api", "openai")

    @staticmethod
    def set_default_api(provider: str) -> Tuple[bool, str]:
        """
        Set the default API provider.

        Args:
            provider: Provider to set as default

        Returns:
            Tuple of (success: bool, message: str)
        """
        config = ConfigManager.load_config()
        config["default_api"] = provider.lower()
        ConfigManager._logger.info(f"Default API provider set to: {provider}")

        success, msg = ConfigManager.save_config(config)

        # If the agents module is already loaded, update its in-memory DEFAULT_API
        if success:
            try:
                agents_mod = sys.modules.get("agents")
                if agents_mod:
                    setattr(agents_mod, "DEFAULT_API", provider.lower())
            except Exception:
                ConfigManager._logger.exception(
                    "Failed to update agents module with new default API"
                )

        return success, msg

    @staticmethod
    def has_any_api_key() -> bool:
        """
        Check if at least one API key is configured.

        Returns:
            True if at least one API key exists, False otherwise
        """
        config = ConfigManager.load_config()
        if "api_keys" not in config:
            return False
        return bool(config["api_keys"])

    @staticmethod
    def get_interaction_mode() -> str:
        """Return 'cxo' or 'analyst'. Default is 'analyst'."""
        config = ConfigManager.load_config()
        return config.get("interaction_mode", "analyst")

    @staticmethod
    def set_interaction_mode(mode: str) -> Tuple[bool, str]:
        """Set the interaction mode to 'cxo' or 'analyst'."""
        normalized = mode.lower().strip()
        if normalized not in ("cxo", "analyst"):
            return False, "Invalid mode. Use 'cxo' or 'analyst'."
        config = ConfigManager.load_config()
        config["interaction_mode"] = normalized
        ConfigManager._logger.info(f"Interaction mode set to: {normalized}")
        return ConfigManager.save_config(config)

    @staticmethod
    def get_semantic_layer_path() -> Optional[str]:
        """Get the saved semantic layer file path, or None if not set."""
        config = ConfigManager.load_config()
        return config.get("semantic_layer_path")

    @staticmethod
    def set_semantic_layer_path(path: str) -> Tuple[bool, str]:
        """Save the semantic layer file path to config."""
        config = ConfigManager.load_config()
        config["semantic_layer_path"] = path
        ConfigManager._logger.info(f"Semantic layer path set to: {path}")
        return ConfigManager.save_config(config)

    @staticmethod
    def get_prompt_expansion_enabled() -> bool:
        """Return whether LLM prompt expansion is enabled for NLP table selection."""
        config = ConfigManager.load_config()
        return config.get("use_prompt_expansion", True)

    @staticmethod
    def set_prompt_expansion_enabled(enabled: bool) -> Tuple[bool, str]:
        """Enable or disable LLM prompt expansion for NLP table selection."""
        config = ConfigManager.load_config()
        config["use_prompt_expansion"] = bool(enabled)
        ConfigManager._logger.info(
            f"Prompt expansion {'enabled' if enabled else 'disabled'}"
        )
        return ConfigManager.save_config(config)

    @staticmethod
    def get_table_selection_method() -> str:
        """
        Get the preferred table selection method.

        Returns:
            "manual" or "nlp"
        """
        config = ConfigManager.load_config()
        return config.get("table_selection_method", "manual")

    @staticmethod
    def set_table_selection_method(method: str) -> Tuple[bool, str]:
        """
        Set the preferred table selection method.

        Args:
            method: "manual" or "nlp"

        Returns:
            Tuple of (success: bool, message: str)
        """
        normalized = method.lower().strip()
        if normalized not in ["manual", "nlp"]:
            return False, "Invalid table selection method. Use 'manual' or 'nlp'."

        config = ConfigManager.load_config()
        config["table_selection_method"] = normalized
        ConfigManager._logger.info(f"Table selection method set to: {normalized}")
        return ConfigManager.save_config(config)
