"""Configuration management for API keys and settings."""

import os
import json
import logging
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path
import sys
import re

from constants import (
    LOCAL_LLM_DEFAULT_URL,
    LOCAL_LLM_DEFAULT_MODEL,
    HOSTED_LLM_DEFAULT_PORT,
    HOSTED_LLM_CONTEXT_SIZE,
    HOSTED_LLM_GPU_LAYERS,
)

# ---------------------------------------------------------------------------
#  Secure credential storage helpers (keyring with graceful fallback)
# ---------------------------------------------------------------------------
_KEYRING_SERVICE = "ai_data_workspace"


def _keyring_available() -> bool:
    """Return True if the keyring package is importable and functional."""
    try:
        import keyring  # noqa: F401
        # Quick smoke-test – some backends (e.g. chainer) silently fail
        keyring.get_credential(_KEYRING_SERVICE, "__probe__")
        return True
    except Exception:
        return False


def _keyring_get(key: str) -> Optional[str]:
    """Retrieve a secret from the OS keyring, or None on failure."""
    try:
        import keyring
        return keyring.get_password(_KEYRING_SERVICE, key)
    except Exception:
        return None


def _keyring_set(key: str, value: str) -> bool:
    """Store a secret in the OS keyring.  Returns True on success."""
    try:
        import keyring
        keyring.set_password(_KEYRING_SERVICE, key, value)
        return True
    except Exception:
        return False


def _keyring_delete(key: str) -> bool:
    """Delete a secret from the OS keyring.  Returns True on success."""
    try:
        import keyring
        keyring.delete_password(_KEYRING_SERVICE, key)
        return True
    except Exception:
        return False


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

        Looks in the OS keyring first, then falls back to config.json.

        Args:
            provider: API provider name ('openai' or 'claude')

        Returns:
            API key string or None if not configured
        """
        p = provider.lower()

        # 1. Try secure storage
        secret = _keyring_get(f"api_key_{p}")
        if secret:
            return secret

        # 2. Fallback to config.json (legacy / keyring unavailable)
        config = ConfigManager.load_config()
        if "api_keys" not in config:
            return None
        return config["api_keys"].get(p)

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

        p = provider.lower()

        # Attempt secure storage first
        if _keyring_set(f"api_key_{p}", api_key):
            ConfigManager._logger.info(
                f"API key for {provider} stored in OS keyring."
            )
            # Remove from config.json if it was previously stored there
            config = ConfigManager.load_config()
            if "api_keys" in config and p in config["api_keys"]:
                del config["api_keys"][p]
                ConfigManager.save_config(config)
        else:
            # Fallback: store in config.json
            ConfigManager._logger.warning(
                "OS keyring unavailable — storing API key in config.json."
            )
            config = ConfigManager.load_config()
            if "api_keys" not in config:
                config["api_keys"] = {}
            config["api_keys"][p] = api_key
            ConfigManager.save_config(config)

        ConfigManager._logger.info(f"API key set for provider: {provider}")
        success = True
        msg = "API key saved."

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
        Check if at least one API key is configured (keyring or config.json).

        Returns:
            True if at least one API key exists, False otherwise
        """
        # Check keyring first
        for provider in ("openai", "claude"):
            if _keyring_get(f"api_key_{provider}"):
                return True

        # Fallback to config.json
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

    # ------------------------------------------------------------------
    # Local LLM configuration (from feature branch)
    # ------------------------------------------------------------------

    @staticmethod
    def get_local_llm_config() -> Dict[str, str]:
        """
        Return local LLM connection settings.

        Returns:
            Dict with keys 'local_llm_url' and 'local_llm_model'
        """
        config = ConfigManager.load_config()
        return {
            "local_llm_url": config.get(
                "local_llm_url", LOCAL_LLM_DEFAULT_URL
            ),
            "local_llm_model": config.get("local_llm_model", LOCAL_LLM_DEFAULT_MODEL),
        }

    @staticmethod
    def set_local_llm_config(
        url: str, model: str
    ) -> Tuple[bool, str]:
        """
        Persist local LLM connection settings.

        Args:
            url: Base URL of the OpenAI-compatible local LLM endpoint
            model: Model name to request from the local server

        Returns:
            Tuple of (success, message)
        """
        url = url.strip().rstrip("/")
        model = model.strip()
        if not url:
            return False, "Local LLM URL must not be empty."
        if not model:
            return False, "Local LLM model name must not be empty."

        config = ConfigManager.load_config()
        config["local_llm_url"] = url
        config["local_llm_model"] = model
        ConfigManager._logger.info(
            f"Local LLM config saved: url={url}, model={model}"
        )
        return ConfigManager.save_config(config)

    # ------------------------------------------------------------------
    # Hosted (built-in) model server configuration (from feature branch)
    # ------------------------------------------------------------------

    @staticmethod
    def get_hosted_llm_config() -> Dict[str, Any]:
        """
        Return hosted model server settings.

        Keys: hosted_model_path, hosted_port, hosted_context_size, hosted_gpu_layers, hosted_auto_start
        """
        config = ConfigManager.load_config()
        return {
            "hosted_model_path": config.get("hosted_model_path", ""),
            "hosted_port": config.get("hosted_port", HOSTED_LLM_DEFAULT_PORT),
            "hosted_context_size": config.get("hosted_context_size", HOSTED_LLM_CONTEXT_SIZE),
            "hosted_gpu_layers": config.get("hosted_gpu_layers", HOSTED_LLM_GPU_LAYERS),
            "hosted_auto_start": config.get("hosted_auto_start", False),
        }

    @staticmethod
    def set_hosted_llm_config(
        model_path: str,
        port: int = HOSTED_LLM_DEFAULT_PORT,
        context_size: int = HOSTED_LLM_CONTEXT_SIZE,
        gpu_layers: int = HOSTED_LLM_GPU_LAYERS,
        auto_start: bool = False,
    ) -> Tuple[bool, str]:
        """
        Persist hosted model server settings.

        Args:
            model_path: Path to the .gguf model file
            port: Server port
            context_size: Context window size in tokens
            gpu_layers: Number of layers to offload to GPU
            auto_start: Whether to auto-start the server on app launch

        Returns:
            Tuple of (success, message)
        """
        config = ConfigManager.load_config()
        config["hosted_model_path"] = model_path
        config["hosted_port"] = port
        config["hosted_context_size"] = context_size
        config["hosted_gpu_layers"] = gpu_layers
        config["hosted_auto_start"] = auto_start
        ConfigManager._logger.info(
            f"Hosted LLM config saved: model={model_path}, port={port}, "
            f"context_size={context_size}, gpu_layers={gpu_layers}, auto_start={auto_start}"
        )
        return ConfigManager.save_config(config)

    # ------------------------------------------------------------------
    # Multi-database connection configuration (from test branch)
    # ------------------------------------------------------------------

    @staticmethod
    def save_multi_db_config(
        configs: List[Dict[str, Any]],
    ) -> Tuple[bool, str]:
        """
        Persist a list of multi-database connection descriptors to config.json.

        Passwords are stripped before saving for security.

        Args:
            configs: List of connection config dicts (db_type, credentials, alias, …)

        Returns:
            Tuple of (success, message)
        """
        safe_configs: List[Dict[str, Any]] = []
        for cfg in configs:
            safe_cfg = {
                "alias": cfg.get("alias", ""),
                "db_type": cfg.get("db_type", ""),
                "table_selection_method": cfg.get("table_selection_method", "manual"),
            }
            creds = cfg.get("credentials", {})
            safe_creds = {k: v for k, v in creds.items() if k != "password"}
            safe_cfg["credentials"] = safe_creds
            # Include semantic layer path/data if present
            if cfg.get("semantic_layer"):
                safe_cfg["semantic_layer"] = cfg["semantic_layer"]
            safe_configs.append(safe_cfg)

        config = ConfigManager.load_config()
        config["multi_db_connections"] = safe_configs
        ConfigManager._logger.info(
            f"Saving {len(safe_configs)} multi-db connection descriptors"
        )
        return ConfigManager.save_config(config)

    @staticmethod
    def load_multi_db_config() -> List[Dict[str, Any]]:
        """
        Load saved multi-database connection descriptors from config.json.

        Returns:
            List of connection config dicts (passwords will be empty strings)
        """
        config = ConfigManager.load_config()
        return config.get("multi_db_connections", [])
