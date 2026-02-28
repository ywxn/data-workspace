"""
Utility module for managing local LLM models.

Handles downloading and managing GGUF format models for local inference.
"""

import os
import sys
from pathlib import Path
from typing import Optional, Tuple
from urllib.request import urlopen
import json

from config import ConfigManager
from logger import get_logger

logger = get_logger(__name__)

# Popular GGUF models for local inference
RECOMMENDED_MODELS = {
    "llama2-7b-q4": {
        "name": "Llama 2 7B Quantized (Q4)",
        "url": "https://huggingface.co/TheBloke/Llama-2-7B-GGUF/resolve/main/llama-2-7b.Q4_K_M.gguf",
        "size_gb": 4.6,
        "description": "Good balance of speed and quality",
    },
    "llama2-7b-q5": {
        "name": "Llama 2 7B Quantized (Q5)",
        "url": "https://huggingface.co/TheBloke/Llama-2-7B-GGUF/resolve/main/llama-2-7b.Q5_K_M.gguf",
        "size_gb": 5.5,
        "description": "Better quality, requires more space",
    },
    "mistral-7b-q4": {
        "name": "Mistral 7B Quantized (Q4)",
        "url": "https://huggingface.co/TheBloke/Mistral-7B-Instruct-v0.1-GGUF/resolve/main/Mistral-7B-Instruct-v0.1.Q4_K_M.gguf",
        "size_gb": 4.3,
        "description": "Fast and capable for most tasks",
    },
}


def get_models_directory() -> str:
    """Get the path to the local models directory."""
    return ConfigManager.get_local_model_path()


def ensure_models_directory() -> bool:
    """Ensure the models directory exists. Returns True on success."""
    return ConfigManager.ensure_local_models_dir()


def list_available_models() -> list[str]:
    """
    List GGUF model files in the models directory.

    Returns:
        List of model file names
    """
    models_dir = get_models_directory()
    try:
        if not os.path.isdir(models_dir):
            return []

        models = [f for f in os.listdir(models_dir) if f.endswith(".gguf")]
        logger.info(f"Found {len(models)} local models")
        return models
    except Exception as e:
        logger.error(f"Error listing models: {e}")
        return []


def get_model_info(model_path: str) -> Optional[dict]:
    """
    Get information about a model file.

    Args:
        model_path: Path to the model file

    Returns:
        Dictionary with file size and other info, or None if file not found
    """
    try:
        if not os.path.isfile(model_path):
            return None

        file_size = os.path.getsize(model_path)
        file_size_gb = file_size / (1024**3)

        return {
            "path": model_path,
            "name": os.path.basename(model_path),
            "size_bytes": file_size,
            "size_gb": round(file_size_gb, 2),
        }
    except Exception as e:
        logger.error(f"Error getting model info: {e}")
        return None


def check_disk_space(required_gb: float) -> bool:
    """
    Check if there's enough disk space for model download.

    Args:
        required_gb: Required space in GB

    Returns:
        True if sufficient space available
    """
    try:
        models_dir = get_models_directory()
        if not os.path.exists(models_dir):
            models_dir = os.path.dirname(models_dir)

        import shutil

        stat = shutil.disk_usage(models_dir)
        available_gb = stat.free / (1024**3)

        return available_gb >= required_gb
    except Exception as e:
        logger.error(f"Error checking disk space: {e}")
        # Assume sufficient space if check fails
        return True


def download_model(url: str, model_name: str, callback=None) -> Tuple[bool, str]:
    """
    Download a GGUF model to the models directory.

    Args:
        url: URL to download from
        model_name: Name to save the model as
        callback: Optional callback function for progress updates

    Returns:
        Tuple of (success: bool, message: str)
    """
    ensure_models_directory()
    models_dir = get_models_directory()
    model_path = os.path.join(models_dir, model_name)

    if os.path.exists(model_path):
        msg = f"Model already exists at {model_path}"
        logger.info(msg)
        return True, msg

    try:
        logger.info(f"Starting download: {model_name}")
        logger.info(f"URL: {url}")

        with urlopen(url) as response:
            total_size = int(response.headers.get("content-length", 0))
            downloaded = 0

            with open(model_path, "wb") as f:
                while True:
                    chunk = response.read(8192)
                    if not chunk:
                        break

                    f.write(chunk)
                    downloaded += len(chunk)

                    if callback and total_size > 0:
                        progress = (downloaded / total_size) * 100
                        callback(progress, downloaded, total_size)

        logger.info(f"Download completed: {model_path}")
        return True, f"Model downloaded successfully to {model_path}"

    except Exception as e:
        logger.error(f"Failed to download model: {str(e)}")
        # Clean up partial download
        if os.path.exists(model_path):
            try:
                os.remove(model_path)
            except Exception:
                pass
        return False, f"Download failed: {str(e)}"


def get_recommended_models() -> dict:
    """
    Get list of recommended models for download.

    Returns:
        Dictionary of model recommendations
    """
    return RECOMMENDED_MODELS


def validate_model_file(model_path: str) -> Tuple[bool, str]:
    """
    Validate that a file is a valid GGUF model.

    Args:
        model_path: Path to the model file

    Returns:
        Tuple of (is_valid: bool, message: str)
    """
    if not os.path.isfile(model_path):
        return False, "File not found"

    if not model_path.endswith(".gguf"):
        return False, "File is not a GGUF model (.gguf extension required)"

    try:
        # Check if file has minimum size for a valid model (typically at least 100MB)
        file_size = os.path.getsize(model_path)
        min_size = 100 * 1024 * 1024  # 100MB

        if file_size < min_size:
            return (
                False,
                f"File too small ({file_size / 1024 / 1024:.1f}MB) - may not be a valid model",
            )

        # Try reading the first few bytes to check for GGUF header
        with open(model_path, "rb") as f:
            magic = f.read(4)
            # GGUF files should start with specific magic bytes
            if magic != b"GGUF":
                return False, "File does not appear to be a valid GGUF model"

        return True, "File appears to be a valid GGUF model"

    except Exception as e:
        return False, f"Error validating file: {str(e)}"
