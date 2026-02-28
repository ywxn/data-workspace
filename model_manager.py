"""
Utility module for managing and hosting local LLM models.

Handles downloading GGUF format models and running a built-in
OpenAI-compatible server via llama-cpp-python so users who don't
have their own local LLM can still run fully offline.
"""

import os
import sys
import shutil
import subprocess
import signal
import time
import threading
from pathlib import Path
from typing import Optional, Tuple, Callable, Dict, Any, List
from urllib.request import urlopen

from logger import get_logger
from constants import (
    MODELS_DIR,
    HOSTED_MODEL_CATALOG,
    HOSTED_LLM_DEFAULT_PORT,
    HOSTED_LLM_DEFAULT_HOST,
    HOSTED_LLM_CONTEXT_SIZE,
    HOSTED_LLM_GPU_LAYERS,
)

logger = get_logger(__name__)

# --------------------------------------------------------------------------
#  Singleton reference to the managed server process
# --------------------------------------------------------------------------
_server_process: Optional[subprocess.Popen] = None
_server_lock = threading.Lock()


def get_models_directory() -> str:
    """Get the path to the local models directory."""
    return str(MODELS_DIR)


def ensure_models_directory() -> bool:
    """Ensure the models directory exists. Returns True on success."""
    try:
        MODELS_DIR.mkdir(parents=True, exist_ok=True)
        return True
    except Exception as e:
        logger.error(f"Failed to create models directory: {e}")
        return False


def list_available_models() -> List[str]:
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
        logger.info(f"Found {len(models)} local GGUF models")
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
        check_path = models_dir if os.path.exists(models_dir) else os.path.dirname(models_dir)
        stat = shutil.disk_usage(check_path)
        available_gb = stat.free / (1024**3)
        return available_gb >= required_gb
    except Exception as e:
        logger.error(f"Error checking disk space: {e}")
        return True  # assume OK if check fails


def download_model(
    url: str,
    model_name: str,
    callback: Optional[Callable[[float, int, int], None]] = None,
) -> Tuple[bool, str]:
    """
    Download a GGUF model to the models directory.

    Args:
        url: URL to download from
        model_name: Filename to save as
        callback: Optional ``callback(progress_pct, downloaded_bytes, total_bytes)``

    Returns:
        Tuple of (success, message)
    """
    ensure_models_directory()
    models_dir = get_models_directory()
    model_path = os.path.join(models_dir, model_name)

    if os.path.exists(model_path):
        msg = f"Model already exists at {model_path}"
        logger.info(msg)
        return True, msg

    try:
        logger.info(f"Starting download: {model_name} from {url}")

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
        logger.error(f"Failed to download model: {e}")
        if os.path.exists(model_path):
            try:
                os.remove(model_path)
            except Exception:
                pass
        return False, f"Download failed: {e}"


def validate_model_file(model_path: str) -> Tuple[bool, str]:
    """
    Validate that a file is a valid GGUF model.

    Returns:
        Tuple of (is_valid, message)
    """
    if not os.path.isfile(model_path):
        return False, "File not found"

    if not model_path.endswith(".gguf"):
        return False, "File is not a GGUF model (.gguf extension required)"

    try:
        file_size = os.path.getsize(model_path)
        if file_size < 100 * 1024 * 1024:
            return False, (
                f"File too small ({file_size / 1024 / 1024:.1f} MB) — "
                "may not be a valid model"
            )

        with open(model_path, "rb") as f:
            magic = f.read(4)
            if magic != b"GGUF":
                return False, "File does not appear to be a valid GGUF model"

        return True, "File appears to be a valid GGUF model"
    except Exception as e:
        return False, f"Error validating file: {e}"


def get_recommended_models() -> Dict[str, dict]:
    """Return the catalog of recommended downloadable models."""
    return dict(HOSTED_MODEL_CATALOG)


# ======================================================================
#  Built-in model server (llama-cpp-python)
# ======================================================================

def is_llama_cpp_available() -> bool:
    """Check whether ``llama_cpp`` is importable."""
    try:
        import llama_cpp  # noqa: F401
        return True
    except ImportError:
        return False


def is_server_running() -> bool:
    """Return True if the managed model server process is alive."""
    with _server_lock:
        return _server_process is not None and _server_process.poll() is None


def get_hosted_url(
    host: str = HOSTED_LLM_DEFAULT_HOST,
    port: int = HOSTED_LLM_DEFAULT_PORT,
) -> str:
    """Return the OpenAI-compatible base URL for the hosted server."""
    return f"http://{host}:{port}/v1"


def start_model_server(
    model_path: str,
    host: str = HOSTED_LLM_DEFAULT_HOST,
    port: int = HOSTED_LLM_DEFAULT_PORT,
    n_ctx: int = HOSTED_LLM_CONTEXT_SIZE,
    n_gpu_layers: int = HOSTED_LLM_GPU_LAYERS,
    timeout: float = 60.0,
) -> Tuple[bool, str]:
    """
    Start the built-in llama-cpp-python OpenAI-compatible server.

    The server runs as a subprocess so it survives GUI thread restarts
    and can be stopped cleanly.

    Args:
        model_path: Absolute path to a .gguf model file
        host:       Bind address (default 127.0.0.1)
        port:       Port number  (default 8911)
        n_ctx:      Context window size
        n_gpu_layers: Layers to offload to GPU (0 = CPU-only)
        timeout:    Seconds to wait for the server to become ready

    Returns:
        Tuple of (success, message)
    """
    global _server_process

    if is_server_running():
        return True, "Server is already running."

    if not os.path.isfile(model_path):
        return False, f"Model file not found: {model_path}"

    valid, msg = validate_model_file(model_path)
    if not valid:
        return False, msg

    if not is_llama_cpp_available():
        return False, (
            "llama-cpp-python is not installed.\n\n"
            "Install it with:\n"
            "  pip install llama-cpp-python\n\n"
            "For GPU support (NVIDIA):\n"
            "  CMAKE_ARGS=\"-DGGML_CUDA=on\" pip install llama-cpp-python"
        )

    # Build the command using the same Python interpreter
    cmd = [
        sys.executable, "-m", "llama_cpp.server",
        "--model", model_path,
        "--host", host,
        "--port", str(port),
        "--n_ctx", str(n_ctx),
        "--n_gpu_layers", str(n_gpu_layers),
    ]

    logger.info(f"Starting hosted model server: {' '.join(cmd)}")

    try:
        creation_flags = 0
        if sys.platform == "win32":
            creation_flags = subprocess.CREATE_NO_WINDOW

        # Write stderr to a temp file so we can read it if the process dies,
        # but don't use PIPE (which deadlocks when the buffer fills up).
        import tempfile

        stderr_file = tempfile.TemporaryFile(mode="w+b")

        with _server_lock:
            _server_process = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=stderr_file,
                creationflags=creation_flags,
            )

        # Give the process a moment to fail on import / arg errors
        time.sleep(2.0)
        if _server_process.poll() is not None:
            stderr_file.seek(0)
            stderr_text = stderr_file.read().decode(errors="replace")
            stderr_file.close()
            return False, f"Server process exited immediately (code {_server_process.returncode}):\n{stderr_text[-2000:]}"

        # Wait for the server to accept connections
        import httpx

        base_url = get_hosted_url(host, port)
        deadline = time.time() + timeout
        last_err = None
        while time.time() < deadline:
            try:
                resp = httpx.get(f"{base_url}/models", timeout=3.0)
                if resp.status_code == 200:
                    logger.info(f"Hosted model server ready at {base_url}")
                    stderr_file.close()
                    return True, f"Server started at {base_url}"
            except Exception as e:
                last_err = e

            # Check if process has died
            if _server_process.poll() is not None:
                stderr_file.seek(0)
                stderr_text = stderr_file.read().decode(errors="replace")
                stderr_file.close()
                return False, f"Server process exited unexpectedly (code {_server_process.returncode}):\n{stderr_text[-2000:]}"

            time.sleep(1.0)

        # Timed out — grab whatever stderr we have for diagnostics
        stderr_file.seek(0)
        stderr_text = stderr_file.read().decode(errors="replace")
        stderr_file.close()

        # Kill the process since it never became ready
        stop_model_server()

        diagnostic = stderr_text[-2000:].strip()
        return False, (
            f"Server did not become ready within {timeout}s.\n"
            f"Last connection error: {last_err}\n"
            + (f"\nServer stderr:\n{diagnostic}" if diagnostic else "")
        )

    except Exception as e:
        logger.error(f"Failed to start model server: {e}")
        stop_model_server()
        return False, f"Failed to start server: {e}"


def stop_model_server() -> Tuple[bool, str]:
    """
    Stop the managed model server process.

    Returns:
        Tuple of (success, message)
    """
    global _server_process

    with _server_lock:
        if _server_process is None:
            return True, "No server is running."

        try:
            if _server_process.poll() is None:
                if sys.platform == "win32":
                    _server_process.terminate()
                else:
                    _server_process.send_signal(signal.SIGTERM)

                try:
                    _server_process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    _server_process.kill()
                    _server_process.wait(timeout=5)

            logger.info("Hosted model server stopped.")
            _server_process = None
            return True, "Server stopped."
        except Exception as e:
            logger.error(f"Error stopping server: {e}")
            _server_process = None
            return False, f"Error stopping server: {e}"


def get_server_status() -> Dict[str, Any]:
    """
    Return a status dict about the hosted model server.

    Keys: running (bool), pid (int|None), url (str|None)
    """
    running = is_server_running()
    pid = None
    url = None
    if running and _server_process is not None:
        pid = _server_process.pid
        url = get_hosted_url()
    return {"running": running, "pid": pid, "url": url}
