"""Process-wide cache for sentence-transformer model instances."""

from threading import Lock
from typing import Any, Dict, Optional, Set, Tuple

from logger import get_logger

logger = get_logger(__name__)

_MODEL_CACHE: Dict[Tuple[str, str], Any] = {}
_FAILED_KEYS: Set[Tuple[str, str]] = set()
_LOCK = Lock()
_IMPORT_WARNING_LOGGED = False


def normalize_sentence_transformer_name(model_name: str) -> str:
    """Normalize short model ids to full Hugging Face sentence-transformers ids."""
    cleaned = (model_name or "").strip()
    if not cleaned:
        return "sentence-transformers/all-MiniLM-L6-v2"
    if "/" in cleaned:
        return cleaned
    return f"sentence-transformers/{cleaned}"


def get_sentence_transformer(
    model_name: str,
    cache_folder: str = "models",
) -> Optional[Any]:
    """Return a shared SentenceTransformer instance for the given model."""
    global _IMPORT_WARNING_LOGGED

    normalized_name = normalize_sentence_transformer_name(model_name)
    key = (normalized_name, cache_folder)

    with _LOCK:
        cached = _MODEL_CACHE.get(key)
        if cached is not None:
            return cached
        if key in _FAILED_KEYS:
            return None

        try:
            from sentence_transformers import SentenceTransformer
        except ImportError:
            if not _IMPORT_WARNING_LOGGED:
                logger.warning(
                    "sentence-transformers not available; semantic features disabled"
                )
                _IMPORT_WARNING_LOGGED = True
            _FAILED_KEYS.add(key)
            return None

        try:
            model = SentenceTransformer(normalized_name, cache_folder=cache_folder)
            _MODEL_CACHE[key] = model
            logger.info("Loaded sentence-transformer model: %s", normalized_name)
            return model
        except Exception as exc:
            _FAILED_KEYS.add(key)
            logger.warning(
                "Failed loading sentence-transformer model '%s': %s",
                normalized_name,
                exc,
            )
            return None
