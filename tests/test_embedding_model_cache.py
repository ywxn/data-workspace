"""Tests for shared sentence-transformer model caching."""

import sys
import types

import embedding_model_cache as cache
from memory.query_memory import UnifiedMemoryService
from nlp_table_selector import NLPTableSelector


def _reset_cache_state() -> None:
    """Reset module-level cache state to keep tests isolated."""
    cache._MODEL_CACHE.clear()
    cache._FAILED_KEYS.clear()
    cache._IMPORT_WARNING_LOGGED = False


class _FakeSentenceTransformer:
    """Tiny fake class to count constructor calls."""

    init_calls = 0

    def __init__(self, model_name: str, cache_folder: str = "models"):
        type(self).init_calls += 1
        self.model_name = model_name
        self.cache_folder = cache_folder

    def encode(self, _text, normalize_embeddings=True):
        return [0.1, 0.2, 0.3]


def test_shared_instance_reused_across_memory_and_nlp_selector(monkeypatch):
    """First load should be reused by later consumers in the same process."""
    _reset_cache_state()
    _FakeSentenceTransformer.init_calls = 0

    fake_module = types.SimpleNamespace(SentenceTransformer=_FakeSentenceTransformer)
    monkeypatch.setitem(sys.modules, "sentence_transformers", fake_module)

    # First use path: memory service.
    memory_service = UnifiedMemoryService(project_id="p1")
    model_from_memory = memory_service._get_embedding_model()

    # Second use path: NLP selector in another component.
    fake_connector = types.SimpleNamespace(engine=True, get_tables=lambda: [])
    selector = NLPTableSelector(fake_connector, model_name="all-MiniLM-L6-v2")
    model_from_selector = selector.model

    assert model_from_memory is not None
    assert model_from_selector is not None
    assert model_from_memory is model_from_selector
    assert _FakeSentenceTransformer.init_calls == 1


def test_model_name_aliases_map_to_single_cached_instance(monkeypatch):
    """Short and fully-qualified model names should share one cache key."""
    _reset_cache_state()
    _FakeSentenceTransformer.init_calls = 0

    fake_module = types.SimpleNamespace(SentenceTransformer=_FakeSentenceTransformer)
    monkeypatch.setitem(sys.modules, "sentence_transformers", fake_module)

    m1 = cache.get_sentence_transformer("all-MiniLM-L6-v2", cache_folder="models")
    m2 = cache.get_sentence_transformer(
        "sentence-transformers/all-MiniLM-L6-v2", cache_folder="models"
    )

    assert m1 is not None
    assert m2 is not None
    assert m1 is m2
    assert _FakeSentenceTransformer.init_calls == 1
