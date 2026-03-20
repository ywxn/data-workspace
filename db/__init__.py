"""Data-access layer package — database connectors, processing, NLP selection."""

from db.connector import DatabaseConnector
from db.processing import load_data
from db.nlp_selector import NLPTableSelector
from db.embedding_cache import get_sentence_transformer

__all__ = [
    "DatabaseConnector",
    "load_data",
    "NLPTableSelector",
    "get_sentence_transformer",
]
