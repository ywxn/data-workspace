"""Backwards-compatibility shim — re-exports from db.nlp package.

All logic has been moved to the db.nlp sub-package.
Import from db.nlp directly for new code.
"""

from db.nlp.selector import NLPTableSelector
from db.nlp.data_models import ColumnMetadata, TableMetadata, TableSelectionResult
from db.nlp.schema_normalizer import SchemaNormalizer

__all__ = [
    "NLPTableSelector",
    "SchemaNormalizer",
    "ColumnMetadata",
    "TableMetadata",
    "TableSelectionResult",
]
