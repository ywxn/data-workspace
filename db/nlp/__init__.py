"""NLP table selection package.

Re-exports the public API so that existing ``from db.nlp_selector import NLPTableSelector``
style imports can be updated to ``from db.nlp import NLPTableSelector``.
"""

from db.nlp.selector import NLPTableSelector
from db.nlp.data_models import TableSelectionResult

__all__ = [
    "NLPTableSelector",
    "TableSelectionResult",
]
