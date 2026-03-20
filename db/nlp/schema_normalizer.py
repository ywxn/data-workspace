"""Schema identifier normalization for semantic understanding.

Handles snake_case, camelCase, and acronym splitting/expansion
to produce human-readable text from database identifiers.
"""

import re
from typing import Dict, List, Optional

from core.constants import DEFAULT_ACRONYMS


class SchemaNormalizer:
    """
    Normalizes schema identifiers and text for semantic understanding.

    Handles:
    - snake_case, camelCase, acronym splitting
    - Common database acronym expansion
    - Token lowercasing and cleaning
    """

    DEFAULT_ACRONYMS = DEFAULT_ACRONYMS

    def __init__(self, acronym_map: Optional[Dict[str, str]] = None):
        self.acronyms = self.DEFAULT_ACRONYMS.copy()
        if acronym_map:
            self.acronyms.update(acronym_map)

    def tokenize_identifier(self, name: str) -> List[str]:
        """
        Split identifier into tokens based on snake_case, camelCase, and acronyms.

        Examples:
            "cust_txn_amt" -> ["cust", "txn", "amt"]
            "CustomerTransactionAmount" -> ["Customer", "Transaction", "Amount"]
        """
        if not name:
            return []

        tokens = []
        for part in name.split("_"):
            if not part:
                continue
            tokens.extend(self._split_camel_case(part))

        return tokens

    def _split_camel_case(self, text: str) -> List[str]:
        """Split camelCase/PascalCase text into tokens."""
        if not text:
            return []

        s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", text)
        result = re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1)

        return [t for t in result.split("_") if t]

    def expand_tokens(self, tokens: List[str]) -> List[str]:
        """Expand acronym tokens to full words."""
        expanded = []
        for token in tokens:
            lower = token.lower()
            if lower in self.acronyms:
                expanded.append(self.acronyms[lower])
            else:
                expanded.append(lower)
        return expanded

    def normalize_identifier(self, name: str) -> str:
        """
        Fully normalize identifier: tokenize, expand, lowercase.

        Examples:
            "cust_txn_amt" -> "customer transaction amount"
            "OrderDT" -> "order date"
        """
        if not name:
            return ""

        tokens = self.tokenize_identifier(name)
        expanded = self.expand_tokens(tokens)
        return " ".join(expanded).lower().strip()

    def normalize_text(self, text: str) -> str:
        """Normalize free-form text: lowercase, remove extra whitespace and special characters."""
        if not text:
            return ""

        text = text.lower()
        text = re.sub(r"\s+", " ", text).strip()
        text = re.sub(r"[^a-z0-9\s]", " ", text)
        text = re.sub(r"\s+", " ", text).strip()

        return text
