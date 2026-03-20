"""Data models for NLP table selection."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class ColumnMetadata:
    """Metadata for a single column."""

    table_name: str
    column_name: str
    normalized_name: str
    semantic_meaning: Optional[str] = None
    comment: Optional[str] = None
    data_type: Optional[str] = None


@dataclass
class TableMetadata:
    """Metadata for a single table."""

    table_name: str
    normalized_name: str
    semantic_meaning: Optional[str] = None
    columns: Dict[str, ColumnMetadata] = field(default_factory=dict)
    comment: Optional[str] = None


@dataclass
class TableSelectionResult:
    """Result of table selection query."""

    status: str  # "success", "ambiguous", "no_match"
    tables: List[str]
    confidences: Dict[str, float]
    top_candidates: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
