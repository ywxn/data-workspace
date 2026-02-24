"""
Production-grade NLP table selector for large SQL schemas.

Allows users to query SQL databases in natural language without manually selecting tables.
Automatically infers relevant tables using semantic embeddings and supports optional semantic layers.

Architecture:
1. Schema normalization (acronym expansion + semantic layer)
2. Column-level semantic embeddings
3. Top-K column retrieval via cosine similarity
4. Aggregation to table scores
5. Foreign-key graph expansion
6. Threshold-based final selection
"""

import hashlib
import logging
import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple
from constants import DEFAULT_ACRONYMS

import numpy as np

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# =============================================================================
# STEP 1: SCHEMA NORMALIZER
# =============================================================================


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
        """
        Initialize the schema normalizer.

        Args:
            acronym_map: Optional custom acronym expansion dictionary.
                        Merged with defaults.
        """
        self.acronyms = self.DEFAULT_ACRONYMS.copy()
        if acronym_map:
            self.acronyms.update(acronym_map)

    def tokenize_identifier(self, name: str) -> List[str]:
        """
        Split identifier into tokens based on snake_case, camelCase, and acronyms.

        Examples:
            "cust_txn_amt" → ["cust", "txn", "amt"]
            "OrderDT" → ["Order", "DT"]
            "CustomerTransactionAmount" → ["Customer", "Transaction", "Amount"]

        Args:
            name: Identifier to tokenize

        Returns:
            List of tokens
        """
        if not name:
            return []

        # Handle snake_case
        tokens = []
        for part in name.split("_"):
            if not part:
                continue
            # Handle camelCase within each part
            tokens.extend(self._split_camel_case(part))

        return tokens

    def _split_camel_case(self, text: str) -> List[str]:
        """Split camelCase/PascalCase text into tokens."""
        if not text:
            return []

        # Insert space before uppercase letters that follow lowercase
        s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", text)
        # Insert space before uppercase letters that follow lowercase/digits
        result = re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1)

        return [t for t in result.split("_") if t]

    def expand_tokens(self, tokens: List[str]) -> List[str]:
        """
        Expand acronym tokens to full words.

        Args:
            tokens: List of tokens

        Returns:
            List of tokens with acronyms expanded
        """
        expanded = []
        for token in tokens:
            lower = token.lower()
            # Try exact match first
            if lower in self.acronyms:
                expanded.append(self.acronyms[lower])
            else:
                expanded.append(lower)
        return expanded

    def normalize_identifier(self, name: str) -> str:
        """
        Fully normalize identifier: tokenize, expand, lowercase.

        Examples:
            "cust_txn_amt" → "customer transaction amount"
            "OrderDT" → "order date"

        Args:
            name: Identifier to normalize

        Returns:
            Normalized string with spaces
        """
        if not name:
            return ""

        tokens = self.tokenize_identifier(name)
        expanded = self.expand_tokens(tokens)
        return " ".join(expanded).lower().strip()

    def normalize_text(self, text: str) -> str:
        """
        Normalize free-form text: lowercase, remove extra whitespace.

        Args:
            text: Text to normalize

        Returns:
            Normalized text
        """
        if not text:
            return ""

        # Lowercase
        text = text.lower()
        # Remove extra whitespace
        text = re.sub(r"\s+", " ", text).strip()
        # Remove special characters except spaces and alphanumerics
        text = re.sub(r"[^a-z0-9\s]", " ", text)
        # Collapse spaces again
        text = re.sub(r"\s+", " ", text).strip()

        return text


# =============================================================================
# STEP 2: DATA CLASSES
# =============================================================================


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


# =============================================================================
# STEP 3: NLP TABLE SELECTOR
# =============================================================================


class NLPTableSelector:
    """
    Production-grade semantic table selector for SQL databases.

    Selects relevant tables for natural language queries without manual selection.
    Supports large schemas (100k+ columns) using column-level embeddings.
    """

    def __init__(
        self,
        db_connector: Any,
        model_name: str = "all-MiniLM-L6-v2",
        confidence_threshold: float = 0.55,
        tie_threshold: float = 0.10,
        table_synonyms: Optional[Dict[str, List[str]]] = None,
        column_comments: Optional[Dict[str, str]] = None,
        acronym_map: Optional[Dict[str, str]] = None,
        semantic_layer: Optional[List[Dict[str, Any]]] = None,
    ):
        """
        Initialize the NLP table selector.

        Args:
            db_connector: DatabaseConnector instance (must be connected)
            model_name: Sentence transformer model name
            confidence_threshold: Minimum confidence for table selection (0-1)
            tie_threshold: Threshold for tie detection (0-1)
            table_synonyms: Optional dict mapping table names to synonym lists
            column_comments: Optional dict mapping column names to comments
            acronym_map: Optional custom acronym dictionary
            semantic_layer: Optional list of table descriptors:
                [
                    {
                        "table_name": "CST_TXN_H",
                        "description": "customer transaction history",
                        "columns": [
                            {
                                "name": "C_AMT",
                                "description": "transaction amount local currency"
                            }
                        ]
                    }
                ]

        Raises:
            RuntimeError: If db_connector is not connected
            ImportError: If required libraries unavailable
        """
        if not db_connector.engine:
            raise RuntimeError(
                "Database connector must be connected before initializing selector"
            )

        self.db_connector = db_connector
        self.confidence_threshold = confidence_threshold
        self.tie_threshold = tie_threshold
        self.table_synonyms = table_synonyms or {}
        self.column_comments = column_comments or {}
        self.semantic_layer = semantic_layer or []

        # Initialize normalizer
        self.normalizer = SchemaNormalizer(acronym_map=acronym_map)

        # Load embedding model
        try:
            from sentence_transformers import SentenceTransformer

            # TODO: If the model is not downloaded already, this will raise an error.
            self.model = SentenceTransformer(
                model_name,
                cache_folder="models",
                local_files_only=True,
            )
        except ImportError:
            raise ImportError(
                "sentence-transformers required. Install with: "
                "pip install sentence-transformers"
            )

        logger.info(f"Initialized NLPTableSelector with model: {model_name}")

        # Storage
        self.schema_hash: Optional[str] = None
        self.table_metadata: Dict[str, TableMetadata] = {}
        self.column_embeddings: List[np.ndarray] = []
        self.column_index: List[Tuple[str, str]] = []
        self.fk_graph: Dict[str, Set[str]] = defaultdict(set)
        self.table_docs: Dict[str, str] = {}

        # Build initial schema
        self._refresh_schema()

    def _refresh_schema(self) -> None:
        """Build or rebuild schema structure from database."""
        logger.info("Building schema structure...")

        # Clear existing data
        self.table_metadata.clear()
        self.column_embeddings.clear()
        self.column_index.clear()
        self.fk_graph.clear()
        self.table_docs.clear()

        # Get tables
        tables = self.db_connector.get_tables()
        logger.info(f"Found {len(tables)} tables")

        if not tables:
            logger.warning("No tables found in database")
            return

        # Build table metadata and documents
        for table_name in tables:
            self._build_table_metadata(table_name)

        # Build foreign key graph
        self._build_fk_graph()

        # Build column embeddings
        self._build_column_embeddings()

        # Compute schema hash
        self._compute_schema_hash(tables)

        logger.info(
            f"Schema built: {len(self.table_metadata)} tables, "
            f"{len(self.column_index)} columns indexed"
        )

    def _build_table_metadata(self, table_name: str) -> None:
        """Build metadata for a single table."""
        # Get columns
        columns = self.db_connector.get_columns(table_name)

        # Create table metadata
        normalized_table_name = self.normalizer.normalize_identifier(table_name)

        # Get semantic table meaning
        semantic_table = None
        semantic_table_map, semantic_column_map = self._build_semantic_maps()
        if semantic_table_map.get(table_name):
            semantic_table = semantic_table_map[table_name]

        table_meta = TableMetadata(
            table_name=table_name,
            normalized_name=normalized_table_name,
            semantic_meaning=semantic_table,
        )

        # Build column metadata
        for col_name in columns:
            normalized_col_name = self.normalizer.normalize_identifier(col_name)

            # Get semantic column meaning
            col_key = f"{table_name}.{col_name}"
            semantic_col = None
            if semantic_column_map.get(col_key):
                semantic_col = semantic_column_map[col_key]

            col_meta = ColumnMetadata(
                table_name=table_name,
                column_name=col_name,
                normalized_name=normalized_col_name,
                semantic_meaning=semantic_col,
                comment=self.column_comments.get(col_key),
            )
            table_meta.columns[col_name] = col_meta

        self.table_metadata[table_name] = table_meta

        # Build table document for embeddings
        self._build_table_document(table_name, table_meta)

    def _build_table_document(self, table_name: str, table_meta: TableMetadata) -> None:
        """Build semantic document for a table."""
        doc_parts = [table_meta.normalized_name]

        # Add semantic table meaning
        if table_meta.semantic_meaning:
            normalized_semantic = self.normalizer.normalize_text(
                table_meta.semantic_meaning
            )
            doc_parts.append(normalized_semantic)

        # Add table synonyms
        if table_name in self.table_synonyms:
            synonyms_text = " ".join(self.table_synonyms[table_name])
            normalized_syns = self.normalizer.normalize_text(synonyms_text)
            doc_parts.append(normalized_syns)

        # Add column names and meanings
        for col_meta in table_meta.columns.values():
            doc_parts.append(col_meta.normalized_name)

            # Add semantic column meaning
            if col_meta.semantic_meaning:
                normalized_semantic = self.normalizer.normalize_text(
                    col_meta.semantic_meaning
                )
                doc_parts.append(normalized_semantic)

            # Add column comment
            if col_meta.comment:
                normalized_comment = self.normalizer.normalize_text(col_meta.comment)
                doc_parts.append(normalized_comment)

        self.table_docs[table_name] = " ".join(doc_parts)

    def _build_column_embeddings(self) -> None:
        """Build embeddings for all columns."""
        logger.info("Building column embeddings...")

        # Create embedding text for each column
        embedding_texts = []

        for table_name, table_meta in self.table_metadata.items():
            table_doc = self.table_docs[table_name]

            for col_name, col_meta in table_meta.columns.items():
                # Combine table doc with column info
                col_text = f"{table_doc} {col_meta.normalized_name}"

                # Add semantic meaning if available
                if col_meta.semantic_meaning:
                    normalized_semantic = self.normalizer.normalize_text(
                        col_meta.semantic_meaning
                    )
                    col_text = f"{col_text} {normalized_semantic}"

                embedding_texts.append(col_text)
                self.column_index.append((table_name, col_name))

        # Encode all at once for efficiency
        if embedding_texts:
            embeddings = self.model.encode(
                embedding_texts,
                normalize_embeddings=True,
                show_progress_bar=False,
            )
            self.column_embeddings = embeddings.tolist()

        logger.info(f"Created {len(self.column_embeddings)} column embeddings")

    def _build_fk_graph(self) -> None:
        """Build foreign key relationship graph."""
        try:
            from sqlalchemy import inspect

            inspector = inspect(self.db_connector.engine)

            # Get foreign key constraints for each table
            tables = self.db_connector.get_tables()

            for table_name in tables:
                fks = inspector.get_foreign_keys(table_name)

                for fk in fks:
                    ref_table = fk.get("referred_table")
                    if ref_table:
                        self.fk_graph[table_name].add(ref_table)
                        self.fk_graph[ref_table].add(table_name)

            logger.info(f"Built FK graph with {len(self.fk_graph)} table relationships")
        except Exception as e:
            logger.warning(f"Could not build FK graph: {e}")

    def _compute_schema_hash(self, tables: List[str]) -> None:
        """Compute hash of schema for change detection."""
        schema_str = "|".join(sorted(tables))
        self.schema_hash = hashlib.sha256(schema_str.encode()).hexdigest()

    def select_tables(
        self,
        prompt: str,
        top_k: int = 3,
        fk_expand: bool = True,
    ) -> TableSelectionResult:
        """
        Select tables relevant to a natural language query.

        Process:
        1. Normalize and embed the query
        2. Find top-K matching columns via cosine similarity
        3. Aggregate column matches to table scores
        4. Apply thresholds for final selection
        5. Expand via foreign key relationships

        Args:
            prompt: Natural language query
            top_k: Number of top tables to return
            fk_expand: Whether to expand selected tables via FK graph

        Returns:
            TableSelectionResult with selected tables and confidences
        """
        logger.info(f"Selecting tables for prompt: {prompt[:100]}")

        if not self.column_embeddings:
            return TableSelectionResult(
                status="no_match",
                tables=[],
                confidences={},
                metadata={"error": "Schema not initialized"},
            )

        # Normalize and embed query
        normalized_prompt = self.normalizer.normalize_text(prompt)
        prompt_embedding = self.model.encode(
            normalized_prompt,
            normalize_embeddings=True,
        )

        # Retrieve top columns via cosine similarity
        top_columns = self._retrieve_top_columns(prompt_embedding, top_k * 5)

        # Aggregate to table scores
        table_scores = self._aggregate_to_tables(top_columns)

        tokens = set(normalized_prompt.split())

        # ---- lexical-only shortcut for short prompts ----
        if len(tokens) <= 2:
            lexical_tables = self._lexical_table_matches(tokens)
            if len(lexical_tables) > 1:
                return TableSelectionResult(
                    status="success",
                    tables=lexical_tables,
                    confidences={t: 1.0 for t in lexical_tables},
                    metadata={"mode": "lexical_keyword"},
                )

        # ---- hybrid lexical + semantic scoring ----
        table_scores = self._apply_lexical_boost(table_scores, tokens)

        # normalize instead of softmax
        confidences = self._normalize_scores(table_scores)

        # Apply thresholds
        selected_tables, top_candidates = self._apply_thresholds(confidences, top_k)

        # Expand via FK graph if requested
        if fk_expand and selected_tables:
            selected_tables = self._expand_via_fk(selected_tables)

        # Determine status
        if not selected_tables:
            status = "no_match"
        elif top_candidates:
            status = "ambiguous"
        else:
            status = "success"

        # Filter confidences to selected tables
        result_confidences = {table: confidences[table] for table in selected_tables}

        result = TableSelectionResult(
            status=status,
            tables=selected_tables,
            confidences=result_confidences,
            top_candidates=top_candidates,
            metadata={
                "prompt": prompt,
                "normalized_prompt": normalized_prompt,
                "all_table_scores": dict(confidences),
            },
        )

        logger.info(
            f"Selection result: status={status}, tables={selected_tables}, "
            f"confidences={result_confidences}"
        )

        return result

    def _retrieve_top_columns(
        self, prompt_embedding: np.ndarray, k: int
    ) -> List[Tuple[str, str, float]]:
        """
        Retrieve top-K columns by cosine similarity.

        Args:
            prompt_embedding: Normalized embedding of query
            k: Number of top columns to retrieve

        Returns:
            List of (table, column, score) tuples
        """
        if not self.column_embeddings:
            return []

        # Compute cosine similarities (dot product with normalized embeddings)
        col_embeddings_array = np.array(self.column_embeddings)
        scores = col_embeddings_array @ prompt_embedding

        # Get top-K indices
        top_indices = np.argsort(scores)[-k:][::-1]

        # Collect results
        results = []
        for idx in top_indices:
            if scores[idx] > 0:  # Only positive similarities
                table_name, col_name = self.column_index[idx]
                results.append((table_name, col_name, float(scores[idx])))

        return results

    def _aggregate_to_tables(
        self, column_matches: List[Tuple[str, str, float]]
    ) -> Dict[str, float]:
        """
        Aggregate column match scores to table scores.

        Args:
            column_matches: List of (table, column, score) tuples

        Returns:
            Dict mapping table names to aggregated scores
        """
        table_scores: Dict[str, float] = defaultdict(float)

        for table_name, col_name, score in column_matches:
            table_scores[table_name] += score

        return dict(table_scores)

#    def _softmax_scores(self, scores: Dict[str, float]) -> Dict[str, float]:
#        """
#        Apply softmax to scores to get probabilities.
#
#        Args:
#            scores: Dict of table scores
#
#        Returns:
#            Dict of normalized confidences (0-1)
#        """
#        # deprecated: replaced by _normalize_scores
#        if not scores:
#            return {}
#
#        # Convert to array
#        tables = sorted(scores.keys())
#        score_array = np.array([scores[t] for t in tables])
#
#        # Subtract max for numerical stability
#        score_array = score_array - np.max(score_array)
#
#        # Apply exp
#        exp_scores = np.exp(score_array)
#
#        # Compute softmax
#        softmax = exp_scores / np.sum(exp_scores)
#
#        return {table: float(prob) for table, prob in zip(tables, softmax)}

    def _normalize_scores(self, scores: Dict[str, float]) -> Dict[str, float]:
        """
        Normalize table scores relative to best score (no softmax).

        This preserves multi-table matches for keyword prompts
        where several tables are equally relevant.
        """
        if not scores:
            return {}

        max_score = max(scores.values())
        if max_score == 0:
            return {t: 0.0 for t in scores}

        return {t: float(s / max_score) for t, s in scores.items()}

    def _lexical_table_matches(self, tokens: Set[str]) -> List[str]:
        """
        Return tables whose normalized name contains any token.
        """
        matches = []
        for table_name, meta in self.table_metadata.items():
            if any(tok in meta.normalized_name for tok in tokens):
                matches.append(table_name)
        return matches

    def _apply_lexical_boost(
        self,
        table_scores: Dict[str, float],
        tokens: Set[str],
        table_boost: float = 0.3,
        column_boost: float = 0.15,
    ) -> Dict[str, float]:
        """
        Boost table scores when prompt tokens appear in
        table or column identifiers.
        """
        for table_name, meta in self.table_metadata.items():

            # table name match
            if any(tok in meta.normalized_name for tok in tokens):
                table_scores[table_name] = table_scores.get(table_name, 0.0) + table_boost

            # column name matches
            for col in meta.columns.values():
                if any(tok in col.normalized_name for tok in tokens):
                    table_scores[table_name] = table_scores.get(table_name, 0.0) + column_boost

        return table_scores

    def _apply_thresholds(
        self, confidences: Dict[str, float], top_k: int
    ) -> Tuple[List[str], List[str]]:
        """
        Apply confidence thresholds to select final tables.

        Logic:
        - Select tables with confidence >= confidence_threshold
        - If no tables meet threshold, return empty
        - Mark as ambiguous if runner-up within tie_threshold

        Args:
            confidences: Dict of table confidences
            top_k: Maximum tables to return

        Returns:
            Tuple of (selected_tables, ambiguous_candidates)
        """
        if not confidences:
            return [], []

        # Sort by confidence descending
        sorted_tables = sorted(confidences.items(), key=lambda x: x[1], reverse=True)

        selected = []
        ambiguous = []

        if not sorted_tables:
            return [], []

        # Get top table(s)
        top_score = sorted_tables[0][1]

        # Select all tables within threshold
        for table_name, score in sorted_tables:
            if score >= self.confidence_threshold:
                selected.append(table_name)
                if len(selected) >= top_k:
                    break

        # Check for ambiguity
        if selected:
            top_selected_score = confidences[selected[0]]
            for table_name, score in sorted_tables:
                if table_name not in selected:
                    if (top_selected_score - score) <= self.tie_threshold:
                        ambiguous.append(table_name)
                    else:
                        break

        return selected, ambiguous[:top_k]

    def _expand_via_fk(self, tables: List[str], max_hops: int = 1) -> List[str]:
        """
        Expand selected tables via foreign key relationships.

        Args:
            tables: List of primary table selections
            max_hops: Maximum hops in FK graph

        Returns:
            Expanded list of related tables
        """
        expanded = set(tables)
        current_level = set(tables)

        for _ in range(max_hops):
            next_level = set()
            for table in current_level:
                related = self.fk_graph.get(table, set())
                next_level.update(related - expanded)

            if not next_level:
                break

            expanded.update(next_level)
            current_level = next_level

        return list(expanded)

    def refresh_schema(self) -> None:
        """
        Refresh schema structure from database.

        Call this after database schema changes.
        Rebuilds all embeddings.
        """
        logger.info("Refreshing schema from database...")
        self._refresh_schema()

    def set_synonyms(self, table_synonyms: Dict[str, List[str]]) -> None:
        """
        Set or update table synonyms and rebuild embeddings.

        Args:
            table_synonyms: Dict mapping table names to synonym lists
        """
        logger.info(f"Setting {len(table_synonyms)} table synonyms")
        self.table_synonyms.update(table_synonyms)
        self._build_table_embeddings_for_tables(list(table_synonyms.keys()))

    def set_column_comments(self, column_comments: Dict[str, str]) -> None:
        """
        Set or update column comments and rebuild embeddings.

        Args:
            column_comments: Dict mapping column keys (table.column) to comments
        """
        logger.info(f"Setting {len(column_comments)} column comments")
        self.column_comments.update(column_comments)
        # Rebuild all embeddings since column info changed
        self._build_column_embeddings()

    def set_semantic_layer(self, semantic_layer: List[Dict[str, Any]]) -> None:
        """
        Set or update semantic layer with business meanings.

        Semantic layer structure:
        [
            {
                "table_name": "CST_TXN_H",
                "description": "customer transaction history",
                "columns": [
                    {"name": "C_AMT", "description": "transaction amount local currency"}
                ]
            }
        ]

        Args:
            semantic_layer: Semantic meanings for tables and columns
        """
        logger.info(f"Setting semantic layer with {len(semantic_layer)} entries")
        self.semantic_layer = semantic_layer
        # Rebuild all embeddings since semantic info changed
        self._refresh_schema()

    def _build_semantic_maps(self) -> Tuple[Dict[str, str], Dict[str, str]]:
        """Build lookup maps for semantic table and column descriptions."""
        table_map: Dict[str, str] = {}
        column_map: Dict[str, str] = {}

        for entry in self.semantic_layer:
            table_name = entry.get("table_name")
            if not table_name:
                continue

            table_desc = entry.get("description")
            if table_desc:
                table_map[table_name] = table_desc

            for col in entry.get("columns", []) or []:
                col_name = col.get("name")
                col_desc = col.get("description")
                if not col_name or not col_desc:
                    continue
                column_map[f"{table_name}.{col_name}"] = col_desc

        return table_map, column_map

    def _build_table_embeddings_for_tables(self, table_names: List[str]) -> None:
        """Rebuild documents and embeddings for specific tables."""
        for table_name in table_names:
            if table_name in self.table_metadata:
                self._build_table_document(table_name, self.table_metadata[table_name])

        # Rebuild all column embeddings
        self._build_column_embeddings()

    def get_schema_info(self) -> Dict[str, Any]:
        """
        Get information about current schema.

        Returns:
            Dict with schema statistics
        """
        return {
            "num_tables": len(self.table_metadata),
            "num_columns": sum(
                len(meta.columns) for meta in self.table_metadata.values()
            ),
            "num_embeddings": len(self.column_embeddings),
            "schema_hash": self.schema_hash,
            "fk_relationships": len(self.fk_graph),
            "model_name": self.model.get_sentence_embedding_dimension(),
            "confidence_threshold": self.confidence_threshold,
            "tie_threshold": self.tie_threshold,
        }
