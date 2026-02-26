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
        semantic_layer: Optional[Any] = None,
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
            semantic_layer: Optional semantic layer data. Supports two formats:
                Simple format (list of table descriptors):
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
                Rich format (dict with entities, relationships, columns, measures):
                {
                    "entities": [{"name": "...", "physical_tables": [...], ...}],
                    "relationships": [{"from_table": "...", "to_table": "...", ...}],
                    "columns": [{"physical_table": "...", "physical_column": "...", ...}],
                    "measures": [{"source_table": "...", "source_column": "...", ...}]
                }

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
        self.semantic_layer = semantic_layer or {}

        # Initialize normalizer
        self.normalizer = SchemaNormalizer(acronym_map=acronym_map)

        # Load embedding model
        try:
            from sentence_transformers import SentenceTransformer

            self.model = SentenceTransformer(
                model_name,
                cache_folder="models", # will download unless file exists
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
        self.structural_links: Dict[str, Set[str]] = defaultdict(set)
        self.canonical_score: Dict[str, float] = {}

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
        self.structural_links.clear()
        self.canonical_score.clear()

        # Enrich synonyms and comments from semantic layer before building metadata
        self._enrich_from_semantic_layer()

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

        # Supplement FK graph from semantic layer relationships
        self._enrich_fk_from_semantic_layer()

        # Build column embeddings
        self._build_column_embeddings()

        # Compute schema hash
        self._compute_schema_hash(tables)

        # Build structural relationships
        self._infer_structural_links()
        self._compute_canonical_scores()

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
                try:
                    fks = inspector.get_foreign_keys(table_name)

                    for fk in fks:
                        ref_table = fk.get("referred_table")
                        if ref_table:
                            self.fk_graph[table_name].add(ref_table)
                            self.fk_graph[ref_table].add(table_name)
                except Exception:
                    # Continue with other tables if one fails
                    continue

            logger.info(f"Built FK graph with {len(self.fk_graph)} table relationships")

            # If FK graph is sparse, infer relationships from naming patterns
            if len(self.fk_graph) < len(tables) * 0.3:
                logger.info("FK graph sparse, inferring relationships from naming patterns")
                self._infer_fk_from_names()

        except Exception as e:
            logger.warning(f"Could not build FK graph: {e}. Inferring from names.")
            self._infer_fk_from_names()

    def _infer_fk_from_names(self) -> None:
        """
        Infer likely FK relationships from table naming patterns.

        Common patterns detected:
        - header + detail (e.g., inward_hdr + inward_dtl)
        - master + detail (e.g., requisition + requisition_dtl)
        - parent + child prefix (e.g., employee_expense_book + employee_expense_book_dtl)
        """
        tables = list(self.table_metadata.keys())

        for table in tables:
            table_lower = table.lower()

            # Header-detail pattern (_hdr + _dtl)
            if '_hdr' in table_lower:
                base = table_lower.replace('_hdr', '')
                detail_name = base + '_dtl'
                for other in tables:
                    if other.lower() == detail_name:
                        self.fk_graph[table].add(other)
                        self.fk_graph[other].add(table)
                        break

            # Master-detail pattern (table + table_dtl)
            for other in tables:
                other_lower = other.lower()
                if other_lower.startswith(table_lower + '_dtl'):
                    self.fk_graph[table].add(other)
                    self.fk_graph[other].add(table)
                elif table_lower.startswith(other_lower + '_dtl'):
                    self.fk_graph[table].add(other)
                    self.fk_graph[other].add(table)

        logger.info(f"Inferred FK relationships, graph now has {len(self.fk_graph)} nodes")

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

        # --- Context-aware table selection ---

        # Step 1: Rank tables using blended semantic + structural scores
        ranked = []
        for t, conf in confidences.items():
            centrality = self._structural_centrality(t)
            # Heavily weight semantic confidence; centrality only as tiebreaker
            anchor_score = conf * 0.9 + centrality * 0.1
            ranked.append((t, anchor_score))

        ranked.sort(key=lambda x: x[1], reverse=True)

        # Step 2: Select top anchors by blended score
        anchor_k = min(max(3, top_k), 5)
        anchors = [t for t, _ in ranked[:anchor_k]]

        # Step 2b: Force entity-synonym-matched tables as anchors
        # When entity synonyms explicitly match query tokens, those tables
        # must be considered regardless of ranking position
        synonym_forced = set()
        if self._is_rich_semantic_layer():
            syn_stems = set()
            for tok in tokens:
                syn_stems.update(self._simple_stems(tok))

            for entity in self.semantic_layer.get("entities", []):
                synonyms = [s.lower() for s in entity.get("synonyms", [])]
                matched = any(
                    stem == syn or stem in syn.split()
                    for stem in syn_stems for syn in synonyms
                )
                if matched:
                    for pt in entity.get("physical_tables", []):
                        if pt in self.table_metadata and pt not in anchors:
                            anchors.append(pt)
                            synonym_forced.add(pt)

        # Apply structural filters to combined anchors
        anchors = [t for t in anchors if not self._is_structurally_peripheral(t)]
        anchors = self._prefer_canonical_variants(anchors, confidences)

        # Require minimum confidence (except for synonym-forced anchors)
        min_anchor_conf = 0.15
        anchors = [
            t for t in anchors
            if confidences.get(t, 0.0) >= min_anchor_conf or t in synonym_forced
        ]

        # Step 3: Expand anchors with entity pairs and header-detail pairing
        core_tables = set(anchors)
        core_tables.update(self._expand_entity_pairs(list(core_tables)))
        core_tables.update(self._expand_header_detail_pairs(list(core_tables)))

        # Step 4: Selective FK expansion (1 hop from core tables only)
        fk_additions = set()
        anchor_fk_dims = set()  # dimension tables directly connected to core tables
        if fk_expand:
            for t in core_tables:
                for neighbor in self.fk_graph.get(t, set()):
                    if neighbor not in core_tables and neighbor in self.table_metadata:
                        fk_additions.add(neighbor)
                        # Track dimension tables connected to core tables
                        if self._is_dimension_table(neighbor):
                            anchor_fk_dims.add(neighbor)

        # Step 5: Apply tiered thresholds
        # Core tables (anchors + entity/header-detail pairs): included unconditionally
        selected_tables = [t for t in core_tables]

        # FK additions: require semantic score, but anchor-connected dimensions always join
        fk_threshold = self.confidence_threshold * 0.35
        for t in fk_additions:
            score = confidences.get(t, 0.0)
            if score >= fk_threshold:
                selected_tables.append(t)
            elif t in anchor_fk_dims:
                # Dimension/master tables directly linked to anchors are critical join targets
                selected_tables.append(t)

        # Step 6: Filter noise
        selected_tables = [t for t in selected_tables if not self._is_structurally_peripheral(t)]
        selected_tables = self._filter_secondary_tables(selected_tables, confidences)
        selected_tables = self._prefer_canonical_variants(selected_tables, confidences)

        # Step 7: Cap results — preserve core tables, trim FK additions only
        max_results = max(top_k * 2, 8)
        if len(selected_tables) > max_results:
            core_set = set(core_tables)
            core_list = [t for t in selected_tables if t in core_set]
            fk_list = [t for t in selected_tables if t not in core_set]
            fk_list = sorted(
                fk_list,
                key=lambda t: (
                    confidences.get(t, 0.0),
                    len(self.fk_graph.get(t, set())),  # prefer well-connected dims
                ),
                reverse=True,
            )
            remaining_slots = max(max_results - len(core_list), 0)
            selected_tables = core_list + fk_list[:remaining_slots]

        # Fallback: if no tables pass, return top non-peripheral candidates
        if not selected_tables and confidences:
            all_non_peripheral = [
                t for t in confidences.keys()
                if not self._is_structurally_peripheral(t)
            ]
            if all_non_peripheral:
                selected_tables = sorted(
                    all_non_peripheral,
                    key=lambda t: confidences[t],
                    reverse=True,
                )[:3]

        # Check for ambiguity among remaining high-confidence candidates
        top_candidates = []
        if selected_tables:
            top_selected_score = max(confidences.get(t, 0.0) for t in selected_tables)
            sorted_by_confidence = sorted(
                confidences.items(), key=lambda x: x[1], reverse=True
            )
            for table_name, score in sorted_by_confidence:
                if table_name not in selected_tables:
                    if (top_selected_score - score) <= self.tie_threshold:
                        top_candidates.append(table_name)
                    else:
                        break
            top_candidates = top_candidates[:top_k]

        # Determine status
        if not selected_tables:
            status = "no_match"
        elif top_candidates:
            status = "ambiguous"
        else:
            status = "success"

        # Filter confidences to selected tables
        result_confidences = {
            table: confidences.get(table, 0.0) for table in selected_tables
        }

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

        # Blend semantic and canonical scores to prefer structurally central tables
        # Additive blending prevents variants from beating canonical tables
        for table in table_scores:
            semantic_score = table_scores[table]
            canonical = self._canonical_weight(table)
            table_scores[table] = semantic_score * 0.7 + canonical * 0.3

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

    def _simple_stems(self, word: str) -> Set[str]:
        """
        Generate simple stem variants for fuzzy matching.

        Handles common English morphological suffixes:
        - Plurals: items → item, categories → category
        - Past tense: issued → issue, delayed → delay
        - Progressive: moving → move
        """
        stems = {word}
        if word.endswith('ies') and len(word) > 4:
            stems.add(word[:-3] + 'y')
        if word.endswith('es') and len(word) > 3:
            stems.add(word[:-2])
        if word.endswith('s') and not word.endswith('ss') and len(word) > 2:
            stems.add(word[:-1])
        if word.endswith('ed') and len(word) > 4:
            stems.add(word[:-2])
            stems.add(word[:-1])  # "issued" → "issue"
        if word.endswith('ing') and len(word) > 5:
            stems.add(word[:-3])
            stems.add(word[:-3] + 'e')  # "moving" → "move"
        return stems

    def _apply_lexical_boost(
        self,
        table_scores: Dict[str, float],
        tokens: Set[str],
        table_boost: float = 0.4,
        column_boost: float = 0.2,
    ) -> Dict[str, float]:
        """
        Boost table scores when prompt tokens appear in
        table or column identifiers. Uses simple stemming for
        morphological variants (items→item, issued→issue).
        """
        # Build stemmed token set for fuzzy matching
        token_stems = set()
        for tok in tokens:
            token_stems.update(self._simple_stems(tok))

        for table_name, meta in self.table_metadata.items():
            table_lower = table_name.lower()

            # Direct table name match (with stems)
            if any(stem in meta.normalized_name or stem in table_lower for stem in token_stems):
                table_scores[table_name] = (
                    table_scores.get(table_name, 0.0) + table_boost
                )

            # Column name matches (capped at 2 per table to prevent dominance)
            col_match_count = 0
            for col in meta.columns.values():
                if any(stem in col.normalized_name for stem in token_stems):
                    col_match_count += 1
                    if col_match_count >= 2:
                        break
            if col_match_count > 0:
                table_scores[table_name] = (
                    table_scores.get(table_name, 0.0)
                    + min(col_match_count, 2) * column_boost
                )

        # Boost from semantic layer entity synonyms and business names
        if self._is_rich_semantic_layer():
            for entity in self.semantic_layer.get("entities", []):
                synonyms = [s.lower() for s in entity.get("synonyms", [])]
                business_words = entity.get("business_name", "").lower().split()

                # Check with stemmed tokens for morphological variants
                matched = any(
                    stem == syn or stem in syn.split()
                    for stem in token_stems for syn in synonyms
                )
                if not matched:
                    matched = any(stem in business_words for stem in token_stems)

                if matched:
                    for phys_table in entity.get("physical_tables", []):
                        if phys_table in self.table_metadata:
                            table_scores[phys_table] = (
                                table_scores.get(phys_table, 0.0) + table_boost
                            )

            # Also boost from dimension synonyms (godown=warehouse, etc.)
            for dim in self.semantic_layer.get("dimensions", []):
                dim_desc = dim.get("description", "").lower()
                source_table = dim.get("source_table")
                if source_table and source_table in self.table_metadata:
                    if any(stem in dim_desc for stem in token_stems):
                        table_scores[source_table] = (
                            table_scores.get(source_table, 0.0) + table_boost * 0.5
                        )

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

    def _expand_via_fk(self, tables: List[str], max_hops: int = 2) -> List[str]:
        """
        Expand selected tables via foreign key relationships.

        Args:
            tables: List of primary table selections
            max_hops: Maximum hops in FK graph (increased to 2 for better recall)

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

    def _expand_entity_pairs(self, tables: List[str]) -> Set[str]:
        """
        Expand tables using semantic layer entity groupings.

        When a physical table from an entity is selected, include all
        physical tables in that entity. For example, selecting requisition_dtl
        also includes requisition (both belong to the 'requisition' entity).

        Args:
            tables: List of selected table names

        Returns:
            Set of additional tables from matched entities
        """
        if not self._is_rich_semantic_layer():
            return set()

        expanded = set()
        for entity in self.semantic_layer.get("entities", []):
            phys_tables = entity.get("physical_tables", [])
            # Only expand if at least one selected table belongs to this entity
            if any(t in tables for t in phys_tables):
                for pt in phys_tables:
                    if pt in self.table_metadata:
                        expanded.add(pt)

        return expanded

    def _expand_header_detail_pairs(self, tables: List[str]) -> Set[str]:
        """
        Ensure header-detail table pairs are both included.

        When a _dtl table is selected, include its header.
        When a _hdr table is selected, include its detail.

        Args:
            tables: List of selected table names

        Returns:
            Set of paired tables to add
        """
        expanded = set()
        all_tables = set(self.table_metadata.keys())

        for table in tables:
            table_lower = table.lower()

            # Detail table → find its header
            if '_dtl' in table_lower:
                base = table_lower.replace('_dtl', '')
                for candidate in all_tables:
                    cand_lower = candidate.lower()
                    if cand_lower == base or cand_lower == base.replace('_dtl', '') + '_hdr':
                        expanded.add(candidate)

            # Header table → find its detail
            if '_hdr' in table_lower:
                base = table_lower.replace('_hdr', '')
                for candidate in all_tables:
                    if candidate.lower() == base + '_dtl':
                        expanded.add(candidate)

        return expanded

    def _is_dimension_table(self, table: str) -> bool:
        """
        Check if a table is a master/dimension table.

        Uses semantic layer entity_type and dimension definitions,
        with fallback to naming convention (mst_ prefix).

        Args:
            table: Table name to check

        Returns:
            True if the table is a dimension/master/lookup table
        """
        if self._is_rich_semantic_layer():
            for entity in self.semantic_layer.get("entities", []):
                if entity.get("entity_type") in ("master", "dimension", "lookup"):
                    if table in entity.get("physical_tables", []):
                        return True

            for dim in self.semantic_layer.get("dimensions", []):
                if dim.get("source_table") == table:
                    return True

        # Fallback: naming convention
        return table.lower().startswith('mst_')

    def _infer_structural_links(self) -> None:
        """
        Infer structural relationships between tables (header-detail,
        fact-dimension, etc.) using FK connectivity and column overlap.

        Two tables are structurally linked if:
        - they are FK neighbors
        - they share a significant portion of column names
        """
        for table, neighbors in self.fk_graph.items():
            if table not in self.table_metadata:
                continue

            cols_a = {
                col.normalized_name
                for col in self.table_metadata[table].columns.values()
            }

            for n in neighbors:
                if n not in self.table_metadata:
                    continue

                cols_b = {
                    col.normalized_name
                    for col in self.table_metadata[n].columns.values()
                }

                if not cols_a or not cols_b:
                    continue

                overlap = len(cols_a & cols_b)
                min_cols = min(len(cols_a), len(cols_b))

                if min_cols == 0:
                    continue

                overlap_ratio = overlap / min_cols

                # Strong structural similarity threshold
                if overlap_ratio >= 0.4:
                    self.structural_links[table].add(n)
                    self.structural_links[n].add(table)

    def _compute_canonical_scores(self) -> None:
        """
        Estimate canonical importance of tables using FK graph centrality
        and column richness.

        Canonical tables:
        - referenced by many others (high FK degree)
        - structurally rich (many columns)

        Peripheral/variant tables:
        - low degree
        - few columns
        """
        if not self.table_metadata:
            return

        max_degree = (
            max(len(self.fk_graph.get(t, [])) for t in self.table_metadata) or 1
        )
        max_cols = max(len(m.columns) for m in self.table_metadata.values()) or 1

        raw_scores = {}

        for t, meta in self.table_metadata.items():
            degree = len(self.fk_graph.get(t, [])) / max_degree
            col_count = len(meta.columns) / max_cols

            raw_scores[t] = degree * 0.6 + col_count * 0.4

        max_score = max(raw_scores.values()) if raw_scores else 1.0

        if max_score == 0:
            max_score = 1.0

        for t in raw_scores:
            self.canonical_score[t] = raw_scores[t] / max_score

    def _canonical_weight(self, table_name: str) -> float:
        """
        Prefer structurally central tables over peripheral variants.
        """
        return self.canonical_score.get(table_name, 0.5)

    def _structural_centrality(self, table: str) -> float:
        """
        Estimate structural importance of a table in schema graph.

        Central tables:
        - higher FK degree
        - more structural links
        - higher canonical score

        Peripheral tables:
        - backups, logs, temp, variants
        - low connectivity

        Returns:
            Float between 0 and 1, higher = more central
        """
        fk_degree = len(self.fk_graph.get(table, []))
        structural_degree = len(self.structural_links.get(table, []))
        canonical = self._canonical_weight(table)

        # Normalize FK and structural degrees using smooth scaling
        return (
            0.5 * canonical +
            0.3 * (fk_degree / (1 + fk_degree)) +
            0.2 * (structural_degree / (1 + structural_degree))
        )

    def _is_structurally_peripheral(self, table: str) -> bool:
        """
        Identify structurally peripheral tables such as backups,
        logs, temp tables, and variants.

        Uses both name-based heuristics and structural analysis:
        - Universal patterns: bkp_*, *_backup, *_log, *_audit, temp_*, *_temp, etc.
        - Structural: low canonical + low FK + low structural links
        - Entity-defined tables are never peripheral

        Returns:
            True if table is likely a backup/log/temp/variant
        """
        # Tables defined in semantic layer entities are always core
        if self._is_rich_semantic_layer():
            for entity in self.semantic_layer.get("entities", []):
                if table in entity.get("physical_tables", []):
                    return False

        table_lower = table.lower()

        # Name-based detection (high confidence)
        peripheral_patterns = [
            'bkp_', '_bkp', '_backup', 'backup_',
            '_log', 'log_', '_audit', 'audit_',
            'temp_', '_temp', '_tmp', 'tmp_',
            '_old', 'old_', '_archive', 'archive_',
            '_copy', 'copy_', '_test', 'test_'
        ]

        for pattern in peripheral_patterns:
            if pattern in table_lower:
                return True

        # Date-stamped variants (e.g., bkp300725_mst_item)
        if re.search(r'bkp\d{6,8}', table_lower):
            return True

        # Structural detection (lower confidence)
        canonical = self._canonical_weight(table)
        fk_degree = len(self.fk_graph.get(table, []))
        structural_degree = len(self.structural_links.get(table, []))

        return (
            canonical < 0.35 and
            fk_degree <= 1 and
            structural_degree == 0
        )

    def _expand_structural(self, tables: List[str]) -> List[str]:
        """
        Expand selected tables using inferred structural links
        (header-detail, fact-dimension, etc.).
        """
        expanded = set(tables)

        for t in tables:
            expanded.update(self.structural_links.get(t, set()))

        return list(expanded)

    def _expand_dimensions(self, tables: List[str]) -> Set[str]:
        """
        Expand to include dimension-like tables linked to fact tables.

        Dimension tables typically have:
        - Lower or equal FK degree compared to fact tables
        - Are referenced by many tables (master/lookup data)
        - Provide attribute enrichment for transactional tables

        Args:
            tables: List of anchor tables (typically fact tables)

        Returns:
            Set of dimension-like tables
        """
        dims = set()

        for t in tables:
            neighbors = self.fk_graph.get(t, set())
            t_degree = len(neighbors)

            for n in neighbors:
                n_degree = len(self.fk_graph.get(n, set()))

                # Dimension-like: lower or equal connectivity
                if n_degree <= t_degree:
                    dims.add(n)

        return dims

    def _filter_secondary_tables(self, tables: List[str], confidences: Dict[str, float], threshold: float = 0.4) -> List[str]:
        """
        Filter out secondary tables unless they have high confidence.

        Secondary tables (plan, release, return variants) are often noise
        in query results unless specifically requested.

        Args:
            tables: List of candidate tables
            confidences: Dict of confidence scores
            threshold: Minimum confidence to keep secondary tables

        Returns:
            Filtered list of tables
        """
        secondary_patterns = ['_plan', '_release', '_return', '_queue', '_staging']

        filtered = []
        for table in tables:
            table_lower = table.lower()
            is_secondary = any(pattern in table_lower for pattern in secondary_patterns)

            if is_secondary:
                # Only keep if high confidence
                if confidences.get(table, 0.0) >= threshold:
                    filtered.append(table)
            else:
                filtered.append(table)

        return filtered

    def _prefer_canonical_variants(self, tables: List[str], confidences: Dict[str, float]) -> List[str]:
        """
        When multiple table variants exist (e.g., mst_item, bkp_mst_item),
        prefer the canonical version.

        Strategy:
        1. Group tables by normalized base name
        2. For each group, keep only the canonical version (highest canonical score)
        3. If no clear canonical, keep the one with highest confidence

        Args:
            tables: List of table names
            confidences: Dict of table confidences

        Returns:
            Filtered list preferring canonical tables
        """
        if not tables:
            return tables

        def get_base_name(table: str) -> str:
            """Extract base name by removing common prefixes/suffixes."""
            name = table.lower()
            for prefix in ['bkp_', 'temp_', 'tmp_', 'old_', 'backup_', 'archive_', 'copy_', 'test_']:
                if name.startswith(prefix):
                    name = name[len(prefix):]
            # Remove date stamps like bkp300725_
            name = re.sub(r'^bkp\d{6,8}_', '', name)
            for suffix in ['_bkp', '_backup', '_temp', '_tmp', '_old', '_archive', '_copy', '_test']:
                if name.endswith(suffix):
                    name = name[:-len(suffix)]
            return name

        # Group variants
        variant_groups = defaultdict(list)
        for table in tables:
            base_name = get_base_name(table)
            variant_groups[base_name].append(table)

        # Select best from each group
        selected = []
        for base_name, variants in variant_groups.items():
            if len(variants) == 1:
                selected.append(variants[0])
            else:
                # Multiple variants - choose canonical
                best = max(variants, key=lambda t: (
                    not self._is_structurally_peripheral(t),
                    self._canonical_weight(t),
                    confidences.get(t, 0.0)
                ))
                selected.append(best)

        return selected

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

    def set_semantic_layer(self, semantic_layer: Any) -> None:
        """
        Set or update semantic layer with business meanings.

        Supports two formats:
        Simple format (list of table descriptors):
        [
            {
                "table_name": "CST_TXN_H",
                "description": "customer transaction history",
                "columns": [
                    {"name": "C_AMT", "description": "transaction amount local currency"}
                ]
            }
        ]
        Rich format (dict with entities, relationships, columns, measures):
        {
            "entities": [{...}],
            "relationships": [{...}],
            "columns": [{...}],
            "measures": [{...}]
        }

        Args:
            semantic_layer: Semantic meanings for tables and columns
        """
        logger.info(f"Setting semantic layer")
        self.semantic_layer = semantic_layer
        # Rebuild all embeddings since semantic info changed
        self._refresh_schema()

    def _is_rich_semantic_layer(self) -> bool:
        """Check if semantic layer uses the rich dict format with entities/relationships."""
        return isinstance(self.semantic_layer, dict) and (
            "entities" in self.semantic_layer or
            "relationships" in self.semantic_layer or
            "columns" in self.semantic_layer
        )

    def _build_semantic_maps(self) -> Tuple[Dict[str, str], Dict[str, str]]:
        """
        Build lookup maps for semantic table and column descriptions.

        Handles both simple list format and rich dict format.
        
        Returns:
            Tuple of (table_description_map, column_description_map)
        """
        table_map: Dict[str, str] = {}
        column_map: Dict[str, str] = {}

        if self._is_rich_semantic_layer():
            # Rich format: extract from entities, columns, measures
            table_map, column_map = self._build_semantic_maps_rich()
        elif isinstance(self.semantic_layer, list):
            # Simple list format
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

    def _build_semantic_maps_rich(self) -> Tuple[Dict[str, str], Dict[str, str]]:
        """
        Build semantic maps from the rich semantic layer format.

        Extracts descriptions from:
        - entities: table descriptions (applied to each physical_table)
        - columns: column business names and synonyms
        - measures: column-level descriptions from measure definitions
        
        Returns:
            Tuple of (table_description_map, column_description_map)
        """
        table_map: Dict[str, str] = {}
        column_map: Dict[str, str] = {}

        # Process entities → table descriptions
        for entity in self.semantic_layer.get("entities", []):
            description = entity.get("description", "")
            business_name = entity.get("business_name", "")
            entity_synonyms = entity.get("synonyms", [])

            # Build rich table description from all available info
            desc_parts = []
            if business_name:
                desc_parts.append(business_name)
            if description:
                desc_parts.append(description)
            if entity_synonyms:
                desc_parts.append(" ".join(entity_synonyms))

            full_desc = " ".join(desc_parts)

            # Apply to each physical table
            for phys_table in entity.get("physical_tables", []):
                if full_desc:
                    table_map[phys_table] = full_desc

        # Process columns → column descriptions with synonyms
        for col_entry in self.semantic_layer.get("columns", []):
            phys_table = col_entry.get("physical_table")
            phys_column = col_entry.get("physical_column")
            if not phys_table or not phys_column:
                continue

            desc_parts = []
            business_name = col_entry.get("business_name", "")
            if business_name:
                desc_parts.append(business_name)

            col_synonyms = col_entry.get("synonyms", [])
            if col_synonyms:
                desc_parts.append(" ".join(col_synonyms))

            if desc_parts:
                column_map[f"{phys_table}.{phys_column}"] = " ".join(desc_parts)

        # Process measures → column descriptions
        for measure in self.semantic_layer.get("measures", []):
            source_table = measure.get("source_table")
            source_column = measure.get("source_column")
            measure_desc = measure.get("description", "")
            measure_name = measure.get("name", "")

            if not source_table or not source_column:
                continue

            col_key = f"{source_table}.{source_column}"
            # Append to existing description if present
            existing = column_map.get(col_key, "")
            desc_parts = [p for p in [existing, measure_name, measure_desc] if p]
            column_map[col_key] = " ".join(desc_parts)

        # Process dimensions → enrich dimension table descriptions
        for dim in self.semantic_layer.get("dimensions", []):
            source_table = dim.get("source_table")
            dim_desc = dim.get("description", "")
            if source_table and dim_desc:
                existing = table_map.get(source_table, "")
                if existing:
                    table_map[source_table] = f"{existing} {dim_desc}"
                else:
                    table_map[source_table] = dim_desc

        return table_map, column_map

    def _enrich_from_semantic_layer(self) -> None:
        """
        Enrich table_synonyms and column_comments from semantic layer data.

        For the rich format, extracts:
        - Entity synonyms → table_synonyms
        - Entity physical_table groupings → table_synonyms cross-references
        - Column synonyms → column_comments
        """
        if not self.semantic_layer:
            return

        if self._is_rich_semantic_layer():
            # Extract entity synonyms → table_synonyms
            for entity in self.semantic_layer.get("entities", []):
                entity_synonyms = entity.get("synonyms", [])
                business_name = entity.get("business_name", "")

                all_synonyms = list(entity_synonyms)
                if business_name:
                    all_synonyms.append(business_name.lower())

                # Apply synonyms to each physical table
                for phys_table in entity.get("physical_tables", []):
                    if all_synonyms:
                        existing = self.table_synonyms.get(phys_table, [])
                        merged = list(dict.fromkeys(existing + all_synonyms))
                        self.table_synonyms[phys_table] = merged

            # Extract column synonyms → column_comments
            for col_entry in self.semantic_layer.get("columns", []):
                phys_table = col_entry.get("physical_table")
                phys_column = col_entry.get("physical_column")
                col_synonyms = col_entry.get("synonyms", [])
                business_name = col_entry.get("business_name", "")

                if not phys_table or not phys_column:
                    continue

                col_key = f"{phys_table}.{phys_column}"
                desc_parts = []
                if business_name:
                    desc_parts.append(business_name)
                if col_synonyms:
                    desc_parts.append(" ".join(col_synonyms))

                if desc_parts and col_key not in self.column_comments:
                    self.column_comments[col_key] = " ".join(desc_parts)

            logger.info(
                f"Enriched from semantic layer: "
                f"{len(self.table_synonyms)} table synonyms, "
                f"{len(self.column_comments)} column comments"
            )

    def _enrich_fk_from_semantic_layer(self) -> None:
        """
        Supplement FK graph using explicit relationships from semantic layer.

        The rich semantic layer format contains relationship definitions
        that can fill gaps in the FK graph (e.g., when FK constraints
        are not defined in the database).
        """
        if not self._is_rich_semantic_layer():
            return

        added = 0
        for rel in self.semantic_layer.get("relationships", []):
            from_table = rel.get("from_table")
            to_table = rel.get("to_table")

            if not from_table or not to_table:
                continue

            # Only add if both tables exist in schema
            if from_table in self.table_metadata and to_table in self.table_metadata:
                if to_table not in self.fk_graph.get(from_table, set()):
                    self.fk_graph[from_table].add(to_table)
                    self.fk_graph[to_table].add(from_table)
                    added += 1

        # Also link physical tables within the same entity
        for entity in self.semantic_layer.get("entities", []):
            phys_tables = entity.get("physical_tables", [])
            for i, t1 in enumerate(phys_tables):
                for t2 in phys_tables[i + 1:]:
                    if t1 in self.table_metadata and t2 in self.table_metadata:
                        if t2 not in self.fk_graph.get(t1, set()):
                            self.fk_graph[t1].add(t2)
                            self.fk_graph[t2].add(t1)
                            added += 1

        if added > 0:
            logger.info(f"Enriched FK graph with {added} relationships from semantic layer")

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
            "structural_links": len(self.structural_links),
            "model_name": self.model.get_sentence_embedding_dimension(),
            "confidence_threshold": self.confidence_threshold,
            "tie_threshold": self.tie_threshold,
        }
