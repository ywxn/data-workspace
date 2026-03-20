"""Schema construction: metadata building, FK graph, embeddings, structural links.

Responsible for transforming raw database schema into the internal
representations used by the NLP table selector.
"""

import hashlib
import logging
import re
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple

import numpy as np

from db.nlp.data_models import ColumnMetadata, TableMetadata
from db.nlp.schema_normalizer import SchemaNormalizer

logger = logging.getLogger(__name__)


class SchemaBuilder:
    """Builds and maintains schema metadata, embeddings, and relationship graphs."""

    def __init__(
        self,
        normalizer: SchemaNormalizer,
        model: Any,
        db_connector: Any,
    ):
        self.normalizer = normalizer
        self.model = model
        self.db_connector = db_connector

        # Schema storage
        self.table_metadata: Dict[str, TableMetadata] = {}
        self.column_embeddings: List[np.ndarray] = []
        self.column_index: List[Tuple[str, str]] = []
        self.fk_graph: Dict[str, Set[str]] = defaultdict(set)
        self.table_docs: Dict[str, str] = {}
        self.structural_links: Dict[str, Set[str]] = defaultdict(set)
        self.canonical_score: Dict[str, float] = {}
        self.schema_hash: Optional[str] = None

    def build(
        self,
        table_synonyms: Dict[str, List[str]],
        column_comments: Dict[str, str],
        semantic_table_map: Dict[str, str],
        semantic_column_map: Dict[str, str],
    ) -> None:
        """
        Build complete schema from database.

        Args:
            table_synonyms: Table name -> synonym list mapping.
            column_comments: "table.column" -> comment mapping.
            semantic_table_map: Table name -> semantic description mapping.
            semantic_column_map: "table.column" -> semantic description mapping.
        """
        logger.info("Building schema structure...")
        self._clear()

        tables = self.db_connector.get_tables()
        logger.info(f"Found {len(tables)} tables")

        if not tables:
            logger.warning("No tables found in database")
            return

        # Build table metadata and documents
        for table_name in tables:
            self._build_table_metadata(
                table_name, table_synonyms, column_comments,
                semantic_table_map, semantic_column_map,
            )

        self._build_fk_graph()
        self._build_column_embeddings()
        self._compute_schema_hash(tables)
        self._infer_structural_links()
        self._compute_canonical_scores()

        logger.info(
            f"Schema built: {len(self.table_metadata)} tables, "
            f"{len(self.column_index)} columns indexed"
        )

    def _clear(self) -> None:
        """Reset all schema data."""
        self.table_metadata.clear()
        self.column_embeddings.clear()
        self.column_index.clear()
        self.fk_graph.clear()
        self.table_docs.clear()
        self.structural_links.clear()
        self.canonical_score.clear()

    # ------------------------------------------------------------------
    # Table metadata
    # ------------------------------------------------------------------

    def _build_table_metadata(
        self,
        table_name: str,
        table_synonyms: Dict[str, List[str]],
        column_comments: Dict[str, str],
        semantic_table_map: Dict[str, str],
        semantic_column_map: Dict[str, str],
    ) -> None:
        """Build metadata for a single table."""
        columns = self.db_connector.get_columns(table_name)
        normalized_table_name = self.normalizer.normalize_identifier(table_name)

        table_meta = TableMetadata(
            table_name=table_name,
            normalized_name=normalized_table_name,
            semantic_meaning=semantic_table_map.get(table_name),
        )

        for col_name in columns:
            col_key = f"{table_name}.{col_name}"
            col_meta = ColumnMetadata(
                table_name=table_name,
                column_name=col_name,
                normalized_name=self.normalizer.normalize_identifier(col_name),
                semantic_meaning=semantic_column_map.get(col_key),
                comment=column_comments.get(col_key),
            )
            table_meta.columns[col_name] = col_meta

        self.table_metadata[table_name] = table_meta
        self._build_table_document(table_name, table_meta, table_synonyms)

    def _build_table_document(
        self,
        table_name: str,
        table_meta: TableMetadata,
        table_synonyms: Dict[str, List[str]],
    ) -> None:
        """Build a semantic document string for the table (used for embeddings)."""
        doc_parts = [table_meta.normalized_name]

        if table_meta.semantic_meaning:
            doc_parts.append(
                self.normalizer.normalize_text(table_meta.semantic_meaning)
            )

        if table_name in table_synonyms:
            synonyms_text = " ".join(table_synonyms[table_name])
            doc_parts.append(self.normalizer.normalize_text(synonyms_text))

        for col_meta in table_meta.columns.values():
            doc_parts.append(col_meta.normalized_name)
            if col_meta.semantic_meaning:
                doc_parts.append(
                    self.normalizer.normalize_text(col_meta.semantic_meaning)
                )
            if col_meta.comment:
                doc_parts.append(self.normalizer.normalize_text(col_meta.comment))

        self.table_docs[table_name] = " ".join(doc_parts)

    def rebuild_table_documents(
        self,
        table_names: List[str],
        table_synonyms: Dict[str, List[str]],
    ) -> None:
        """Rebuild documents and embeddings for specific tables."""
        for table_name in table_names:
            if table_name in self.table_metadata:
                self._build_table_document(
                    table_name, self.table_metadata[table_name], table_synonyms
                )
        self._build_column_embeddings()

    # ------------------------------------------------------------------
    # Column embeddings
    # ------------------------------------------------------------------

    def _build_column_embeddings(self) -> None:
        """Build embeddings for all columns."""
        logger.info("Building column embeddings...")

        self.column_embeddings.clear()
        self.column_index.clear()

        embedding_texts = []
        for table_name, table_meta in self.table_metadata.items():
            table_doc = self.table_docs[table_name]

            for col_name, col_meta in table_meta.columns.items():
                col_text = f"{table_doc} {col_meta.normalized_name}"
                if col_meta.semantic_meaning:
                    col_text = (
                        f"{col_text} "
                        f"{self.normalizer.normalize_text(col_meta.semantic_meaning)}"
                    )

                embedding_texts.append(col_text)
                self.column_index.append((table_name, col_name))

        if embedding_texts:
            embeddings = self.model.encode(
                embedding_texts,
                batch_size=32,
                normalize_embeddings=True,
                show_progress_bar=False,
            )
            self.column_embeddings = embeddings.tolist()

        logger.info(f"Created {len(self.column_embeddings)} column embeddings")

    # ------------------------------------------------------------------
    # Foreign key graph
    # ------------------------------------------------------------------

    def _build_fk_graph(self) -> None:
        """Build foreign key relationship graph."""
        try:
            from sqlalchemy import inspect

            inspector = inspect(self.db_connector.engine)
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
                    continue

            logger.info(
                f"Built FK graph with {len(self.fk_graph)} table relationships"
            )

            # If FK graph is sparse, infer from naming patterns
            if len(self.fk_graph) < len(tables) * 0.3:
                logger.info(
                    "FK graph sparse, inferring relationships from naming patterns"
                )
                self._infer_fk_from_names()

        except Exception as e:
            logger.warning(f"Could not build FK graph: {e}. Inferring from names.")
            self._infer_fk_from_names()

    def _infer_fk_from_names(self) -> None:
        """
        Infer FK relationships from table naming patterns.

        Detects header/detail pairs (e.g. inward_hdr + inward_dtl)
        and master/detail patterns (e.g. requisition + requisition_dtl).
        """
        tables = list(self.table_metadata.keys())

        for table in tables:
            table_lower = table.lower()

            # Header-detail pattern
            if "_hdr" in table_lower:
                base = table_lower.replace("_hdr", "")
                detail_name = base + "_dtl"
                for other in tables:
                    if other.lower() == detail_name:
                        self.fk_graph[table].add(other)
                        self.fk_graph[other].add(table)
                        break

            # Master-detail pattern
            for other in tables:
                other_lower = other.lower()
                if other_lower.startswith(table_lower + "_dtl"):
                    self.fk_graph[table].add(other)
                    self.fk_graph[other].add(table)
                elif table_lower.startswith(other_lower + "_dtl"):
                    self.fk_graph[table].add(other)
                    self.fk_graph[other].add(table)

        logger.info(
            f"Inferred FK relationships, graph now has {len(self.fk_graph)} nodes"
        )

    def enrich_fk_from_relationships(
        self, relationships: List[Dict], entities: List[Dict]
    ) -> None:
        """
        Supplement FK graph using explicit relationships from semantic layer.

        Args:
            relationships: List of {from_table, to_table, ...} dicts.
            entities: List of entity dicts with physical_tables lists.
        """
        added = 0
        for rel in relationships:
            from_table = rel.get("from_table")
            to_table = rel.get("to_table")

            if not from_table or not to_table:
                continue

            if from_table in self.table_metadata and to_table in self.table_metadata:
                if to_table not in self.fk_graph.get(from_table, set()):
                    self.fk_graph[from_table].add(to_table)
                    self.fk_graph[to_table].add(from_table)
                    added += 1

        # Link physical tables within the same entity
        for entity in entities:
            phys_tables = entity.get("physical_tables", [])
            for i, t1 in enumerate(phys_tables):
                for t2 in phys_tables[i + 1 :]:
                    if t1 in self.table_metadata and t2 in self.table_metadata:
                        if t2 not in self.fk_graph.get(t1, set()):
                            self.fk_graph[t1].add(t2)
                            self.fk_graph[t2].add(t1)
                            added += 1

        if added > 0:
            logger.info(
                f"Enriched FK graph with {added} relationships from semantic layer"
            )

    # ------------------------------------------------------------------
    # Structural analysis
    # ------------------------------------------------------------------

    def _infer_structural_links(self) -> None:
        """
        Infer structural relationships between tables using FK connectivity
        and column overlap (e.g. header-detail, fact-dimension).
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

                min_cols = min(len(cols_a), len(cols_b))
                if min_cols == 0:
                    continue

                overlap_ratio = len(cols_a & cols_b) / min_cols

                if overlap_ratio >= 0.4:
                    self.structural_links[table].add(n)
                    self.structural_links[n].add(table)

    def _compute_canonical_scores(self) -> None:
        """
        Estimate canonical importance of tables using FK centrality and column richness.

        High-scoring tables are referenced by many others and have many columns.
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

    def _compute_schema_hash(self, tables: List[str]) -> None:
        """Compute hash of schema for change detection."""
        schema_str = "|".join(sorted(tables))
        self.schema_hash = hashlib.sha256(schema_str.encode()).hexdigest()
