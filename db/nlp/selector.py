"""NLPTableSelector — orchestrator for semantic table selection.

Thin facade that composes SchemaBuilder, scoring functions,
table filters, and semantic layer utilities to select relevant
database tables for natural language queries.
"""

import logging
from typing import Any, Dict, List, Optional, Set, Tuple

from db.embedding_cache import get_sentence_transformer
from db.nlp.data_models import TableSelectionResult
from db.nlp.schema_builder import SchemaBuilder
from db.nlp.schema_normalizer import SchemaNormalizer
from db.nlp import scoring
from db.nlp import semantic_layer as sl
from db.nlp import table_filters as tf

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


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

        # Components
        self.normalizer = SchemaNormalizer(acronym_map=acronym_map)

        self.model = get_sentence_transformer(model_name, cache_folder="models")
        if self.model is None:
            raise ImportError(
                "sentence-transformers required. Install with: "
                "pip install sentence-transformers"
            )

        self.schema = SchemaBuilder(
            normalizer=self.normalizer,
            model=self.model,
            db_connector=db_connector,
        )
        self.common_prompt_matcher = sl.CommonPromptMatcher()

        logger.info(f"Initialized NLPTableSelector with model: {model_name}")

        self._refresh_schema()

    # ------------------------------------------------------------------
    # Convenience accessors (preserve external interface)
    # ------------------------------------------------------------------

    @property
    def table_metadata(self):
        return self.schema.table_metadata

    @property
    def column_embeddings(self):
        return self.schema.column_embeddings

    @property
    def column_index(self):
        return self.schema.column_index

    @property
    def fk_graph(self):
        return self.schema.fk_graph

    @property
    def schema_hash(self):
        return self.schema.schema_hash

    # ------------------------------------------------------------------
    # Schema lifecycle
    # ------------------------------------------------------------------

    def _refresh_schema(self) -> None:
        """Build or rebuild schema structure from database."""
        # Enrich synonyms/comments from semantic layer before building
        sl.enrich_from_semantic_layer(
            self.semantic_layer,
            self.table_synonyms,
            self.column_comments,
        )

        # Build semantic description maps
        semantic_table_map, semantic_column_map = sl.build_semantic_maps(
            self.semantic_layer
        )

        # Build schema
        self.schema.build(
            table_synonyms=self.table_synonyms,
            column_comments=self.column_comments,
            semantic_table_map=semantic_table_map,
            semantic_column_map=semantic_column_map,
        )

        # Enrich FK graph from semantic layer relationships
        if sl.is_rich_semantic_layer(self.semantic_layer):
            self.schema.enrich_fk_from_relationships(
                self.semantic_layer.get("relationships", []),
                self.semantic_layer.get("entities", []),
            )

        # Build common-prompt embeddings
        self.common_prompt_matcher.build(
            self.semantic_layer, self.normalizer, self.model
        )

    def refresh_schema(self) -> None:
        """Refresh schema structure from database. Call after schema changes."""
        logger.info("Refreshing schema from database...")
        self._refresh_schema()

    def set_synonyms(self, table_synonyms: Dict[str, List[str]]) -> None:
        """Set or update table synonyms and rebuild embeddings."""
        logger.info(f"Setting {len(table_synonyms)} table synonyms")
        self.table_synonyms.update(table_synonyms)
        self.schema.rebuild_table_documents(
            list(table_synonyms.keys()), self.table_synonyms
        )

    def set_column_comments(self, column_comments: Dict[str, str]) -> None:
        """Set or update column comments and rebuild embeddings."""
        logger.info(f"Setting {len(column_comments)} column comments")
        self.column_comments.update(column_comments)
        self.schema._build_column_embeddings()

    def set_semantic_layer(self, semantic_layer: Any) -> None:
        """Set or update semantic layer and rebuild schema."""
        logger.info("Setting semantic layer")
        self.semantic_layer = semantic_layer
        self._refresh_schema()

    # ------------------------------------------------------------------
    # Helper shortcuts (delegate to sub-modules with current state)
    # ------------------------------------------------------------------

    def _is_rich(self) -> bool:
        return sl.is_rich_semantic_layer(self.semantic_layer)

    def _is_peripheral(self, table: str) -> bool:
        return tf.is_structurally_peripheral(
            table,
            self.semantic_layer,
            self._is_rich(),
            self.schema.canonical_score,
            self.schema.fk_graph,
            self.schema.structural_links,
        )

    def _canonical_weight(self, table: str) -> float:
        return self.schema.canonical_score.get(table, 0.5)

    def _centrality(self, table: str) -> float:
        return tf.structural_centrality(
            table,
            self.schema.fk_graph,
            self.schema.structural_links,
            self.schema.canonical_score,
        )

    def _is_dimension(self, table: str) -> bool:
        return tf.is_dimension_table(table, self.semantic_layer, self._is_rich())

    def _expand_entities(self, tables: List[str]) -> Set[str]:
        return tf.expand_entity_pairs(
            tables,
            self.semantic_layer,
            self._is_rich(),
            self.schema.table_metadata,
        )

    def _expand_hdr_dtl(self, tables: List[str]) -> Set[str]:
        return tf.expand_header_detail_pairs(
            tables, set(self.schema.table_metadata.keys())
        )

    # ------------------------------------------------------------------
    # Public API (database prefix support)
    # ------------------------------------------------------------------

    def get_database_prefixes(self) -> Optional[List[str]]:
        """Return the database_prefix list from the semantic layer."""
        return sl.get_database_prefixes(self.semantic_layer)

    def resolve_qualified_table(self, table_name: str) -> Tuple[Optional[str], str]:
        """Split a possibly-qualified table name into (prefix, base_table)."""
        return sl.resolve_qualified_table(table_name, self.semantic_layer)

    def get_schema_info(self) -> Dict[str, Any]:
        """Get information about current schema."""
        info = {
            "num_tables": len(self.schema.table_metadata),
            "num_columns": sum(
                len(meta.columns) for meta in self.schema.table_metadata.values()
            ),
            "num_embeddings": len(self.schema.column_embeddings),
            "schema_hash": self.schema.schema_hash,
            "fk_relationships": len(self.schema.fk_graph),
            "structural_links": len(self.schema.structural_links),
            "model_name": self.model.get_sentence_embedding_dimension(),
            "confidence_threshold": self.confidence_threshold,
            "tie_threshold": self.tie_threshold,
        }

        if self._is_rich():
            info["common_prompts_count"] = len(
                self.semantic_layer.get("common_prompts", [])
            )
            info["query_patterns_count"] = len(
                self.semantic_layer.get("query_patterns", [])
            )
            info["term_glossary_count"] = len(
                self.semantic_layer.get("term_glossary", {})
            )
            info["database_prefixes"] = self.get_database_prefixes()

        return info

    # ------------------------------------------------------------------
    # Core selection
    # ------------------------------------------------------------------

    def select_tables(
        self,
        prompt: str,
        top_k: int = 3,
        fk_expand: bool = True,
    ) -> TableSelectionResult:
        """
        Select tables relevant to a natural language query.

        Process:
        0. Check common_prompts / query_patterns for shortcuts
        1. Normalize and embed the query
        2. Find top-K matching columns via cosine similarity
        3. Aggregate column matches to table scores
        4. Apply lexical + glossary boosts
        5. Rank by blended semantic + structural scores
        6. Expand via entity pairs, header-detail, and FK
        7. Filter noise, cap results

        Args:
            prompt: Natural language query
            top_k: Number of top tables to return
            fk_expand: Whether to expand via FK graph

        Returns:
            TableSelectionResult with selected tables and confidences
        """
        logger.info(f"Selecting tables for prompt: {prompt[:100]}")

        if not self.schema.column_embeddings:
            return TableSelectionResult(
                status="no_match",
                tables=[],
                confidences={},
                metadata={"error": "Schema not initialized"},
            )

        # --- Step 0a: Common-prompt shortcut ---
        common_result = self.common_prompt_matcher.match(
            prompt, self.normalizer, self.model, self.schema.table_metadata
        )
        if common_result is not None:
            logger.info(
                f"Common-prompt shortcut — returning predefined result: "
                f"{common_result.tables}"
            )
            return common_result

        # --- Step 0b: Query-pattern shortcut ---
        pattern_result = sl.match_query_patterns(
            prompt,
            self.semantic_layer,
            self.normalizer,
            self.schema.table_metadata,
            self.schema.fk_graph,
            self._expand_entities,
            self._expand_hdr_dtl,
            self._is_dimension,
        )
        if pattern_result is not None:
            logger.info(
                f"Query pattern matched — returning deterministic result: "
                f"{pattern_result.tables}"
            )
            return pattern_result

        # --- Step 1: Embed query ---
        normalized_prompt = self.normalizer.normalize_text(prompt)
        prompt_embedding = self.model.encode(
            normalized_prompt,
            normalize_embeddings=True,
        )

        # --- Step 2: Top columns ---
        top_columns = scoring.retrieve_top_columns(
            prompt_embedding,
            self.schema.column_embeddings,
            self.schema.column_index,
            top_k * 5,
        )

        # --- Step 3: Table scores ---
        table_scores = scoring.aggregate_to_tables(
            top_columns,
            self.schema.canonical_score,
        )

        tokens = set(normalized_prompt.split())

        # Lexical-only shortcut for very short prompts
        if len(tokens) <= 2:
            lexical_tables = scoring.lexical_table_matches(
                tokens,
                self.schema.table_metadata,
            )
            if len(lexical_tables) > 1:
                return TableSelectionResult(
                    status="success",
                    tables=lexical_tables,
                    confidences={t: 1.0 for t in lexical_tables},
                    metadata={"mode": "lexical_keyword"},
                )

        # --- Step 4: Boost scores ---
        table_scores = scoring.apply_lexical_boost(
            table_scores,
            tokens,
            self.schema.table_metadata,
            self.semantic_layer,
            self._is_rich(),
        )
        table_scores = scoring.apply_glossary_boost(
            table_scores,
            tokens,
            self.semantic_layer,
            self._is_rich(),
            self.schema.table_metadata,
        )

        confidences = scoring.normalize_scores(table_scores)

        # --- Step 5: Rank by blended semantic + structural score ---
        ranked = []
        for t, conf in confidences.items():
            centrality = self._centrality(t)
            anchor_score = conf * 0.9 + centrality * 0.1
            ranked.append((t, anchor_score))
        ranked.sort(key=lambda x: x[1], reverse=True)

        anchor_k = min(max(3, top_k), 5)
        anchors = [t for t, _ in ranked[:anchor_k]]

        # Force entity-synonym-matched tables as anchors
        synonym_forced = set()
        if self._is_rich():
            syn_stems = set()
            for tok in tokens:
                syn_stems.update(scoring.simple_stems(tok))

            for entity in self.semantic_layer.get("entities", []):
                synonyms = [s.lower() for s in entity.get("synonyms", [])]
                matched = any(
                    stem == syn or stem in syn.split()
                    for stem in syn_stems
                    for syn in synonyms
                )
                if matched:
                    for pt in entity.get("physical_tables", []):
                        if pt in self.schema.table_metadata and pt not in anchors:
                            anchors.append(pt)
                            synonym_forced.add(pt)

        # Filter peripheral anchors, prefer canonical
        anchors = [t for t in anchors if not self._is_peripheral(t)]
        anchors = tf.prefer_canonical_variants(
            anchors,
            confidences,
            self._canonical_weight,
            self._is_peripheral,
        )

        # Minimum confidence (except synonym-forced)
        min_anchor_conf = 0.15
        anchors = [
            t
            for t in anchors
            if confidences.get(t, 0.0) >= min_anchor_conf or t in synonym_forced
        ]

        # --- Step 6: Expand ---
        core_tables = set(anchors)
        core_tables.update(self._expand_entities(list(core_tables)))
        core_tables.update(self._expand_hdr_dtl(list(core_tables)))

        # FK expansion (1 hop from core)
        fk_additions = set()
        anchor_fk_dims = set()
        if fk_expand:
            for t in core_tables:
                for neighbor in self.schema.fk_graph.get(t, set()):
                    if (
                        neighbor not in core_tables
                        and neighbor in self.schema.table_metadata
                    ):
                        fk_additions.add(neighbor)
                        if self._is_dimension(neighbor):
                            anchor_fk_dims.add(neighbor)

        # Tiered thresholds
        selected_tables = list(core_tables)

        fk_threshold = self.confidence_threshold * 0.35
        for t in fk_additions:
            score = confidences.get(t, 0.0)
            if score >= fk_threshold or t in anchor_fk_dims:
                selected_tables.append(t)

        # --- Step 7: Filter noise ---
        selected_tables = [t for t in selected_tables if not self._is_peripheral(t)]
        selected_tables = tf.filter_secondary_tables(selected_tables, confidences)
        selected_tables = tf.prefer_canonical_variants(
            selected_tables,
            confidences,
            self._canonical_weight,
            self._is_peripheral,
        )

        # Cap results
        max_results = max(top_k * 2, 8)
        if len(selected_tables) > max_results:
            core_set = set(core_tables)
            core_list = [t for t in selected_tables if t in core_set]
            fk_list = sorted(
                [t for t in selected_tables if t not in core_set],
                key=lambda t: (
                    confidences.get(t, 0.0),
                    len(self.schema.fk_graph.get(t, set())),
                ),
                reverse=True,
            )
            remaining_slots = max(max_results - len(core_list), 0)
            selected_tables = core_list + fk_list[:remaining_slots]

        # Fallback: top non-peripheral candidates
        if not selected_tables and confidences:
            non_peripheral = [
                t for t in confidences.keys() if not self._is_peripheral(t)
            ]
            if non_peripheral:
                selected_tables = sorted(
                    non_peripheral,
                    key=lambda t: confidences[t],
                    reverse=True,
                )[:3]

        # Ambiguity check
        top_candidates = []
        if selected_tables:
            top_selected_score = max(confidences.get(t, 0.0) for t in selected_tables)
            sorted_by_conf = sorted(
                confidences.items(),
                key=lambda x: x[1],
                reverse=True,
            )
            for table_name, score in sorted_by_conf:
                if table_name not in selected_tables:
                    if (top_selected_score - score) <= self.tie_threshold:
                        top_candidates.append(table_name)
                    else:
                        break
            top_candidates = top_candidates[:top_k]

        # Status
        if not selected_tables:
            status = "no_match"
        elif top_candidates:
            status = "ambiguous"
        else:
            status = "success"

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
