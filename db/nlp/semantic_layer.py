"""Semantic layer parsing, enrichment, and pattern/prompt matching.

Handles both the simple list format and the rich dict format
of semantic layer data, including:
- Building semantic description maps
- Enriching table synonyms and column comments
- Query pattern matching (deterministic shortcuts)
- Common prompt matching (embedding-based shortcuts)
"""

import logging
from typing import Any, Dict, List, Optional, Set, Tuple

import numpy as np

from db.nlp.data_models import TableMetadata, TableSelectionResult
from db.nlp.schema_normalizer import SchemaNormalizer

logger = logging.getLogger(__name__)


def is_rich_semantic_layer(semantic_layer: Any) -> bool:
    """Check if semantic layer uses the rich dict format with entities/relationships."""
    return isinstance(semantic_layer, dict) and (
        "entities" in semantic_layer
        or "relationships" in semantic_layer
        or "columns" in semantic_layer
        or "query_patterns" in semantic_layer
        or "term_glossary" in semantic_layer
    )


def get_database_prefixes(semantic_layer: Any) -> Optional[List[str]]:
    """
    Return the ``database_prefix`` list from the semantic layer.

    A single-string value is normalised to a one-element list.
    Returns None if not configured.
    """
    if not is_rich_semantic_layer(semantic_layer):
        return None

    prefix = semantic_layer.get("database_prefix")
    if prefix is None:
        return None
    if isinstance(prefix, str):
        return [prefix]
    if isinstance(prefix, list):
        return [str(p) for p in prefix]
    return None


def resolve_qualified_table(
    table_name: str,
    semantic_layer: Any,
) -> Tuple[Optional[str], str]:
    """
    Split a possibly-qualified table name into (prefix, base_table).

    If no database_prefix is configured, returns (None, table_name).
    """
    prefixes = get_database_prefixes(semantic_layer)
    if not prefixes:
        return None, table_name

    for pfx in prefixes:
        separator = f"{pfx}__"
        if table_name.startswith(separator):
            return pfx, table_name[len(separator):]

    return None, table_name


# ------------------------------------------------------------------
# Semantic map building
# ------------------------------------------------------------------

def build_semantic_maps(
    semantic_layer: Any,
) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Build lookup maps for semantic table and column descriptions.

    Handles both simple list format and rich dict format.

    Returns:
        (table_description_map, column_description_map)
    """
    if is_rich_semantic_layer(semantic_layer):
        return _build_semantic_maps_rich(semantic_layer)

    table_map: Dict[str, str] = {}
    column_map: Dict[str, str] = {}

    if isinstance(semantic_layer, list):
        for entry in semantic_layer:
            table_name = entry.get("table_name")
            if not table_name:
                continue

            table_desc = entry.get("description")
            if table_desc:
                table_map[table_name] = table_desc

            for col in entry.get("columns", []) or []:
                col_name = col.get("name")
                col_desc = col.get("description")
                if col_name and col_desc:
                    column_map[f"{table_name}.{col_name}"] = col_desc

    return table_map, column_map


def _build_semantic_maps_rich(
    semantic_layer: dict,
) -> Tuple[Dict[str, str], Dict[str, str]]:
    """Build semantic maps from the rich semantic layer format."""
    table_map: Dict[str, str] = {}
    column_map: Dict[str, str] = {}

    # Entities -> table descriptions
    for entity in semantic_layer.get("entities", []):
        desc_parts = []
        business_name = entity.get("business_name", "")
        description = entity.get("description", "")
        entity_synonyms = entity.get("synonyms", [])

        if business_name:
            desc_parts.append(business_name)
        if description:
            desc_parts.append(description)
        if entity_synonyms:
            desc_parts.append(" ".join(entity_synonyms))

        full_desc = " ".join(desc_parts)
        for phys_table in entity.get("physical_tables", []):
            if full_desc:
                table_map[phys_table] = full_desc

    # Columns -> column descriptions with synonyms
    for col_entry in semantic_layer.get("columns", []):
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

    # Measures -> column descriptions
    for measure in semantic_layer.get("measures", []):
        source_table = measure.get("source_table")
        source_column = measure.get("source_column")
        if not source_table or not source_column:
            continue

        col_key = f"{source_table}.{source_column}"
        existing = column_map.get(col_key, "")
        measure_name = measure.get("name", "")
        measure_desc = measure.get("description", "")
        desc_parts = [p for p in [existing, measure_name, measure_desc] if p]
        column_map[col_key] = " ".join(desc_parts)

    # Dimensions -> enrich dimension table descriptions
    for dim in semantic_layer.get("dimensions", []):
        source_table = dim.get("source_table")
        dim_desc = dim.get("description", "")
        if source_table and dim_desc:
            existing = table_map.get(source_table, "")
            table_map[source_table] = (
                f"{existing} {dim_desc}" if existing else dim_desc
            )

    # Term glossary -> enrich table descriptions
    for term, mapping in semantic_layer.get("term_glossary", {}).items():
        gl_table = mapping.get("table")
        if gl_table:
            existing = table_map.get(gl_table, "")
            table_map[gl_table] = f"{existing} {term}" if existing else term

    return table_map, column_map


# ------------------------------------------------------------------
# Semantic layer enrichment
# ------------------------------------------------------------------

def enrich_from_semantic_layer(
    semantic_layer: Any,
    table_synonyms: Dict[str, List[str]],
    column_comments: Dict[str, str],
) -> None:
    """
    Enrich table_synonyms and column_comments in-place from semantic layer data.

    For the rich format, extracts:
    - Entity synonyms -> table_synonyms
    - Column synonyms -> column_comments
    """
    if not semantic_layer:
        return

    if not is_rich_semantic_layer(semantic_layer):
        return

    # Entity synonyms -> table_synonyms
    for entity in semantic_layer.get("entities", []):
        entity_synonyms = entity.get("synonyms", [])
        business_name = entity.get("business_name", "")

        all_synonyms = list(entity_synonyms)
        if business_name:
            all_synonyms.append(business_name.lower())

        for phys_table in entity.get("physical_tables", []):
            if all_synonyms:
                existing = table_synonyms.get(phys_table, [])
                merged = list(dict.fromkeys(existing + all_synonyms))
                table_synonyms[phys_table] = merged

    # Column synonyms -> column_comments
    for col_entry in semantic_layer.get("columns", []):
        phys_table = col_entry.get("physical_table")
        phys_column = col_entry.get("physical_column")
        if not phys_table or not phys_column:
            continue

        col_key = f"{phys_table}.{phys_column}"
        desc_parts = []
        business_name = col_entry.get("business_name", "")
        if business_name:
            desc_parts.append(business_name)
        col_synonyms = col_entry.get("synonyms", [])
        if col_synonyms:
            desc_parts.append(" ".join(col_synonyms))

        if desc_parts and col_key not in column_comments:
            column_comments[col_key] = " ".join(desc_parts)

    logger.info(
        f"Enriched from semantic layer: "
        f"{len(table_synonyms)} table synonyms, "
        f"{len(column_comments)} column comments"
    )

    # Log feature availability
    num_patterns = len(semantic_layer.get("query_patterns", []))
    num_glossary = len(semantic_layer.get("term_glossary", {}))
    db_prefix = semantic_layer.get("database_prefix")
    if num_patterns:
        logger.info(f"Semantic layer has {num_patterns} query patterns")
    if num_glossary:
        logger.info(f"Semantic layer has {num_glossary} glossary terms")
    if db_prefix:
        prefixes = db_prefix if isinstance(db_prefix, list) else [db_prefix]
        logger.info(f"Semantic layer database prefixes: {prefixes}")


# ------------------------------------------------------------------
# Common-prompt shortcut (embedding-based)
# ------------------------------------------------------------------

class CommonPromptMatcher:
    """Pre-computed embedding matcher for common/frequent prompts."""

    def __init__(self):
        self.texts: List[str] = []
        self.tables: List[List[str]] = []
        self.embeddings: Optional[np.ndarray] = None

    def build(
        self,
        semantic_layer: Any,
        normalizer: SchemaNormalizer,
        model: Any,
    ) -> None:
        """
        Pre-compute embeddings for ``common_prompts`` entries.

        Called once during schema build. No-op if semantic layer
        has no common_prompts.
        """
        self.texts = []
        self.tables = []
        self.embeddings = None

        if not is_rich_semantic_layer(semantic_layer):
            return

        entries = semantic_layer.get("common_prompts")
        if not entries:
            return

        texts: List[str] = []
        tables: List[List[str]] = []
        for entry in entries:
            prompt_text = entry.get("prompt", "").strip()
            table_list = entry.get("tables", [])
            if prompt_text and table_list:
                texts.append(normalizer.normalize_text(prompt_text))
                tables.append(table_list)

        if not texts:
            return

        self.texts = texts
        self.tables = tables
        self.embeddings = model.encode(
            texts,
            batch_size=32,
            normalize_embeddings=True,
            show_progress_bar=False,
        )

        logger.info(f"Built embeddings for {len(texts)} common prompts")

    def match(
        self,
        prompt: str,
        normalizer: SchemaNormalizer,
        model: Any,
        table_metadata: Dict[str, TableMetadata],
        similarity_threshold: float = 0.85,
    ) -> Optional[TableSelectionResult]:
        """
        Check if prompt is semantically close to a pre-defined common prompt.

        Returns a TableSelectionResult on match, else None.
        """
        if self.embeddings is None:
            return None

        normalized = normalizer.normalize_text(prompt)
        prompt_embedding = model.encode(normalized, normalize_embeddings=True)

        similarities = prompt_embedding @ self.embeddings.T
        best_idx = int(np.argmax(similarities))
        best_score = float(similarities[best_idx])

        if best_score < similarity_threshold:
            return None

        matched_tables = [
            t for t in self.tables[best_idx] if t in table_metadata
        ]
        if not matched_tables:
            return None

        logger.info(
            f"Common-prompt shortcut matched (score={best_score:.3f}): "
            f'"{self.texts[best_idx]}" -> {matched_tables}'
        )

        return TableSelectionResult(
            status="success",
            tables=matched_tables,
            confidences={t: best_score for t in matched_tables},
            metadata={
                "mode": "common_prompt",
                "matched_prompt": self.texts[best_idx],
                "similarity": best_score,
                "prompt": prompt,
                "normalized_prompt": normalized,
            },
        )


# ------------------------------------------------------------------
# Query-pattern shortcut (deterministic)
# ------------------------------------------------------------------

def match_query_patterns(
    prompt: str,
    semantic_layer: Any,
    normalizer: SchemaNormalizer,
    table_metadata: Dict[str, TableMetadata],
    fk_graph: Dict[str, Set[str]],
    expand_entity_pairs_fn,
    expand_header_detail_fn,
    is_dimension_fn,
) -> Optional[TableSelectionResult]:
    """
    Check semantic layer ``query_patterns`` for a deterministic match.

    If any pattern phrase is found as a substring of the normalised
    prompt, resolve the referenced entities to physical tables and
    return a high-confidence result immediately.
    """
    if not is_rich_semantic_layer(semantic_layer):
        return None

    patterns = semantic_layer.get("query_patterns")
    if not patterns:
        return None

    normalized_prompt = normalizer.normalize_text(prompt)

    matched_entities: List[str] = []
    matched_filters: Dict[str, str] = {}
    matched_phrases: List[str] = []

    for entry in patterns:
        phrases = entry.get("pattern", [])
        for phrase in phrases:
            normalized_phrase = normalizer.normalize_text(phrase)
            if normalized_phrase and normalized_phrase in normalized_prompt:
                matched_entities.extend(entry.get("entities", []))
                matched_filters.update(entry.get("filters") or {})
                matched_phrases.append(phrase)
                break

    if not matched_entities:
        return None

    # Resolve entity names -> physical tables
    entity_map: Dict[str, List[str]] = {}
    for entity in semantic_layer.get("entities", []):
        entity_map[entity["name"]] = entity.get("physical_tables", [])

    selected_tables: List[str] = []
    for ent_name in dict.fromkeys(matched_entities):
        for phys_table in entity_map.get(ent_name, []):
            if phys_table in table_metadata and phys_table not in selected_tables:
                selected_tables.append(phys_table)

    if not selected_tables:
        return None

    # Expand with entity pairs and header-detail pairing
    core = set(selected_tables)
    core.update(expand_entity_pairs_fn(list(core)))
    core.update(expand_header_detail_fn(list(core)))

    # 1-hop FK expansion for dimension tables
    fk_dims: List[str] = []
    for t in core:
        for neighbor in fk_graph.get(t, set()):
            if neighbor not in core and neighbor in table_metadata:
                if is_dimension_fn(neighbor):
                    fk_dims.append(neighbor)

    final_tables = list(core) + list(dict.fromkeys(fk_dims))

    return TableSelectionResult(
        status="success",
        tables=final_tables,
        confidences={t: 1.0 for t in final_tables},
        metadata={
            "mode": "query_pattern",
            "matched_phrases": matched_phrases,
            "matched_entities": list(dict.fromkeys(matched_entities)),
            "matched_filters": matched_filters,
            "prompt": prompt,
            "normalized_prompt": normalized_prompt,
        },
    )
