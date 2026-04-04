"""Score aggregation, normalization, lexical/glossary boosts, and stemming.

Contains all logic for computing and transforming table relevance scores
from raw column-level similarities to final confidence values.
"""

import logging
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple

import numpy as np

from db.nlp.data_models import TableMetadata

logger = logging.getLogger(__name__)


def simple_stems(word: str) -> Set[str]:
    """
    Generate simple stem variants for fuzzy matching.

    Handles common English morphological suffixes:
    - Plurals: items -> item, categories -> category
    - Past tense: issued -> issue, delayed -> delay
    - Progressive: moving -> move
    """
    stems = {word}
    if word.endswith("ies") and len(word) > 4:
        stems.add(word[:-3] + "y")
    if word.endswith("es") and len(word) > 3:
        stems.add(word[:-2])
    if word.endswith("s") and not word.endswith("ss") and len(word) > 2:
        stems.add(word[:-1])
    if word.endswith("ed") and len(word) > 4:
        stems.add(word[:-2])
        stems.add(word[:-1])  # "issued" -> "issue"
    if word.endswith("ing") and len(word) > 5:
        stems.add(word[:-3])
        stems.add(word[:-3] + "e")  # "moving" -> "move"
    return stems


def build_token_stems(tokens: Set[str]) -> Set[str]:
    """Build a set of all stem variants for the given tokens."""
    token_stems: Set[str] = set()
    for tok in tokens:
        token_stems.update(simple_stems(tok))
    return token_stems


def retrieve_top_columns(
    prompt_embedding: np.ndarray,
    column_embeddings: list,
    column_index: List[Tuple[str, str]],
    k: int,
) -> List[Tuple[str, str, float]]:
    """
    Retrieve top-K columns by cosine similarity.

    Returns:
        List of (table, column, score) tuples.
    """
    if not column_embeddings:
        return []

    col_embeddings_array = np.array(column_embeddings)
    scores = col_embeddings_array @ prompt_embedding

    top_indices = np.argsort(scores)[-k:][::-1]

    results = []
    for idx in top_indices:
        if scores[idx] > 0:
            table_name, col_name = column_index[idx]
            results.append((table_name, col_name, float(scores[idx])))

    return results


def aggregate_to_tables(
    column_matches: List[Tuple[str, str, float]],
    canonical_score: Dict[str, float],
) -> Dict[str, float]:
    """
    Aggregate column match scores to table scores.

    Blends semantic similarity with canonical (structural) importance.
    """
    table_scores: Dict[str, float] = defaultdict(float)

    for table_name, _, score in column_matches:
        table_scores[table_name] += score

    for table in table_scores:
        semantic_score = table_scores[table]
        canonical = canonical_score.get(table, 0.5)
        table_scores[table] = semantic_score * 0.7 + canonical * 0.3

    return dict(table_scores)


def normalize_scores(scores: Dict[str, float]) -> Dict[str, float]:
    """
    Normalize table scores relative to the best score.

    Preserves multi-table matches where several tables are equally relevant.
    """
    if not scores:
        return {}

    max_score = max(scores.values())
    if max_score == 0:
        return {t: 0.0 for t in scores}

    return {t: float(s / max_score) for t, s in scores.items()}


def lexical_table_matches(
    tokens: Set[str],
    table_metadata: Dict[str, TableMetadata],
) -> List[str]:
    """Return tables whose normalized name contains any token."""
    matches = []
    for table_name, meta in table_metadata.items():
        if any(tok in meta.normalized_name for tok in tokens):
            matches.append(table_name)
    return matches


def apply_lexical_boost(
    table_scores: Dict[str, float],
    tokens: Set[str],
    table_metadata: Dict[str, TableMetadata],
    semantic_layer: Any,
    is_rich: bool,
    table_boost: float = 0.4,
    column_boost: float = 0.2,
) -> Dict[str, float]:
    """
    Boost table scores when prompt tokens appear in table/column identifiers.

    Also boosts from semantic layer entity synonyms and dimension descriptions.
    """
    token_stems = build_token_stems(tokens)

    for table_name, meta in table_metadata.items():
        table_lower = table_name.lower()

        # Direct table name match
        if any(
            stem in meta.normalized_name or stem in table_lower for stem in token_stems
        ):
            table_scores[table_name] = table_scores.get(table_name, 0.0) + table_boost

        # Column name matches (capped at 2 per table)
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
    if is_rich:
        for entity in semantic_layer.get("entities", []):
            synonyms = [s.lower() for s in entity.get("synonyms", [])]
            business_words = entity.get("business_name", "").lower().split()

            matched = any(
                stem == syn or stem in syn.split()
                for stem in token_stems
                for syn in synonyms
            )
            if not matched:
                matched = any(stem in business_words for stem in token_stems)

            if matched:
                for phys_table in entity.get("physical_tables", []):
                    if phys_table in table_metadata:
                        table_scores[phys_table] = (
                            table_scores.get(phys_table, 0.0) + table_boost
                        )

        # Boost from dimension synonyms
        for dim in semantic_layer.get("dimensions", []):
            dim_desc = dim.get("description", "").lower()
            source_table = dim.get("source_table")
            if source_table and source_table in table_metadata:
                if any(stem in dim_desc for stem in token_stems):
                    table_scores[source_table] = (
                        table_scores.get(source_table, 0.0) + table_boost * 0.5
                    )

    return table_scores


def apply_glossary_boost(
    table_scores: Dict[str, float],
    tokens: Set[str],
    semantic_layer: Any,
    is_rich: bool,
    table_metadata: Dict[str, TableMetadata],
    boost: float = 0.45,
) -> Dict[str, float]:
    """
    Boost table scores when prompt tokens match term_glossary keys.

    Maps colloquial business terms (e.g. "revenue") to physical tables.
    """
    if not is_rich:
        return table_scores

    glossary = semantic_layer.get("term_glossary")
    if not glossary:
        return table_scores

    token_stems = build_token_stems(tokens)

    for term, mapping in glossary.items():
        term_lower = term.lower()
        term_tokens = set(term_lower.split())

        # Single-word: stem match. Multi-word: all tokens must appear.
        if len(term_tokens) == 1:
            matched = any(
                stem == term_lower or term_lower in stem for stem in token_stems
            )
        else:
            matched = term_tokens.issubset(token_stems)

        if matched:
            table_name = mapping.get("table")
            if table_name and table_name in table_metadata:
                table_scores[table_name] = table_scores.get(table_name, 0.0) + boost
                logger.debug(f"Glossary boost: '{term}' -> {table_name} (+{boost})")

    return table_scores
