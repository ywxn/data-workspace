"""Table filtering: peripheral detection, canonical variant preference, secondary filtering.

Contains heuristics for identifying noise tables (backups, logs, temps)
and preferring canonical table variants over copies.
"""

import re
import logging
from collections import defaultdict
from typing import Any, Dict, List, Set

from db.nlp.data_models import TableMetadata

logger = logging.getLogger(__name__)

# Name substrings that indicate a peripheral/non-core table
_PERIPHERAL_PATTERNS = [
    "bkp_", "_bkp", "_backup", "backup_",
    "_log", "log_", "_audit", "audit_",
    "temp_", "_temp", "_tmp", "tmp_",
    "_old", "old_", "_archive", "archive_",
    "_copy", "copy_", "_test", "test_",
]

# Prefixes/suffixes stripped when computing a table's base name
_VARIANT_PREFIXES = [
    "bkp_", "temp_", "tmp_", "old_", "backup_", "archive_", "copy_", "test_",
]
_VARIANT_SUFFIXES = [
    "_bkp", "_backup", "_temp", "_tmp", "_old", "_archive", "_copy", "_test",
]


def is_dimension_table(
    table: str,
    semantic_layer: Any,
    is_rich: bool,
) -> bool:
    """
    Check if a table is a master/dimension table.

    Uses semantic layer entity_type and dimension definitions,
    with fallback to naming convention (mst_ prefix).
    """
    if is_rich:
        for entity in semantic_layer.get("entities", []):
            if entity.get("entity_type") in ("master", "dimension", "lookup"):
                if table in entity.get("physical_tables", []):
                    return True

        for dim in semantic_layer.get("dimensions", []):
            if dim.get("source_table") == table:
                return True

    return table.lower().startswith("mst_")


def is_structurally_peripheral(
    table: str,
    semantic_layer: Any,
    is_rich: bool,
    canonical_score: Dict[str, float],
    fk_graph: Dict[str, Set[str]],
    structural_links: Dict[str, Set[str]],
) -> bool:
    """
    Identify structurally peripheral tables (backups, logs, temp, variants).

    Entity-defined tables are never peripheral.
    """
    # Tables in semantic layer entities are always core
    if is_rich:
        for entity in semantic_layer.get("entities", []):
            if table in entity.get("physical_tables", []):
                return False

    table_lower = table.lower()

    # Name-based detection
    for pattern in _PERIPHERAL_PATTERNS:
        if pattern in table_lower:
            return True

    # Date-stamped variants (e.g. bkp300725_mst_item)
    if re.search(r"bkp\d{6,8}", table_lower):
        return True

    # Structural detection
    canonical = canonical_score.get(table, 0.5)
    fk_degree = len(fk_graph.get(table, []))
    structural_degree = len(structural_links.get(table, []))

    return canonical < 0.35 and fk_degree <= 1 and structural_degree == 0


def filter_secondary_tables(
    tables: List[str],
    confidences: Dict[str, float],
    threshold: float = 0.4,
) -> List[str]:
    """
    Filter out secondary tables (plan, release, return variants)
    unless they have high confidence.
    """
    secondary_patterns = ["_plan", "_release", "_return", "_queue", "_staging"]

    filtered = []
    for table in tables:
        table_lower = table.lower()
        is_secondary = any(p in table_lower for p in secondary_patterns)

        if is_secondary:
            if confidences.get(table, 0.0) >= threshold:
                filtered.append(table)
        else:
            filtered.append(table)

    return filtered


def prefer_canonical_variants(
    tables: List[str],
    confidences: Dict[str, float],
    canonical_weight_fn,
    is_peripheral_fn,
) -> List[str]:
    """
    When multiple table variants exist (e.g. mst_item, bkp_mst_item),
    keep only the canonical version (highest canonical score / confidence).

    Args:
        tables: Table names to deduplicate.
        confidences: Confidence scores.
        canonical_weight_fn: Callable(table) -> float for canonical weight.
        is_peripheral_fn: Callable(table) -> bool for peripheral detection.
    """
    if not tables:
        return tables

    def get_base_name(table: str) -> str:
        name = table.lower()
        for prefix in _VARIANT_PREFIXES:
            if name.startswith(prefix):
                name = name[len(prefix):]
        name = re.sub(r"^bkp\d{6,8}_", "", name)
        for suffix in _VARIANT_SUFFIXES:
            if name.endswith(suffix):
                name = name[: -len(suffix)]
        return name

    variant_groups = defaultdict(list)
    for table in tables:
        variant_groups[get_base_name(table)].append(table)

    selected = []
    for variants in variant_groups.values():
        if len(variants) == 1:
            selected.append(variants[0])
        else:
            best = max(
                variants,
                key=lambda t: (
                    not is_peripheral_fn(t),
                    canonical_weight_fn(t),
                    confidences.get(t, 0.0),
                ),
            )
            selected.append(best)

    return selected


def structural_centrality(
    table: str,
    fk_graph: Dict[str, Set[str]],
    structural_links: Dict[str, Set[str]],
    canonical_score: Dict[str, float],
) -> float:
    """
    Estimate structural importance of a table (0-1).

    Blends canonical score, FK degree, and structural link count.
    """
    fk_degree = len(fk_graph.get(table, []))
    structural_degree = len(structural_links.get(table, []))
    canonical = canonical_score.get(table, 0.5)

    return (
        0.5 * canonical
        + 0.3 * (fk_degree / (1 + fk_degree))
        + 0.2 * (structural_degree / (1 + structural_degree))
    )


def expand_entity_pairs(
    tables: List[str],
    semantic_layer: Any,
    is_rich: bool,
    table_metadata: Dict[str, TableMetadata],
) -> Set[str]:
    """
    Expand tables using semantic layer entity groupings.

    When a physical table from an entity is selected, include all
    physical tables in that entity.
    """
    if not is_rich:
        return set()

    expanded = set()
    for entity in semantic_layer.get("entities", []):
        phys_tables = entity.get("physical_tables", [])
        if any(t in tables for t in phys_tables):
            for pt in phys_tables:
                if pt in table_metadata:
                    expanded.add(pt)

    return expanded


def expand_header_detail_pairs(
    tables: List[str],
    all_table_names: Set[str],
) -> Set[str]:
    """
    Ensure header-detail table pairs are both included.

    When a _dtl table is selected, include its header, and vice versa.
    """
    expanded = set()

    for table in tables:
        table_lower = table.lower()

        if "_dtl" in table_lower:
            base = table_lower.replace("_dtl", "")
            for candidate in all_table_names:
                cand_lower = candidate.lower()
                if (
                    cand_lower == base
                    or cand_lower == base.replace("_dtl", "") + "_hdr"
                ):
                    expanded.add(candidate)

        if "_hdr" in table_lower:
            base = table_lower.replace("_hdr", "")
            for candidate in all_table_names:
                if candidate.lower() == base + "_dtl":
                    expanded.add(candidate)

    return expanded
