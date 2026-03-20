"""Schema metadata utilities for building prompt context."""

import json
from typing import Dict, Any, List, Optional

from core.constants import SAMPLE_ROWS_INFO


def build_schema_metadata(context: Dict[str, Any]) -> Dict[str, Any]:
    """Build schema metadata for prompt context."""
    tables = context.get("tables", [])
    table_info = context.get("table_info", {})

    columns_by_table: Dict[str, List[str]] = {}
    column_types: Dict[str, str] = {}
    row_counts: Dict[str, int] = {}
    samples: Dict[str, List[Dict[str, Any]]] = {}

    for table in tables:
        info = table_info.get(table, {})
        columns = info.get("columns", [])
        columns_by_table[table] = columns
        row_counts[table] = int(info.get("row_count", 0) or 0)
        sample_rows = info.get("sample_rows", [])
        samples[table] = sample_rows[:SAMPLE_ROWS_INFO]
        types = info.get("column_types", {})
        for col in columns:
            qualified = f"{table}.{col}"
            if col in types:
                column_types[qualified] = str(types[col])

    qualified_columns = [
        f"{t}.{c}" for t, cols in columns_by_table.items() for c in cols
    ]

    return {
        "tables": tables,
        "columns": qualified_columns,
        "columns_by_table": columns_by_table,
        "column_types": column_types,
        "row_counts": row_counts,
        "sample_rows": samples,
    }


def build_semantic_layer_prompt_context(
    semantic_layer: Optional[Dict[str, Any]] = None,
) -> str:
    """Serialize the most useful semantic-layer hints for prompt grounding."""
    if not semantic_layer:
        return ""

    entities = []
    for entity in semantic_layer.get("entities", [])[:20]:
        entities.append(
            {
                "name": entity.get("name"),
                "business_name": entity.get("business_name"),
                "table": entity.get("table") or entity.get("table_name"),
                "synonyms": entity.get("synonyms", [])[:8],
                "key_columns": entity.get("key_columns", [])[:8],
            }
        )

    measures = []
    for measure in semantic_layer.get("measures", [])[:20]:
        measures.append(
            {
                "name": measure.get("name"),
                "business_name": measure.get("business_name"),
                "table": measure.get("table") or measure.get("table_name"),
                "column": measure.get("column") or measure.get("column_name"),
                "synonyms": measure.get("synonyms", [])[:8],
            }
        )

    dimensions = []
    for dimension in semantic_layer.get("dimensions", [])[:20]:
        dimensions.append(
            {
                "name": dimension.get("name"),
                "business_name": dimension.get("business_name"),
                "table": dimension.get("table") or dimension.get("table_name"),
                "column": dimension.get("column") or dimension.get("column_name"),
                "synonyms": dimension.get("synonyms", [])[:8],
            }
        )

    relationships = []
    for relationship in semantic_layer.get("relationships", [])[:20]:
        relationships.append(
            {
                "from": relationship.get("from") or relationship.get("source"),
                "to": relationship.get("to") or relationship.get("target"),
                "type": relationship.get("type"),
                "join": relationship.get("join") or relationship.get("join_condition"),
            }
        )

    glossary = dict(list((semantic_layer.get("term_glossary") or {}).items())[:40])

    semantic_summary = {
        "entities": entities,
        "measures": measures,
        "dimensions": dimensions,
        "relationships": relationships,
        "term_glossary": glossary,
    }

    return (
        "\n\nSEMANTIC LAYER\n"
        "Use this business mapping to resolve ambiguous business terms to the correct schema objects.\n"
        "Prefer these mappings over name guessing when selecting tables, columns, joins, and display fields.\n"
        f"{json.dumps(semantic_summary, ensure_ascii=True)}"
    )
