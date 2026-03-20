"""Standalone formatting and normalization helpers extracted from AIAgent."""

import json
import re
from typing import Dict, Any, List, Optional

from tabulate import tabulate

from core.config import ConfigManager
from core.logger import get_logger

logger = get_logger(__name__)

ANALYSIS_CONTEXT_RESULT_MAX_CHARS = 2000


def json_safe_value(v: Any) -> Any:
    """Convert DB / SQLAlchemy values into JSON-serializable primitives."""
    if v is None:
        return None
    if isinstance(v, (str, int, float, bool)):
        return v
    try:
        import decimal

        if isinstance(v, decimal.Decimal):
            return float(v)
    except Exception:
        pass
    try:
        import datetime

        if isinstance(v, (datetime.datetime, datetime.date, datetime.time)):
            return v.isoformat()
    except Exception:
        pass
    try:
        import uuid

        if isinstance(v, uuid.UUID):
            return str(v)
    except Exception:
        pass
    if isinstance(v, (bytes, bytearray, memoryview)):
        try:
            return bytes(v).decode("utf-8", "replace")
        except Exception:
            return str(v)
    try:
        import numpy as np

        if isinstance(v, np.generic):
            return v.item()
    except Exception:
        pass
    return str(v)


def normalize_sql_rows(rows: List[Any], columns: List[str]) -> List[Dict[str, Any]]:
    """Normalize SQLAlchemy rows / tuples / dicts into JSON-safe dict rows."""
    normalized: List[Dict[str, Any]] = []
    for r in rows:
        if hasattr(r, "_mapping"):
            r = dict(r._mapping)
        elif not isinstance(r, dict):
            r = dict(zip(columns, r))
        safe_row = {k: json_safe_value(v) for k, v in r.items()}
        normalized.append(safe_row)
    return normalized


def sanitize_dataframe_for_json(df):
    """Convert DataFrame types that are not JSON-serializable into native Python types."""
    import decimal
    import pandas as pd

    for col in df.columns:
        sample = df[col].dropna().head(1)
        if sample.empty:
            continue
        val = sample.iloc[0]
        if isinstance(val, decimal.Decimal):
            df[col] = df[col].apply(
                lambda v: float(v) if isinstance(v, decimal.Decimal) else v
            )
        elif isinstance(val, bytes):
            df[col] = df[col].apply(
                lambda v: (
                    v.decode("utf-8", errors="replace") if isinstance(v, bytes) else v
                )
            )
        elif hasattr(val, "item"):
            df[col] = df[col].apply(lambda v: v.item() if hasattr(v, "item") else v)
        if "date" in col.lower() or "time" in col.lower():
            try:
                df[col] = pd.to_datetime(df[col])
            except Exception:
                pass
    return df


def clean_sql_output(sql: str) -> str:
    """Remove markdown formatting from generated SQL output."""
    cleaned = sql.strip()
    cleaned = re.sub(r"^\s*corrected\s+sql\s*:\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"^\s*sql\s*:\s*", "", cleaned, flags=re.IGNORECASE)
    if "```" in cleaned:
        fence_match = re.search(
            r"```(?:sql)?\s*(.*?)\s*```", cleaned, flags=re.IGNORECASE | re.DOTALL
        )
        if fence_match:
            cleaned = fence_match.group(1).strip()
    if cleaned.startswith("```sql"):
        cleaned = cleaned[6:]
    if cleaned.startswith("```"):
        cleaned = cleaned[3:]
    if cleaned.endswith("```"):
        cleaned = cleaned[:-3]
    cleaned = cleaned.strip()
    if cleaned.endswith(";"):
        cleaned = cleaned[:-1]
    return cleaned.strip()


def clean_python_output(code: str) -> str:
    """Remove markdown formatting from generated Python output."""
    cleaned = code.strip()
    if cleaned.startswith("```python"):
        cleaned = cleaned[9:]
    elif cleaned.startswith("```"):
        cleaned = cleaned[3:]
    if cleaned.endswith("```"):
        cleaned = cleaned[:-3]
    return cleaned.strip()


def parse_json_response(
    response: str, response_type: str, fallback: Any
) -> Dict[str, Any]:
    """Parse JSON response from LLM with fallback."""
    try:
        if response.startswith("```"):
            response = response.split("```")[1]
            if response.startswith("json"):
                response = response[4:]
        return json.loads(response.strip())
    except Exception as e:
        logger.warning(
            f"Failed to parse {response_type} JSON response: {str(e)}. Using fallback."
        )
        return fallback


def format_query_result(result: Any) -> str:
    """Format code execution result into readable string."""
    if isinstance(result, dict):
        if "error" in result:
            return str(result["error"])
        if "columns" in result and "rows" in result:
            columns = result.get("columns") or []
            rows = result.get("rows") or []
            if not rows:
                return "No rows returned."
            if rows and isinstance(rows[0], dict):
                table_md = tabulate(rows, headers="keys", tablefmt="github")
            else:
                table_md = tabulate(rows, headers=columns, tablefmt="github")
            if result.get("truncated"):
                return f"{table_md}\n\n_Results truncated._"
            return table_md
        parts = [f"**{key}:** {value}" for key, value in result.items()]
        return "\n".join(parts)
    if isinstance(result, str):
        return result
    return str(result)


def compact_code_output_for_prompt(result: Any) -> str:
    """Compact query results for LLM context without flooding tokens."""

    def trim_text(text: str, max_chars: int) -> str:
        if len(text) <= max_chars:
            return text
        head = text[: max_chars - 40]
        return f"{head}\n... [truncated {len(text) - len(head)} chars]"

    if isinstance(result, dict):
        if "error" in result:
            return f"Error: {result['error']}"
        if "columns" in result and "rows" in result:
            columns = result.get("columns") or []
            rows = result.get("rows") or []
            if rows and isinstance(rows[0], dict):
                sample_rows = rows[:5]
            else:
                sample_rows = [dict(zip(columns, row)) for row in rows[:5]]
            summary = {
                "columns": columns,
                "rows_returned": len(rows),
                "truncated": bool(result.get("truncated")),
                "sample_rows": sample_rows,
            }
            return trim_text(
                json.dumps(summary, ensure_ascii=True),
                ANALYSIS_CONTEXT_RESULT_MAX_CHARS,
            )
        return trim_text(
            json.dumps(result, ensure_ascii=True), ANALYSIS_CONTEXT_RESULT_MAX_CHARS
        )
    if isinstance(result, str):
        return trim_text(result, ANALYSIS_CONTEXT_RESULT_MAX_CHARS)
    return trim_text(str(result), ANALYSIS_CONTEXT_RESULT_MAX_CHARS)


def format_cxo_table_preview(result: Optional[Dict[str, Any]]) -> Optional[str]:
    """Render a compact table preview for ranked/top-N style CxO outputs."""
    MAX_COLUMNS = 5
    MAX_ROWS = 15
    if not isinstance(result, dict) or result.get("error"):
        return None
    columns = (result.get("columns") or [])[:MAX_COLUMNS]
    rows = (result.get("rows") or [])[:MAX_ROWS]
    if not columns or not rows:
        return None
    try:
        headers = "keys" if isinstance(rows[0], dict) else columns
        return tabulate(rows, headers=headers, tablefmt="github")
    except (ValueError, TypeError):
        return None


def format_cxo_response(
    analysis: str,
    chart_path: Optional[str] = None,
    query_result: Optional[Dict[str, Any]] = None,
    generated_sql: Optional[str] = None,
) -> str:
    """Format a CxO-friendly response with optional chart and SQL."""
    parts = []
    if ConfigManager.get_show_sql_in_responses() and generated_sql:
        parts.append("### Generated SQL:")
        parts.append("")
        parts.append(f"```sql\n{clean_sql_output(generated_sql)}\n```")
        parts.append("")
    if chart_path:
        parts.append("")
        parts.append(f"![Chart]({chart_path})")
        parts.append("")
    preview = format_cxo_table_preview(query_result)
    if preview:
        parts.append("### Executive Snapshot")
        parts.append("")
        parts.append(preview)
        parts.append("")
    if analysis:
        parts.append(analysis)
    return "\n".join(parts) if parts else "No insights available for this query."


def format_response(
    code_result: Any,
    analysis: str,
    chart_path: Optional[str] = None,
    generated_sql: Optional[str] = None,
) -> str:
    """Format final response with code execution results, visualizations, and analysis."""
    response_parts = []
    if ConfigManager.get_show_sql_in_responses() and generated_sql:
        response_parts.append("### Generated SQL:")
        response_parts.append("")
        response_parts.append(f"```sql\n{clean_sql_output(generated_sql)}\n```")
    if chart_path:
        response_parts.append("")
        response_parts.append("### Visualization:")
        response_parts.append("")
        response_parts.append(f"![Generated Visualization]({chart_path})")
        response_parts.append("")
    elif code_result is not None:
        response_parts.append("")
        response_parts.append("### Result:")
        response_parts.append("")
        response_parts.append(format_query_result(code_result))
        response_parts.append("")
    response_parts.append("### Analysis:")
    response_parts.append(analysis)
    return "\n".join(response_parts)


def is_low_signal(result: Dict[str, Any]) -> bool:
    """Detect if query result has insufficient data for meaningful analysis."""
    if not result or "rows" not in result:
        return True
    if "error" in result:
        return True
    rows = result.get("rows", [])
    if not rows:
        return True
    return False


def has_table_output(result: Optional[Dict[str, Any]]) -> bool:
    """Check if tabular query output exists and is non-empty."""
    if not result or not isinstance(result, dict):
        return False
    rows = result.get("rows", [])
    return bool(rows)


def summarize_query_result(result: Dict[str, Any]) -> Dict[str, Any]:
    """Generate statistical summary of query results for LLM context."""
    if not result or "columns" not in result:
        return {"type": "none"}
    columns = result.get("columns", [])
    rows = result.get("rows", [])
    if not rows:
        return {"type": "empty", "columns": columns}
    summary = {
        "row_count": len(rows),
        "columns": columns,
        "truncated": bool(result.get("truncated")),
    }
    normalized_rows = normalize_sql_rows(rows, columns)
    col_profiles = {}
    for col in columns:
        values = [r.get(col) for r in normalized_rows]
        numeric_vals = [v for v in values if isinstance(v, (int, float))]
        unique_vals = set(str(v) for v in values if v is not None)
        col_profiles[col] = {
            "unique_count": len(unique_vals),
            "sample_values": list(unique_vals)[:5],
        }
        if numeric_vals:
            col_profiles[col].update(
                {
                    "min": min(numeric_vals),
                    "max": max(numeric_vals),
                    "mean": round(sum(numeric_vals) / len(numeric_vals), 2),
                }
            )
    summary["column_profiles"] = col_profiles
    if len(normalized_rows) <= 50:
        summary["sample_rows"] = normalized_rows[:5]
    return summary


def query_requests_visualization(user_query: str) -> bool:
    """Heuristic to force visualization when the user explicitly asks for it."""
    query = user_query.lower()
    keywords = [
        "graph",
        "chart",
        "plot",
        "visualize",
        "visualisation",
        "visualization",
        "trend",
        "over time",
        "distribution",
        "compare",
        "correlation",
        "relationship",
        "histogram",
        "scatter",
    ]
    return any(keyword in query for keyword in keywords)


def strip_alias_prefix(sql: str, alias: str) -> str:
    """Remove the alias__ prefix from all table references in sql."""
    return re.sub(
        rf"\b{re.escape(alias)}__(\w+)",
        r"\1",
        sql,
        flags=re.IGNORECASE,
    )
