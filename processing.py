"""
Data loading and processing utilities.

Handles data import from various sources (database, CSV, Excel)
and merging of multiple dataframes using intelligent strategies.
"""

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype, is_numeric_dtype
from connector import DatabaseConnector
from logger import get_logger
from typing import Any, Dict, List, Tuple, Optional
from security_validators import validate_sql_security
from constants import (
    MERGE_COMMON_COLS_THRESHOLD,
    MERGE_KEY_PATTERNS,
    MERGE_MAX_ESTIMATED_ROWS,
    MERGE_MAX_ROW_MULTIPLIER,
    MERGE_WARN_DUPLICATE_RATE,
    DB_MAX_ROWS_IN_MEMORY,
    DB_READ_CHUNK_SIZE,
    SQL_LARGE_TYPES,
)

logger = get_logger(__name__)

# Use constants from constants.py for merge strategy decisions
COMMON_KEY_THRESHOLD = MERGE_COMMON_COLS_THRESHOLD
COMMON_KEY_PATTERNS = MERGE_KEY_PATTERNS


def _quote_identifier(connector: DatabaseConnector, name: str) -> str:
    preparer = connector.engine.dialect.identifier_preparer
    parts = name.split(".")
    return ".".join(preparer.quote(part) for part in parts)


def _get_columns_with_types(
    connector: DatabaseConnector, table_name: str
) -> List[Dict[str, Any]]:
    from sqlalchemy import inspect

    inspector = inspect(connector.engine)
    return inspector.get_columns(table_name)


def _filter_large_columns(columns: List[Dict[str, Any]]) -> Tuple[List[str], List[str]]:
    safe_cols: List[str] = []
    skipped: List[str] = []

    for col in columns:
        col_name = col.get("name")
        col_type = str(col.get("type", "")).lower()
        if any(t in col_type for t in SQL_LARGE_TYPES):
            skipped.append(col_name)
            continue
        safe_cols.append(col_name)

    return safe_cols, skipped


def _estimate_table_rows(
    connector: DatabaseConnector, table_name: str
) -> Optional[int]:
    if not connector.connection:
        return None

    quoted_table = _quote_identifier(connector, table_name)
    query = f"SELECT COUNT(1) AS row_count FROM {quoted_table}"
    is_safe, error_msg = validate_sql_security(query)
    if not is_safe:
        logger.warning(error_msg)
        return None

    try:
        df = pd.read_sql_query(query, connector.connection)
        return int(df.iloc[0]["row_count"])
    except Exception as e:
        logger.warning(f"Row count estimate failed for {table_name}: {str(e)}")
        return None


def _read_sql_limited(
    query: str,
    connector: DatabaseConnector,
    max_rows: int,
    chunksize: int = DB_READ_CHUNK_SIZE,
) -> Tuple[pd.DataFrame, bool]:
    is_safe, error_msg = validate_sql_security(query)
    if not is_safe:
        raise RuntimeError(error_msg)

    rows: List[pd.DataFrame] = []
    total_rows = 0
    truncated = False

    for chunk in pd.read_sql_query(query, connector.connection, chunksize=chunksize):
        rows.append(chunk)
        total_rows += len(chunk)
        if total_rows >= max_rows:
            truncated = True
            break

    if not rows:
        return pd.DataFrame(), False

    df = pd.concat(rows, ignore_index=True)
    if len(df) > max_rows:
        df = df.iloc[:max_rows]
        truncated = True

    return df, truncated


def _load_table_via_sql(
    connector: DatabaseConnector, table_name: str
) -> Tuple[Optional[pd.DataFrame], str, List[str]]:
    if not connector.connection:
        return None, "No active database connection.", []

    columns = _get_columns_with_types(connector, table_name)
    safe_cols, skipped_cols = _filter_large_columns(columns)

    if not safe_cols:
        return None, "No safe columns available to load.", skipped_cols

    quoted_table = _quote_identifier(connector, table_name)
    quoted_cols = ", ".join(
        f"{quoted_table}.{_quote_identifier(connector, col)}" for col in safe_cols
    )
    query = f"SELECT {quoted_cols} FROM {quoted_table}"

    row_count = _estimate_table_rows(connector, table_name)
    if row_count is not None and row_count <= DB_MAX_ROWS_IN_MEMORY:
        is_safe, error_msg = validate_sql_security(query)
        if not is_safe:
            return None, error_msg, skipped_cols
        df = pd.read_sql_query(query, connector.connection)
        return df, "", skipped_cols

    df, truncated = _read_sql_limited(query, connector, DB_MAX_ROWS_IN_MEMORY)
    msg = "Loaded using chunked reads"
    if truncated:
        msg = f"{msg}; results truncated to {DB_MAX_ROWS_IN_MEMORY} rows"
    return df, msg, skipped_cols


def _find_join_condition(
    inspector,
    left_table: str,
    right_table: str,
) -> Optional[Tuple[List[str], List[str], str, str]]:
    for fk in inspector.get_foreign_keys(left_table):
        if fk.get("referred_table") == right_table:
            return (
                fk.get("constrained_columns", []),
                fk.get("referred_columns", []),
                left_table,
                right_table,
            )

    for fk in inspector.get_foreign_keys(right_table):
        if fk.get("referred_table") == left_table:
            return (
                fk.get("referred_columns", []),
                fk.get("constrained_columns", []),
                left_table,
                right_table,
            )

    return None


def _build_join_query(
    connector: DatabaseConnector, table_names: List[str]
) -> Tuple[Optional[str], Dict[str, List[str]], List[str]]:
    from sqlalchemy import inspect

    inspector = inspect(connector.engine)
    safe_cols_by_table: Dict[str, List[str]] = {}
    skipped_cols: List[str] = []

    for table_name in table_names:
        columns = _get_columns_with_types(connector, table_name)
        safe_cols, skipped = _filter_large_columns(columns)
        safe_cols_by_table[table_name] = safe_cols
        skipped_cols.extend([f"{table_name}.{col}" for col in skipped])

    base_table = table_names[0]
    joined = {base_table}
    join_clauses: List[str] = []

    for table_name in table_names[1:]:
        join_found = None
        for candidate in list(joined):
            join_found = _find_join_condition(inspector, candidate, table_name)
            if join_found:
                break

        if not join_found:
            return None, safe_cols_by_table, skipped_cols

        left_cols, right_cols, left_table, right_table = join_found
        if not left_cols or not right_cols:
            return None, safe_cols_by_table, skipped_cols

        left_quoted = _quote_identifier(connector, left_table)
        right_quoted = _quote_identifier(connector, right_table)
        join_conditions = []
        for left_col, right_col in zip(left_cols, right_cols):
            join_conditions.append(
                f"{left_quoted}.{_quote_identifier(connector, left_col)} = "
                f"{right_quoted}.{_quote_identifier(connector, right_col)}"
            )
        join_clause = f"LEFT JOIN {right_quoted} ON " + " AND ".join(join_conditions)
        join_clauses.append(join_clause)
        joined.add(table_name)

    select_parts: List[str] = []
    for table_name, columns in safe_cols_by_table.items():
        table_quoted = _quote_identifier(connector, table_name)
        for col in columns:
            col_quoted = _quote_identifier(connector, col)
            alias = _quote_identifier(connector, f"{table_name}__{col}")
            select_parts.append(f"{table_quoted}.{col_quoted} AS {alias}")

    if not select_parts:
        return None, safe_cols_by_table, skipped_cols

    base_quoted = _quote_identifier(connector, base_table)
    query = f"SELECT {', '.join(select_parts)} FROM {base_quoted} "
    if join_clauses:
        query += " ".join(join_clauses)

    return query, safe_cols_by_table, skipped_cols


def _identify_key_columns(common_cols: set, all_cols_union: set) -> Optional[List[str]]:
    """
    Identify potential key columns for merging.

    Args:
        common_cols: Set of columns present in all dataframes
        all_cols_union: Union of all columns across dataframes

    Returns:
        List of key column candidates, or None if no suitable keys found
    """
    # Look for columns with key-like names
    key_candidates = [
        col
        for col in common_cols
        if col.lower() in COMMON_KEY_PATTERNS
        or col.lower().endswith("_id")
        or col.lower().endswith("_key")
    ]

    return key_candidates if key_candidates else None


def _merge_on_keys(
    dataframes: List[pd.DataFrame], key_columns: List[str]
) -> Tuple[pd.DataFrame, str]:
    """
    Attempt to merge dataframes on specified key columns.

    Falls back to concatenation if merge fails.

    Args:
        dataframes: List of dataframes to merge
        key_columns: Columns to merge on

    Returns:
        Tuple of (merged_dataframe, strategy_description)
    """
    try:
        dataframes = _normalize_key_dtypes(dataframes, key_columns)
        merged_df = dataframes[0]
        for idx, df in enumerate(dataframes[1:], 1):
            safe_to_merge, reason = _preflight_merge(merged_df, df, key_columns)
            if not safe_to_merge:
                logger.warning(
                    f"Preflight merge check failed on {key_columns}: {reason}. Using concatenation."
                )
                merged_df = pd.concat(dataframes, ignore_index=True, sort=False)
                return (
                    merged_df,
                    f"Merge skipped ({reason}), using vertical concatenation",
                )
            merged_df = pd.merge(
                merged_df,
                df,
                on=key_columns,
                how="outer",
                suffixes=("", f"_table{idx+1}"),
            )

        keys_str = ", ".join(key_columns)
        return merged_df, f"Horizontal merge on key(s): {keys_str}"
    except Exception as e:
        logger.warning(
            f"Merge on {key_columns} failed, falling back to concatenation: {str(e)}"
        )
        merged_df = pd.concat(dataframes, ignore_index=True, sort=False)
        return merged_df, "Merge failed, using vertical concatenation"


def _normalize_key_dtypes(
    dataframes: List[pd.DataFrame], key_columns: List[str]
) -> List[pd.DataFrame]:
    """
    Normalize key column dtypes across dataframes to improve merge behavior.
    """
    if not dataframes:
        return dataframes

    normalized = []
    for df in dataframes:
        df_copy = df.copy()
        for key in key_columns:
            if key not in df_copy.columns:
                continue
            series_list = [d[key] for d in dataframes if key in d.columns]
            if all(is_numeric_dtype(s) for s in series_list):
                df_copy[key] = pd.to_numeric(df_copy[key], errors="coerce")
            elif all(is_datetime64_any_dtype(s) for s in series_list):
                df_copy[key] = pd.to_datetime(df_copy[key], errors="coerce")
            else:
                df_copy[key] = df_copy[key].astype("string")
        normalized.append(df_copy)

    return normalized


def _estimate_outer_join_rows(
    left: pd.DataFrame, right: pd.DataFrame, key_columns: List[str]
) -> Optional[int]:
    """
    Estimate outer-join row count based on key frequency.
    """
    if not key_columns:
        return None
    try:
        left_counts = (
            left.groupby(key_columns, dropna=False).size().rename("left_count")
        )
        right_counts = (
            right.groupby(key_columns, dropna=False).size().rename("right_count")
        )

        merged_counts = (
            left_counts.to_frame()
            .merge(
                right_counts.to_frame(),
                left_index=True,
                right_index=True,
                how="outer",
            )
            .fillna(0)
        )

        left_count = merged_counts["left_count"].astype("int64")
        right_count = merged_counts["right_count"].astype("int64")
        both_mask = (left_count > 0) & (right_count > 0)

        matched = (left_count[both_mask] * right_count[both_mask]).sum()
        left_only = left_count[~both_mask].sum()
        right_only = right_count[~both_mask].sum()
        return int(matched + left_only + right_only)
    except Exception as e:
        logger.warning(f"Join size estimate failed: {str(e)}")
        return None


def _preflight_merge(
    left: pd.DataFrame, right: pd.DataFrame, key_columns: List[str]
) -> Tuple[bool, str]:
    """
    Validate merge safety with duplicate-rate and join-size checks.
    """
    left_rows = len(left)
    right_rows = len(right)
    if left_rows == 0 or right_rows == 0:
        return True, "one side empty"

    left_dup_rate = left.duplicated(subset=key_columns).mean()
    right_dup_rate = right.duplicated(subset=key_columns).mean()

    if (
        left_dup_rate > MERGE_WARN_DUPLICATE_RATE
        or right_dup_rate > MERGE_WARN_DUPLICATE_RATE
    ):
        logger.warning(
            "High duplicate rate on merge keys. "
            f"Left: {left_dup_rate:.1%}, Right: {right_dup_rate:.1%}"
        )

    estimated_rows = _estimate_outer_join_rows(left, right, key_columns)
    if estimated_rows is None:
        return True, "no estimate"

    max_side = max(left_rows, right_rows)
    if (
        estimated_rows > MERGE_MAX_ESTIMATED_ROWS
        and estimated_rows > max_side * MERGE_MAX_ROW_MULTIPLIER
    ):
        return (
            False,
            f"estimated {estimated_rows} rows exceeds thresholds",
        )

    return True, "ok"


def _standardize_dtypes(dataframes: List[pd.DataFrame]) -> List[pd.DataFrame]:
    """
    Standardize data types across dataframes before merging.
    Handles both object types and numeric type conflicts.
    """
    if not dataframes:
        return dataframes

    standardized = []

    # First pass: identify target types for each column
    column_types = {}
    for df in dataframes:
        for col in df.columns:
            if col not in column_types:
                column_types[col] = df[col].dtype
            else:
                # If types differ, prefer the more general type
                existing_type = column_types[col]
                current_type = df[col].dtype
                if existing_type != current_type:
                    # float64 ≻ int64 ≻ object
                    if existing_type == "object" or current_type == "object":
                        column_types[col] = "object"
                    elif "float" in str(current_type) or "float" in str(existing_type):
                        column_types[col] = "float64"

    # Second pass: convert all dataframes to use standardized types
    for df in dataframes:
        df_copy = df.copy()
        for col in df_copy.columns:
            target_type = column_types.get(col)
            if target_type and df_copy[col].dtype != target_type:
                try:
                    if target_type == "object":
                        df_copy[col] = df_copy[col].astype("object")
                    elif "float" in str(target_type):
                        df_copy[col] = pd.to_numeric(df_copy[col], errors="coerce")
                    else:
                        df_copy[col] = df_copy[col].astype(target_type)
                except Exception as e:
                    logger.warning(
                        f"Could not convert {col} to {target_type}: {str(e)}"
                    )
        standardized.append(df_copy)

    return standardized


def merge_dataframes(dataframes: List[pd.DataFrame]) -> Tuple[pd.DataFrame, str]:
    """
    Intelligently merge multiple dataframes using optimal strategy.

    For tables with no universal common columns, performs sequential merges
    on pairwise shared key columns.

    Args:
        dataframes: List of pandas DataFrames to merge
    Returns:
        Tuple of (merged_dataframe, strategy_description)

    Strategies:
    1. Identical schemas: vertical concatenation
    2. High overlap of columns: vertical concatenation
    3. All tables share common key columns: horizontal merge on those keys
    4. No universal common columns: sequential/chain merges on pairwise keys
    5. No common columns at all: horizontal concatenation
    """
    if not dataframes:
        return pd.DataFrame(), "No dataframes provided"

    dataframes = [df for df in dataframes if not df.empty]
    dataframes = _standardize_dtypes(dataframes)

    if not dataframes:
        return pd.DataFrame(), "All dataframes were empty"

    if len(dataframes) == 1:
        return dataframes[0], ""

    all_column_sets = [set(df.columns) for df in dataframes]
    common_cols = set.intersection(*all_column_sets)
    all_cols_union = set.union(*all_column_sets)

    identical_schemas = all(cols == all_column_sets[0] for cols in all_column_sets)

    # Strategy 1: Identical schemas: simple vertical concatenation
    if identical_schemas:
        merged_df = pd.concat(dataframes, ignore_index=True, sort=False)
        return merged_df, "Vertical concatenation (identical columns)"

    # Strategy 2: High overlap (>70%) - vertical concatenation
    overlap_ratio = len(common_cols) / len(all_cols_union) if all_cols_union else 0
    if common_cols and overlap_ratio > COMMON_KEY_THRESHOLD:
        merged_df = pd.concat(dataframes, ignore_index=True, sort=False)
        strategy = f"Vertical concatenation ({len(common_cols)} common columns, {overlap_ratio:.1%} overlap)"
        return merged_df, strategy

    # Strategy 3: All tables share common key columns: horizontal merge
    if common_cols:
        key_candidates = _identify_key_columns(common_cols, all_cols_union)

        # Also include common ID columns that weren't caught
        for col in common_cols:
            if (
                "_id" in col.lower() or col.lower() in ["id"]
            ) and col not in key_candidates:
                key_candidates.append(col)

        if key_candidates:
            logger.info(
                f"Attempting horizontal merge on universal keys: {key_candidates}"
            )
            return _merge_on_keys(dataframes, key_candidates)

    # Strategy 4: No universal common columns: try sequential/chain merges on pairwise keys
    logger.info(
        "No universal common columns detected. Attempting sequential merge strategy."
    )
    merged_df = _sequential_merge(dataframes)
    if merged_df is not None:
        return merged_df, "Sequential merge on pairwise key columns"

    # Strategy 5: No common columns at all: horizontal concatenation
    merged_df = pd.concat(dataframes, axis=1, ignore_index=False)
    return merged_df, "Horizontal concatenation (no common columns)"


def _sequential_merge(dataframes: List[pd.DataFrame]) -> Optional[pd.DataFrame]:
    """
    Merge multiple dataframes sequentially when they don't share universal keys.

    Example: If table1-table2 share key_a, and table2-table3 share key_b,
    merge (table1 on key_a with table2) then (result on key_b with table3).

    Args:
        dataframes: List of dataframes to merge

    Returns:
        Merged dataframe or None if sequential merge fails
    """
    if not dataframes or len(dataframes) < 2:
        return None

    try:
        merged_df = dataframes[0]
        merge_log = []

        for idx, right_df in enumerate(dataframes[1:], 1):
            # Find shared key columns between current merged_df and next table
            left_cols = set(merged_df.columns)
            right_cols = set(right_df.columns)

            # Find potential key columns in both
            shared_cols = left_cols & right_cols
            key_candidates = [
                col
                for col in shared_cols
                if "_id" in col.lower() or col.lower() in ["id"]
            ]

            if not key_candidates:
                # No ID columns shared, look for any common columns
                key_candidates = list(shared_cols)

            if key_candidates:
                logger.info(f"Merging table {idx} on keys: {key_candidates}")
                merged_df = pd.merge(
                    merged_df,
                    right_df,
                    on=key_candidates,
                    how="outer",
                    suffixes=("", f"_table{idx+1}"),
                )
                merge_log.append(f"Table {idx} merged on {', '.join(key_candidates)}")
            else:
                # No common columns with this table, skip merge
                logger.warning(
                    f"No common key columns found to merge table {idx}, using concatenation"
                )
                merged_df = pd.concat([merged_df, right_df], axis=1, ignore_index=False)
                merge_log.append(f"Table {idx} concatenated (no common keys)")

        logger.info(f"Sequential merge completed: {'; '.join(merge_log)}")
        return merged_df

    except Exception as e:
        logger.error(f"Sequential merge failed: {str(e)}")
        return None


def load_data(
    source_type: str, source_config: Dict[str, Any]
) -> Tuple[Optional[pd.DataFrame], str]:
    """
    Load data from a database, CSV, or Excel file into a pandas DataFrame.

    Args:
        source_type: Type of source ('database', 'csv', or 'excel')
        source_config: Configuration dictionary containing:
            - For database: {
                'db_type': str,
                'credentials': Dict,
                'table': str | List[str],
                ...
              }
            - For CSV: {'file_path': str}
            - For Excel: {'file_path': str, 'sheet_name': str (optional)}

    Returns:
        Tuple of (DataFrame or None, status_message: str)

    Raises:
        Returns (None, error_message) on failure instead of raising
    """
    try:
        source_type = source_type.lower()
        logger.info(f"Loading data from {source_type} source")

        if source_type == "database":
            return _load_from_database(source_config)
        elif source_type == "csv":
            return _load_from_csv(source_config)
        elif source_type == "excel":
            return _load_from_excel(source_config)
        else:
            msg = f"Unsupported source type: {source_type}"
            logger.error(msg)
            return None, msg

    except Exception as e:
        msg = f"Error loading data: {str(e)}"
        logger.error(msg, exc_info=True)
        return None, msg


def _load_from_database(config: Dict[str, Any]) -> Tuple[Optional[pd.DataFrame], str]:
    """
    Load data from a database connection.

    Args:
        config: Configuration with db_type, credentials, and table(s)

    Returns:
        Tuple of (DataFrame or None, message: str)
    """
    table = config.get("table")
    if not table:
        return None, "Error: 'table' field is required for database source"

    db_type = config.get("db_type")
    credentials = config.get("credentials", {})

    logger.info(f"Connecting to {db_type} database...")
    connector = DatabaseConnector()
    success, message = connector.connect(db_type, credentials)

    if not success:
        return None, message

    try:
        if isinstance(table, list):
            logger.info(f"Loading {len(table)} tables from database")
            return _load_multiple_tables(connector, table)
        else:
            logger.info(f"Loading table: {table}")
            df, load_msg, skipped_cols = _load_table_via_sql(connector, table)
            if df is None:
                return None, load_msg
            skip_msg = (
                f" Skipped large columns: {', '.join(skipped_cols)}"
                if skipped_cols
                else ""
            )
            msg = load_msg or "Data loaded successfully"
            return df, f"{msg} from table: {table}.{skip_msg}"
    finally:
        connector.close()


def _load_multiple_tables(
    connector: DatabaseConnector, table_names: List[str]
) -> Tuple[Optional[pd.DataFrame], str]:
    """
    Load and merge multiple tables from a database.

    Args:
        connector: Active database connector
        table_names: List of table names to load

    Returns:
        Tuple of (merged_dataframe or None, message: str)
    """
    try:
        query, _, skipped_cols = _build_join_query(connector, table_names)
        if not query:
            return (
                None,
                "Unable to build safe SQL joins across selected tables. "
                "Please choose tables with foreign-key relationships.",
            )

        df, truncated = _read_sql_limited(query, connector, DB_MAX_ROWS_IN_MEMORY)
        tables_str = ", ".join(table_names)
        msg = f"Data loaded from tables: {tables_str} using SQL joins"
        if truncated:
            msg = f"{msg}. Results truncated to {DB_MAX_ROWS_IN_MEMORY} rows"
        if skipped_cols:
            msg = f"{msg}. Skipped large columns: {', '.join(skipped_cols)}"
        return df, msg

    except Exception as e:
        logger.error(f"Error loading multiple tables: {str(e)}")
        return None, f"Error loading tables: {str(e)}"


def _load_from_csv(config: Dict[str, Any]) -> Tuple[Optional[pd.DataFrame], str]:
    """
    Load data from a CSV file.

    Args:
        config: Configuration with file_path

    Returns:
        Tuple of (DataFrame or None, message: str)
    """
    file_path = config.get("file_path")
    if not file_path:
        return None, "Error: 'file_path' is required for CSV source"

    try:
        logger.info(f"Loading CSV: {file_path}")
        df = pd.read_csv(file_path)
        return df, f"CSV file loaded successfully: {file_path}"
    except Exception as e:
        logger.error(f"Error loading CSV: {str(e)}")
        return None, f"Error loading CSV: {str(e)}"


def _load_from_excel(config: Dict[str, Any]) -> Tuple[Optional[pd.DataFrame], str]:
    """
    Load data from an Excel file.

    Args:
        config: Configuration with file_path and optional sheet_name

    Returns:
        Tuple of (DataFrame or None, message: str)
    """
    file_path = config.get("file_path")
    if not file_path:
        return None, "Error: 'file_path' is required for Excel source"

    sheet_name = config.get("sheet_name", 0)

    try:
        logger.info(f"Loading Excel: {file_path}, sheet: {sheet_name}")
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        return df, f"Excel file loaded successfully: {file_path}"
    except Exception as e:
        logger.error(f"Error loading Excel: {str(e)}")
        return None, f"Error loading Excel: {str(e)}"
