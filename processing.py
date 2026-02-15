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
from constants import (
    MERGE_COMMON_COLS_THRESHOLD,
    MERGE_KEY_PATTERNS,
    MERGE_MAX_ESTIMATED_ROWS,
    MERGE_MAX_ROW_MULTIPLIER,
    MERGE_WARN_DUPLICATE_RATE,
)

logger = get_logger(__name__)

# Use constants from constants.py for merge strategy decisions
COMMON_KEY_THRESHOLD = MERGE_COMMON_COLS_THRESHOLD
COMMON_KEY_PATTERNS = MERGE_KEY_PATTERNS


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
            df = pd.read_sql(f"SELECT * FROM {table}", connector.connection)
            return df, f"Data loaded successfully from table: {table}"
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
        dataframes = []
        for table_name in table_names:
            logger.info(f"Loading table: {table_name}")
            df = pd.read_sql(f"SELECT * FROM {table_name}", connector.connection)
            dataframes.append(df)

        merged_df, merge_strategy = merge_dataframes(dataframes)
        strategy_msg = f". Merge strategy: {merge_strategy}" if merge_strategy else ""
        tables_str = ", ".join(table_names)
        return merged_df, f"Data loaded from tables: {tables_str}{strategy_msg}"

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
