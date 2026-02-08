"""
Data loading and processing utilities.

Handles data import from various sources (database, CSV, Excel)
and merging of multiple dataframes using intelligent strategies.
"""

import pandas as pd
from connector import DatabaseConnector
from logger import get_logger
from typing import Any, Dict, List, Tuple, Optional
from constants import MERGE_COMMON_COLS_THRESHOLD, MERGE_KEY_PATTERNS

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
        merged_df = dataframes[0]
        for idx, df in enumerate(dataframes[1:], 1):
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


def merge_dataframes(dataframes: List[pd.DataFrame]) -> Tuple[pd.DataFrame, str]:
    """
    Intelligently merge multiple dataframes using optimal strategy.

    Strategy selection (in order of preference):
    1. Vertical concatenation if schemas are identical
    2. Vertical concatenation if column overlap is >70%
    3. Horizontal merge on key columns if detected
    4. Horizontal concatenation if no common columns
    5. Vertical concatenation with mixed columns as fallback

    Args:
        dataframes: List of DataFrames to merge

    Returns:
        Tuple of (merged_dataframe, strategy_description)
    """
    if not dataframes:
        return pd.DataFrame(), "No dataframes provided"

    # Filter out empty dataframes
    dataframes = [df for df in dataframes if not df.empty]

    if not dataframes:
        return pd.DataFrame(), "All dataframes were empty"

    if len(dataframes) == 1:
        return dataframes[0], ""

    # Analyze column structure
    all_column_sets = [set(df.columns) for df in dataframes]
    common_cols = set.intersection(*all_column_sets)
    all_cols_union = set.union(*all_column_sets)

    identical_schemas = all(cols == all_column_sets[0] for cols in all_column_sets)

    # Strategy 1: Identical schemas - simple vertical concatenation
    if identical_schemas:
        merged_df = pd.concat(dataframes, ignore_index=True, sort=False)
        return merged_df, "Vertical concatenation (identical columns)"

    # Strategy 2: High overlap (>70%) - vertical concatenation
    overlap_ratio = len(common_cols) / len(all_cols_union) if all_cols_union else 0
    if common_cols and overlap_ratio > COMMON_KEY_THRESHOLD:
        merged_df = pd.concat(dataframes, ignore_index=True, sort=False)
        strategy = f"Vertical concatenation ({len(common_cols)} common columns, {overlap_ratio:.1%} overlap)"
        return merged_df, strategy

    # Strategy 3: Has common columns and potential key columns - try horizontal merge
    if common_cols:
        key_candidates = _identify_key_columns(common_cols, all_cols_union)
        if key_candidates:
            logger.info(f"Attempting horizontal merge on keys: {key_candidates}")
            return _merge_on_keys(dataframes, key_candidates)

    # Strategy 4: No common columns - horizontal concatenation
    if not common_cols:
        merged_df = pd.concat(dataframes, axis=1, ignore_index=False)
        return merged_df, "Horizontal concatenation (no common columns)"

    # Strategy 5: Mixed columns - fallback to vertical concat
    merged_df = pd.concat(dataframes, ignore_index=True, sort=False)
    cols_description = f"{len(common_cols)} common, {len(all_cols_union)} total"
    return merged_df, f"Vertical concatenation with mixed columns ({cols_description})"


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
