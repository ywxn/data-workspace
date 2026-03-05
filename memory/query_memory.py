"""
Unified memory service for caching prompts, SQL queries, and execution results.

This module implements a hybrid storage system with project-scoped records
and optional global indexing, along with configurable retention policies.
"""

import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum
from logger import get_logger

logger = get_logger(__name__)


class RetentionPolicy(Enum):
    """Retention policies for memory cleanup."""

    KEEP_ALL = "keep_all"
    ROLLING_N = "rolling_n"
    TTL_DAYS = "ttl_days"


@dataclass
class QueryMemoryRecord:
    """Record of a single query execution with full context."""

    record_id: str
    project_id: str
    timestamp: str  # ISO format
    user_prompt: str
    normalized_prompt: str
    generated_sql: Optional[str] = None
    execution_success: bool = False
    execution_metadata: Dict[str, Any] = field(default_factory=dict)
    model_provider: Optional[str] = None
    model_name: Optional[str] = None
    result_summary: Optional[str] = None
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "QueryMemoryRecord":
        """Create instance from dictionary."""
        return cls(**data)


class UnifiedMemoryService:
    """
    Unified memory service for query prompt and SQL output caching.

    Features:
    - Project-scoped storage in project directories
    - Optional global index in data/ directory
    - Configurable retention policies
    - Efficient lookup and cleanup
    """

    def __init__(
        self,
        project_id: Optional[str] = None,
        retention_policy: str = "keep_all",
        rolling_n: int = 100,
        ttl_days: int = 90,
        global_index_enabled: bool = True,
    ):
        """
        Initialize the memory service.

        Args:
            project_id: Project identifier for scoping records
            retention_policy: One of 'keep_all', 'rolling_n', 'ttl_days'
            rolling_n: Number of records to keep when using rolling_n policy
            ttl_days: Days to keep records when using ttl_days policy
            global_index_enabled: Whether to maintain a global index
        """
        self.project_id = project_id
        self.retention_policy = RetentionPolicy(retention_policy)
        self.rolling_n = rolling_n
        self.ttl_days = ttl_days
        self.global_index_enabled = global_index_enabled

        # Storage paths
        self.data_dir = Path("data")
        self.data_dir.mkdir(exist_ok=True)

        self.global_index_path = self.data_dir / "query_memory_index.jsonl"

        logger.info(
            f"UnifiedMemoryService initialized: "
            f"policy={retention_policy}, project_id={project_id}"
        )

    def _get_project_memory_path(self, project_id: str) -> Path:
        """Get the memory file path for a specific project."""
        projects_dir = Path("projects")
        projects_dir.mkdir(exist_ok=True)
        return projects_dir / f"{project_id}_memory.jsonl"

    def store_query(
        self,
        user_prompt: str,
        normalized_prompt: str,
        generated_sql: Optional[str] = None,
        execution_success: bool = False,
        execution_metadata: Optional[Dict[str, Any]] = None,
        model_provider: Optional[str] = None,
        model_name: Optional[str] = None,
        result_summary: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> str:
        """
        Store a query execution record.

        Args:
            user_prompt: Original user query
            normalized_prompt: Normalized/expanded query
            generated_sql: Generated SQL query
            execution_success: Whether execution succeeded
            execution_metadata: Metadata about execution (rows, duration, etc.)
            model_provider: LLM provider used (openai, claude, local)
            model_name: Specific model used
            result_summary: Brief summary of results
            error_message: Error message if execution failed

        Returns:
            Record ID of the stored query
        """
        if not self.project_id:
            logger.warning("No project_id set - skipping memory storage")
            return ""

        # Generate record ID
        timestamp = datetime.now()
        record_id = f"{self.project_id}_{timestamp.strftime('%Y%m%d_%H%M%S_%f')}"

        # Create record
        record = QueryMemoryRecord(
            record_id=record_id,
            project_id=self.project_id,
            timestamp=timestamp.isoformat(),
            user_prompt=user_prompt,
            normalized_prompt=normalized_prompt,
            generated_sql=generated_sql,
            execution_success=execution_success,
            execution_metadata=execution_metadata or {},
            model_provider=model_provider,
            model_name=model_name,
            result_summary=result_summary,
            error_message=error_message,
        )

        # Store in project-scoped file
        self._append_to_project_memory(record)

        # Store in global index if enabled
        if self.global_index_enabled:
            self._append_to_global_index(record)

        # Apply retention policy
        self._apply_retention_policy()

        logger.info(f"Query stored in memory: {record_id}")
        return record_id

    def _append_to_project_memory(self, record: QueryMemoryRecord) -> None:
        """Append record to project memory file."""
        memory_path = self._get_project_memory_path(self.project_id)

        try:
            with open(memory_path, "a", encoding="utf-8") as f:
                json.dump(record.to_dict(), f, ensure_ascii=False)
                f.write("\n")
            logger.debug(f"Record appended to project memory: {memory_path}")
        except Exception as e:
            logger.error(f"Failed to append to project memory: {e}", exc_info=True)

    def _append_to_global_index(self, record: QueryMemoryRecord) -> None:
        """Append record to global index."""
        try:
            with open(self.global_index_path, "a", encoding="utf-8") as f:
                json.dump(record.to_dict(), f, ensure_ascii=False)
                f.write("\n")
            logger.debug(f"Record appended to global index")
        except Exception as e:
            logger.error(f"Failed to append to global index: {e}", exc_info=True)

    def search_similar_queries(
        self, prompt: str, limit: int = 5, project_scoped: bool = True
    ) -> List[QueryMemoryRecord]:
        """
        Search for similar queries in memory.

        Args:
            prompt: Query to search for
            limit: Maximum number of results
            project_scoped: Whether to limit search to current project

        Returns:
            List of matching QueryMemoryRecord objects
        """
        # Simple substring search for now - can be enhanced with embeddings
        results = []

        if project_scoped and self.project_id:
            memory_path = self._get_project_memory_path(self.project_id)
            if memory_path.exists():
                results = self._search_in_file(memory_path, prompt, limit)
        else:
            # Search global index
            if self.global_index_path.exists():
                results = self._search_in_file(self.global_index_path, prompt, limit)

        return results

    def _search_in_file(
        self, file_path: Path, prompt: str, limit: int
    ) -> List[QueryMemoryRecord]:
        """Search for similar queries in a JSONL file."""
        results = []
        prompt_lower = prompt.lower()

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    if not line.strip():
                        continue

                    try:
                        data = json.loads(line)
                        record = QueryMemoryRecord.from_dict(data)

                        # Simple substring match
                        if (
                            prompt_lower in record.user_prompt.lower()
                            or prompt_lower in record.normalized_prompt.lower()
                        ):
                            results.append(record)

                            if len(results) >= limit:
                                break
                    except Exception as e:
                        logger.warning(f"Failed to parse record: {e}")
                        continue
        except Exception as e:
            logger.error(f"Failed to search file {file_path}: {e}", exc_info=True)

        return results

    def get_recent_queries(self, limit: int = 10) -> List[QueryMemoryRecord]:
        """
        Get most recent queries for current project.

        Args:
            limit: Maximum number of records to return

        Returns:
            List of QueryMemoryRecord objects, newest first
        """
        if not self.project_id:
            return []

        memory_path = self._get_project_memory_path(self.project_id)
        if not memory_path.exists():
            return []

        records = []
        try:
            with open(memory_path, "r", encoding="utf-8") as f:
                for line in f:
                    if not line.strip():
                        continue

                    try:
                        data = json.loads(line)
                        records.append(QueryMemoryRecord.from_dict(data))
                    except Exception as e:
                        logger.warning(f"Failed to parse record: {e}")
                        continue
        except Exception as e:
            logger.error(f"Failed to read memory file: {e}", exc_info=True)
            return []

        # Sort by timestamp descending
        records.sort(key=lambda r: r.timestamp, reverse=True)
        return records[:limit]

    def _apply_retention_policy(self) -> None:
        """Apply configured retention policy to clean up old records."""
        if self.retention_policy == RetentionPolicy.KEEP_ALL:
            return

        if not self.project_id:
            return

        memory_path = self._get_project_memory_path(self.project_id)
        if not memory_path.exists():
            return

        try:
            # Read all records
            records = []
            with open(memory_path, "r", encoding="utf-8") as f:
                for line in f:
                    if not line.strip():
                        continue
                    try:
                        data = json.loads(line)
                        records.append(QueryMemoryRecord.from_dict(data))
                    except Exception as e:
                        logger.warning(f"Failed to parse record during cleanup: {e}")
                        continue

            # Filter based on policy
            if self.retention_policy == RetentionPolicy.ROLLING_N:
                # Keep only the N most recent records
                records.sort(key=lambda r: r.timestamp, reverse=True)
                records = records[: self.rolling_n]
                logger.info(f"Applied rolling_n policy: kept {len(records)} records")

            elif self.retention_policy == RetentionPolicy.TTL_DAYS:
                # Keep only records within TTL window
                cutoff = datetime.now() - timedelta(days=self.ttl_days)
                original_count = len(records)
                records = [
                    r for r in records if datetime.fromisoformat(r.timestamp) > cutoff
                ]
                logger.info(
                    f"Applied ttl_days policy: kept {len(records)}/{original_count} records"
                )

            # Rewrite file with filtered records
            with open(memory_path, "w", encoding="utf-8") as f:
                for record in records:
                    json.dump(record.to_dict(), f, ensure_ascii=False)
                    f.write("\n")

        except Exception as e:
            logger.error(f"Failed to apply retention policy: {e}", exc_info=True)

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about stored queries.

        Returns:
            Dictionary with statistics
        """
        if not self.project_id:
            return {
                "total_queries": 0,
                "successful_queries": 0,
                "failed_queries": 0,
                "project_id": None,
            }

        memory_path = self._get_project_memory_path(self.project_id)
        if not memory_path.exists():
            return {
                "total_queries": 0,
                "successful_queries": 0,
                "failed_queries": 0,
                "project_id": self.project_id,
            }

        total = 0
        successful = 0
        failed = 0

        try:
            with open(memory_path, "r", encoding="utf-8") as f:
                for line in f:
                    if not line.strip():
                        continue

                    try:
                        data = json.loads(line)
                        total += 1
                        if data.get("execution_success"):
                            successful += 1
                        else:
                            failed += 1
                    except Exception:
                        continue
        except Exception as e:
            logger.error(f"Failed to compute statistics: {e}", exc_info=True)

        return {
            "total_queries": total,
            "successful_queries": successful,
            "failed_queries": failed,
            "project_id": self.project_id,
            "retention_policy": self.retention_policy.value,
            "retention_limit": (
                self.rolling_n
                if self.retention_policy == RetentionPolicy.ROLLING_N
                else self.ttl_days
            ),
        }
