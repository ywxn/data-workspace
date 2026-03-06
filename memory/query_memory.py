"""
Unified memory service for caching prompts, SQL queries, and execution results.

This module implements a hybrid storage system with project-scoped records
and optional global indexing, along with configurable retention policies.
Includes semantic search using sentence-transformers for intelligent query matching.
"""

import json
import os
import hashlib
import math
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum
from logger import get_logger

logger = get_logger(__name__)

try:
    from sentence_transformers import SentenceTransformer
    import numpy as np
except ImportError:
    SentenceTransformer = None
    np = None
    logger.warning(
        "sentence-transformers not available; semantic search will use lexical fallback"
    )


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
    generated_viz_code: Optional[str] = None
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


@dataclass
class QuerySearchResult:
    """Result from a similarity search with score."""

    record: QueryMemoryRecord
    similarity_score: float


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

        # Semantic search components
        self._embedding_model = None
        self._model_load_attempted = False
        self._embedding_cache: Dict[str, List[float]] = {}

        logger.info(
            f"UnifiedMemoryService initialized: "
            f"policy={retention_policy}, project_id={project_id}"
        )

    def _get_project_memory_path(self, project_id: str) -> Path:
        """Get the memory file path for a specific project."""
        projects_dir = Path("projects")
        projects_dir.mkdir(exist_ok=True)
        return projects_dir / f"{project_id}_memory.jsonl"

    def _get_embedding_model(self):
        """Lazy-load embedding model for semantic search, with graceful fallback."""
        if self._embedding_model is not None:
            return self._embedding_model
        if self._model_load_attempted:
            return None

        self._model_load_attempted = True

        if SentenceTransformer is None:
            logger.warning(
                "sentence-transformers not installed; using lexical search only"
            )
            return None

        try:
            self._embedding_model = SentenceTransformer(
                "sentence-transformers/all-MiniLM-L6-v2",
                cache_folder="models",
            )
            logger.info("Semantic search model loaded: all-MiniLM-L6-v2")
        except Exception as exc:
            logger.warning(f"Semantic model unavailable, using lexical fallback: {exc}")
            self._embedding_model = None

        return self._embedding_model

    def _embedding_cache_key(self, text: str) -> str:
        """Stable cache key for embedding text."""
        return hashlib.sha256(text.strip().lower().encode("utf-8")).hexdigest()

    def _embed_text(self, text: str) -> Optional[List[float]]:
        """Embed normalized text using cached vectors when available."""
        if not text or not text.strip():
            return None

        cache_key = self._embedding_cache_key(text)
        cached = self._embedding_cache.get(cache_key)
        if cached is not None:
            return cached

        model = self._get_embedding_model()
        if model is None:
            return None

        try:
            vector = model.encode(text, normalize_embeddings=True)
            if np is not None and isinstance(vector, np.ndarray):
                vector_list: List[float] = vector.tolist()
            elif not isinstance(vector, list):
                vector_list = list(vector)
            else:
                vector_list = vector

            self._embedding_cache[cache_key] = vector_list
            return vector_list
        except Exception as exc:
            logger.warning(f"Embedding failed, using lexical fallback: {exc}")
            return None

    def _normalize_text(self, text: str) -> str:
        """Normalize text for lexical matching."""
        if not text:
            return ""
        import re

        # Simple normalization: lowercase, remove extra whitespace
        normalized = text.lower().strip()
        normalized = re.sub(r"\s+", " ", normalized)
        return normalized

    def _lexical_similarity(self, query_text: str, memory_text: str) -> float:
        """Compute lexical similarity using Jaccard + containment."""
        query_tokens = set(self._normalize_text(query_text).split())
        memory_tokens = set(self._normalize_text(memory_text).split())

        if not query_tokens or not memory_tokens:
            return 0.0

        intersection = query_tokens & memory_tokens
        union = query_tokens | memory_tokens

        jaccard = len(intersection) / len(union) if union else 0.0
        containment = len(intersection) / len(query_tokens) if query_tokens else 0.0

        return 0.7 * jaccard + 0.3 * containment

    def _cosine_similarity(self, vec_a: List[float], vec_b: List[float]) -> float:
        """Compute cosine similarity between two vectors."""
        if not vec_a or not vec_b or len(vec_a) != len(vec_b):
            return 0.0

        dot_product = sum(a * b for a, b in zip(vec_a, vec_b))
        norm_a = math.sqrt(sum(a * a for a in vec_a))
        norm_b = math.sqrt(sum(b * b for b in vec_b))

        if norm_a == 0.0 or norm_b == 0.0:
            return 0.0

        return max(0.0, min(1.0, dot_product / (norm_a * norm_b)))

    def store_query(
        self,
        user_prompt: str,
        normalized_prompt: str,
        generated_sql: Optional[str] = None,
        generated_viz_code: Optional[str] = None,
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
            generated_viz_code: Generated Python visualization code
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
            generated_viz_code=generated_viz_code,
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
        if not self.project_id:
            return

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
        self,
        prompt: str,
        limit: int = 5,
        project_scoped: bool = True,
        similarity_threshold: float = 0.7,
        min_success_score: float = 0.8,
    ) -> List[QuerySearchResult]:
        """
        Search for similar queries using semantic embeddings with lexical fallback.

        Args:
            prompt: Query to search for
            limit: Maximum number of results
            project_scoped: Whether to limit search to current project
            similarity_threshold: Minimum similarity score (0.0 to 1.0)
            min_success_score: Score threshold for considering a cache hit

        Returns:
            List of QuerySearchResult objects with similarity scores
        """
        results = []

        if project_scoped and self.project_id:
            memory_path = self._get_project_memory_path(self.project_id)
            if memory_path.exists():
                results = self._search_in_file(
                    memory_path, prompt, limit, similarity_threshold
                )
        else:
            # Search global index
            if self.global_index_path.exists():
                results = self._search_in_file(
                    self.global_index_path, prompt, limit, similarity_threshold
                )

        return results

    def _search_in_file(
        self, file_path: Path, prompt: str, limit: int, similarity_threshold: float
    ) -> List[QuerySearchResult]:
        """Search for similar queries in a JSONL file using semantic + lexical similarity."""
        results = []
        query_embedding = self._embed_text(prompt)

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    if not line.strip():
                        continue

                    try:
                        data = json.loads(line)
                        record = QueryMemoryRecord.from_dict(data)

                        # Compute similarity using both semantic and lexical approaches
                        memory_embedding = self._embed_text(record.user_prompt)

                        semantic_similarity = (
                            self._cosine_similarity(query_embedding, memory_embedding)
                            if query_embedding is not None
                            and memory_embedding is not None
                            else 0.0
                        )

                        lexical_similarity = self._lexical_similarity(
                            prompt, record.user_prompt
                        )

                        # Combine semantic and lexical (favor semantic when available)
                        similarity = (
                            0.75 * semantic_similarity + 0.25 * lexical_similarity
                            if semantic_similarity > 0.0
                            else lexical_similarity
                        )

                        if similarity >= similarity_threshold:
                            results.append(
                                QuerySearchResult(
                                    record=record, similarity_score=similarity
                                )
                            )

                    except Exception as e:
                        logger.warning(f"Failed to parse record: {e}")
                        continue
        except Exception as e:
            logger.error(f"Failed to search file {file_path}: {e}", exc_info=True)

        # Sort by similarity descending and limit
        results.sort(key=lambda x: x.similarity_score, reverse=True)
        return results[:limit]

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

    def update_cached_sql(
        self,
        record_id: str,
        generated_sql: str,
        execution_success: bool = True,
        error_message: Optional[str] = None,
    ) -> bool:
        """
        Update SQL (and success/error state) for an existing cached record.

        This enables write-through cache behavior after a corrected cached query
        executes successfully.

        Args:
            record_id: Memory record ID to update
            generated_sql: SQL to persist on the record
            execution_success: Whether execution succeeded
            error_message: Optional error message to persist

        Returns:
            True if any record was updated, False otherwise
        """
        if not record_id or not generated_sql:
            return False

        updated_any = False

        if self.project_id:
            memory_path = self._get_project_memory_path(self.project_id)
            if memory_path.exists():
                updated_any = (
                    self._update_record_in_file(
                        memory_path,
                        record_id,
                        generated_sql,
                        execution_success,
                        error_message,
                    )
                    or updated_any
                )

        if self.global_index_enabled and self.global_index_path.exists():
            updated_any = (
                self._update_record_in_file(
                    self.global_index_path,
                    record_id,
                    generated_sql,
                    execution_success,
                    error_message,
                )
                or updated_any
            )

        if updated_any:
            logger.info(f"Updated cached SQL for record: {record_id}")

        return updated_any

    def _update_record_in_file(
        self,
        file_path: Path,
        record_id: str,
        generated_sql: str,
        execution_success: bool,
        error_message: Optional[str],
    ) -> bool:
        """Update one record by record_id in a JSONL file."""
        updated = False
        rewritten_lines: List[str] = []

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    raw = line.rstrip("\n")
                    if not raw.strip():
                        continue
                    try:
                        data = json.loads(raw)
                    except Exception:
                        rewritten_lines.append(raw)
                        continue

                    if data.get("record_id") == record_id:
                        data["generated_sql"] = generated_sql
                        data["execution_success"] = bool(execution_success)
                        data["error_message"] = error_message
                        updated = True

                    rewritten_lines.append(json.dumps(data, ensure_ascii=False))

            if updated:
                with open(file_path, "w", encoding="utf-8") as f:
                    for rewritten in rewritten_lines:
                        f.write(rewritten)
                        f.write("\n")

        except Exception as exc:
            logger.error(
                f"Failed to update cache file {file_path}: {exc}", exc_info=True
            )
            return False

        return updated

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
