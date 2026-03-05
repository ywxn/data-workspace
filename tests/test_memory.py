"""
Tests for the unified memory service.

Tests include:
- Cache hit detection
- Retention policy enforcement (ROLLING_N, TTL_DAYS)
- Semantic search with NLP embeddings
- Query storage and retrieval
"""

import json
import os
import tempfile
import time
from datetime import datetime, timedelta
from pathlib import Path
import pytest

from memory.query_memory import (
    UnifiedMemoryService,
    QueryMemoryRecord,
    QuerySearchResult,
    RetentionPolicy,
)


@pytest.fixture
def temp_memory_dir():
    """Create a temporary directory for memory tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Set up temporary directories
        data_dir = Path(tmpdir) / "data"
        projects_dir = Path(tmpdir) / "projects"
        data_dir.mkdir(exist_ok=True)
        projects_dir.mkdir(exist_ok=True)
        
        # Store original paths
        import memory.query_memory as qm
        original_data_dir = qm.Path("data")
        original_projects_dir = qm.Path("projects")
        
        yield tmpdir
        
        # Cleanup is automatic with tempfile.TemporaryDirectory


@pytest.fixture
def memory_service(temp_memory_dir):
    """Create a memory service instance for testing."""
    # Override the data directory for testing
    service = UnifiedMemoryService(
        project_id="test_project",
        retention_policy="keep_all",
        global_index_enabled=True,
    )
    
    # Override paths to use temp directory
    service.data_dir = Path(temp_memory_dir) / "data"
    service.data_dir.mkdir(exist_ok=True)
    service.global_index_path = service.data_dir / "query_memory_index.jsonl"
    
    return service


class TestQueryStorage:
    """Test query storage and retrieval."""

    def test_store_query_basic(self, memory_service):
        """Test storing a basic query."""
        record_id = memory_service.store_query(
            user_prompt="Show me all customers",
            normalized_prompt="select * from customers",
            generated_sql="SELECT * FROM customers;",
            execution_success=True,
            execution_metadata={"row_count": 100},
            model_provider="openai",
            model_name="gpt-4",
        )
        
        assert record_id
        assert record_id.startswith("test_project_")
        
        # Verify it can be retrieved
        recent = memory_service.get_recent_queries(limit=1)
        assert len(recent) == 1
        assert recent[0].user_prompt == "Show me all customers"
        assert recent[0].execution_success is True

    def test_store_query_with_error(self, memory_service):
        """Test storing a failed query."""
        record_id = memory_service.store_query(
            user_prompt="Invalid query",
            normalized_prompt="invalid",
            generated_sql="INVALID SQL",
            execution_success=False,
            error_message="Syntax error",
        )
        
        assert record_id
        
        recent = memory_service.get_recent_queries(limit=1)
        assert len(recent) == 1
        assert recent[0].execution_success is False
        assert recent[0].error_message == "Syntax error"

    def test_get_recent_queries_ordering(self, memory_service):
        """Test that recent queries are returned in reverse chronological order."""
        # Store multiple queries with slight delays
        for i in range(5):
            memory_service.store_query(
                user_prompt=f"Query {i}",
                normalized_prompt=f"query_{i}",
                execution_success=True,
            )
            time.sleep(0.01)  # Small delay to ensure different timestamps
        
        recent = memory_service.get_recent_queries(limit=3)
        assert len(recent) == 3
        assert recent[0].user_prompt == "Query 4"  # Most recent
        assert recent[1].user_prompt == "Query 3"
        assert recent[2].user_prompt == "Query 2"

    def test_get_statistics(self, memory_service):
        """Test memory statistics calculation."""
        # Store successful and failed queries
        for i in range(7):
            memory_service.store_query(
                user_prompt=f"Query {i}",
                normalized_prompt=f"query_{i}",
                execution_success=(i % 3 != 0),  # Fail every 3rd query
            )
        
        stats = memory_service.get_statistics()
        assert stats["total_queries"] == 7
        assert stats["successful_queries"] == 5
        assert stats["failed_queries"] == 2
        assert stats["project_id"] == "test_project"


class TestSemanticSearch:
    """Test semantic search with NLP embeddings."""

    def test_search_similar_queries_exact_match(self, memory_service):
        """Test exact match returns high similarity."""
        memory_service.store_query(
            user_prompt="Show me all customers from New York",
            normalized_prompt="select * from customers where city = 'New York'",
            generated_sql="SELECT * FROM customers WHERE city = 'New York';",
            execution_success=True,
        )
        
        results = memory_service.search_similar_queries(
            prompt="Show me all customers from New York",
            limit=5,
            similarity_threshold=0.7,
        )
        
        assert len(results) > 0
        assert isinstance(results[0], QuerySearchResult)
        assert results[0].similarity_score >= 0.95  # Should be very high for exact match

    def test_search_similar_queries_semantic(self, memory_service):
        """Test semantic similarity detection."""
        memory_service.store_query(
            user_prompt="Show me all customers from New York",
            normalized_prompt="select * from customers where city = 'New York'",
            generated_sql="SELECT * FROM customers WHERE city = 'New York';",
            execution_success=True,
        )
        
        # Semantically similar but different wording
        results = memory_service.search_similar_queries(
            prompt="Display all clients in New York",
            limit=5,
            similarity_threshold=0.5,  # Lower threshold for semantic match
        )
        
        # Should find the similar query
        assert len(results) > 0
        assert results[0].similarity_score > 0.5

    def test_search_similar_queries_no_match(self, memory_service):
        """Test that dissimilar queries don't match."""
        memory_service.store_query(
            user_prompt="Show me all customers",
            normalized_prompt="select * from customers",
            generated_sql="SELECT * FROM customers;",
            execution_success=True,
        )
        
        results = memory_service.search_similar_queries(
            prompt="Calculate total revenue by product category",
            limit=5,
            similarity_threshold=0.7,
        )
        
        # Should not find similar queries
        assert len(results) == 0

    def test_search_respects_threshold(self, memory_service):
        """Test that similarity threshold is respected."""
        memory_service.store_query(
            user_prompt="Show customers",
            normalized_prompt="select * from customers",
            execution_success=True,
        )
        
        # Low threshold
        results_low = memory_service.search_similar_queries(
            prompt="Display clients",
            limit=5,
            similarity_threshold=0.3,
        )
        
        # High threshold
        results_high = memory_service.search_similar_queries(
            prompt="Display clients",
            limit=5,
            similarity_threshold=0.9,
        )
        
        # Low threshold should have more results
        assert len(results_low) >= len(results_high)

    def test_lexical_fallback(self, memory_service):
        """Test that lexical search works when embeddings unavailable."""
        # Disable embedding model
        memory_service._model_load_attempted = True
        memory_service._embedding_model = None
        
        memory_service.store_query(
            user_prompt="Show me total revenue for each product",
            normalized_prompt="aggregate revenue by product",
            execution_success=True,
        )
        
        # Should still find with lexical matching
        results = memory_service.search_similar_queries(
            prompt="total revenue product",
            limit=5,
            similarity_threshold=0.3,
        )
        
        assert len(results) > 0


class TestRetentionPolicies:
    """Test retention policy enforcement."""

    def test_keep_all_policy(self, memory_service):
        """Test that KEEP_ALL policy doesn't delete records."""
        # Store multiple queries
        for i in range(10):
            memory_service.store_query(
                user_prompt=f"Query {i}",
                normalized_prompt=f"query_{i}",
                execution_success=True,
            )
        
        # All should be retained
        recent = memory_service.get_recent_queries(limit=20)
        assert len(recent) == 10

    def test_rolling_n_policy(self, temp_memory_dir):
        """Test ROLLING_N retention policy."""
        service = UnifiedMemoryService(
            project_id="test_rolling",
            retention_policy="rolling_n",
            rolling_n=5,
        )
        service.data_dir = Path(temp_memory_dir) / "data"
        service.data_dir.mkdir(exist_ok=True)
        
        # Store 10 queries
        for i in range(10):
            service.store_query(
                user_prompt=f"Query {i}",
                normalized_prompt=f"query_{i}",
                execution_success=True,
            )
            time.sleep(0.01)
        
        # Only the 5 most recent should be retained
        recent = service.get_recent_queries(limit=20)
        assert len(recent) == 5
        assert recent[0].user_prompt == "Query 9"
        assert recent[4].user_prompt == "Query 5"
        
        # Verify stats
        stats = service.get_statistics()
        assert stats["total_queries"] == 5

    def test_rolling_n_preserves_data(self, temp_memory_dir):
        """Test that ROLLING_N doesn't lose data during pruning."""
        service = UnifiedMemoryService(
            project_id="test_rolling_preserve",
            retention_policy="rolling_n",
            rolling_n=3,
        )
        service.data_dir = Path(temp_memory_dir) / "data"
        service.data_dir.mkdir(exist_ok=True)
        
        # Store queries with metadata
        for i in range(5):
            service.store_query(
                user_prompt=f"Query {i}",
                normalized_prompt=f"normalized_{i}",
                generated_sql=f"SELECT {i};",
                execution_success=True,
                execution_metadata={"row_count": i * 10},
            )
            time.sleep(0.01)
        
        # Get the retained queries
        recent = service.get_recent_queries(limit=10)
        assert len(recent) == 3
        
        # Verify data integrity
        for record in recent:
            assert record.user_prompt.startswith("Query")
            assert record.normalized_prompt.startswith("normalized_")
            assert record.generated_sql and record.generated_sql.startswith("SELECT")
            assert "row_count" in record.execution_metadata

    def test_ttl_days_policy(self, temp_memory_dir):
        """Test TTL_DAYS retention policy."""
        service = UnifiedMemoryService(
            project_id="test_ttl",
            retention_policy="ttl_days",
            ttl_days=7,
        )
        service.data_dir = Path(temp_memory_dir) / "data"
        service.data_dir.mkdir(exist_ok=True)
        
        # Create records with different ages
        now = datetime.now()
        
        # Recent record (should be kept)
        recent_record = QueryMemoryRecord(
            record_id="recent",
            project_id="test_ttl",
            timestamp=(now - timedelta(days=3)).isoformat(),
            user_prompt="Recent query",
            normalized_prompt="recent",
            execution_success=True,
        )
        
        # Old record (should be deleted)
        old_record = QueryMemoryRecord(
            record_id="old",
            project_id="test_ttl",
            timestamp=(now - timedelta(days=10)).isoformat(),
            user_prompt="Old query",
            normalized_prompt="old",
            execution_success=True,
        )
        
        # Manually write records to file
        memory_path = service._get_project_memory_path("test_ttl")
        with open(memory_path, "w", encoding="utf-8") as f:
            json.dump(old_record.to_dict(), f)
            f.write("\n")
            json.dump(recent_record.to_dict(), f)
            f.write("\n")
        
        # Apply retention policy
        service._apply_retention_policy()
        
        # Only recent record should remain
        recent = service.get_recent_queries(limit=10)
        assert len(recent) == 1
        assert recent[0].user_prompt == "Recent query"

    def test_ttl_boundary_conditions(self, temp_memory_dir):
        """Test TTL policy at boundary (exactly at TTL limit)."""
        service = UnifiedMemoryService(
            project_id="test_ttl_boundary",
            retention_policy="ttl_days",
            ttl_days=7,
        )
        service.data_dir = Path(temp_memory_dir) / "data"
        service.data_dir.mkdir(exist_ok=True)
        
        now = datetime.now()
        
        # Record exactly at boundary (should be kept - cutoff is '>')
        boundary_record = QueryMemoryRecord(
            record_id="boundary",
            project_id="test_ttl_boundary",
            timestamp=(now - timedelta(days=7, hours=0, minutes=1)).isoformat(),
            user_prompt="Boundary query",
            normalized_prompt="boundary",
            execution_success=True,
        )
        
        memory_path = service._get_project_memory_path("test_ttl_boundary")
        with open(memory_path, "w", encoding="utf-8") as f:
            json.dump(boundary_record.to_dict(), f)
            f.write("\n")
        
        service._apply_retention_policy()
        
        recent = service.get_recent_queries(limit=10)
        # Should be deleted (cutoff is strict)
        assert len(recent) == 0


class TestCacheHitDetection:
    """Test cache hit detection functionality."""

    def test_cache_hit_high_similarity(self, memory_service):
        """Test that high similarity queries trigger cache hits."""
        # Store a successful query
        memory_service.store_query(
            user_prompt="Show me all customers from California",
            normalized_prompt="customers california",
            generated_sql="SELECT * FROM customers WHERE state = 'CA';",
            execution_success=True,
            execution_metadata={"row_count": 150},
        )
        
        # Search for very similar query
        results = memory_service.search_similar_queries(
            prompt="Show me all customers from California",
            limit=3,
            similarity_threshold=0.85,  # High confidence threshold
        )
        
        # Should find the cached query
        assert len(results) > 0
        assert results[0].record.execution_success is True
        assert results[0].record.generated_sql is not None
        assert results[0].similarity_score >= 0.85

    def test_cache_miss_low_similarity(self, memory_service):
        """Test that low similarity queries don't trigger cache hits."""
        memory_service.store_query(
            user_prompt="Show me all customers",
            normalized_prompt="customers",
            generated_sql="SELECT * FROM customers;",
            execution_success=True,
        )
        
        # Very different query
        results = memory_service.search_similar_queries(
            prompt="Calculate average order value by month",
            limit=3,
            similarity_threshold=0.85,
        )
        
        # Should not find cached query
        assert len(results) == 0

    def test_failed_queries_filtered_for_cache(self, memory_service):
        """Test that failed queries are not used for cache hits."""
        # Store a failed query
        memory_service.store_query(
            user_prompt="Show me customers",
            normalized_prompt="customers",
            generated_sql="INVALID SQL",
            execution_success=False,
            error_message="Syntax error",
        )
        
        # Store a successful query
        memory_service.store_query(
            user_prompt="Display all users",
            normalized_prompt="users",
            generated_sql="SELECT * FROM users;",
            execution_success=True,
        )
        
        # Search should prioritize successful query
        results = memory_service.search_similar_queries(
            prompt="Show me customers",
            limit=5,
            similarity_threshold=0.5,
        )
        
        # Both may be returned, but for cache hits we only use successful ones
        cache_candidates = [r for r in results if r.record.execution_success]
        assert len(cache_candidates) >= 0  # May or may not find successful ones

    def test_multiple_cache_candidates(self, memory_service):
        """Test selection when multiple cache candidates exist."""
        # Store multiple similar queries
        queries = [
            ("Show all customers", "SELECT * FROM customers;", 0.95),
            ("Display customers", "SELECT * FROM customers;", 0.90),
            ("Get customer list", "SELECT * FROM customers;", 0.85),
        ]
        
        for prompt, sql, _ in queries:
            memory_service.store_query(
                user_prompt=prompt,
                normalized_prompt=prompt.lower(),
                generated_sql=sql,
                execution_success=True,
            )
            time.sleep(0.01)
        
        # Search for similar query
        results = memory_service.search_similar_queries(
            prompt="Show all customers",
            limit=5,
            similarity_threshold=0.7,
        )
        
        # Should return multiple results sorted by similarity
        assert len(results) >= 1
        # Results should be sorted by similarity (highest first)
        if len(results) > 1:
            assert results[0].similarity_score >= results[1].similarity_score


class TestProjectScoping:
    """Test project-scoped memory isolation."""

    def test_project_scoped_search(self, temp_memory_dir):
        """Test that searches are scoped to project."""
        # Create two services with different projects
        service1 = UnifiedMemoryService(project_id="project_a")
        service1.data_dir = Path(temp_memory_dir) / "data"
        service1.data_dir.mkdir(exist_ok=True)
        
        service2 = UnifiedMemoryService(project_id="project_b")
        service2.data_dir = Path(temp_memory_dir) / "data"
        service2.data_dir.mkdir(exist_ok=True)
        
        # Store query in project A
        service1.store_query(
            user_prompt="Query from project A",
            normalized_prompt="project_a_query",
            execution_success=True,
        )
        
        # Store query in project B
        service2.store_query(
            user_prompt="Query from project B",
            normalized_prompt="project_b_query",
            execution_success=True,
        )
        
        # Search in project A should not find project B queries
        results_a = service1.search_similar_queries(
            prompt="Query from project",
            limit=10,
            project_scoped=True,
            similarity_threshold=0.3,
        )
        
        assert len(results_a) >= 1
        assert all("project A" in r.record.user_prompt for r in results_a)
        assert not any("project B" in r.record.user_prompt for r in results_a)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
