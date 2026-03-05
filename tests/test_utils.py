"""
Test utilities and helper functions for the test suite.

Provides:
- Factory functions for test objects
- Fixture helpers
- Assertion helpers
- Mock utilities
"""

import os
import json
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta
import random
import string


# ============================================================================
# Factory Functions for Test Data
# ============================================================================


class UserFactory:
    """Factory for creating test user objects."""

    @staticmethod
    def create(
        user_id: int = 1,
        name: str = "Test User",
        email: str = "test@example.com",
        status: str = "active",
    ) -> Dict[str, Any]:
        """Create a test user."""
        return {
            "id": user_id,
            "name": name,
            "email": email,
            "status": status,
            "created_at": datetime.now().isoformat(),
        }

    @staticmethod
    def create_batch(count: int = 10) -> List[Dict[str, Any]]:
        """Create multiple test users."""
        return [
            UserFactory.create(
                user_id=i, name=f"User {i}", email=f"user{i}@example.com"
            )
            for i in range(1, count + 1)
        ]


class OrderFactory:
    """Factory for creating test order objects."""

    @staticmethod
    def create(
        order_id: int = 1,
        user_id: int = 1,
        total_price: float = 99.99,
        status: str = "completed",
    ) -> Dict[str, Any]:
        """Create a test order."""
        return {
            "id": order_id,
            "user_id": user_id,
            "total_price": total_price,
            "status": status,
            "order_date": datetime.now().isoformat(),
            "items_count": random.randint(1, 5),
        }

    @staticmethod
    def create_batch(count: int = 10, user_id: int = 1) -> List[Dict[str, Any]]:
        """Create multiple test orders."""
        return [
            OrderFactory.create(
                order_id=i,
                user_id=user_id,
                total_price=round(random.uniform(10, 500), 2),
            )
            for i in range(1, count + 1)
        ]


class ProductFactory:
    """Factory for creating test product objects."""

    @staticmethod
    def create(
        product_id: int = 1,
        name: str = "Test Product",
        price: float = 99.99,
        category: str = "Electronics",
    ) -> Dict[str, Any]:
        """Create a test product."""
        return {
            "id": product_id,
            "name": name,
            "price": price,
            "category": category,
            "stock": random.randint(0, 100),
        }

    @staticmethod
    def create_batch(count: int = 10) -> List[Dict[str, Any]]:
        """Create multiple test products."""
        categories = ["Electronics", "Clothing", "Books", "Food", "Home"]
        return [
            ProductFactory.create(
                product_id=i,
                name=f"Product {i}",
                price=round(random.uniform(5, 500), 2),
                category=random.choice(categories),
            )
            for i in range(1, count + 1)
        ]


# ============================================================================
# Test Data Builders
# ============================================================================


class QueryResultBuilder:
    """Builder for creating structured query results."""

    def __init__(self):
        self.rows: List[Dict[str, Any]] = []
        self.columns: List[str] = []
        self.metadata: Dict[str, Any] = {}

    def add_row(self, row: Dict[str, Any]) -> "QueryResultBuilder":
        """Add a row to the result."""
        self.rows.append(row)
        if not self.columns:
            self.columns = list(row.keys())
        return self

    def add_rows(self, rows: List[Dict[str, Any]]) -> "QueryResultBuilder":
        """Add multiple rows."""
        for row in rows:
            self.add_row(row)
        return self

    def set_metadata(self, key: str, value: Any) -> "QueryResultBuilder":
        """Set metadata."""
        self.metadata[key] = value
        return self

    def build(self) -> Dict[str, Any]:
        """Build the result."""
        return {
            "rows": self.rows,
            "columns": self.columns,
            "row_count": len(self.rows),
            "metadata": self.metadata,
        }


# ============================================================================
# File Utilities
# ============================================================================


class TestFileManager:
    """Utilities for managing test files."""

    @staticmethod
    def create_temp_config(data: Dict[str, Any]) -> str:
        """Create a temporary config file."""
        fd, path = tempfile.mkstemp(suffix=".json")
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(data, f)
        except Exception as e:
            os.close(fd)
            os.unlink(path)
            raise e
        return path

    @staticmethod
    def create_temp_csv(rows: List[Dict[str, Any]]) -> str:
        """Create a temporary CSV file."""
        if not rows:
            raise ValueError("Must provide at least one row")

        fd, path = tempfile.mkstemp(suffix=".csv")
        try:
            with os.fdopen(fd, "w") as f:
                # Write header
                header = list(rows[0].keys())
                f.write(",".join(header) + "\n")

                # Write rows
                for row in rows:
                    values = [str(row.get(col, "")) for col in header]
                    f.write(",".join(values) + "\n")
        except Exception as e:
            os.close(fd)
            os.unlink(path)
            raise e
        return path

    @staticmethod
    def cleanup_temp_file(path: str) -> None:
        """Clean up a temporary file."""
        if os.path.exists(path):
            os.unlink(path)


# ============================================================================
# Assertion Helpers
# ============================================================================


class AssertHelper:
    """Custom assertion helpers."""

    @staticmethod
    def assert_valid_email(email: str) -> None:
        """Assert that email is valid format."""
        assert "@" in email
        assert "." in email.split("@")[1]

    @staticmethod
    def assert_dict_subset(subset: Dict[str, Any], full: Dict[str, Any]) -> None:
        """Assert that subset dict is contained in full dict."""
        for key, value in subset.items():
            assert key in full
            assert full[key] == value

    @staticmethod
    def assert_list_contains_dict(lst: List[Dict], expected: Dict) -> None:
        """Assert that list contains dict with matching values."""
        for item in lst:
            if all(item.get(k) == v for k, v in expected.items()):
                return
        raise AssertionError(f"List does not contain dict matching {expected}")

    @staticmethod
    def assert_query_valid_sql(query: str) -> None:
        """Assert that query looks like valid SQL."""
        assert query.strip()
        assert any(
            keyword in query.upper()
            for keyword in ["SELECT", "INSERT", "UPDATE", "DELETE"]
        )


# ============================================================================
# Mock Utilities
# ============================================================================


class MockDataGenerator:
    """Generate realistic mock data."""

    @staticmethod
    def generate_random_string(length: int = 10) -> str:
        """Generate random string."""
        return "".join(random.choices(string.ascii_letters, k=length))

    @staticmethod
    def generate_random_email() -> str:
        """Generate random email."""
        username = MockDataGenerator.generate_random_string(8)
        domain = MockDataGenerator.generate_random_string(5)
        return f"{username}@{domain}.com"

    @staticmethod
    def generate_date_range(days: int = 30) -> tuple:
        """Generate date range."""
        end = datetime.now()
        start = end - timedelta(days=days)
        return (start, end)

    @staticmethod
    def generate_price() -> float:
        """Generate random price."""
        return round(random.uniform(1, 999), 2)

    @staticmethod
    def generate_phone() -> str:
        """Generate random phone number."""
        return f"{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"


# ============================================================================
# Decorator Helpers
# ============================================================================


class TestDecorators:
    """Custom decorators for tests."""

    @staticmethod
    def requires_env_var(var_name: str):
        """Decorator to skip test if environment variable not set."""
        import pytest

        def decorator(func):
            return pytest.mark.skipif(
                var_name not in os.environ,
                reason=f"Requires {var_name} environment variable",
            )(func)

        return decorator

    @staticmethod
    def timeout(seconds: int = 30):
        """Decorator to add timeout to test."""
        import pytest

        def decorator(func):
            return pytest.mark.timeout(seconds)(func)

        return decorator


# ============================================================================
# Context Managers
# ============================================================================


@dataclass
class DatabaseTestContext:
    """Context manager for database testing."""

    connection: Any

    def __enter__(self):
        """Setup database."""
        # Initialize test database
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Teardown database."""
        # Cleanup test database
        if self.connection:
            self.connection.close()

    def create_table(self, table_def: str) -> None:
        """Create a test table."""
        # Execute create table statement
        pass

    def insert_data(self, table: str, data: List[Dict]) -> None:
        """Insert test data."""
        # Execute inserts
        pass


# ============================================================================
# Test Data Constants
# ============================================================================

VALID_SQL_QUERIES = [
    "SELECT * FROM users",
    "SELECT id, name FROM users WHERE id = 1",
    "SELECT COUNT(*) FROM orders",
    "SELECT * FROM users u JOIN orders o ON u.id = o.user_id",
]

INVALID_SQL_QUERIES = [
    "'; DROP TABLE users; --",
    "DROP TABLE users",
    "DELETE FROM users",
    "INVALID SQL STATEMENT",
]

SAMPLE_CONFIG = {
    "default_api": "openai",
    "apis": {"openai": {"key": "test-key", "model": "gpt-4"}},
    "database": {"type": "sqlite", "path": ":memory:"},
}

SAMPLE_SCHEMA = {
    "tables": ["users", "orders", "products"],
    "columns": {
        "users": ["id", "name", "email", "created_at"],
        "orders": ["id", "user_id", "product_id", "quantity", "total"],
        "products": ["id", "name", "price", "category"],
    },
}


# ============================================================================
# Utility Functions
# ============================================================================


def cleanup_temp_files(file_paths: List[str]) -> None:
    """Clean up multiple temporary files."""
    for path in file_paths:
        TestFileManager.cleanup_temp_file(path)


def wait_for_condition(condition_func, timeout: int = 5, interval: int = 0.1) -> bool:
    """Wait for a condition to be true."""
    import time

    start = time.time()

    while time.time() - start < timeout:
        if condition_func():
            return True
        time.sleep(interval)

    return False
