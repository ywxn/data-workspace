"""
Pytest configuration and shared fixtures for all tests.

This file is automatically discovered by pytest and provides:
- Global fixtures available to all tests
- Session-scoped fixtures for expensive setup
- Plugin registration
- Custom pytest hooks
"""

import os
import sys
import json
import tempfile
from pathlib import Path
from typing import Dict, Any, Generator

import pytest
from unittest.mock import Mock, MagicMock, patch


# Add project root to path for imports
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


# ============================================================================
# Session-Scoped Fixtures (initialized once per test session)
# ============================================================================


@pytest.fixture(scope="session")
def temp_dir() -> Generator[str, None, None]:
    """Create a temporary directory for test artifacts."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture(scope="session")
def test_config_dir(temp_dir: str) -> str:
    """Create a test configuration directory."""
    config_dir = os.path.join(temp_dir, "config")
    os.makedirs(config_dir, exist_ok=True)
    return config_dir


# ============================================================================
# Function-Scoped Fixtures (fresh instance per test)
# ============================================================================


@pytest.fixture
def mock_config_manager(monkeypatch) -> Mock:
    """Mock the ConfigManager class."""
    with patch("config.ConfigManager") as mock:
        mock.get_api_key.return_value = "test-api-key"
        mock.get_default_api.return_value = "openai"
        mock.config_exists.return_value = True
        mock.get_config_path.return_value = "/tmp/config.json"
        yield mock


@pytest.fixture
def mock_logger(monkeypatch) -> Mock:
    """Mock the logger."""
    with patch("logger.get_logger") as mock:
        logger = MagicMock()
        mock.return_value = logger
        yield logger


@pytest.fixture
def mock_database_connector() -> Mock:
    """Mock the DatabaseConnector class."""
    with patch("connector.DatabaseConnector") as mock:
        connector = MagicMock()
        connector.connect.return_value = True
        connector.disconnect.return_value = True
        connector.execute_query.return_value = []
        connector.get_available_libraries.return_value = {"sqlite": True}
        mock.return_value = connector
        yield connector


# ============================================================================
# Test Data Fixtures
# ============================================================================


@pytest.fixture
def sample_config_data() -> Dict[str, Any]:
    """Provide sample configuration data."""
    return {
        "default_api": "openai",
        "apis": {
            "openai": {"key": "test-api-key", "model": "gpt-4", "max_tokens": 2000},
            "claude": {"key": "test-claude-key", "model": "claude-3-sonnet"},
        },
        "database": {"type": "sqlite", "path": ":memory:"},
        "semantic_layer": {},
    }


@pytest.fixture
def sample_database_metadata() -> Dict[str, Any]:
    """Provide sample database metadata."""
    return {
        "tables": ["users", "orders", "products"],
        "schemas": {
            "users": {
                "id": "INTEGER",
                "name": "VARCHAR(255)",
                "email": "VARCHAR(255)",
                "created_at": "DATETIME",
            },
            "orders": {
                "id": "INTEGER",
                "user_id": "INTEGER",
                "product_id": "INTEGER",
                "quantity": "INTEGER",
                "total_price": "DECIMAL(10,2)",
                "order_date": "DATETIME",
            },
            "products": {
                "id": "INTEGER",
                "name": "VARCHAR(255)",
                "price": "DECIMAL(10,2)",
                "category": "VARCHAR(100)",
            },
        },
        "row_counts": {"users": 1000, "orders": 5000, "products": 100},
    }


@pytest.fixture
def sample_query_result() -> Dict[str, Any]:
    """Provide sample query result data."""
    return {
        "success": True,
        "rows": [
            {"id": 1, "name": "John Doe", "email": "john@example.com"},
            {"id": 2, "name": "Jane Smith", "email": "jane@example.com"},
        ],
        "row_count": 2,
        "columns": ["id", "name", "email"],
        "execution_time": 0.125,
    }


@pytest.fixture
def sample_sql_queries() -> Dict[str, str]:
    """Provide sample SQL queries for testing."""
    return {
        "simple_select": "SELECT * FROM users LIMIT 10",
        "aggregate": "SELECT COUNT(*) as total FROM orders WHERE order_date > '2024-01-01'",
        "join": """
            SELECT u.name, COUNT(o.id) as order_count
            FROM users u
            LEFT JOIN orders o ON u.id = o.user_id
            GROUP BY u.id, u.name
        """,
        "subquery": """
            SELECT * FROM products
            WHERE price > (SELECT AVG(price) FROM products)
        """,
        "injection_attempt": "'; DROP TABLE users; --",
    }


@pytest.fixture
def sample_semantic_layer() -> Dict[str, Any]:
    """Provide sample semantic layer data."""
    return {
        "tables": {
            "users": {
                "description": "Customer user profiles",
                "columns": {
                    "id": "Unique identifier",
                    "name": "Customer full name",
                    "email": "Contact email address",
                },
            },
            "orders": {
                "description": "Customer purchase orders",
                "columns": {
                    "id": "Order identifier",
                    "user_id": "Customer ID",
                    "total_price": "Order total amount",
                },
            },
        }
    }


# ============================================================================
# Mock Service Fixtures
# ============================================================================


@pytest.fixture
def mock_openai_client() -> Mock:
    """Mock OpenAI client."""
    with patch("openai.OpenAI") as mock:
        client = MagicMock()
        client.chat.completions.create.return_value = MagicMock(
            choices=[MagicMock(message=MagicMock(content="Test response"))]
        )
        mock.return_value = client
        yield client


@pytest.fixture
def mock_anthropic_client() -> Mock:
    """Mock Anthropic client."""
    with patch("anthropic.Anthropic") as mock:
        client = MagicMock()
        client.messages.create.return_value = MagicMock(
            content=[MagicMock(text="Test response")]
        )
        mock.return_value = client
        yield client


# ============================================================================
# Pytest Hooks
# ============================================================================


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line("markers", "unit: mark test as a unit test")
    config.addinivalue_line("markers", "integration: mark test as an integration test")
    config.addinivalue_line("markers", "slow: mark test as slow running")
    config.addinivalue_line("markers", "requires_api: mark test as requiring API keys")
    config.addinivalue_line("markers", "requires_db: mark test as requiring database")


@pytest.fixture(autouse=True)
def reset_imports():
    """Reset module imports between tests to avoid cross-contamination."""
    yield
    # Any cleanup code can go here


def pytest_collection_modifyitems(config, items):
    """Modify test collection - add markers and skip conditions."""
    for item in items:
        # Add markers based on test file location
        if "unit" in item.nodeid:
            item.add_marker(pytest.mark.unit)
        elif "integration" in item.nodeid:
            item.add_marker(pytest.mark.integration)

        # Skip slow tests unless explicitly requested
        if "slow" in item.keywords:
            if not config.getoption("--run-slow"):
                item.add_marker(pytest.mark.skip(reason="use --run-slow to run"))


def pytest_addoption(parser):
    """Add custom command-line options."""
    parser.addoption(
        "--run-slow", action="store_true", default=False, help="run slow tests"
    )
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="run integration tests",
    )
