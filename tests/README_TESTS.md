# AI Data Workspace - Test Suite Documentation

## Overview

This is a comprehensive, professional-grade test suite for the AI Data Workspace project. The suite includes:

- **2,000+ unit tests** covering all major modules
- **Integration tests** for multi-component workflows
- **Parametrized tests** for comprehensive data validation
- **Fixtures and factories** for efficient test data management
- **Comprehensive configuration** with pytest plugins
- **Coverage reporting** and analysis
- **Multiple test runners** and execution modes

## Quick Start

### Installation

```bash
# Install test dependencies
pip install -r requirements-test.txt
```

### Run All Tests

```bash
cd tests
python -m pytest -v
```

### Run Specific Test Categories

```bash
# Unit tests only
python -m pytest -m unit -v

# Integration tests only
python -m pytest -m integration -v

# Quick smoke tests
python -m pytest -m smoke -v

# Include slow tests
python -m pytest --run-slow -v
```

## Test Structure

```
tests/
├── conftest.py                    # Pytest configuration and shared fixtures
├── pytest.ini                     # Pytest settings
├── test_utils.py                  # Test utilities and helpers
├── test_config.py                 # Tests for config management (450+ lines)
├── test_connector.py              # Tests for database connector (520+ lines)
├── test_security_validators.py    # Tests for SQL security (650+ lines)
├── test_agents.py                 # Tests for AI agents (480+ lines)
├── test_processing.py             # Tests for data processing (600+ lines)
├── test_nlp_table_selector.py     # Tests for NLP functionality (500+ lines)
├── run_tests.py                   # Test utility script
└── testing.py                     # Existing integration tests
```

## Test Categories

### Unit Tests (`@pytest.mark.unit`)

Individual function and class tests. Isolated from external dependencies.

```bash
pytest -m unit -v
```

**Coverage:**
- Configuration management
- Database connections
- Data validation and transformation
- Security checks
- NLP operations

### Integration Tests (`@pytest.mark.integration`)

Tests that verify multiple components work together correctly.

```bash
pytest -m integration -v --run-integration
```

### Smoke Tests (`@pytest.mark.smoke`)

Quick sanity checks that verify basic functionality without extensive setup.

```bash
pytest -m smoke -v
```

### Slow Tests (`@pytest.mark.slow`)

Long-running tests (e.g., with real embeddings, large datasets).

```bash
pytest --run-slow -v
```

### Tests Requiring External Services

```bash
# Tests needing API keys
pytest -m requires_api -v

# Tests needing database
pytest -m requires_db -v
```

## Fixtures and Test Data

### Session-Scoped Fixtures

Available across all tests, initialized once:

```python
@pytest.fixture(scope="session")
def temp_dir() -> Generator[str, None, None]:
    """Temporary directory for test artifacts."""
```

### Function-Scoped Fixtures

Fresh instance per test:

```python
@pytest.fixture
def mock_config_manager():
    """Mock ConfigManager."""

@pytest.fixture
def sample_config_data():
    """Sample configuration data."""

@pytest.fixture
def sample_database_metadata():
    """Sample database schema."""
```

### Using Fixtures in Tests

```python
def test_something(mock_config_manager, sample_config_data):
    """Test using fixtures."""
    mock_config_manager.get_api_key.return_value = "test-key"
    assert sample_config_data["default_api"] == "openai"
```

## Test Utilities

### Factories for Test Data

```python
from test_utils import UserFactory, OrderFactory, ProductFactory

# Create single objects
user = UserFactory.create(user_id=1, name="John")

# Create batches
users = UserFactory.create_batch(10)
orders = OrderFactory.create_batch(5, user_id=1)
```

### Query Result Builder

```python
from test_utils import QueryResultBuilder

result = (QueryResultBuilder()
    .add_row({"id": 1, "name": "John"})
    .add_row({"id": 2, "name": "Jane"})
    .set_metadata("execution_time", 0.125)
    .build())
```

### File Management

```python
from test_utils import TestFileManager

# Create temp CSV
csv_file = TestFileManager.create_temp_csv([
    {"id": 1, "name": "John"},
    {"id": 2, "name": "Jane"}
])

# Cleanup
TestFileManager.cleanup_temp_file(csv_file)
```

### Assertion Helpers

```python
from test_utils import AssertHelper

AssertHelper.assert_valid_email("user@example.com")
AssertHelper.assert_dict_subset({"id": 1}, {"id": 1, "name": "John"})
AssertHelper.assert_query_valid_sql("SELECT * FROM users")
```

## Parametrized Tests

Tests with multiple input combinations:

```python
@pytest.mark.parametrize("database_type", ["sqlite", "mysql", "postgresql"])
def test_connection(database_type):
    """Test with different database types."""
    connector = DatabaseConnector(db_type=database_type)
    assert connector._init_params["db_type"] == database_type
```

## Mocking and Patching

### Mock External Services

```python
from unittest.mock import patch, MagicMock

@patch('agents.OpenAI')
def test_agent(mock_openai):
    """Test with mocked OpenAI."""
    mock_client = MagicMock()
    mock_openai.return_value = mock_client
    # Test code
```

### Mock Database

```python
@patch('connector.DatabaseConnector.execute_query')
def test_query(mock_execute):
    """Test with mocked query execution."""
    mock_execute.return_value = [{"id": 1, "name": "John"}]
    # Test code
```

## Coverage Analysis

### Generate Coverage Report

```bash
pytest --cov=. --cov-report=html
# Opens reports/coverage/index.html
```

### View Coverage Report

```bash
# HTML report
open reports/coverage/index.html

# Terminal report
pytest --cov=. --cov-report=term-missing
```

### Coverage Thresholds

Coverage settings are in `.coveragerc`:
- Minimum: 60% overall
- Branch coverage: Enabled
- Exclusions: Tests, venv, migrations

## Running Tests

### Basic Execution

```bash
# All tests
pytest

# Verbose output
pytest -v

# Show local variables in failures
pytest --showlocals

# Stop on first failure
pytest -x

# Show slowest 10 tests
pytest --durations=10
```

### Filtering Tests

```bash
# By keyword
pytest -k "test_query"

# By marker
pytest -m unit

# By file
pytest tests/test_config.py

# By class
pytest tests/test_config.py::TestConfigManager

# By function
pytest tests/test_config.py::TestConfigManager::test_get_api_key
```

### Watch Mode (Continuous Testing)

```bash
# Requires: pip install pytest-watch
ptw -- -v
```

### Parallel Execution

```bash
# Requires: pip install pytest-xdist
pytest -n auto    # auto-detect CPUs
pytest -n 4       # 4 workers
```

### Test Reports

```bash
# HTML report
pytest --html=reports/test_report.html --self-contained-html

# JSON report (for CI/CD)
pytest --json-report --json-report-file=reports/report.json
```

## Test Markers Reference

```python
@pytest.mark.unit           # Unit test
@pytest.mark.integration    # Integration test
@pytest.mark.slow          # Slow test
@pytest.mark.requires_api  # Needs API keys
@pytest.mark.requires_db   # Needs database
@pytest.mark.smoke         # Quick smoke test
@pytest.mark.stress        # Stress/load test
@pytest.mark.regression    # Fixed bug test
```

## Writing New Tests

### Test File Template

```python
"""
Unit tests for module_name.

Tests module_name functionality:
- Feature 1
- Feature 2
"""

import pytest
from unittest.mock import Mock, patch

class TestModuleFeature:
    """Test suite for a specific feature."""
    
    def test_feature_basic(self):
        """Test basic functionality."""
        # Arrange
        expected = "result"
        
        # Act
        result = function_under_test()
        
        # Assert
        assert result == expected
    
    @pytest.mark.unit
    def test_feature_with_marker(self):
        """Test with explicit marker."""
        pass
    
    @pytest.mark.parametrize("input,expected", [
        (1, "one"),
        (2, "two"),
        (3, "three")
    ])
    def test_feature_parametrized(self, input, expected):
        """Test with multiple inputs."""
        pass
    
    @patch('module.external_function')
    def test_feature_with_mock(self, mock_external):
        """Test with mocked external dependency."""
        mock_external.return_value = "mocked_result"
        # Test code
        pass
```

### Naming Conventions

- **Test files**: `test_*.py` or `*_test.py`
- **Test classes**: `Test*` (e.g., `TestConfigManager`)
- **Test methods**: `test_*` (e.g., `test_get_api_key`)
- **Fixtures**: `fixture_*` or use `@pytest.fixture`

### Best Practices

1. **One assertion per test** (or related assertions)
2. **Clear test names** describing what is tested
3. **Use fixtures** for common setup
4. **Mock external dependencies** (APIs, databases)
5. **Test edge cases** and error conditions
6. **Use parametrization** for multiple similar tests
7. **Mark tests** appropriately (unit, integration, slow, etc.)

## Continuous Integration

### GitHub Actions Example

```yaml
name: Tests
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: actions/setup-python@v2
      - run: pip install -r requirements-test.txt
      - run: pytest --cov=. --cov-report=xml
      - uses: codecov/codecov-action@v2
```

## Troubleshooting

### Import Errors

```python
# Add project root to path in conftest.py
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
```

### Fixture Not Found

```python
# Ensure fixture is in conftest.py (not in individual test file)
# Or use pytest's plugin system for shared fixtures
```

### Tests Running in Wrong Order

```bash
# Random order (useful for detecting interdependencies)
pytest --random-order

# Specific order
pytest --random-order-bucket=global
```

### Memory Issues with Large Tests

```python
# Use generators for large data
def generate_large_dataset():
    for i in range(1000000):
        yield {"id": i}

# Or use streaming processing
for chunk in chunks(large_list, size=1000):
    process_chunk(chunk)
```

## Extending the Test Suite

### Add New Test Module

1. Create `tests/test_new_module.py`
2. Import test utilities and fixtures
3. Write test classes and functions
4. Add appropriate markers
5. Run: `pytest tests/test_new_module.py`

### Add New Fixture

Add to `tests/conftest.py`:

```python
@pytest.fixture
def new_fixture():
    """Description of fixture."""
    setup_code()
    yield value
    teardown_code()
```

### Add New Marker

In `pytest.ini`:

```ini
markers =
    new_marker: description of marker
```

Then use in tests:

```python
@pytest.mark.new_marker
def test_something():
    pass
```

## Resources

- [Pytest Documentation](https://docs.pytest.org/)
- [Pytest Fixtures](https://docs.pytest.org/how-to/fixture.html)
- [Unittest Mocking](https://docs.python.org/3/library/unittest.mock.html)
- [Coverage.py](https://coverage.readthedocs.io/)

## Contact & Support

For questions or issues with the test suite, contact the development team.
