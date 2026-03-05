# Quick Reference Guide - Pytest Commands

## Installation

```bash
# Install all test dependencies
pip install -r requirements-test.txt
```

## Basic Commands

```bash
# Run all tests
pytest

# Run with verbose output
pytest -v

# Run single file
pytest tests/test_config.py -v

# Run single test class
pytest tests/test_config.py::TestConfigManager -v

# Run single test method
pytest tests/test_config.py::TestConfigManager::test_config_file_path -v
```

## By Test Category

```bash
# Unit tests only (fast)
pytest -m unit -v

# Integration tests
pytest -m integration -v --run-integration

# Smoke tests (quick sanity checks)
pytest -m smoke -v

# All tests including slow ones
pytest --run-slow -v

# Tests requiring API keys
pytest -m requires_api -v

# Tests requiring database
pytest -m requires_db -v

# Stress tests
pytest -m stress --run-slow -v
```

## By Module

```bash
# Config module
pytest tests/test_config.py -v

# Connector module
pytest tests/test_connector.py -v

# Security validators
pytest tests/test_security_validators.py -v

# Agents
pytest tests/test_agents.py -v

# Data processing
pytest tests/test_processing.py -v

# NLP table selector
pytest tests/test_nlp_table_selector.py -v
```

## Filtering Tests

```bash
# Tests matching keyword
pytest -k "test_query" -v

# Tests NOT matching keyword
pytest -k "not slow" -v

# Tests with specific marker
pytest -m unit -v

# Tests matching marker expression
pytest -m "unit or smoke" -v

# Tests excluding marker
pytest -m "not slow" -v
```

## Coverage Analysis

```bash
# Run with coverage report
pytest --cov=. -v

# Generate HTML coverage report
pytest --cov=. --cov-report=html:reports/coverage -v

# Terminal coverage report
pytest --cov=. --cov-report=term-missing -v

# Coverage for specific module
pytest --cov=connector --cov-report=term-missing -v
```

## Report Generation

```bash
# HTML test report
pytest --html=reports/test_report.html --self-contained-html -v

# Generate both coverage and test reports
pytest --cov=. --cov-report=html:reports/coverage \
       --html=reports/test_report.html --self-contained-html -v
```

## Execution Modes

```bash
# Stop at first failure
pytest -x

# Show output from print statements
pytest -s

# Show local variables in failures
pytest --showlocals

# Show slowest 10 tests
pytest --durations=10

# Random test order (detect interdependencies)
pytest --random-order

# Run last failed tests
pytest --lf -v

# Run last failed tests first
pytest --ff -v

# Only failed tests
pytest --failed -v
```

## Parallel and Watch Modes

```bash
# Parallel execution with 4 workers
pytest -n 4 -v

# Parallel execution auto-detect CPUs
pytest -n auto -v

# Watch mode (requires pytest-watch)
ptw -- -v

# Watch specific file
ptw tests/test_config.py -- -v
```

## Advanced Options

```bash
# Increase verbosity
pytest -vv  # Very verbose
pytest -vvv # Extra verbose

# Quiet mode
pytest -q

# Skip tests on markers
pytest -m "not slow" -v  # Skip slow tests

# Set log level
pytest --log-cli-level=DEBUG -v

# Timeout for tests (2 seconds)
pytest --timeout=2 -v

# Show test collection without running
pytest --collect-only -q
```

## Common Workflows

### Development Workflow
```bash
# Fast unit tests only
pytest -m unit -v

# Watch specific module
ptw tests/test_config.py -- -v
```

### Pre-commit Workflow
```bash
# Smoke tests + coverage
pytest -m smoke --cov=. --cov-report=term -v
```

### Full Test Cycle
```bash
# All tests with reports
pytest --cov=. \
       --cov-report=html:reports/coverage \
       --html=reports/test_report.html \
       --self-contained-html \
       --durations=10 -v
```

### CI/CD Workflow
```bash
# Fast, with output for CI
pytest -v --tb=short --cov=. --cov-report=term

# With JUnit XML for CI integration
pytest -v --junit-xml=reports/junit.xml
```

### Debugging Workflow
```bash
# Show local variables and output
pytest tests/test_config.py::TestConfigManager::test_config_file_path \
       --showlocals -s -vv

# With pdb on failure
pytest --pdb
```

## Environment Variables

```bash
# Skip slow tests by default
PYTEST_SKIP_SLOW=1 pytest -v

# Run specific test by name pattern
PYTEST_NAME_PATTERN="test_query" pytest -v
```

## Docker/Container Usage

```bash
# Run tests in Docker
docker run -v $(pwd):/workspace python:3.10 bash -c \
  "cd /workspace && pip install -r requirements-test.txt && pytest -v"
```

## Pytest Configuration

Tests respect settings in:
- `pytest.ini` - Main configuration
- `.coveragerc` - Coverage configuration
- `pyproject.toml` - Modern Python projects
- `setup.cfg` - Alternative configuration

## View Test Results

```bash
# Open coverage report
open reports/coverage/index.html

# Open test report
open reports/test_report.html

# View test log
tail -f reports/pytest.log
```

## Troubleshooting

```bash
# Verbose error output
pytest -v --tb=long

# Show ALL output (large!)
pytest -vvv -s --tb=long

# Debug import errors
pytest --collect-only -v

# Check fixture availability
pytest --fixtures

# List all markers
pytest --markers
```

## Tips

1. **Fast Development**: Use `pytest -m unit -v` for quick feedback
2. **Quick Errors**: Use `pytest -x` to stop at first failure
3. **Performance**: Check slowest tests with `--durations=10`
4. **Coverage Goal**: Aim for 70%+ coverage, 80%+ is excellent
5. **Watch Mode**: Use `ptw` for instant feedback while coding
6. **CI Integration**: Always run full suite before pushing

## Quick Links

- Test Documentation: See `tests/README_TESTS.md`
- Test Suite Summary: See `TEST_SUITE_SUMMARY.md`
- Coverage Report: `reports/coverage/index.html`
- Test Report: `reports/test_report.html`
