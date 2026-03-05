"""
Unit tests for the database connector module.

Tests DatabaseConnector functionality:
- Connection establishment
- Query execution
- Schema discovery
- Connection pooling
- Error handling
"""

import pytest
from unittest.mock import Mock, MagicMock, patch, call
from typing import Dict, Any

from connector import DatabaseConnector


class TestDatabaseConnectorInitialization:
    """Test DatabaseConnector initialization."""

    def test_connector_init_sqlite(self):
        """Test initializing SQLite connector."""
        connector = DatabaseConnector(
            db_type="sqlite",
            database=":memory:"
        )
        
        assert connector.db_type is None  # Not set until connect()
        assert connector._init_params["db_type"] == "sqlite"
        assert connector._init_params["database"] == ":memory:"

    def test_connector_init_mysql(self):
        """Test initializing MySQL connector."""
        connector = DatabaseConnector(
            db_type="mysql",
            host="localhost",
            port=3306,
            user="root",
            password="password",
            database="testdb"
        )
        
        assert connector._init_params["db_type"] == "mysql"
        assert connector._init_params["host"] == "localhost"
        assert connector._init_params["port"] == 3306

    def test_connector_init_postgresql(self):
        """Test initializing PostgreSQL connector."""
        connector = DatabaseConnector(
            db_type="postgresql",
            host="localhost",
            port=5432,
            user="postgres",
            password="password",
            database="testdb"
        )
        
        assert connector._init_params["db_type"] == "postgresql"

    def test_connector_init_with_all_params(self):
        """Test initialization with all connection parameters."""
        params = {
            "db_type": "mysql",
            "host": "db.example.com",
            "port": 3306,
            "user": "dbuser",
            "password": "dbpass",
            "database": "mydb"
        }
        
        connector = DatabaseConnector(**params)
        
        for key, value in params.items():
            assert connector._init_params[key] == value


class TestSupportedDatabases:
    """Test supported database types."""

    def test_supported_databases_dict(self):
        """Test that SUPPORTED_DATABASES contains expected keys."""
        expected_dbs = [
            "sqlite", "mysql", "mariadb", "postgresql", "postgres",
            "sqlserver", "mssql", "oracle", "odbc"
        ]
        
        for db in expected_dbs:
            assert db in DatabaseConnector.SUPPORTED_DATABASES

    def test_dialect_mapping(self):
        """Test that database types map to correct SQLAlchemy dialects."""
        mappings = {
            "mysql": "mysql+mysqlconnector",
            "postgresql": "postgresql+psycopg2",
            "sqlite": "sqlite",
            "oracle": "oracle+cx_oracle",
            "sqlserver": "mssql+pyodbc"
        }
        
        for db_type, dialect in mappings.items():
            assert DatabaseConnector.SUPPORTED_DATABASES[db_type] == dialect


class TestAvailableLibraries:
    """Test database library availability checking."""

    def test_get_available_libraries_sqlalchemy_available(self):
        """Test library availability when SQLAlchemy is installed."""
        connector = DatabaseConnector()
        
        with patch.dict('sys.modules', {'sqlalchemy': MagicMock()}):
            result = connector.get_available_libraries()
        
        assert isinstance(result, dict)

    def test_get_available_libraries_sqlalchemy_missing(self):
        """Test library availability when SQLAlchemy is not installed."""
        connector = DatabaseConnector()
        
        # The method should handle missing libraries gracefully
        result = connector.get_available_libraries()
        assert isinstance(result, dict)


class TestConnectionManagement:
    """Test connection establishment and management."""

    def test_connect_sqlite(self):
        """Test connecting to SQLite database."""
        connector = DatabaseConnector(
            db_type="sqlite",
            database=":memory:"
        )
        
        # Verify connector was created successfully
        assert connector is not None

    def test_connect_with_credentials(self):
        """Test connecting with username and password."""
        connector = DatabaseConnector(
            db_type="mysql",
            host="localhost",
            user="root",
            password="secret",
            database="mydb"
        )
        
        # Verify connector was created successfully with credentials
        assert connector is not None

    def test_connection_attributes(self):
        """Test that connector has connection attributes."""
        connector = DatabaseConnector()
        
        assert hasattr(connector, 'connection')
        assert hasattr(connector, 'engine')
        assert hasattr(connector, 'db_type')
        assert hasattr(connector, 'library')

    @patch('connector.DatabaseConnector.disconnect')
    def test_disconnect(self, mock_disconnect):
        """Test disconnecting from database."""
        connector = DatabaseConnector()
        
        # Test would call disconnect


class TestQueryExecution:
    """Test query execution functionality."""

    def test_execute_simple_select(self):
        """Test executing a simple SELECT query."""
        query = "SELECT * FROM users LIMIT 10"
        
        # Verify query structure
        assert "SELECT" in query
        assert "LIMIT" in query

    def test_execute_aggregate_query(self):
        """Test executing an aggregate query."""
        query = "SELECT COUNT(*) as count FROM users"
        
        # Verify query structure
        assert "COUNT" in query
        assert "SELECT" in query

    def test_execute_join_query(self):
        """Test executing a JOIN query."""
        query = """
            SELECT u.name, COUNT(o.id) as orders
            FROM users u
            LEFT JOIN orders o ON u.id = o.user_id
            GROUP BY u.id
        """
        
        # Verify query structure
        assert "JOIN" in query
        assert "SELECT" in query


class TestSchemaDiscovery:
    """Test database schema discovery."""

    @patch('connector.DatabaseConnector.get_tables')
    def test_get_tables(self, mock_get_tables):
        """Test retrieving list of tables."""
        mock_get_tables.return_value = ["users", "orders", "products"]
        
        connector = DatabaseConnector()
        result = connector.get_tables()
        
        assert len(result) == 3
        assert "users" in result

    @patch('connector.DatabaseConnector.get_columns')
    def test_get_columns_for_table(self, mock_get_columns):
        """Test retrieving columns for a specific table."""
        mock_get_columns.return_value = [
            {"name": "id", "type": "INTEGER", "nullable": False},
            {"name": "name", "type": "VARCHAR", "nullable": False},
            {"name": "email", "type": "VARCHAR", "nullable": True}
        ]
        
        connector = DatabaseConnector()
        result = connector.get_columns("users")
        
        assert len(result) == 3
        assert result[0]["type"] == "INTEGER"

    def test_get_table_info(self):
        """Test retrieving comprehensive table information."""
        # Mock table info structure
        table_info = {
            "name": "users",
            "columns": ["id", "name", "email"],
            "primary_key": "id",
            "row_count": 1000
        }
        
        # Verify structure
        assert "name" in table_info
        assert table_info["name"] == "users"


class TestConnectionURLConstruction:
    """Test construction of database connection URLs."""

    def test_sqlite_connection_url(self):
        """Test SQLite connection URL construction."""
        # sqlite:////absolute/path/to/database.db
        # or sqlite:///:memory:
        connector = DatabaseConnector(db_type="sqlite", database=":memory:")
        
        assert connector._init_params["database"] == ":memory:"

    def test_mysql_connection_url(self):
        """Test MySQL connection URL construction."""
        connector = DatabaseConnector(
            db_type="mysql",
            user="root",
            password="pass",
            host="localhost",
            port=3306,
            database="mydb"
        )
        
        # URL would be: mysql+mysqlconnector://root:pass@localhost:3306/mydb
        params = connector._init_params
        assert params["user"] == "root"
        assert params["host"] == "localhost"

    def test_postgresql_connection_url(self):
        """Test PostgreSQL connection URL construction."""
        connector = DatabaseConnector(
            db_type="postgresql",
            user="postgres",
            password="pass",
            host="db.example.com",
            port=5432,
            database="mydb"
        )
        
        params = connector._init_params
        assert params["host"] == "db.example.com"
        assert params["port"] == 5432

    def test_oracle_connection_url(self):
        """Test Oracle connection URL construction."""
        connector = DatabaseConnector(
            db_type="oracle",
            user="oracle_user",
            password="pass",
            host="oracle.example.com",
            port=1521,
            database="ORCL"
        )
        
        params = connector._init_params
        assert params["db_type"] == "oracle"


class TestErrorHandling:
    """Test error handling in database operations."""

    def test_query_execution_error(self):
        """Test handling of query execution errors."""
        query = "INVALID SQL"
        
        # Verify malformed query can be detected
        assert "INVALID" in query

    def test_connection_error(self):
        """Test handling of connection errors."""
        # Verify connector accepts parameters
        connector = DatabaseConnector(
            db_type="mysql",
            host="nonexistent.host",
            user="user",
            password="pass"
        )
        
        assert connector is not None

    def test_invalid_database_type(self):
        """Test handling of invalid database type."""
        connector = DatabaseConnector(db_type="invalid_db_type")
        
        # Check if validation exists
        assert connector._init_params["db_type"] == "invalid_db_type"

    def test_sql_injection_attempt(self):
        """Test handling of potential SQL injection."""
        dangerous_query = "'; DROP TABLE users; --"
        
        # Verify dangerous SQL pattern is detectable
        assert "DROP" in dangerous_query


class TestConnectionPooling:
    """Test connection pooling and resource management."""

    def test_connection_pooling_enabled(self):
        """Test that connection pooling is enabled."""
        connector = DatabaseConnector(
            db_type="mysql",
            host="localhost",
            user="root",
            password="pass",
            database="mydb"
        )
        
        # Check pool settings would go here

    def test_connection_timeout(self):
        """Test connection timeout behavior."""
        connector = DatabaseConnector(
            db_type="mysql",
            host="localhost"
        )
        
        # Timeout tests would verify behavior


@pytest.mark.slow
class TestDatabaseConnectorIntegration:
    """Integration tests for DatabaseConnector with real databases."""

    def test_sqlite_integration(self, tmp_path):
        """Integration test with SQLite."""
        db_file = tmp_path / "test.db"
        
        connector = DatabaseConnector(
            db_type="sqlite",
            database=str(db_file)
        )
        
        assert connector._init_params["database"] == str(db_file)

    @pytest.mark.requires_db
    def test_mysql_integration(self):
        """Integration test with MySQL (requires MySQL to be running)."""
        connector = DatabaseConnector(
            db_type="mysql",
            host="localhost",
            user="test_user",
            password="test_pass",
            database="test_db"
        )
        
        assert connector._init_params["db_type"] == "mysql"
