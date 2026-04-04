from typing import Any, Dict, Optional, Tuple
import os
from urllib.parse import quote_plus

from security_validators import validate_sql_security


class DatabaseConnector:
    """
    A flexible database connector that supports multiple SQL libraries and database types.
    Supports: SQLite, MySQL, PostgreSQL, SQL Server, Oracle, and ODBC connections.
    """

    SUPPORTED_DATABASES = {
        "sqlite": "sqlite",
        "mysql": "mysql+mysqlconnector",
        "mariadb": "mysql+mysqlconnector",
        "postgresql": "postgresql+psycopg2",
        "postgres": "postgresql+psycopg2",
        "sqlserver": "mssql+pyodbc",
        "mssql": "mssql+pyodbc",
        "oracle": "oracle+cx_oracle",
        "odbc": "odbc",
    }

    def __init__(
        self,
        db_type: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
        **kwargs,
    ):
        """Initialize the database connector.

        Args:
            db_type: Database type (e.g., 'mysql', 'sqlite')
            host: Database host
            port: Database port
            user: Database user
            password: Database password
            database: Database name
            **kwargs: Additional connection parameters
        """
        self.connection = None
        self.engine = None
        self.db_type = None
        self.library = None

        self._init_params = {
            "db_type": db_type,
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "database": database,
            **kwargs,
        }

    def get_available_libraries(self) -> Dict[str, bool]:
        """
        Check whether SQLAlchemy is available in the current environment.

        Returns:
            Dict mapping database types to availability status
        """
        try:
            import sqlalchemy  # noqa: F401

            return {db_type: True for db_type in self.SUPPORTED_DATABASES}
        except ImportError:
            return {db_type: False for db_type in self.SUPPORTED_DATABASES}

    def connect(
        self,
        db_type: Optional[str] = None,
        credentials: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> Tuple[bool, str]:
        """
        Connect to a database using the appropriate SQL library.

        Args:
            db_type: Type of database ('sqlite', 'mysql', 'postgresql', 'sqlserver', 'oracle', 'odbc')
            credentials: Dictionary containing connection parameters
                - For SQLite: {'database': 'path/to/db.sqlite'}
                - For MySQL/MariaDB: {'host', 'user', 'password', 'database', 'port' (optional)}
                - For PostgreSQL: {'host', 'user', 'password', 'database', 'port' (optional)}
                - For SQL Server: {'server', 'database', 'user', 'password', 'driver' (optional)}
                - For Oracle: {'user', 'password', 'dsn'}
                - For ODBC: {'connection_string'}
            **kwargs: Additional connection parameters

        Returns:
            Tuple of (success: bool, message: str)
        """
        # Allow using init params if not provided explicitly
        if db_type is None:
            db_type = self._init_params.get("db_type")

        if credentials is None:
            credentials = self._build_credentials_from_init()

        if db_type is None:
            return False, "db_type is required to connect"

        db_type = db_type.lower()

        if db_type not in self.SUPPORTED_DATABASES:
            return (
                False,
                f"Unsupported database type: {db_type}. Supported types: {', '.join(self.SUPPORTED_DATABASES.keys())}",
            )

        driver_name = self.SUPPORTED_DATABASES[db_type]

        try:
            from sqlalchemy import create_engine

            sqlalchemy_url = self._build_sqlalchemy_url(
                db_type, credentials, driver_name
            )
            self.engine = create_engine(sqlalchemy_url, **kwargs)
            self.connection = self.engine.connect()
            self.library = None
            self.db_type = db_type
            return True, f"Successfully connected to {db_type} database via SQLAlchemy"
        except ImportError:
            return (
                False,
                "SQLAlchemy is not installed. Install it with: pip install sqlalchemy",
            )
        except Exception as e:
            return False, f"Connection failed: {str(e)}"

    def disconnect(self):
        """Disconnect from the database (alias for close)."""
        self.close()

    def _build_credentials_from_init(self) -> Dict[str, Any]:
        """Build credentials dict from init params for common DBs."""
        creds: Dict[str, Any] = {}
        for key in ["host", "port", "user", "password", "database"]:
            value = self._init_params.get(key)
            if value is not None:
                creds[key] = value
        # Include any extra kwargs passed to init that are not in standard keys
        for key, value in self._init_params.items():
            if key in ["db_type", "host", "port", "user", "password", "database"]:
                continue
            if value is not None:
                creds[key] = value
        return creds

    def _build_sqlalchemy_url(
        self, db_type: str, credentials: Dict[str, Any], sqlalchemy_driver: str
    ) -> Any:
        """Build a SQLAlchemy URL for the given database type and credentials."""
        from sqlalchemy.engine import URL

        if db_type == "sqlite":
            database = credentials.get("database")
            if not database:
                raise ValueError("SQLite requires 'database' parameter (file path)")
            return URL.create(drivername="sqlite", database=database)

        if db_type == "odbc":
            conn_str = credentials.get("connection_string")
            if not conn_str:
                raise ValueError("ODBC requires 'connection_string' parameter")
            return f"mssql+pyodbc:///?odbc_connect={quote_plus(conn_str)}"

        if db_type in ["sqlserver", "mssql"]:
            driver = credentials.get("driver", "ODBC Driver 17 for SQL Server")
            server = credentials.get("server") or credentials.get("host")
            database = credentials.get("database")
            if not server or not database:
                raise ValueError(
                    "SQL Server requires 'server' (or host) and 'database' parameters"
                )

            return URL.create(
                drivername=sqlalchemy_driver,
                username=credentials.get("user"),
                password=credentials.get("password"),
                host=server,
                database=database,
                port=credentials.get("port"),
                query={"driver": driver},
            )

        if db_type == "oracle":
            required = ["user", "password", "dsn"]
            missing = [key for key in required if key not in credentials]
            if missing:
                raise ValueError(f"Oracle requires parameters: {', '.join(missing)}")

            return URL.create(
                drivername=sqlalchemy_driver,
                username=credentials["user"],
                password=credentials["password"],
                host=credentials.get("host"),
                port=credentials.get("port"),
                database=credentials.get("database"),
                query={"dsn": credentials["dsn"]},
            )

        if db_type in ["mysql", "mariadb"]:
            required = ["host", "user", "password", "database"]
            missing = [key for key in required if key not in credentials]
            if missing:
                raise ValueError(f"MySQL connection requires: {', '.join(missing)}")
            return URL.create(
                drivername=sqlalchemy_driver,
                username=credentials["user"],
                password=credentials["password"],
                host=credentials["host"],
                port=credentials.get("port", 3306),
                database=credentials["database"],
            )

        if db_type in ["postgresql", "postgres"]:
            required = ["host", "user", "password", "database"]
            missing = [key for key in required if key not in credentials]
            if missing:
                raise ValueError(
                    f"PostgreSQL requires parameters: {', '.join(missing)}"
                )
            return URL.create(
                drivername=sqlalchemy_driver,
                username=credentials["user"],
                password=credentials["password"],
                host=credentials["host"],
                port=credentials.get("port", 5432),
                database=credentials["database"],
            )

        raise ValueError(f"Unsupported database type: {db_type}")

    def execute_non_query(self, query: str, params: Optional[Tuple] = None) -> int:
        """
        Execute a non-query SQL statement (INSERT, UPDATE, DELETE).

        Args:
            query: SQL statement
            params: Optional tuple of parameters for parameterized queries

        Returns:
            Number of affected rows
        """
        if not self.connection:
            raise RuntimeError("No active database connection. Call connect() first.")

        if not self.engine:
            raise RuntimeError("No active SQLAlchemy engine. Call connect() first.")

        from sqlalchemy import text

        is_safe, error_msg = validate_sql_security(query)
        if not is_safe:
            raise RuntimeError(error_msg)

        try:
            with self.engine.begin() as conn:
                result = conn.execute(text(query), params or {})
                return result.rowcount or 0
        except Exception as e:
            raise RuntimeError(f"Query execution failed: {str(e)}")

    def execute_sql_file(
        self, sql_file_path: str, encoding: str = "utf-8"
    ) -> Tuple[bool, str]:
        """
        Execute SQL statements from a .sql file.

        Args:
            sql_file_path: Path to the .sql file
            encoding: File encoding (default: 'utf-8')

        Returns:
            Tuple of (success: bool, message: str)
        """
        if not self.connection:
            raise RuntimeError("No active database connection. Call connect() first.")

        if not os.path.exists(sql_file_path):
            return False, f"SQL file not found: {sql_file_path}"

        try:
            with open(sql_file_path, "r", encoding=encoding) as f:
                sql_script = f.read()

            from sqlalchemy import text

            try:
                # Split by semicolon and execute each statement
                # Note: This is a simple split and may not handle all edge cases
                statements = [
                    stmt.strip() for stmt in sql_script.split(";") if stmt.strip()
                ]

                executed_count = 0
                with self.engine.begin() as conn:  # type: ignore
                    for statement in statements:
                        is_safe, error_msg = validate_sql_security(statement)
                        if not is_safe:
                            return False, error_msg
                        if statement:
                            conn.execute(text(statement))
                            executed_count += 1

                return (
                    True,
                    f"Successfully executed {executed_count} SQL statements from {os.path.basename(sql_file_path)}",
                )

            except Exception as e:
                return False, f"Error executing SQL file: {str(e)}"

        except Exception as e:
            return False, f"Error reading SQL file: {str(e)}"

    def load_sql_into_sqlite(
        self, sql_file_path: str, sqlite_db_path: Optional[str] = None
    ) -> Tuple[bool, str]:
        """
        Convenience method to load a .sql file into a SQLite database.
        If no database is connected, creates a new in-memory or file-based SQLite database.

        Args:
            sql_file_path: Path to the .sql file
            sqlite_db_path: Optional path for SQLite database. If None, uses in-memory database.

        Returns:
            Tuple of (success: bool, message: str)
        """
        # If not connected or not SQLite, create a new SQLite connection
        if not self.connection or self.db_type != "sqlite":
            db_path = sqlite_db_path or ":memory:"
            success, msg = self.connect("sqlite", {"database": db_path})
            if not success:
                return False, f"Failed to connect to SQLite: {msg}"

        # Execute the SQL file
        return self.execute_sql_file(sql_file_path)

    def get_tables(self) -> list:
        """
        Get list of tables in the connected database.

        Returns:
            List of table names
        """
        if not self.connection:
            raise RuntimeError("No active database connection")

        if not self.engine:
            raise RuntimeError("No active SQLAlchemy engine. Call connect() first.")

        from sqlalchemy import inspect

        inspector = inspect(self.engine)
        return inspector.get_table_names()

    def get_columns(self, table_name: str) -> list:
        """
        Get list of columns for a given table.

        Args:
            table_name: Name of the table
        Returns:
            List of column names
        """
        if not self.connection:
            raise RuntimeError("No active database connection")

        if not self.engine:
            raise RuntimeError("No active SQLAlchemy engine. Call connect() first.")

        from sqlalchemy import inspect

        inspector = inspect(self.engine)
        columns_info = inspector.get_columns(table_name)
        return [col["name"] for col in columns_info]

    def close(self):
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
        if self.engine:
            self.engine.dispose()
            self.engine = None
        self.db_type = None
        self.library = None

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
