from connector import DatabaseConnector


def test_database_connection():
    """Test the DatabaseConnector class for establishing a connection."""
    config = {
        "db_type": "mysql",
        "host": "localhost",
        "port": 3306,
        "user": "appuser",
        "password": "testing",
        "database": "pmsuite_uat",
    }
    database_connector = None
    try:
        database_connector = DatabaseConnector(**config)
        success, msg = database_connector.connect(config["db_type"], config)
        if success:
            print("Database connection established successfully.")
        else:
            print(f"Failed to establish database connection: {msg}")
            return

        tables = database_connector.get_tables()
        print(f"Tables in the database: {tables}")

    except Exception as e:
        print(f"Error during database connection test: {str(e)}")
        import traceback

        traceback.print_exc()
    finally:
        if database_connector:
            database_connector.close()


def main():
    """Main function to run tests."""
    test_database_connection()


if __name__ == "__main__":
    main()
