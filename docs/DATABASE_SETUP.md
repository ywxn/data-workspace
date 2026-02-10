# Database Configuration Guide

## Supported Databases

| Database | Driver | Status | Notes |
|---|---|---|---|
| SQLite | Built-in | ✅ | Best for local development |
| MySQL | mysql-connector | ✅ | Popular open-source |
| MariaDB | mysql-connector | ✅ | MySQL fork |
| PostgreSQL | psycopg2 | ✅ | Enterprise-grade |
| SQL Server | pyodbc | ✅ | Windows/cloud |
| Oracle | cx_Oracle | ✅ | Enterprise |
| ODBC | pyodbc | ✅ | Generic driver |

## Installation by Database

### SQLite (Local Development)

No installation needed - included with Python.

**Create test database**:
```bash
python
>>> import sqlite3
>>> conn = sqlite3.connect('test.db')
>>> cursor = conn.cursor()
>>> cursor.execute('CREATE TABLE users (id INTEGER, name TEXT)')
>>> cursor.execute("INSERT INTO users VALUES (1, 'Alice')")
>>> conn.commit()
>>> conn.close()
```

**Use in app**:
```python
from connector import DatabaseConnector

db = DatabaseConnector()
db.connect("sqlite", {"database": "test.db"})
tables = db.get_tables()
```

### MySQL / MariaDB

**Install connector**:
```bash
pip install mysql-connector-python
```

**Test connection**:
```bash
# Linux/macOS
mysql -h localhost -u root -p

# Windows (if installed)
mysql -h 127.0.0.1 -u root -p
```

**Create test database**:
```sql
CREATE DATABASE test_db;
USE test_db;
CREATE TABLE users (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100),
    email VARCHAR(100)
);
INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com');
```

**Use in app**:
```python
db = DatabaseConnector()
db.connect("mysql", {
    "host": "localhost",
    "user": "root",
    "password": "your_password",
    "database": "test_db",
    "port": 3306
})
```

### PostgreSQL

**Install connector**:
```bash
pip install psycopg2-binary
```

**Setup**:
```bash
# macOS (via Homebrew)
brew install postgresql

# Ubuntu/Debian
sudo apt-get install postgresql postgresql-contrib

# Windows: Download from https://www.postgresql.org/download/windows/
```

**Test connection**:
```bash
psql -h localhost -U postgres -d postgres
```

**Create test database**:
```sql
CREATE DATABASE test_db;
\c test_db
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100)
);
INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com');
```

**Use in app**:
```python
db = DatabaseConnector()
db.connect("postgresql", {
    "host": "localhost",
    "user": "postgres",
    "password": "your_password",
    "database": "test_db",
    "port": 5432
})
```

### SQL Server

**Install connector**:
```bash
pip install pyodbc
```

**Install ODBC Driver**:

Windows:
- Download from: https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server
- Run installer
- Restart computer

macOS:
```bash
brew install msodbcsql17
```

Linux (Ubuntu):
```bash
sudo add-apt-repository ppa:ubuntu-toolchain-r/test
sudo apt-get update
sudo apt-get install msodbcsql17
```

**Use in app**:
```python
db = DatabaseConnector()
db.connect("sqlserver", {
    "server": "localhost\\SQLEXPRESS",  # or server IP
    "database": "test_db",
    "user": "sa",
    "password": "your_password",
    "driver": "ODBC Driver 17 for SQL Server"
})
```

### Oracle

**Install connector**:
```bash
pip install cx_Oracle
```

**Install Oracle Client**:
- Download from: https://www.oracle.com/database/technologies/instant-client/downloads.html
- Extract to local directory
- Set environment variable:

Windows:
```bash
set ORACLE_HOME=C:\instantclient_21_9
```

macOS/Linux:
```bash
export ORACLE_HOME=/usr/local/instantclient_21_9
export LD_LIBRARY_PATH=$ORACLE_HOME:$LD_LIBRARY_PATH
```

**Use in app**:
```python
db = DatabaseConnector()
db.connect("oracle", {
    "user": "admin",
    "password": "your_password",
    "dsn": "localhost:1521/ORCL"
})
```

## Connection Examples

### SQLite

```python
from connector import DatabaseConnector
import pandas as pd

db = DatabaseConnector()
success, msg = db.connect("sqlite", {
    "database": "data/mydata.sqlite"
})

if success:
    # Load data
    df = pd.read_sql("SELECT * FROM users", db.engine)
    print(df)
    db.close()
```

### MySQL

```python
db = DatabaseConnector()
success, msg = db.connect("mysql", {
    "host": "192.168.1.100",
    "port": 3306,
    "user": "analyst",
    "password": "secure_password",
    "database": "sales"
})

if success:
    df = pd.read_sql("SELECT * FROM orders LIMIT 1000", db.engine)
    print(df)
    db.close()
```

### PostgreSQL

```python
db = DatabaseConnector()
success, msg = db.connect("postgresql", {
    "host": "analytics.example.com",
    "port": 5432,
    "user": "readonly_user",
    "password": "read_only_pass",
    "database": "analytics"
})

if success:
    # Get table list
    tables = db.get_tables()
    
    # Get columns
    columns = db.get_columns("sales_data")
    
    # Load specific table
    df = pd.read_sql("SELECT * FROM sales_data WHERE year=2024", db.engine)
    
    db.close()
```

## Connecting via GUI

1. **Start application**:
   ```bash
   python gui_frontend.py
   ```

2. **Create/Load Project**: Choose "Create New Project"

3. **Select Data Source**: Choose "Database"

4. **Database Type**: Select from dropdown

5. **Enter Credentials**:
   - **SQLite**: File path (e.g., `data/mydb.sqlite`)
   - **MySQL/PostgreSQL**: Host, Port, User, Password, Database
   - **SQL Server**: Server, Database, User, Password, Driver
   - **Oracle**: User, Password, DSN

6. **Test Connection**: Button validates before proceeding

7. **Select Tables**: Choose single or multiple tables

8. **Auto-Merge**: Multiple tables merged intelligently

## Merge Strategies

When loading multiple tables:

### Strategy Selection (in order):

1. **Same columns** → Vertical concatenation (UNION)
2. **>70% overlap** → Vertical concatenation
3. **All have common keys** → Horizontal merge (JOIN)
4. **Partial key overlap** → Sequential merge (pairwise)
5. **No common columns** → Side-by-side concatenation

### Examples

**Scenario 1: Same columns**
```
users table: id, name, email
users_backup: id, name, email
```
→ Vertically merged (combined rows)

**Scenario 2: Common ID column**
```
orders: order_id, customer_id, amount
customers: customer_id, name, country
```
→ Horizontally merged on customer_id

**Scenario 3: Pairwise keys**
```
sales: sales_id, product_id
products: product_id, category
inventory: product_id, stock
```
→ Sequential: (sales+products) then join with inventory

## Performance Tips

### Large Datasets

1. **Filter in database** (before load):
   - Load only needed date range
   - Exclude unnecessary columns
   - Use WHERE clauses

2. **Use indexes**:
   ```sql
   CREATE INDEX idx_date ON sales(date);
   CREATE INDEX idx_customer ON orders(customer_id);
   ```

3. **Limit rows**:
   - Load 100K rows for testing
   - Full load for final analysis

4. **Database-level aggregation**:
   - Pre-calculate summaries
   - Use database queries for heavy lifting

### Connection Pooling

For production use (modify `connector.py`):

```python
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool, QueuePool

# Minimal connections (default)
engine = create_engine(url, poolclass=NullPool)

# Connection pool (production)
engine = create_engine(
    url,
    poolclass=QueuePool,
    pool_size=10,
    max_overflow=20,
    pool_recycle=3600
)
```

## Troubleshooting

### "Could not connect to database server"

**Check**:
1. Server is running
2. Correct host/port
3. Network accessibility (ping host)
4. Firewall allows connection
5. User has permission to connect

**Test**:
```bash
# MySQL
mysql -h 192.168.1.100 -u analyst -p

# PostgreSQL
psql -h 192.168.1.100 -U analyst -d mydb

# SQL Server
osql -S 192.168.1.100 -U sa -P password
```

### "Access Denied" / "Authentication Failed"

**Check**:
1. Username is correct
2. Password is correct
3. User exists in database
4. User has required permissions

**SQL to grant permissions**:

MySQL:
```sql
GRANT SELECT ON database.* TO 'user'@'host' IDENTIFIED BY 'password';
FLUSH PRIVILEGES;
```

PostgreSQL:
```sql
GRANT CONNECT ON DATABASE dbname TO username;
GRANT USAGE ON SCHEMA public TO username;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO username;
```

### "Table not found"

**Check**:
1. Table name is correct (case-sensitive in some DBs)
2. You're connected to correct database/schema
3. User has permission to view table
4. Table actually exists

**List available tables**:
```python
db = DatabaseConnector()
db.connect(...credentials...)
tables = db.get_tables()
print(tables)
```

### "ODBC Driver not found"

**SQL Server on Windows**:
1. Download ODBC Driver from Microsoft
2. Install it
3. Verify in "ODBC Data Source Administrator"
4. Update driver name in connection dialog

**SQL Server on Linux**:
```bash
sudo apt-get install odbcinst unixodbc
# Then install msodbcsql17
```

### Slow Connections

**Optimize**:
1. Check network latency: `ping host`
2. Use connection closer to client
3. Check server load/resources
4. Enable connection pooling (production)
5. Pre-filter large datasets

## Security Considerations

### Credential Storage

**In config.json** (local development only):
```json
{
  "database": {
    "host": "localhost",
    "user": "dev_user",
    "password": "dev_password"
  }
}
```

**Via environment variables** (production):
```bash
export DB_HOST=prod.db.example.com
export DB_USER=prod_user
export DB_PASS=prod_password
```

**In code** (temporary):
```python
db = DatabaseConnector()
db.connect("mysql", {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
    "database": os.getenv("DB_NAME")
})
```

### SQL Injection Prevention

The application uses parameterized queries:

```python
# Safe - uses parameterization
df = pd.read_sql(
    "SELECT * FROM users WHERE id = ?",
    db.engine,
    params=(user_id,)
)

# Avoid - string concatenation
df = pd.read_sql(f"SELECT * FROM users WHERE id = {user_id}")  # UNSAFE!
```

### Firewall Rules

**Allow specific IPs**:
```sql
-- MySQL
GRANT SELECT ON database.* TO 'user'@'192.168.1.%';

-- PostgreSQL
host    database    user    192.168.1.0/24    md5
```

## Cloud Databases

### AWS RDS (MySQL/PostgreSQL)

1. Create RDS instance in AWS Console
2. Note: endpoint, port, database name, user, password
3. Add security group rule for your IP
4. Connect:

```python
db = DatabaseConnector()
db.connect("mysql" or "postgresql", {
    "host": "mydb.c9akciq32.us-east-1.rds.amazonaws.com",
    "user": "admin",
    "password": "password",
    "database": "mydb",
    "port": 3306
})
```

### Google Cloud SQL

Similar to AWS RDS - get instance IP and credentials from Google Cloud Console.

### Azure SQL Database

SQL Server compatible:
```python
db = DatabaseConnector()
db.connect("sqlserver", {
    "server": "myserver.database.windows.net",
    "database": "mydb",
    "user": "admin@myserver",
    "password": "password",
    "driver": "ODBC Driver 17 for SQL Server"
})
```

## Backup and Export

### Export data to CSV

```python
from processing import load_data
import pandas as pd

df, msg = load_data("database", {
    "db_type": "sqlite",
    "credentials": {"database": "mydb.sqlite"},
    "table": "users"
})

# Export
df.to_csv("users_export.csv", index=False)
```

### Backup database

SQLite:
```bash
cp mydb.sqlite mydb_backup_2024-02-10.sqlite
```

MySQL:
```bash
mysqldump -h localhost -u root -p mydb > mydb_backup.sql
```

PostgreSQL:
```bash
pg_dump -h localhost -U postgres mydb > mydb_backup.sql
```
