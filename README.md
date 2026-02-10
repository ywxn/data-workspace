# AI Data Workspace

An intelligent data analysis platform that combines multi-agent AI systems with a PyQt6 GUI for interactive data exploration, SQL database support, and AI-powered insights generation.

## Overview

**AI Data Workspace** is a comprehensive data analysis tool that leverages large language models (OpenAI GPT-4 or Anthropic Claude) to automatically generate pandas code, explore data, and provide actionable insights. The application features a modern GUI, support for multiple data sources, and an intelligent agent-based architecture.

### Key Features

- **Multi-Agent AI System**: Orchestrated planner, code generator, and analyzer agents
- **Dual LLM Support**: OpenAI (GPT-4) and Anthropic (Claude) backends
- **Multiple Data Sources**: CSV, Excel, SQLite, MySQL, PostgreSQL, SQL Server, Oracle, and ODBC
- **Intelligent Data Merging**: Automatic merge strategy detection for multiple tables
- **Code Generation**: AI-generated pandas code with security validation
- **Chat-Based Interface**: Multi-session chat with project persistence
- **Data Visualization**: Altair-based chart generation
- **Security**: Input validation, code sandboxing, and security rule enforcement

## Project Structure

```
d:\Projects\
├── agents.py              # Multi-agent AI orchestration (Planner, CodeGen, Analyzer)
├── connector.py           # Database connectivity (SQLite, MySQL, PostgreSQL, etc.)
├── config.py              # Configuration and API key management
├── config.json            # Persisted API keys and settings
├── constants.py           # Application-wide constants
├── gui_backend.py         # Backend logic for GUI (projects, chat sessions)
├── gui_frontend.py        # PyQt6 GUI implementation
├── processing.py          # Data loading and merging utilities
├── markdown_converter.py  # Markdown to HTML conversion
├── security_validators.py # Code security validation rules
├── logger.py              # Centralized logging configuration
├── projects/              # Saved projects (JSON)
├── docs/                  # Documentation
│   ├── INSTALLATION.md
│   ├── API_SETUP.md
│   └── DATABASE_SETUP.md
├── README.md              # Project overview and documentation
└── logs/                  # Application logs
```

## Installation

### Prerequisites

- Python 3.10+
- pip or conda
- 4GB RAM minimum (8GB+ recommended)

### Quick Start

1. **Clone/download the project**:
   ```bash
   cd d:\Projects
   ```

2. **Create a virtual environment**:
   ```bash
   python -m venv .venv
   .venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Run the application**:
   ```bash
   python gui_frontend.py
   ```

5. **Configure API keys** on first startup

See [INSTALLATION.md](docs/INSTALLATION.md) for detailed setup instructions.

## API Configuration

The application supports two leading AI providers:

1. **OpenAI** (GPT-4) - Default
2. **Anthropic Claude** - Alternative

### First-Time Setup

When you first launch the application, it will prompt you to configure API keys:

1. Get your API key from:
   - [OpenAI](https://platform.openai.com/api-keys)
   - [Anthropic](https://console.anthropic.com/account/keys)

2. Select provider and enter API key in the setup dialog
3. Keys are stored securely in `config.json`

See [API_SETUP.md](docs/API_SETUP.md) for detailed API configuration.

## Database Support

Connect to multiple database types:

- **SQLite** - Local development
- **MySQL/MariaDB** - Open source
- **PostgreSQL** - Enterprise-grade
- **SQL Server** - Microsoft ecosystem
- **Oracle** - Enterprise
- **ODBC** - Generic driver

See [DATABASE_SETUP.md](docs/DATABASE_SETUP.md) for connection examples and setup.

## Usage

### Starting the Application

```bash
python gui_frontend.py
```

### Workflow

1. **Create or Load a Project** - Organize analyses in named projects
2. **Connect Data Source** - CSV, Excel, or SQL database
3. **Start Chat Session** - Ask questions about your data
4. **Query Data** - Natural language questions generate code and insights
5. **Review Results** - AI-generated code, execution results, and analysis
6. **Save Project** - Auto-saved with chat history

### Chat Interface Controls

- **Enter** - Submit query
- **Shift+Enter** - Add newline in message
- View chat history and generated code/results

### Example Queries

```
"What are the top 10 customers by revenue?"
"Show me the monthly sales trend"
"Which products have declining demand?"
"Analyze customer segmentation"
"What's the average order value by region?"
```

## Core Modules

### agents.py - Multi-Agent AI System

Orchestrates three specialized agents:

- **Planner Agent** - Breaks down queries into execution plans
- **Code Generator** - Creates executable pandas code
- **Analyzer** - Generates human-readable insights

```python
from agents import AIAgent
import pandas as pd

agent = AIAgent(api_provider="openai")
df = pd.read_csv("data.csv")
response = await agent.execute_query("What are trends?", df)
```

### connector.py - Database Connections

Flexible connector supporting SQLite, MySQL, PostgreSQL, SQL Server, Oracle, ODBC:

```python
from connector import DatabaseConnector

db = DatabaseConnector()
db.connect("mysql", {
    "host": "localhost",
    "user": "root",
    "password": "pass",
    "database": "mydb"
})

tables = db.get_tables()
df = pd.read_sql("SELECT * FROM table", db.engine)
db.close()
```

### processing.py - Data Loading & Merging

Automated data import with intelligent merging strategies:

```python
from processing import load_data, merge_dataframes

# Load from CSV
df, status = load_data("csv", {"file_path": "data.csv"})

# Load from database
df, status = load_data("database", {
    "db_type": "sqlite",
    "credentials": {"database": "db.sqlite"},
    "table": "users"
})

# Merge multiple tables
merged_df, strategy = merge_dataframes([df1, df2, df3])
```

### config.py - Configuration Management

Manage API keys and application settings:

```python
from config import ConfigManager

key = ConfigManager.get_api_key("openai")
ConfigManager.set_api_key("openai", "sk-...")
ConfigManager.set_default_api("claude")
```

### security_validators.py - Code Security

Validates generated code against dangerous patterns:

```python
from security_validators import validate_code_security

code = "df.to_csv('output.csv')"
is_safe, msg = validate_code_security(code)  # Returns: False, "CSV forbidden"
```

Forbidden patterns:
- File writing (`to_csv`, `to_excel`, `open()`)
- Shell commands (`os.system`, `subprocess`)
- Code evaluation (`eval`, `exec`)
- Path traversal (`os.chdir`, absolute paths)

## Configuration

### API Models

Edit `constants.py` to change LLM models:

```python
LLM_MODELS = {
    "claude": "claude-3-5-sonnet-20241022",
    "openai": "gpt-4o-2024-08-06"
}
```

### LLM Parameters

```python
# Token limits
LLM_MAX_TOKENS_DEFAULT = 800
LLM_MAX_TOKENS_CODE = 1000
LLM_MAX_TOKENS_ANALYSIS = 1000

# Temperature (0-1, lower = more deterministic)
LLM_TEMPERATURE_DEFAULT = 0.3
LLM_TEMPERATURE_CODE = 0.4
LLM_TEMPERATURE_ANALYSIS = 0.6
```

### Merge Thresholds

```python
MERGE_COMMON_COLS_THRESHOLD = 0.7      # % of columns that must be common
MERGE_KEY_PATTERNS = ["id", "key", "index", "name", "code"]
```

## Troubleshooting

### API Key Issues

**"API key not configured"**
- Run app and use API Key Configuration dialog
- Or manually edit `config.json` with your keys

**"Invalid API key format"**
- OpenAI keys start with `sk-`
- Claude keys start with `sk-ant-`
- Verify keys at provider dashboards

### Database Connection Issues

**SQLite**: Ensure .db file path is correct
```
data/mydb.sqlite
```

**MySQL/PostgreSQL**: Test independently
```bash
mysql -h localhost -u user -p database
psql -h localhost -U user -d database
```

**SQL Server**: Ensure ODBC driver installed
```
ODBC Driver 17 for SQL Server
```

### Data Loading Issues

**Large Files**: Warning at 1M+ rows
- Filter before loading
- Use database sources for best performance

**Code Errors**: Check `logs/app.log` for:
- Syntax errors
- Security violations
- Data type mismatches

## Performance Tips

- **DataFrame Size**: Best with <1M rows
- **API Calls**: Reduce token limits for faster responses
- **Database**: Use indexed columns for filters/joins
- **Visualizations**: Limit data points for responsive charts

## Logging

Logs are written to `logs/app.log` with format:
```
2026-02-10 14:30:45 - module.name - INFO - Message
```

Adjust log level in `constants.py`:
```python
LOG_LEVEL_DEFAULT = "INFO"  # DEBUG, INFO, WARNING, ERROR, CRITICAL
```

## Security Notes

1. **API Keys**: Never commit `config.json` to version control
2. **Code Execution**: Generated code runs with current environment permissions
3. **SQL Injection**: Connections use parameterized queries
4. **File Access**: Code execution restricted to temp directory

## Documentation

- [Installation Guide](docs/INSTALLATION.md) - Detailed setup and troubleshooting
- [API Configuration](docs/API_SETUP.md) - LLM providers, keys, models
- [Database Setup](docs/DATABASE_SETUP.md) - Database connections and examples

## References

- [OpenAI API Docs](https://platform.openai.com/docs)
- [Anthropic Claude Docs](https://docs.anthropic.com/claude)
- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [PyQt6 Documentation](https://www.riverbankcomputing.com/static/Docs/PyQt6/)
- [SQLAlchemy Documentation](https://docs.sqlalchemy.org/)

## License

[Specify your license here]

## Support

For issues or questions:

1. Check `logs/app.log` for detailed error messages
2. Verify API keys and database connections
3. Test with sample data files in `data/`
4. Review error messages in the GUI
5. See documentation files for detailed guides
