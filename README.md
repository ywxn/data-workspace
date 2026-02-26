## Requirements

- Python 3.10 or higher
- PostgreSQL/MySQL/SQLite/Oracle/SQL Server (optional, for database connections)
- API key for OpenAI or Anthropic Claude

## Installation

1. Clone or download the project:
```bash
git clone https://github.com/ywxn/data-workspace.git
cd data-workspace
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure your API keys by running the application (you'll be prompted on first startup)

## Usage

### Running the Application

```bash
python main.py
```

Or run the compiled executable (see Compilation section below)

### Basic Workflow

1. Launch the application
2. Provide your API key (OpenAI or Claude) when prompted
3. Create a new project or load an existing one
4. Connect a data source (database or files)
5. Use the chat interface to query your data with natural language

## Compilation with Nuitka

To compile the application into a standalone executable:

### Prerequisites

```bash
pip install nuitka
```

### Compile for Windows

```bash
nuitka --onefile --windows-console-mode=hide --follow-imports main.py
```

This creates a standalone `.exe` file in the `main.dist` directory.

### Compile with Optimizations

```bash
nuitka --onefile --follow-imports -O main.py
```

## Configuration

Configuration is stored in `config.json`. You can:
- Set your default AI provider (openai or claude)
- Configure table selection method (manual or nlp)
- Save theme preference

API keys are insecurely stored in the config file. This is a temporary measure, users are encouraged to store their API keys as environment variables for better security. Future versions will implement a more secure key management system.

## Documentation

For detailed information, see:
- [API Setup Guide](docs/API_SETUP.md)
- [Database Setup Guide](docs/DATABASE_SETUP.md)
- [Installation Instructions](docs/INSTALLATION.md)
- [NLP Table Selector](docs/NLP_TABLE_SELECTOR_GUIDE.md)

## Troubleshooting

### API Key Issues
- Ensure your API key is valid and has proper permissions
- Check `config.json` in the application directory for stored keys

### Database Connection Failures
- Verify database credentials and network connectivity
- Check that the database server is running
- Ensure the specified database/tables exist

### Compilation Issues
- Use `--follow-imports` flag to ensure all imports are included
- For Windows, use `--windows-console-mode=hide` to hide console window
- Use `-O` flag for optimizations (slower build but faster runtime)
- Enable QT6 support.
