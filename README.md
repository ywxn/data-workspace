# AI Data Workspace

An intelligent application for data analysis and visualization with AI assistance. Connect to databases or local files, analyze data with natural language queries, and manage project sessions seamlessly.

## Features

- **Multi-source data loading** - Connect to databases (SQLite, MySQL, PostgreSQL, Oracle, SQL Server, MariaDB, ODBC) or load local CSV/Excel files
- **AI-powered analysis** - Query your data using natural language with OpenAI or Claude AI
- **Project management** - Create and organize multiple projects with persistent chat sessions
- **NLP table selection** - Automatically discover relevant tables using semantic understanding
- **Theme support** - Dark, light, and system theme options
- **Cross-platform** - Built with PyQt6 for Windows, macOS, and Linux

## Requirements

- Python 3.10 or higher
- PostgreSQL/MySQL/SQLite/Oracle/SQL Server (optional, for database connections)
- API key for OpenAI or Anthropic Claude

## Installation

1. Clone or download the project:
```bash
cd Projects
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

### Output

The compiled executable will be created in `main.dist/` directory. You can distribute this as a standalone application.

## Configuration

Configuration is stored in `config.json`. You can:
- Set your default AI provider (openai or claude)
- Configure table selection method (manual or nlp)
- Save theme preference

API keys are securely stored in the config file.

## Project Structure

```
├── main.py                          # Application entry point
├── gui_frontend_markdown.py         # UI components
├── gui_backend_markdown.py          # Backend logic
├── agents.py                        # AI agent for queries
├── connector.py                     # Database connector
├── processing.py                    # Data processing utilities
├── config.py                        # Configuration management
├── logger.py                        # Logging setup
├── data/                            # Sample data files
├── docs/                            # Documentation
└── requirements.txt                 # Python dependencies
```

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

## License

© 2026 AI Data Workspace. All rights reserved.

## Support

For issues or questions, refer to the documentation in the `docs/` directory or contact support.
