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
nuitka main.py --standalone --windows-console-mode=hide --enable-plugin=pyside6 --enable-plugin=anti-bloat --follow-imports --nofollow-import-to=torch._dynamo --nofollow-import-to=torch._inductor --nofollow-import-to=torch.fx --include-data-dir=css=css --include-data-files=icon.svg=icon.svg --lto=yes --clang --assume-yes-for-downloads --remove-output --output-dir=dist --python-flag=no_site --python-flag=no_asserts --python-flag=-O
```

API keys are insecurely stored in the config file. This is a temporary measure, users are encouraged to store their API keys as environment variables for better security. Future versions will implement a more secure key management system.
