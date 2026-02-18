# Installation Guide

## System Requirements

- **Operating System**: Windows, macOS, or Linux
- **Python**: 3.10 or higher
- **RAM**: 4GB minimum (8GB+ recommended)
- **Disk Space**: 2GB for dependencies and logs

## Step-by-Step Installation

### 1. Clone/Download Repository

```bash
cd d:\Projects
```

Or download and extract the project folder.

### 2. Set Up Python Environment

**Option A: Using venv (Recommended)**

```bash
# Navigate to project directory
cd d:\Projects

# Create virtual environment
python -m venv .venv

# Activate virtual environment
# On Windows:
.venv\Scripts\activate

# On macOS/Linux:
source .venv/bin/activate
```

**Option B: Using conda**

```bash
conda create -n data_workspace python=3.10
conda activate data_workspace
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### Manual Dependency Installation

If `requirements.txt` is unavailable:

```bash
# Core dependencies
pip install PyQt6
pip install openai
pip install anthropic
pip install openpyxl
pip install tabulate

# Database support
pip install sqlalchemy
pip install mysql-connector-python
pip install psycopg2-binary
pip install pyodbc
pip install cx_Oracle

# Visualization
pip install altair
pip install chardet

# Utilities
pip install pydantic
pip install requests
pip install python-markdown
```

### 4. Verify Installation

```bash
python -c "import openpyxl; import tabulate; import PyQt6; import openai; print('Installation successful')"
```

## API Key Setup

### Option 1: On First Launch (Recommended)

1. Start the application:
   ```bash
   python gui_frontend.py
   ```

2. Follow the API Key Configuration dialog
3. Enter your OpenAI and/or Claude API keys
4. Keys are saved in `config.json`

### Option 2: Manual Setup

1. Get your API keys:
   - [OpenAI](https://platform.openai.com/api-keys)
   - [Anthropic](https://console.anthropic.com/account/keys)

2. Create/edit `config.json`:
   ```json
   {
     "api_keys": {
       "openai": "sk-YOUR_KEY_HERE",
       "claude": "sk-ant-YOUR_KEY_HERE"
     },
     "default_api": "openai"
   }
   ```

3. Save and restart the application

### Option 3: Environment Variables

Instead of storing in `config.json`, use environment variables:

```bash
# Windows
set OPENAI_API_KEY=sk-YOUR_KEY_HERE
set ANTHROPIC_API_KEY=sk-ant-YOUR_KEY_HERE

# macOS/Linux
export OPENAI_API_KEY=sk-YOUR_KEY_HERE
export ANTHROPIC_API_KEY=sk-ant-YOUR_KEY_HERE
```

The `ConfigManager` will check environment variables as fallback.

## Database Driver Installation

### SQLite

Included with Python. No installation needed.

### MySQL/MariaDB

```bash
pip install mysql-connector-python
```

Or using conda:
```bash
conda install mysql-connector-python
```

### PostgreSQL

```bash
pip install psycopg2-binary
```

### SQL Server

```bash
pip install pyodbc
```

Windows ODBC driver setup:
1. Download ODBC Driver 17 for SQL Server from Microsoft
2. Run the installer
3. Select the ODBC driver in the connection dialog

### Oracle

```bash
pip install cx_Oracle
```

**Note**: Requires Oracle Client library installed separately.

## Verification Steps

### 1. Test Python Installation

```bash
python --version
# Should output: Python 3.10.x or higher
```

### 2. Test Virtual Environment

```bash
# Active venv shows in prompt like: (.venv) C:\Projects>
python -m pip --version
# Should show path to .venv/Scripts/pip
```

### 3. Test Package Imports

```bash
python
>>> import openpyxl
>>> import tabulate
>>> import PyQt6
>>> from openai import AsyncOpenAI
>>> from anthropic import Anthropic
>>> print("All imports successful")
```

### 4. Test Database Connection

```bash
python connector.py  # If it has a test block
# Or test manually:
python
>>> from connector import DatabaseConnector
>>> db = DatabaseConnector()
>>> print(db.get_available_libraries())
```

### 5. Test Application Launch

```bash
python gui_frontend.py
```

The GUI should open. If it hangs on "API Key Configuration", you need to set up API keys.

## Troubleshooting Installation

### "Python not found" or "python: command not found"

**Windows**:
- Ensure Python is in System PATH
- Or use full path: `C:\Python310\python.exe gui_frontend.py`
- Reinstall Python with "Add Python to PATH" checked

**macOS/Linux**:
```bash
# Make sure correct Python version is used
which python3
python3 --version
```

### "ModuleNotFoundError: No module named 'openpyxl'"

```bash
# Ensure virtual environment is activated
# Then reinstall:
pip install --upgrade pip
pip install openpyxl
```

### "No module named 'PyQt6'"

```bash
pip install --upgrade PyQt6
# If that fails:
pip uninstall PyQt6
pip install PyQt6==6.6.1
```

### "SSL: CERTIFICATE_VERIFY_FAILED"

Affects macOS and some Windows setups:

```bash
# Install certificates (macOS)
/Applications/Python\ 3.10/Install\ Certificates.command

# Or modify pip call (all platforms)
pip install --trusted-host pypi.python.org --trusted-host files.pythonhosted.org openpyxl
```

### Database Driver Issues

**MySQL**: 
```bash
pip uninstall mysql-connector-python
pip install mysql-connector-python==8.0.33
```

**PostgreSQL**:
```bash
pip install --upgrade psycopg2-binary
```

**SQL Server**:
- Windows: Install ODBC Driver 17 from Microsoft
- Linux: `sudo apt-get install odbc-postgresql`
- macOS: `brew install odbcinstal unixodbc`

## Uninstallation

To remove the application:

```bash
# Deactivate virtual environment
deactivate

# Remove venv folder
rmdir /s .venv  # Windows
rm -rf .venv    # macOS/Linux

# Remove project directory
cd ..
rmdir /s Projects  # Windows
rm -rf Projects    # macOS/Linux
```

To remove from conda:
```bash
conda deactivate
conda env remove --name data_workspace
```

## Supported Python Versions

- ✅ Python 3.10
- ✅ Python 3.11
- ✅ Python 3.12
- ⚠️ Python 3.9 (may work but older than recommended)
- ❌ Python 2.x (not supported)

## Supported Operating Systems

| OS | Status | Notes |
|---|---|---|
| Windows 10/11 | ✅ Fully supported | Recommended |
| macOS 11+ | ✅ Fully supported | Use python3, not python |
| Ubuntu 20.04+ | ✅ Fully supported | May need extra deps |
| Other Linux | ⚠️ Should work | Database drivers may need setup |

## Getting Help

1. **Check logs**: `logs/app.log`
2. **Verify installation**: Run verification steps above
3. **Test with sample data**: Use files in `data/` folder
4. **Review error messages**: Full trace in logs
