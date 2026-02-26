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
pip install pyinstaller
```

### Compile for Windows

```bash
pyinstaller main.py --name "Data Workspace" --onedir --noconsole --add-data "css;css" --add-data "icon.svg;." --add-data "models;models" --hidden-import=PySide6.QtCore --hidden-import=PySide6.QtGui --hidden-import=PySide6.QtWidgets --hidden-import=anthropic --hidden-import=openai --hidden-import=altair --hidden-import=sqlalchemy --hidden-import=sqlalchemy.dialects.sqlite --hidden-import=sqlalchemy.dialects.mysql --hidden-import=sqlalchemy.dialects.postgresql --hidden-import=sqlalchemy.dialects.mssql --hidden-import=sqlalchemy.dialects.oracle --hidden-import=pandas --hidden-import=sentence_transformers --hidden-import=torch --hidden-import=transformers --hidden-import=markdown --hidden-import=pymysql --hidden-import=psycopg2 --hidden-import=pyodbc --hidden-import=cx_Oracle --hidden-import=oracledb --exclude-module=torch._dynamo --exclude-module=torch._inductor --exclude-module=torch.fx --exclude-module=torch.compiler --exclude-module=torch.distributed --exclude-module=torch.testing --exclude-module=torch.utils.tensorboard --exclude-module=transformers.models --exclude-module=transformers.generation --exclude-module=transformers.pipelines --exclude-module=transformers.trainer --exclude-module=transformers.training_args --exclude-module=tensorboard --exclude-module=tensorflow --exclude-module=keras --exclude-module=scipy --exclude-module=matplotlib --exclude-module=PIL --exclude-module=cv2 --exclude-module=IPython --exclude-module=jupyter --exclude-module=notebook --exclude-module=pytest --exclude-module=unittest --exclude-module=setuptools --exclude-module=pip --exclude-module=torchaudio --exclude-module=torchvision --exclude-module=triton --exclude-module=sympy --exclude-module=networkx --exclude-module=flash_attn --exclude-module=apex --exclude-module=bitsandbytes --exclude-module=datasets --exclude-module=evaluate --exclude-module=accelerate --exclude-module=tkinter --exclude-module=tcl --exclude-module=tk --distpath=dist --workpath=build --clean
```

API keys are insecurely stored in the config file. This is a temporary measure, users are encouraged to store their API keys as environment variables for better security. Future versions will implement a more secure key management system.
