## Requirements

- Python 3.10 or higher
- PostgreSQL/MySQL/SQLite/Oracle/SQL Server (optional, for database connections)
- API key for OpenAI or Anthropic Claude, **or** a local LLM server (e.g. Ollama)

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

3. Run the application — you'll be prompted to configure an AI provider and create a project on first startup.

## Usage

### Running the Application

```bash
python main.py
```

Or run the compiled executable (see Compilation section below).

### Basic Workflow

1. Launch the application
2. Configure your AI provider when prompted (OpenAI, Claude, Local LLM, or Self-Host Model)
3. Select an interaction mode — **CxO** (executive summaries) or **Analyst** (detailed technical analysis)
4. Create a new project or load an existing one
5. Connect a data source (database or files)
6. Use the chat interface to query your data with natural language

### Features

- **Natural language queries** — ask questions about your data without writing SQL
- **Interaction modes** — CxO mode delivers concise executive insights; Analyst mode provides detailed technical analysis with column names and methodology
- **NLP table selection** — tables are automatically selected based on your question using a local embedding model
- **Semantic layer import** — import a JSON mapping of business-friendly table/column descriptions for more accurate query understanding. See ![Semantic Layer Documentation](https://github.com/ywxn/data-workspace/blob/test/semantic_layer_documentation.md) for more information.
- **Multiple data sources** — connect additional databases or files without overwriting existing data (File → Connect Additional Data Source)
- **Project management** — organize analyses into projects with persistent chat histories, create multiple chat sessions per project
- **Export** — export query results or chat sessions to file (File → Export Results / Export Chat)
- **Themes** — switch between Dark, Light, and System themes (View menu)
- **Font scaling** — increase or decrease font size with Ctrl++ / Ctrl+-

### Menu Reference

| Menu | Action | Shortcut |
|---|---|---|
| File | New Project | Ctrl+N |
| File | Load Project | Ctrl+O |
| File | Save Project | Ctrl+S |
| File | Connect Data Source | — |
| File | Connect Additional Data Source | — |
| File | API Key Settings | — |
| File | AI Host Settings | — |
| File | Export Results | Ctrl+E |
| File | Export Chat | — |
| Settings | Local LLM Settings | — |
| View | Dark / Light / System Theme | — |
| View | Increase Font Size | Ctrl++ |
| View | Decrease Font Size | Ctrl+- |
| Help | Documentation | — |
| Help | About | — |

## Compilation

To compile the application into a standalone executable:

### Prerequisites

```bash
pip install pyinstaller
```

### Compile for Windows

```bash
pyinstaller main.py --name "Data Workspace" --onedir --noconsole --add-data "css;css" --add-data "icon.svg;." --add-data "models;models" --hidden-import=PySide6.QtCore --hidden-import=PySide6.QtGui --hidden-import=PySide6.QtWidgets --hidden-import=anthropic --hidden-import=openai --hidden-import=altair --hidden-import=sqlalchemy --hidden-import=sqlalchemy.dialects.sqlite --hidden-import=sqlalchemy.dialects.mysql --hidden-import=sqlalchemy.dialects.postgresql --hidden-import=sqlalchemy.dialects.mssql --hidden-import=sqlalchemy.dialects.oracle --hidden-import=pandas --hidden-import=sentence_transformers --hidden-import=torch --hidden-import=transformers --hidden-import=markdown --hidden-import=pymysql --hidden-import=psycopg2 --hidden-import=pyodbc --hidden-import=cx_Oracle --hidden-import=oracledb --exclude-module=torch._dynamo --exclude-module=torch._inductor --exclude-module=torch.fx --exclude-module=torch.compiler --exclude-module=torch.distributed --exclude-module=torch.testing --exclude-module=torch.utils.tensorboard --exclude-module=transformers.models --exclude-module=transformers.generation --exclude-module=transformers.pipelines --exclude-module=transformers.trainer --exclude-module=transformers.training_args --exclude-module=tensorboard --exclude-module=tensorflow --exclude-module=keras --exclude-module=scipy --exclude-module=matplotlib --exclude-module=PIL --exclude-module=cv2 --exclude-module=IPython --exclude-module=jupyter --exclude-module=notebook --exclude-module=pytest --exclude-module=unittest --exclude-module=setuptools --exclude-module=pip --exclude-module=torchaudio --exclude-module=torchvision --exclude-module=triton --exclude-module=sympy --exclude-module=networkx --exclude-module=flash_attn --exclude-module=apex --exclude-module=bitsandbytes --exclude-module=datasets --exclude-module=evaluate --exclude-module=accelerate --exclude-module=tkinter --exclude-module=tcl --exclude-module=tk --distpath=dist --workpath=build --clean
```

API keys are stored securely using the OS keyring (via the `keyring` package). If keyring is unavailable, the application falls back gracefully.

## Fully Local Setup (No Cloud APIs)

You can run the entire pipeline without any cloud API calls by using a local LLM server.

### 1. Install and start Ollama

Download and install [Ollama](https://ollama.com/), then pull a model:

```bash
ollama pull mistral
```

Other recommended models: `llama3`, `codellama`, `mixtral`.

### 2. Set provider to Local

In the application:

- Go to **File → AI Host Settings** and select **Local LLM** as the provider
- Or go to **Settings → Local LLM Settings** to configure the URL and model name

Default settings:

| Field | Default |
|---|---|
| Server URL | `http://localhost:11434/v1` |
| Model | `mistral` |

You can also edit `config.json` directly:

```json
{
  "default_api": "local",
  "local_llm_url": "http://localhost:11434/v1",
  "local_llm_model": "mistral"
}
```

### 3. NLP Table Selector

The NLP table selector uses a local embedding model (`sentence-transformers/all-MiniLM-L6-v2`) — no cloud dependency. The model is downloaded automatically on first use and cached in the `./models/` directory.

### 4. Built-in Model Hosting (no separate server needed)

If you don't have Ollama or another local LLM server, the application can **download and host a model for you** using `llama-cpp-python`.

#### One-time setup

```bash
pip install llama-cpp-python
```

For NVIDIA GPU acceleration:
```bash
CMAKE_ARGS="-DGGML_CUDA=on" pip install llama-cpp-python
```

#### Using the built-in host

1. Go to **Settings → Local LLM Settings** and open the **"Host a Model"** tab, or select **Self-Host Model** in **File → AI Host Settings**.
2. Select a model from the catalog (e.g. *Mistral 7B Instruct Q4_K_M*) or browse for your own `.gguf` file.
3. Click **Download Selected Model** — the model is saved to `./models/`.
4. Click **Start Server** — the application starts an OpenAI-compatible server on `http://127.0.0.1:8911/v1`.
5. Enable **"Auto-start server when app launches"** for a fully hands-off experience.

The server URL is automatically configured; no manual URL/model editing is needed.

| Setting | Default |
|---|---|
| Port | `8911` |
| GPU Layers | `0` (CPU only) |
| Context Size | `4096` tokens |

Available models in the built-in catalog:

| Model | Size | Notes |
|---|---|---|
| Mistral 7B Instruct v0.3 Q4_K_M | 4.4 GB | Recommended — fast, capable |
| Qwen 2.5 7B Instruct Q4_K_M | 4.7 GB | Excellent at code and data analysis |
| Llama 3.1 8B Instruct Q4_K_M | 4.9 GB | Updated Llama 3, improved reasoning |
| Llama 3 8B Instruct Q4_K_M | 4.9 GB | Strong general-purpose |
| Gemma 2 9B Instruct Q4_K_M | 5.8 GB | Google's high-quality analytical model |
| Qwen 2.5 Coder 7B Instruct Q4_K_M | 4.7 GB | Specialized for code & SQL generation |
| Phi-3 Mini 4K Instruct Q4 | 2.4 GB | Small & fast, low-resource machines |
| Gemma 2 2B Instruct Q4_K_M | 1.6 GB | Ultra-lightweight, fastest option |

### 5. Alternative local servers

Any OpenAI-compatible API server works. Examples:

- **Ollama** — `ollama pull mistral` then point at `http://localhost:11434/v1`
- **LM Studio** — Enable the local server in settings
- **vLLM** — `vllm serve mistral --api-key token-abc123`

Set the matching URL in the Local LLM settings (Connect to Server tab).
