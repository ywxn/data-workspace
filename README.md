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

| Menu | Option | Shortcut | Description |
|------|--------|----------|-------------|
| **File** | New Project | Ctrl+N | Create a new project |
| | Load Project | Ctrl+O | Load a previously saved project |
| | Save Project | Ctrl+S | Save the current project |
| | Connect Data Source | — | Add a new database or file data source |
| | Export Results | — | Export query results to file |
| | Export Chat | — | Export the current chat session to file |
| | Exit | — | Close the application |
| **View** | Theme | — | Switch between Dark, Light, and System themes |
| | Font Size | Ctrl++/Ctrl+- | Increase or decrease font size |
| **Settings** | API Settings | — | Configure API keys for OpenAI or Claude |
| | Model Settings | — | Set default model for the active provider |
| | Local LLM Settings | — | Configure local LLM server or model hosting |
| | AI Host Settings | — | Configure cloud provider (OpenAI/Claude) or local LLM |
| | Table Selection Method | — | Choose between Manual or NLP-based table selection |
| | Prompt Expansion | — | Enable/disable automatic prompt enrichment |
| | Interaction Mode | — | Switch between CxO (executive) or Analyst mode |
| **Tools** | Clear Conversation | — | Clear the current chat history |
| | Reset Workspace | — | Reset to initial state and remove all data |
| **Help** | Documentation | — | Open the online documentation |
| | About | — | Show application version and information |

## Compilation

To compile the application into a standalone executable:

### Prerequisites

```bash
pip install pyinstaller
```

### Compile for Windows

```bash
pyinstaller --distpath ./dist --workpath ./build --name "Data Workspace" --console --add-data "icon.svg:." --add-data "css:css" --hidden-import "PySide6.QtCore" --hidden-import "PySide6.QtGui" --hidden-import "PySide6.QtWidgets" --hidden-import "anthropic" --hidden-import "openai" --hidden-import "altair" --hidden-import "sqlalchemy.dialects.sqlite" --hidden-import "sqlalchemy.dialects.mysql" --hidden-import "sqlalchemy.dialects.postgresql" --hidden-import "sqlalchemy.dialects.mssql" --hidden-import "sqlalchemy.dialects.oracle" --hidden-import "pymysql" --hidden-import "psycopg2" --hidden-import "pyodbc" --hidden-import "cx_Oracle" --hidden-import "oracledb" --hidden-import "sentence_transformers.SentenceTransformer" --hidden-import "transformers.models.auto.modeling_auto" --hidden-import "vl_convert" --exclude-module "tensorflow" --exclude-module "keras" --exclude-module "tensorboard" --exclude-module "tkinter" main.py --clean --noconfirm
```

API keys are stored securely using the OS keyring (via the `keyring` package). If keyring is unavailable, the application falls back gracefully.

## Architecture Contracts

### Model Selection and Provider Management

The application enforces a **three-tier model resolution precedence**:

1. **Session override** — per-chat or per-project model selection (highest priority)
2. **Provider default** — user-configured default model for each provider (OpenAI, Claude)
3. **System fallback** — hardcoded defaults from `constants.py` (lowest priority)

This allows users to:
- Set a preferred model per provider (e.g., always use `gpt-4o` for OpenAI)
- Override at the session level for specific analyses requiring different capabilities
- Fall back to sensible defaults when no explicit choice is made

**Configuration locations:**
- Provider defaults: stored in `config.json` under `model_defaults`
- Session overrides: stored in project files alongside chat history
- System fallback: defined in `constants.py` as `LLM_MODELS`

### Unified Memory and Query History

The application maintains a **hybrid unified memory system** that tracks all prompts, generated SQL, execution results, and metadata:

- **Project-scoped records** — each project maintains its own query history
- **Optional global index** — cross-project memory for reusing common patterns (stored in `data/`)
- **Configurable retention policies**:
  - `keep_all` — preserve all query history indefinitely
  - `rolling_n` — keep only the most recent N queries per project
  - `ttl_days` — automatically expire queries older than X days

**Memory storage includes:**
- Original user prompt
- Normalized prompt (for similarity matching)
- Generated SQL query
- Execution metadata (status, row count, execution time)
- Model and provider context (which model generated the query)

This enables:
- Faster responses for repeated or similar questions
- Learning from past corrections and refinements
- Audit trails for compliance and debugging

### Clarification Flow

When the agent detects **ambiguous business meaning** (e.g., unclear ID codes, unmapped terminology), it triggers a **pre-SQL clarification stage**:

1. **Must-guess detector** — identifies when the agent would need to infer unknown business context
2. **Clarification prompt** — asks a targeted follow-up question
3. **Chat-loop pause/resume** — waits for user response, then continues with enriched context

This prevents:
- Incorrect assumptions about business logic
- Silent failures from guessing column meanings
- Wasted compute on invalid SQL generation

**Clarification trigger policy:**
- Enabled by default for CxO and Analyst modes
- Can be disabled in settings for users who prefer speed over accuracy
- Bypassed when semantic layer provides sufficient business context

### Rollout Boundaries and Success Criteria

**Stage 1 (Finished):** Core architecture and configuration contracts established
- ✅ Architecture contracts documented
- ✅ Config schema extended for retention and model settings

**Stage 2 (Finished):** Model selection system fully implemented and tested
- ✅ Model selection persists and is used at runtime
- ✅ Precedence rule enforced correctly in all code paths

**Stage 3 (Finished):** Clarification flow implemented and tested
- ✅ Ambiguous requests trigger clarification (not all requests)
- ✅ Clarifications do not break chat flow or lose context

**Stage 4 (Next):** Memory system fully implemented and integrated
- Repeated prompts produce memory cache hits
- Retention policies prune correctly without data loss

**Stage 5 (Future):** Enhanced visualization features
- Interactive tables/graphs function without breaking static render/export

**Stage 6 (Future):** Full refactor for separation of concerns
- No functional regressions in query generation, execution, or analysis
- Separation of concerns improves testability and maintainability

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
