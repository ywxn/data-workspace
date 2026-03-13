# Data Workspace

## Requirements

- Python 3.10 or higher
- Optional database drivers depending on your data source
- One AI provider:
  - OpenAI API key
  - Anthropic Claude API key
  - Local OpenAI-compatible endpoint (for example Ollama)
  - Self-hosted model through the built-in host

Supported databases:

- SQLite
- MySQL
- MariaDB
- PostgreSQL
- SQL Server
- Oracle
- ODBC connections

Supported file inputs:

- CSV
- Excel (`.xlsx`, `.xls`)

## Installation

1. Clone the repository.

```bash
git clone https://github.com/ywxn/data-workspace.git
cd data-workspace
```

2. Install dependencies.

```bash
pip install -r requirements.txt
```

3. Start the app.

```bash
python main.py
```

On first launch, the app guides you through provider setup, interaction mode selection, project creation or load, and data source connection.

## Usage

### Startup Sequence

Each session follows this startup order:

1. Configure AI provider if missing.
2. Select interaction mode if not set yet.
3. Create a project or load an existing project.
4. Load a data source if the project does not already have one.
5. Start chatting in natural language.

### Workflow by Interaction Mode

#### CxO Mode Workflow

CxO mode is tuned for executive-style answers and a lower-technical experience.

1. Connect a database or files.
2. If using a database, table selection is skipped at load time.
3. For each question, NLP table selection runs automatically using your prompt.
4. The app builds query context from the selected tables and semantic layer (if present).
5. If business meaning is ambiguous, the app asks a clarification question before SQL planning.
6. Responses focus on concise insights and visual output, with reduced technical detail.

Best for:

- Fast summaries and decisions
- Stakeholder readouts
- Low SQL exposure

#### Analyst Mode Workflow

Analyst mode is tuned for deeper technical analysis and transparent query behavior.

1. Connect a database or files.
2. For databases, select tables up front using Manual or NLP selection.
3. The selected schema is loaded into working context.
4. Ask analytical questions and iterate with follow-ups.
5. Clarification prompts appear when business terms are ambiguous.
6. Use detailed answers, methodology, and SQL visibility controls for deeper inspection.

Best for:

- Technical investigation
- Data validation and debugging
- Query-level analysis

### Core Features

- Natural language querying over databases and files
- CxO and Analyst interaction modes
- NLP table selection with local embeddings
- Semantic layer import for business-aware column and table descriptions
- Multi-database query context (with alias-prefixed table names)
- Prompt expansion toggle before NLP table selection
- Query memory with configurable retention
- Export results and chat history
- Theme switching and font scaling

Semantic layer details: [semantic_layer_documentation.md](semantic_layer_documentation.md)

## Menu Reference

| Menu | Option | Shortcut | Description |
|------|--------|----------|-------------|
| **File** | New Project | Ctrl+N | Create a project |
| **File** | Load Project | Ctrl+O | Load a saved project |
| **File** | Save Project | Ctrl+S | Save current project |
| **File** | Connect Data Source | N/A | Connect database or files |
| **File** | Change Selected Tables | N/A | Reselect DB tables and start a fresh chat |
| **File** | Export Results | Ctrl+E | Export current result set |
| **File** | Export Chat | N/A | Export current chat session |
| **File** | Clear Query Cache | N/A | Clear cached query memory entries |
| **File** | Exit | Ctrl+Q | Close application |
| **Edit** | Clear Conversation | N/A | Clear active chat history |
| **Edit** | Reset Workspace | N/A | Reset workspace state and data |
| **View** | Dark Theme / Light Theme / System Theme | N/A | Switch UI theme |
| **View** | Increase Font Size | Ctrl++ | Increase UI/chat font size |
| **View** | Decrease Font Size | Ctrl+- | Decrease UI/chat font size |
| **View** | Show SQL In Responses | N/A | Toggle SQL display in assistant responses |
| **Settings** | API Key Settings | N/A | Configure OpenAI or Claude keys |
| **Settings** | AI Host Settings | N/A | Choose provider (cloud, local, self-host) |
| **Settings** | Model Settings | N/A | Set default provider models |
| **Settings** | Interaction Mode > CxO / Analyst | N/A | Switch interaction mode |
| **Settings** | Prompt Expansion (NLP) | N/A | Toggle prompt expansion before NLP selection |
| **Settings** | Local LLM Settings | N/A | Configure local endpoint and self-host controls |
| **Settings** | Memory Retention Settings | N/A | Configure query memory retention policies |
| **Help** | Documentation | N/A | Open online documentation |
| **Help** | About | N/A | Show app info and version |

## Architecture Contracts

### Model Selection Precedence

Model resolution uses this precedence:

1. Session override (per chat or project)
2. Provider default from `config.json` (`model_defaults`)
3. System fallback from `constants.py` (`LLM_MODELS`)

### Unified Query Memory

The unified memory system stores prompt and SQL execution history with metadata, scoped per project with optional global indexing.

Retention policies:

- `keep_all`
- `rolling_n` (default limit: 100)
- `ttl_days` (default age: 90 days)

Config location: `config.json` -> `memory_retention`

### Clarification Flow

When business meaning is unclear, the app asks a targeted clarification question before query planning. The chat pauses and then resumes with user-provided context.

Clarification is enabled by default and controlled by `clarification_enabled` in `config.json`.

## Fully Local Setup (No Cloud APIs)

You can run the full pipeline without cloud API calls.

### Option A: Local LLM Server (Ollama or compatible)

1. Install Ollama: <https://ollama.com/>
2. Pull a model.

```bash
ollama pull mistral
```

3. In the app, open **Settings > AI Host Settings** and choose **Local LLM**.
4. Or open **Settings > Local LLM Settings** and set:

- URL: `http://localhost:11434/v1`
- Model: `mistral`

### Option B: Built-in Self-Host Model

The app can download and host a GGUF model locally using `llama-cpp-python`.

1. Open **Settings > AI Host Settings** and choose **Self-Host Model**, or use **Settings > Local LLM Settings** and the host tab.
2. Pick a catalog model or browse to a local `.gguf` file.
3. Download model (stored in `./models/`).
4. Start server. Default endpoint is `http://127.0.0.1:8911/v1`.
5. Optionally enable auto-start on launch.

Default host settings:

| Setting | Default |
|---|---|
| Port | `8911` |
| GPU Layers | `0` |
| Context Size | `4096` |

Built-in model catalog:

| Model | Size | Notes |
|---|---|---|
| Mistral 7B Instruct v0.3 (Q4_K_M) | 4.4 GB | Recommended baseline |
| Qwen 2.5 7B Instruct (Q4_K_M) | 4.7 GB | Strong coding and analysis |
| Llama 3.1 8B Instruct (Q4_K_M) | 4.9 GB | Improved reasoning |
| Llama 3 8B Instruct (Q4_K_M) | 4.9 GB | Strong general purpose |
| Gemma 2 9B Instruct (Q4_K_M) | 5.8 GB | Strong analytical quality |
| Qwen 2.5 Coder 7B Instruct (Q4_K_M) | 4.7 GB | SQL and code oriented |
| Phi-3 Mini 4K Instruct (Q4_K_M) | 2.4 GB | Small and fast |
| Gemma 2 2B Instruct (Q4_K_M) | 1.6 GB | Lightweight option |

### Alternative OpenAI-Compatible Servers

- Ollama
- LM Studio
- vLLM

Point Local LLM settings to the matching server URL and model name.

## Compilation

Use the included PyInstaller spec file to build on Windows.

```bash
pip install pyinstaller
pyinstaller "Data Workspace.spec"
```

Build output is generated under `dist/Data Workspace/`.

## Security Note

API keys are stored using OS keyring via `keyring` when available. If keyring is unavailable, the app falls back gracefully.

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

**Stage 4 (Finished):** Memory system fully implemented and integrated
- ✅ Repeated prompts produce memory cache hits
- ✅ Retention policies prune correctly without data loss

**Stage 5 (Future):** Enhanced visualization features
- Interactive tables/graphs function without breaking static render/export

**Stage 6 (Future):** Full refactor for separation of concerns
- No functional regressions in query generation, execution, or analysis
- Separation of concerns improves testability and maintainability