"""
Application-wide constants and configuration.

Centralizes constants used across modules for easier maintenance
and configuration changes.
"""

# LLM Configuration
DEFAULT_LLM_PROVIDER = "openai"
LLM_MAX_TOKENS_DEFAULT = 800
LLM_MAX_TOKENS_CODE = 1000
LLM_MAX_TOKENS_ANALYSIS = 1000

LLM_TEMPERATURE_DEFAULT = 0.3
LLM_TEMPERATURE_CODE = 0.4
LLM_TEMPERATURE_ANALYSIS = 0.6

# Note that these may not be available, so changing them may be necessary. Users should check the latest model availability from OpenAI and Anthropic.
LLM_MODELS = {
    "claude": "claude-3-5-sonnet-20241022",
    "openai": "gpt-4o-2024-08-06",
}  # TODO: Add model management UI?

# LLM Prompt Templates
PLANNER_SYSTEM_PROMPT_TEMPLATE = """You are a senior data analysis planner responsible for translating a user’s question into a precise, executable analysis plan based ONLY on the provided DataFrame schema and sample.

You do NOT write code. You ONLY produce a structured plan.

DATAFRAME METADATA
Columns: $columns
Shape: $shape  # (rows, columns)
Data types: $dtypes
Sample rows:
${sample}

USER QUESTION
$user_query

PLANNING RULES
- Base all reasoning ONLY on the provided columns and data types
- NEVER assume columns or data that are not listed
- If the question cannot be answered with available data, mark task_type="unsupported"
- Prefer the simplest valid approach
- Visualization is required only if it meaningfully improves interpretation
- Code is required if computation, grouping, filtering, statistics, or plotting is needed
- Summary-only tasks require no computation

OUTPUT SCHEMA (STRICT JSON ONLY)
{
    "task_type": "analysis" | "visualization" | "summary" | "transformation" | "unsupported",
    "objective": "one-sentence description of what will be computed or examined",
    "analysis_focus": ["specific metrics, segments, or relationships to evaluate"],
    "steps": ["ordered atomic actions referencing exact column names"],
    "requires_code": true | false,
    "requires_visualization": true | false,
    "expected_result_type": "scalar" | "table" | "chart" | "text" | "unknown"
}

Return ONLY valid JSON. No markdown. No commentary.
"""

CODE_GENERATION_SYSTEM_PROMPT_TEMPLATE = """You are a production-grade Python data analysis engine that generates safe, deterministic pandas and Altair code from an approved analysis plan.

You operate in a restricted execution environment.

DATAFRAME METADATA
Columns: $columns
Data types: $dtypes
Shape: $shape
Sample rows:
${sample}

APPROVED PLAN
$plan

EXECUTION CONTRACT
- The DataFrame is preloaded as: df
- NEVER modify df in-place
- ALL outputs must be assigned to a variable named: result
- result must match the plan’s expected_result_type when possible
- Use only referenced columns from the schema
- Handle nulls and type issues defensively
- Prefer vectorized pandas operations
- Avoid unnecessary computation or copies

VISUALIZATION CONTRACT (ALTair ONLY)
- Use Altair for ALL charts
- Charts must be saved to a PNG file
- Save ONLY to: tempfile.gettempdir()
- Use: tempfile.NamedTemporaryFile(delete=False, suffix=".png")
- Save via: chart.save(file_path)
- result must contain {"chart_path": path}

FORMAT & READABILITY
- Round floats to 2–4 decimals when appropriate
- Convert timestamps to readable strings if returned
- Sort grouped outputs for interpretability
- Use clear column names in outputs

SECURITY RULES (MANDATORY)
- FORBIDDEN: to_csv, to_excel, to_json, to_parquet, to_sql, to_pickle with paths
- FORBIDDEN: os.system, subprocess, eval, exec, __import__
- FORBIDDEN: writing outside tempfile directory
- FORBIDDEN: network or shell access
- Assume all inputs are untrusted
- Do not access environment variables or filesystem except tempfile

FAILURE HANDLING
If the plan cannot be executed with given data:
    result = {"error": "reason"}

OUTPUT
Return ONLY executable Python code.
No markdown.
No explanations.
"""
# TODO: check if this fixes image issue
ANALYSIS_SYSTEM_PROMPT_TEMPLATE = """You are a clear, practical data analyst explaining results from a DataFrame analysis to non-technical stakeholders.

CONTEXT
$context

Write a concise explanation that:

1. Answers the user’s question directly in 1–2 sentences.
2. Summarizes the key supporting values, comparisons, or trends from the result.
3. Explains what the finding means in plain language and why it matters.
4. Notes any important limitations, assumptions, or missing data if relevant.
5. Do not mention temporary file paths.
6. Code details can be mentioned ONLY if they aid understanding.

STYLE
- Use concrete numbers from the result
- Do not speculate beyond provided data
- Prefer clarity over technical jargon
- Keep length moderate (≈120–180 words)
"""


# Database Configuration
SQLITE_DEFAULT_PORT = 3306
MYSQL_DEFAULT_PORT = 3306
POSTGRESQL_DEFAULT_PORT = 5432
ODBC_DEFAULT_DRIVER = "ODBC Driver 17 for SQL Server"

# Data Processing
MERGE_COMMON_COLS_THRESHOLD = 0.7  # % of columns that must be common
MERGE_KEY_PATTERNS = ["id", "key", "index", "name", "code"]
MERGE_MAX_ESTIMATED_ROWS = 10_000_000
MERGE_MAX_ROW_MULTIPLIER = 20
MERGE_WARN_DUPLICATE_RATE = 0.1

# DataFrame Preview
SAMPLE_ROWS_DEFAULT = 5
SAMPLE_ROWS_INFO = 3

# File Size / Performance
MAX_DATAFRAME_ROWS_WARNING = 1_000_000

# HTML/Markdown Conversion
MARKDOWN_CSS_TABLE_STYLE = (
    "border-collapse: collapse; width: 100%; margin: 10px 0; font-family: monospace;"
)
MARKDOWN_CSS_TABLE_HEADER = "background-color: #f0f0f0;"
MARKDOWN_CSS_TABLE_CELL = "border: 1px solid #999; padding: 10px;"
MARKDOWN_CSS_TABLE_CELL_ALT = "#f9f9f9"

MARKDOWN_CSS_CODE_BLOCK = "background-color: #f5f5f5; padding: 10px; border-radius: 5px; overflow-x: auto; white-space: pre-wrap;"

# Image Handling
SUPPORTED_IMAGE_FORMATS = {
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".gif": "image/gif",
    ".svg": "image/svg+xml",
    ".webp": "image/webp",
}

# GUI Configuration
PLACEHOLDER_PROJECT_NAMES = [
    "Sales Analysis 2026",
    "Customer Segmentation",
    "Market Research",
    "Financial Forecasting",
    "Product Performance",
    "Social Media Insights",
    "Website Analytics",
    "Inventory Management",
    "Employee Productivity",
    "Customer Feedback",
]

PLACEHOLDER_PROJECT_DESCRIPTIONS = [
    "Analyze Q1 sales trends and opportunities.",
    "Segment customers by behavior and demographics.",
    "Research market trends for new products.",
    "Forecast financial performance.",
    "Evaluate regional product performance.",
    "Analyze social media engagement.",
    "Examine website traffic patterns.",
    "Optimize inventory levels.",
    "Assess employee productivity.",
    "Analyze customer feedback.",
]

# API Security
API_KEY_PATTERNS = {"openai": "sk-", "claude": "sk-ant-"}

# Logging
LOG_LEVEL_DEFAULT = "INFO"
LOG_FILE_NAME = "app.log"
LOG_DIR_NAME = "logs"




# ============================================================================
# Theme Stylesheets
# ============================================================================

DARK_THEME_STYLESHEET = """
    QMainWindow, QDialog, QWidget {
        background-color: #1e1e1e;
        color: #e0e0e0;
    }
    
    QLabel {
        color: #e0e0e0;
    }
    
    QTextEdit, QLineEdit {
        background-color: #2d2d2d;
        color: #e0e0e0;
        border: 1px solid #3d3d3d;
        border-radius: 4px;
        padding: 5px;
    }
    
    QTextEdit:focus, QLineEdit:focus {
        border: 1px solid #0d47a1;
    }
    
    QPushButton {
        background-color: #0d47a1;
        color: #ffffff;
        border: none;
        border-radius: 4px;
        padding: 5px 15px;
        font-weight: bold;
    }
    
    QPushButton:hover {
        background-color: #1565c0;
    }
    
    QPushButton:pressed {
        background-color: #0d3f8f;
    }
    
    QPushButton:disabled {
        background-color: #424242;
        color: #666666;
    }
    
    QComboBox {
        background-color: #2d2d2d;
        color: #e0e0e0;
        border: 1px solid #3d3d3d;
        border-radius: 4px;
        padding: 5px;
    }
    
    QComboBox::drop-down {
        border: none;
    }
    
    QComboBox QAbstractItemView {
        background-color: #2d2d2d;
        color: #e0e0e0;
        selection-background-color: #0d47a1;
    }
    
    QListWidget {
        background-color: #2d2d2d;
        color: #e0e0e0;
        border: 1px solid #3d3d3d;
        border-radius: 4px;
    }
    
    QListWidget::item:selected {
        background-color: #0d47a1;
    }
    
    QListWidget::item:hover {
        background-color: #383838;
    }
    
    QScrollBar:vertical {
        background-color: #2d2d2d;
        width: 12px;
        border: none;
    }
    
    QScrollBar::handle:vertical {
        background-color: #505050;
        border-radius: 6px;
        min-height: 20px;
    }
    
    QScrollBar::handle:vertical:hover {
        background-color: #606060;
    }
    
    QScrollBar:horizontal {
        background-color: #2d2d2d;
        height: 12px;
        border: none;
    }
    
    QScrollBar::handle:horizontal {
        background-color: #505050;
        border-radius: 6px;
        min-width: 20px;
    }
    
    QScrollBar::handle:horizontal:hover {
        background-color: #606060;
    }
    
    QMenuBar {
        background-color: #2d2d2d;
        color: #e0e0e0;
        border-bottom: 1px solid #3d3d3d;
    }
    
    QMenuBar::item:selected {
        background-color: #0d47a1;
    }
    
    QMenu {
        background-color: #2d2d2d;
        color: #e0e0e0;
        border: 1px solid #3d3d3d;
    }
    
    QMenu::item:selected {
        background-color: #0d47a1;
    }
    
    QHeaderView::section {
        background-color: #2d2d2d;
        color: #e0e0e0;
        padding: 5px;
        border: 1px solid #3d3d3d;
    }
"""

LIGHT_THEME_STYLESHEET = """
    QMainWindow, QDialog, QWidget {
        background-color: #ffffff;
        color: #000000;
    }
    
    QLabel {
        color: #000000;
    }
    
    QTextEdit, QLineEdit {
        background-color: #f5f5f5;
        color: #000000;
        border: 1px solid #d0d0d0;
        border-radius: 4px;
        padding: 5px;
    }
    
    QTextEdit:focus, QLineEdit:focus {
        border: 1px solid #1976d2;
    }
    
    QPushButton {
        background-color: #1976d2;
        color: #ffffff;
        border: none;
        border-radius: 4px;
        padding: 5px 15px;
        font-weight: bold;
    }
    
    QPushButton:hover {
        background-color: #1565c0;
    }
    
    QPushButton:pressed {
        background-color: #0d47a1;
    }
    
    QPushButton:disabled {
        background-color: #e0e0e0;
        color: #999999;
    }
    
    QComboBox {
        background-color: #f5f5f5;
        color: #000000;
        border: 1px solid #d0d0d0;
        border-radius: 4px;
        padding: 5px;
    }
    
    QComboBox::drop-down {
        border: none;
    }
    
    QComboBox QAbstractItemView {
        background-color: #ffffff;
        color: #000000;
        selection-background-color: #1976d2;
    }
    
    QListWidget {
        background-color: #f5f5f5;
        color: #000000;
        border: 1px solid #d0d0d0;
        border-radius: 4px;
    }
    
    QListWidget::item:selected {
        background-color: #1976d2;
        color: #ffffff;
    }
    
    QListWidget::item:hover {
        background-color: #eeeeee;
    }
    
    QScrollBar:vertical {
        background-color: #f5f5f5;
        width: 12px;
        border: none;
    }
    
    QScrollBar::handle:vertical {
        background-color: #c0c0c0;
        border-radius: 6px;
        min-height: 20px;
    }
    
    QScrollBar::handle:vertical:hover {
        background-color: #a0a0a0;
    }
    
    QScrollBar:horizontal {
        background-color: #f5f5f5;
        height: 12px;
        border: none;
    }
    
    QScrollBar::handle:horizontal {
        background-color: #c0c0c0;
        border-radius: 6px;
        min-width: 20px;
    }
    
    QScrollBar::handle:horizontal:hover {
        background-color: #a0a0a0;
    }
    
    QMenuBar {
        background-color: #f5f5f5;
        color: #000000;
        border-bottom: 1px solid #d0d0d0;
    }
    
    QMenuBar::item:selected {
        background-color: #1976d2;
        color: #ffffff;
    }
    
    QMenu {
        background-color: #ffffff;
        color: #000000;
        border: 1px solid #d0d0d0;
    }
    
    QMenu::item:selected {
        background-color: #1976d2;
        color: #ffffff;
    }
    
    QHeaderView::section {
        background-color: #f5f5f5;
        color: #000000;
        padding: 5px;
        border: 1px solid #d0d0d0;
    }
"""