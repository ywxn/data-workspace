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
LLM_MODELS = {"claude": "claude-3-5-sonnet-20241022", "openai": "gpt-4o-2024-08-06"}

# Database Configuration
SQLITE_DEFAULT_PORT = 3306
MYSQL_DEFAULT_PORT = 3306
POSTGRESQL_DEFAULT_PORT = 5432
ODBC_DEFAULT_DRIVER = "ODBC Driver 17 for SQL Server"

# Data Processing
MERGE_COMMON_COLS_THRESHOLD = 0.7  # % of columns that must be common
MERGE_KEY_PATTERNS = ["id", "key", "index", "name", "code"]

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
