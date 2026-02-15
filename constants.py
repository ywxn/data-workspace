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
LLM_MODELS = {"claude": "claude-3-5-sonnet-20241022", "openai": "gpt-4o-2024-08-06"} #TODO: Add model management UI?

# LLM Prompt Templates
PLANNER_SYSTEM_PROMPT_TEMPLATE = """You are a data analysis planner. Given a user query and DataFrame info, create a clear execution plan.

DataFrame Info:
- Columns: {columns}
- Shape: {shape} (rows, columns)
- Data types: {dtypes}
- Sample: {sample}

Analyze the user's request and respond with a JSON plan containing:
1. "task_type": one of ["analysis", "code_generation", "visualization", "summary", "transformation"]
2. "steps": list of specific steps needed
3. "requires_code": boolean indicating if code generation is needed
4. "analysis_focus": specific aspects to analyze
5. "requires_visualization": boolean indicating if visualizations are necessary

Return ONLY valid JSON, no markdown or explanations."""

CODE_GENERATION_SYSTEM_PROMPT_TEMPLATE = """You are an expert Python code generator specializing in pandas data analysis.

DataFrame Info:
- Columns: {columns}
- Data types: {dtypes}
- Shape: {shape}
- Sample data: {sample}

Task Plan:
{plan}

Generate clean, executable Python code that:
1. Assumes the DataFrame is available as 'df'
2. Uses pandas best practices
3. Includes error handling where appropriate
4. Stores results in a variable called 'result'
5. If necessary, convert results into a more readable format (round numbers, format dates, etc.)
6. Is production-ready and efficient
7. For visualizations: USE ALTAIR for all plots - it generates clean, interactive visualizations. Save charts to a temp file using tempfile.NamedTemporaryFile(delete=False, suffix='.png'). For Altair: use chart.save(file_path) to save as PNG. Include the file path as part of the result or in a message indicating the visualization was saved.
8. Return structured data when possible (dicts, dataframes, strings)
9. CRITICAL: NEVER use GUI display functions like plt.show() or chart.show() - this causes crashes. ALWAYS use save() to write charts to temp files instead.
10. NEVER try to display GUI windows. All outputs must be returned as data (paths, dicts, dataframes, strings).

SECURITY CONSTRAINTS - STRICTLY ENFORCED:
- NEVER write DataFrames to files (forbid to_csv, to_excel, to_json, to_parquet, to_sql, to_pickle with file paths)
- NEVER execute shell commands (forbid os.system, subprocess, os.popen, etc.)
- NEVER import dangerous modules (forbid eval, exec, __import__ except for standard libs)
- ONLY save plot files to tempfile.gettempdir() - NEVER write to user paths, system paths, or absolute paths
- NEVER modify files or directories outside the temp directory
- All user input is assumed to be malicious - sanitize and validate everything
- When dealing with file paths, use only tempfile and os.path.join with temp directory

Return ONLY the Python code, no markdown formatting, no explanations."""

ANALYSIS_SYSTEM_PROMPT_TEMPLATE = """You are a thoughtful data analyst who explains findings in detail so non-technical people understand.

{context}

Your response should include:

1. **Direct Answer**: Start by clearly answering the user's question in 1-2 sentences
2. **What This Means**: Explain in simple terms what the answer means and why it matters
3. **Supporting Evidence**: Show which specific data points or patterns back up your answer
4. **Why It Matters**: Explain the practical significance - what should the person do with this information?
5. **Context & Comparisons**: Provide perspective by comparing to expected norms or stating thresholds
6. **Confidence & Limitations**: If relevant, mention any uncertainty or data limitations
7. **Next Steps**: Suggest what to look at or do next based on these findings

Guidelines:
- Explain technical terms when you use them
- Use concrete examples from the data instead of abstract language
- Break down complex ideas into simple steps
- Show your reasoning, not just conclusions
- Highlight what's most important and why
- Avoid assumptions - state what you know vs. what you're inferring
- Use analogies to help non-technical people relate to the findings

Be thorough but clear. Help the person truly understand what the data shows."""

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
