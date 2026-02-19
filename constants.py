"""
Application-wide constants and configuration.

Centralizes constants used across modules for easier maintenance
and configuration changes.
"""

from pathlib import Path

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
PLANNER_SYSTEM_PROMPT_TEMPLATE = """You are a senior data analysis planner responsible for translating a user’s question into a precise, executable analysis plan based ONLY on the provided SQL schema and samples.

You do NOT write SQL. You ONLY produce a structured plan.

SCHEMA METADATA
Tables: $tables
Columns (qualified): $columns
Columns by table: $columns_by_table
Row counts: $row_counts
Column types: $dtypes
Sample rows:
${sample}

USER QUESTION
$user_query

PLANNING RULES
- Base all reasoning ONLY on the provided tables and columns
- NEVER assume columns or data that are not listed
- If the question cannot be answered with available data, mark task_type="unsupported"
- Prefer the simplest valid approach
- Visualization is required only if it meaningfully improves interpretation
- SQL is required if computation, grouping, filtering, statistics, or joins are needed
- Summary-only tasks require no query

OUTPUT SCHEMA (STRICT JSON ONLY)
{
    "task_type": "analysis" | "visualization" | "summary" | "transformation" | "unsupported",
    "objective": "one-sentence description of what will be computed or examined",
    "analysis_focus": ["specific metrics, segments, or relationships to evaluate"],
    "steps": ["ordered atomic actions referencing exact table and column names"],
    "requires_sql": true | false,
    "requires_visualization": true | false,
    "expected_result_type": "scalar" | "table" | "chart" | "text" | "unknown"
}

Return ONLY valid JSON. No markdown. No commentary.
"""

CODE_GENERATION_SYSTEM_PROMPT_TEMPLATE = """You are a production-grade SQL generation engine that produces safe, deterministic SQL from an approved analysis plan.

SCHEMA METADATA
Tables: $tables
Columns (qualified): $columns
Columns by table: $columns_by_table
Row counts: $row_counts
Column types: $dtypes
Sample rows:
${sample}

APPROVED PLAN
$plan

SQL CONTRACT
- Output ONLY a single SELECT query
- Use only the listed tables and columns
- Fully qualify columns when joins are involved
- Prefer explicit JOINs with clear ON conditions
- Avoid SELECT *
- Include ORDER BY for ranked or time-series results
- Limit results when it improves performance, unless full output is required

SECURITY RULES (MANDATORY)
- FORBIDDEN: INSERT, UPDATE, DELETE, DROP, ALTER, TRUNCATE
- FORBIDDEN: multiple statements
- FORBIDDEN: comments or dynamic SQL
- FORBIDDEN: external or network access

FAILURE HANDLING
If the plan cannot be executed with given data:
Return a SQL query that yields a single row with a clear error message in a column named error.

OUTPUT
Return ONLY SQL. No markdown. No explanations.
"""

ANALYSIS_SYSTEM_PROMPT_TEMPLATE = """You are a clear, practical data analyst explaining results from a SQL analysis to non-technical stakeholders.

CONTEXT
$context

Write a concise explanation that:

1. Answers the user’s question directly in 1–2 sentences.
2. Summarizes the key supporting values, comparisons, or trends from the result.
3. Explains what the finding means in plain language and why it matters.
4. Notes any important limitations, assumptions, or missing data if relevant.
5. Do not mention temporary file paths.
6. Query details can be mentioned ONLY if they aid understanding.

STYLE
- Use concrete numbers from the result
- Do not speculate beyond provided data
- Prefer clarity over technical jargon
- Keep length moderate (≈120–180 words)
GUARDRAILS (MANDATORY)
- Do NOT describe the schema or table structure
- Do NOT restate obvious counts like "the query returned X rows"
- Focus on patterns, trends, anomalies, comparisons, and insights
- If the dataset is large, rely only on summary statistics and profiles
- If there is insufficient signal, say so briefly
- Do NOT invent observations that are not supported by the data"""

VISUALIZATION_SYSTEM_PROMPT_TEMPLATE = """You are a production-grade Altair visualization engine that generates a single, valid chart from SQL query results.

You MUST return Python code that creates exactly ONE Altair chart object named 'chart'.

DATASET METADATA
Columns: $columns
Sample rows (first 5): $sample_rows

USER REQUIREMENTS
$requirements

AVAILABLE DATA
A pandas DataFrame named df contains the full query result.
Columns available: $columns

OBJECTIVE
Create the most appropriate Altair visualization that best answers the user requirement or represents the dataset structure.

CHART SELECTION RULES (STRICT)
Select chart type based on column semantics:

- Temporal trend (date/time + numeric) -> line or area
- Categorical comparison -> bar
- Ranking / top-N -> sorted bar
- Part-to-whole -> arc (pie/donut)
- Numeric vs numeric -> scatter
- Distribution of numeric -> binned bar (histogram)
- If only one numeric column -> aggregated bar or histogram
- If only categorical columns -> count bar chart

AGGREGATION RULES (MANDATORY)
If multiple rows share the same category or time value:
- Aggregate numeric fields using sum() unless requirement specifies otherwise
- Use count() when measuring frequency
- Never plot duplicate raw rows over categories

TYPE INFERENCE RULES
You MUST assign correct Vega-Lite types:

- Date/time columns -> :T
- Numeric columns -> :Q
- Categorical/text columns -> :N

If needed, convert:
- df[col] = pd.to_datetime(df[col]) for temporal
- df[col] = pd.to_numeric(df[col], errors="coerce") for numeric

ENCODING RULES
- X = category or time
- Y = numeric measure
- Color = secondary category (only if useful)
- Tooltip MUST include key fields
- Sort categorical axes by descending value when meaningful

VISUAL QUALITY RULES
- width and height between 300 and 600
- Title describing what is shown
- No overlapping marks
- No excessive categories (>50) without aggregation
- Prefer clear readable axes

ALTAIR CONTRACT (MANDATORY)
- Use ONLY altair (imported as alt)
- Do NOT print df
- Do NOT output tables or text
- Do NOT create multiple charts
- Do NOT use display()
- Do NOT return Vega JSON
- Final object MUST be assigned to variable: chart

FAILSAFE
If visualization is impossible with given columns:
Create a simple count bar chart of the first categorical column.

OUTPUT FORMAT
Return ONLY valid Python code.
No markdown.
No explanations.
The code must define exactly one Altair chart assigned to variable 'chart'.
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

# Result Preview
SAMPLE_ROWS_DEFAULT = 5
SAMPLE_ROWS_INFO = 3

# File Size / Performance
MAX_RESULT_ROWS_WARNING = 1_000_000
DB_MAX_ROWS_IN_MEMORY = 200_000
DB_READ_CHUNK_SIZE = 50_000

# SQL Column Type Handling
SQL_LARGE_TYPES = [
    "blob",
    "bytea",
    "binary",
    "varbinary",
    "image",
    "json",
    "jsonb",
    "xml",
    "geography",
    "geometry",
]

# HTML/Markdown Conversion
_CSS_DIR = Path(__file__).resolve().parent / "css"


def _read_css_file(filename: str) -> str:
    return (_CSS_DIR / filename).read_text(encoding="utf-8").strip()


MARKDOWN_CSS_TABLE_STYLE = _read_css_file("markdown_table_style.css")
MARKDOWN_CSS_TABLE_HEADER = _read_css_file("markdown_table_header.css")
MARKDOWN_CSS_TABLE_CELL = _read_css_file("markdown_table_cell.css")
MARKDOWN_CSS_TABLE_CELL_ALT = _read_css_file("markdown_table_cell_alt.css")

MARKDOWN_CSS_CODE_BLOCK = _read_css_file("markdown_code_block.css")

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

DARK_THEME_STYLESHEET = _read_css_file("dark_theme.qss")
LIGHT_THEME_STYLESHEET = _read_css_file("light_theme.qss")

# ============================================================================
# NLP Table Selector
# ============================================================================

# Default acronym dictionary for database schemas
DEFAULT_ACRONYMS = {
    # --- Quantities / numeric ---
    "amt": "amount",
    "qty": "quantity",
    "num": "number",
    "cnt": "count",
    "val": "value",
    "pct": "percent",
    "avg": "average",
    "sum": "total",
    "tot": "total",
    "max": "maximum",
    "min": "minimum",
    "ratio": "ratio",
    "rate": "rate",
    "diff": "difference",
    "chg": "change",
    "bal": "balance",
    "calc": "calculated",
    # --- Identifiers / keys ---
    "id": "identifier",
    "pk": "primary_key",
    "fk": "foreign_key",
    "uid": "unique_identifier",
    "guid": "global_identifier",
    "seq": "sequence",
    "key": "key",
    "ref": "reference",
    "xref": "cross_reference",
    # --- Dates / time ---
    "dt": "date",
    "ts": "timestamp",
    "tm": "time",
    "yr": "year",
    "yrmo": "year_month",
    "mo": "month",
    "wk": "week",
    "qtr": "quarter",
    "fy": "fiscal_year",
    "fym": "fiscal_month",
    "dow": "day_of_week",
    "doy": "day_of_year",
    "start": "start",
    "end": "end",
    "dur": "duration",
    "age": "age",
    # --- Status / flags ---
    "status": "status",
    "stat": "status",
    "st": "state",
    "flag": "flag",
    "ind": "indicator",
    "active": "active",
    "inactive": "inactive",
    "pend": "pending",
    "comp": "completed",
    "fail": "failed",
    "succ": "successful",
    "valid": "valid",
    "invalid": "invalid",
    "req": "required",
    "opt": "optional",
    # --- Text / description ---
    "desc": "description",
    "msg": "message",
    "note": "note",
    "comment": "comment",
    "title": "title",
    "label": "label",
    "name": "name",
    "alias": "alias",
    "abbr": "abbreviation",
    "txt": "text",
    # --- Categories / grouping ---
    "type": "type",
    "cat": "category",
    "grp": "group",
    "class": "classification",
    "seg": "segment",
    "dept": "department",
    "div": "division",
    "region": "region",
    "zone": "zone",
    # --- Financial / sales ---
    "acct": "account",
    "txn": "transaction",
    "gl": "general_ledger",
    "ar": "accounts_receivable",
    "ap": "accounts_payable",
    "rev": "revenue",
    "exp": "expense",
    "inc": "income",
    "cost": "cost",
    "price": "price",
    "amt_due": "amount_due",
    "disc": "discount",
    "tax": "tax",
    "fee": "fee",
    "margin": "margin",
    "profit": "profit",
    "sale": "sale",
    "sales": "sales",
    "purchase": "purchase",
    "purch": "purchase",
    # --- Orders / inventory / logistics ---
    "ord": "order",
    "po": "purchase_order",
    "so": "sales_order",
    "inv": "invoice",
    "ship": "shipment",
    "rcv": "received",
    "recv": "received",
    "deliv": "delivered",
    "qty_on_hand": "quantity_on_hand",
    "qty_avail": "quantity_available",
    "qty_alloc": "quantity_allocated",
    "sku": "stock_keeping_unit",
    "item": "item",
    "prod": "product",
    "inventory": "inventory",
    "stock": "stock",
    "wh": "warehouse",
    "bin": "bin",
    "lot": "lot",
    "batch": "batch",
    "uom": "unit_of_measure",
    "pack": "package",
    # --- People / org ---
    "cust": "customer",
    "client": "client",
    "user": "user",
    "usr": "user",
    "emp": "employee",
    "empl": "employee",
    "mgr": "manager",
    "sup": "supervisor",
    "owner": "owner",
    "vendor": "vendor",
    "supp": "supplier",
    "partner": "partner",
    # --- Location / address ---
    "addr": "address",
    "street": "street",
    "apt": "apartment",
    "suite": "suite",
    "bldg": "building",
    "fl": "floor",
    "rm": "room",
    "loc": "location",
    "city": "city",
    "state": "state",
    "prov": "province",
    "region": "region",
    "country": "country",
    "zip": "zipcode",
    "postal": "postal_code",
    "lat": "latitude",
    "lon": "longitude",
    "lng": "longitude",
    "geo": "geolocation",
    # --- Contact ---
    "phone": "phone",
    "tel": "telephone",
    "fax": "fax",
    "mobile": "mobile",
    "cell": "cellular",
    "email": "email",
    "ext": "extension",
    "home": "home",
    "work": "work",
    "other": "other",
    # --- File / system / metadata ---
    "src": "source",
    "dst": "destination",
    "path": "path",
    "url": "url",
    "uri": "uri",
    "file": "file",
    "fname": "filename",
    "fpath": "filepath",
    "dir": "directory",
    "size": "size",
    "len": "length",
    "hash": "hash",
    "chk": "checksum",
    "enc": "encoding",
    "fmt": "format",
    # --- Versioning / lifecycle ---
    "ver": "version",
    "rev": "revision",
    "iter": "iteration",
    "stage": "stage",
    "step": "step",
    "init": "initial",
    "final": "final",
    "curr": "current",
    "prev": "previous",
    "next": "next",
    "crt": "created",
    "upd": "updated",
    "mod": "modified",
    "del": "deleted",
    "arch": "archive",
    "hist": "history",
    # --- Quality / metrics ---
    "score": "score",
    "metric": "metric",
    "kpi": "key_performance_indicator",
    "sla": "service_level_agreement",
    "err": "error",
    "warn": "warning",
    "info": "information",
    # --- Misc ---
    "std": "standard",
    "cfg": "configuration",
    "param": "parameter",
    "attr": "attribute",
    "prop": "property",
    "meta": "metadata",
}