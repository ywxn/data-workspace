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
    "local": "mistral",  # default local model name; overridden by config
}

# Local LLM defaults
LOCAL_LLM_DEFAULT_URL = "http://localhost:11434/v1"
LOCAL_LLM_DEFAULT_MODEL = "mistral"
LOCAL_LLM_REQUEST_TIMEOUT = 120.0  # seconds

# Hosted (built-in) LLM server defaults
HOSTED_LLM_DEFAULT_PORT = 8911
HOSTED_LLM_DEFAULT_HOST = "127.0.0.1"
HOSTED_LLM_CONTEXT_SIZE = 4096
HOSTED_LLM_GPU_LAYERS = 0  # 0 = CPU-only by default; increase for GPU offload

# Models directory (relative to project root)
MODELS_DIR = Path(__file__).resolve().parent / "models"

# Recommended GGUF models for self-hosted inference
HOSTED_MODEL_CATALOG = {
    "mistral-7b-q4": {
        "name": "Mistral 7B Instruct v0.3 (Q4_K_M)",
        "filename": "Mistral-7B-Instruct-v0.3.Q4_K_M.gguf",
        "url": "https://huggingface.co/MaziyarPanahi/Mistral-7B-Instruct-v0.3-GGUF/resolve/main/Mistral-7B-Instruct-v0.3.Q4_K_M.gguf",
        "size_gb": 4.4,
        "description": "Fast and capable for data analysis tasks (recommended)",
        "recommended": True,
    },
    "llama3-8b-q4": {
        "name": "Llama 3 8B Instruct (Q4_K_M)",
        "filename": "Meta-Llama-3-8B-Instruct.Q4_K_M.gguf",
        "url": "https://huggingface.co/QuantFactory/Meta-Llama-3-8B-Instruct-GGUF/resolve/main/Meta-Llama-3-8B-Instruct.Q4_K_M.gguf",
        "size_gb": 4.9,
        "description": "Strong general-purpose model from Meta",
        "recommended": False,
    },
    "phi3-mini-q4": {
        "name": "Phi-3 Mini 4K Instruct (Q4_K_M)",
        "filename": "Phi-3-mini-4k-instruct-q4.gguf",
        "url": "https://huggingface.co/microsoft/Phi-3-mini-4k-instruct-gguf/resolve/main/Phi-3-mini-4k-instruct-q4.gguf",
        "size_gb": 2.4,
        "description": "Small, fast model — ideal for low-resource machines",
        "recommended": False,
    },
}


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

NLP_PLACEHOLDER_TEXT = [
    "Find the top 5 products by sales in the last quarter.",
    "What is the monthly revenue trend for the past year?",
    "Identify customer segments based on purchasing behavior.",
    "Calculate the average order value for each region.",
    "Which marketing channels are driving the most traffic?",
    "Customer orders joined with payments and refunds.",
    "Analyze the impact of discounts on sales performance.",
    "Evaluate the effectiveness of marketing campaigns.",
    "Assess customer satisfaction through survey responses.",
    "Monitor inventory levels and stock movements.",
    "Track employee performance and productivity.",
    "Analyze financial statements and key performance indicators.",
]
