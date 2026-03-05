"""
Multi-agent system for AI-powered data analysis.

This module provides an orchestrated agent system that breaks down data analysis
queries into manageable tasks, generates SQL, creates visualizations, and provides insights.
"""

import json
import os
import tempfile
from string import Template
from typing import Dict, Any, List, Optional, Tuple, Callable
from pathlib import Path

from anthropic import Anthropic
from openai import AsyncOpenAI
from openai.types.chat import ChatCompletionMessageParam
from tabulate import tabulate
import altair as alt
import pandas as pd

from config import ConfigManager
from connector import DatabaseConnector
from logger import get_logger
from security_validators import validate_sql_security
from memory.query_memory import UnifiedMemoryService
from constants import (
    LLM_MAX_TOKENS_DEFAULT,
    LLM_MAX_TOKENS_CODE,
    LLM_MAX_TOKENS_ANALYSIS,
    LLM_TEMPERATURE_DEFAULT,
    LLM_TEMPERATURE_CODE,
    LLM_TEMPERATURE_ANALYSIS,
    LLM_MODELS,
    LOCAL_LLM_DEFAULT_URL,
    LOCAL_LLM_DEFAULT_MODEL,
    LOCAL_LLM_REQUEST_TIMEOUT,
    DB_MAX_ROWS_IN_MEMORY,
    DB_READ_CHUNK_SIZE,
    SAMPLE_ROWS_INFO,
)

# Load API keys from configuration
OPENAI_API_KEY = ConfigManager.get_api_key("openai") or os.getenv("OPENAI_API_KEY", "")
CLAUDE_API_KEY = ConfigManager.get_api_key("claude") or os.getenv(
    "ANTHROPIC_API_KEY", ""
)

# Default API to use: "openai" or "claude"
DEFAULT_API = ConfigManager.get_default_api()

logger = get_logger(__name__)

# LLM configuration values are read from constants.py
# (kept as module-level names for compatibility with existing code below)
DEFAULT_MAX_TOKENS = LLM_MAX_TOKENS_DEFAULT
CODE_GENERATION_MAX_TOKENS = LLM_MAX_TOKENS_CODE
ANALYSIS_MAX_TOKENS = LLM_MAX_TOKENS_ANALYSIS

DEFAULT_TEMPERATURE = LLM_TEMPERATURE_DEFAULT
CODE_GENERATION_TEMPERATURE = LLM_TEMPERATURE_CODE
ANALYSIS_TEMPERATURE = LLM_TEMPERATURE_ANALYSIS

# Supported LLM models (from constants mapping)
CLAUDE_MODEL = LLM_MODELS.get("claude")
OPENAI_MODEL = LLM_MODELS.get("openai")
LOCAL_MODEL = LLM_MODELS.get("local")

# Visualization configuration
VISUALIZATION_MAX_TOKENS = 800
VISUALIZATION_TEMPERATURE = 0.3
VISUALIZATION_TEMP_DIR = os.path.join(tempfile.gettempdir(), "ai_data_workspace_charts")
ANALYSIS_CONTEXT_RESULT_MAX_CHARS = 2000

# ============================================================================
# Agent Prompt Templates
# ============================================================================

PLANNER_SYSTEM_PROMPT_TEMPLATE = """You are a senior data analysis planner responsible for translating a user's question into a precise, executable analysis plan based ONLY on the provided SQL schema and samples.

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

1. Answers the user's question directly in 1\u20132 sentences.
2. Summarizes the key supporting values, comparisons, or trends from the result.
3. Explains what the finding means in plain language and why it matters.
4. Notes any important limitations, assumptions, or missing data if relevant.
5. Do not mention temporary file paths.
6. Query details can be mentioned ONLY if they aid understanding.

STYLE
- Use concrete numbers from the result
- Do not speculate beyond provided data
- Prefer clarity over technical jargon
- Keep length moderate (\u2248120\u2013180 words)
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


SQL_CORRECTION_SYSTEM_PROMPT_TEMPLATE = """You are a SQL debugging and correction engine. A previously generated SQL query failed during execution. Your job is to fix the query so it runs successfully.

SCHEMA METADATA
Tables: $tables
Columns (qualified): $columns
Columns by table: $columns_by_table
Row counts: $row_counts
Column types: $dtypes
Sample rows:
${sample}

FAILED SQL
$failed_sql

ERROR MESSAGE
$error_message

CORRECTION RULES
- Identify the root cause of the error from the error message
- Fix ONLY the issue causing the error
- Use only the listed tables and columns
- Fully qualify columns when joins are involved
- Preserve the original intent of the query
- Output ONLY a single SELECT query

SECURITY RULES (MANDATORY)
- FORBIDDEN: INSERT, UPDATE, DELETE, DROP, ALTER, TRUNCATE
- FORBIDDEN: multiple statements
- FORBIDDEN: comments or dynamic SQL
- FORBIDDEN: external or network access

OUTPUT
Return ONLY the corrected SQL. No markdown. No explanations.
"""


class AIAgent:
    """
    Multi-agent system for data analysis with planner, SQL generator, visualizer, and analyzer.

    Coordinates four specialized agents:
    - Planner: Breaks down queries into execution plans
    - SQL Generator: Creates executable SQL queries
    - Visualizer: Generates Altair visualizations from data
    - Analyzer: Provides human-readable insights
    """

    def __init__(self, api_provider: Optional[str] = None, session_model: Optional[str] = None):
        """
        Initialize the AI Agent.

        Args:
            api_provider: Which LLM provider to use ('openai' or 'claude').
                         If None, uses DEFAULT_API from config.
            session_model: Optional model override for this session.
                          Takes precedence over provider defaults.

        Raises:
            ValueError: If provider is unknown or API key is not configured
        """
        self.conversation_history: List[ChatCompletionMessageParam] = []

        # Determine provider at initialization time (read default lazily)
        chosen_default = ConfigManager.get_default_api()
        self.api_provider = (api_provider or chosen_default).lower()
        
        # Store session model override
        self.session_model = session_model
        
        # Initialize unified memory service (will be set per project)
        self.memory_service: Optional[UnifiedMemoryService] = None

        logger.info(f"Initializing AIAgent with provider: {self.api_provider}")
        if session_model:
            logger.info(f"Session model override: {session_model}")

        # Read API keys lazily from config or environment
        openai_key = ConfigManager.get_api_key("openai") or os.getenv(
            "OPENAI_API_KEY", ""
        )
        claude_key = ConfigManager.get_api_key("claude") or os.getenv(
            "ANTHROPIC_API_KEY", ""
        )

        # Initialize appropriate client using the fresh keys
        if self.api_provider == "claude":
            if not claude_key:
                raise ValueError(
                    "Claude API key not configured. Please set up your API key in the application settings."
                )
            self.client = Anthropic(api_key=claude_key)
        elif self.api_provider == "openai":
            if not openai_key:
                raise ValueError(
                    "OpenAI API key not configured. Please set up your API key in the application settings."
                )
            self.client = AsyncOpenAI(api_key=openai_key)
        elif self.api_provider == "local":
            # No API key needed for local LLM — just validate connectivity config
            self.client = None
            config = ConfigManager.load_config()
            self._local_llm_url = config.get("local_llm_url", LOCAL_LLM_DEFAULT_URL)
            self._local_llm_model = config.get(
                "local_llm_model", LOCAL_LLM_DEFAULT_MODEL
            )
            self._server_starting = (
                False  # True while background auto-start is in progress
            )

            # Auto-start the built-in model server in a background thread
            # so that agent creation is never blocked by model loading.
            if config.get("hosted_auto_start", False):
                hosted_model = config.get("hosted_model_path", "")
                if hosted_model and os.path.isfile(hosted_model):
                    hosted_port = config.get("hosted_port", 8911)
                    hosted_gpu = config.get("hosted_gpu_layers", 0)
                    self._server_starting = True

                    import threading as _threading

                    def _auto_start_server():
                        try:
                            from model_manager import (
                                is_server_running,
                                start_model_server,
                                get_hosted_url,
                            )

                            if not is_server_running():
                                logger.info(
                                    f"Auto-starting hosted model server: {hosted_model}"
                                )
                                ok, msg = start_model_server(
                                    hosted_model,
                                    port=hosted_port,
                                    n_gpu_layers=hosted_gpu,
                                )
                                if ok:
                                    self._local_llm_url = get_hosted_url(
                                        port=hosted_port
                                    )
                                    logger.info(f"Hosted server started: {msg}")
                                else:
                                    logger.warning(
                                        f"Failed to auto-start hosted server: {msg}"
                                    )
                        except Exception as e:
                            logger.warning(
                                f"Could not auto-start hosted model server: {e}"
                            )
                        finally:
                            self._server_starting = False

                    t = _threading.Thread(target=_auto_start_server, daemon=True)
                    t.start()

            logger.info(
                f"Local LLM configured: url={self._local_llm_url}, model={self._local_llm_model}"
            )
        else:
            raise ValueError(
                f"Unknown API provider: {self.api_provider}. Use 'openai', 'claude', or 'local'"
            )

        self.execution_context: Dict[str, Any] = {}

        # Ensure temp directory exists
        Path(VISUALIZATION_TEMP_DIR).mkdir(exist_ok=True)

    def _resolve_model(self) -> str:
        """
        Resolve the model ID using three-tier precedence.

        Precedence order:
        1. Session override (set at agent initialization)
        2. Provider default (from config.json model_defaults)
        3. System fallback (from constants.py LLM_MODELS)

        Returns:
            Resolved model ID string
        """
        # Tier 1: Session override
        if self.session_model:
            logger.debug(f"Using session model override: {self.session_model}")
            return self.session_model
        
        # Tier 2: Provider default from config
        model_defaults = ConfigManager.get_model_defaults()
        provider_default = model_defaults.get(self.api_provider)
        if provider_default:
            logger.debug(f"Using provider default for {self.api_provider}: {provider_default}")
            return provider_default
        
        # Tier 3: System fallback
        from constants import LLM_MODELS
        fallback = LLM_MODELS.get(self.api_provider, "")
        logger.debug(f"Using system fallback for {self.api_provider}: {fallback}")
        return fallback

    async def _call_llm(
        self,
        messages: List[Dict[str, str]],
        max_tokens: int = DEFAULT_MAX_TOKENS,
        temperature: float = DEFAULT_TEMPERATURE,
        stream: bool = False,
        stream_callback: Optional[Callable[[str], None]] = None,
    ) -> str:
        """
        Unified LLM call interface that works with both OpenAI and Claude.

        Args:
            messages: List of message dictionaries with 'role' and 'content'
            max_tokens: Maximum tokens for response
            temperature: Sampling temperature (0-1)

        Returns:
            Generated text response

        Raises:
            RuntimeError: If API call fails
        """
        try:
            if self.api_provider == "claude":
                response = self._call_claude(messages, max_tokens, temperature)
                if stream_callback:
                    stream_callback(response)
                return response
            elif self.api_provider == "local":
                response = await self._call_local(messages, max_tokens, temperature)
                if stream_callback:
                    stream_callback(response)
                return response
            else:  # openai
                if stream:
                    return await self._call_openai_stream(
                        messages, max_tokens, temperature, stream_callback
                    )
                return await self._call_openai(messages, max_tokens, temperature)
        except Exception as e:
            logger.error(f"LLM call failed: {str(e)}")
            raise RuntimeError(f"Failed to call {self.api_provider}: {str(e)}")

    def _call_claude(
        self, messages: List[Dict[str, str]], max_tokens: int, temperature: float
    ) -> str:
        """Call Claude API synchronously."""
        system_message = (
            messages[0]["content"] if messages[0]["role"] == "system" else ""
        )
        other_messages = [
            {"role": m["role"], "content": m["content"]}
            for m in messages[1:]
            if m["role"] != "system"
        ]

        model_id = self._resolve_model()
        response = self.client.messages.create(
            model=model_id,
            max_tokens=max_tokens,
            temperature=temperature,
            system=system_message,
            messages=other_messages,
        )
        return response.content[0].text

    async def _call_openai(
        self, messages: List[Dict[str, str]], max_tokens: int, temperature: float
    ) -> str:
        """Call OpenAI API asynchronously."""
        model_id = self._resolve_model()
        response = await self.client.chat.completions.create(
            model=model_id,
            messages=messages,
            max_tokens=max_tokens,
            temperature=temperature,
        )
        return response.choices[0].message.content or ""

    async def _call_openai_stream(
        self,
        messages: List[Dict[str, str]],
        max_tokens: int,
        temperature: float,
        stream_callback: Optional[Callable[[str], None]] = None,
    ) -> str:
        """Call OpenAI API with streaming and return full text."""
        model_id = self._resolve_model()
        response = await self.client.chat.completions.create(
            model=model_id,
            messages=messages,
            max_tokens=max_tokens,
            temperature=temperature,
            stream=True,
        )

        full_text = ""
        async for event in response:
            if not event.choices:
                continue
            delta = event.choices[0].delta
            chunk = getattr(delta, "content", None)
            if chunk:
                full_text += chunk
                if stream_callback:
                    stream_callback(chunk)

        return full_text

    async def _call_local(
        self, messages: List[Dict[str, str]], max_tokens: int, temperature: float
    ) -> str:
        """Call a local LLM via an OpenAI-compatible HTTP endpoint (e.g. Ollama)."""
        import httpx

        base_url = getattr(self, "_local_llm_url", LOCAL_LLM_DEFAULT_URL)
        model_name = getattr(self, "_local_llm_model", LOCAL_LLM_DEFAULT_MODEL)

        # Wait briefly for the background auto-start thread to finish if it's
        # still bringing the server up, so callers get a clear error instead
        # of an immediate connection-refused.
        if getattr(self, "_server_starting", False):
            import asyncio

            for _ in range(120):  # up to ~60 s
                if not self._server_starting:
                    break
                await asyncio.sleep(0.5)

        logger.info(f"Calling local LLM at {base_url} with model {model_name}")

        try:
            async with httpx.AsyncClient(timeout=LOCAL_LLM_REQUEST_TIMEOUT) as client:
                response = await client.post(
                    f"{base_url}/chat/completions",
                    json={
                        "model": model_name,
                        "messages": messages,
                        "max_tokens": max_tokens,
                        "temperature": temperature,
                    },
                )
            response.raise_for_status()
            result = response.json()
            content = result["choices"][0]["message"]["content"]
            logger.info(f"Local LLM response received ({len(content)} chars)")
            return content
        except httpx.ConnectError:
            raise RuntimeError(
                f"Could not connect to local LLM at {base_url}. "
                "Make sure your local LLM server (e.g. Ollama) is running."
            )
        except httpx.TimeoutException:
            raise RuntimeError(
                f"Local LLM request timed out after {LOCAL_LLM_REQUEST_TIMEOUT}s. "
                "The model may be loading or the request may be too large."
            )
        except (KeyError, IndexError) as e:
            raise RuntimeError(f"Unexpected response format from local LLM: {e}")

    @staticmethod
    def _build_schema_metadata(context: Dict[str, Any]) -> Dict[str, Any]:
        """Build schema metadata for prompt context."""
        tables = context.get("tables", [])
        table_info = context.get("table_info", {})

        columns_by_table: Dict[str, List[str]] = {}
        column_types: Dict[str, str] = {}
        row_counts: Dict[str, int] = {}
        samples: Dict[str, List[Dict[str, Any]]] = {}

        for table in tables:
            info = table_info.get(table, {})
            columns = info.get("columns", [])
            columns_by_table[table] = columns
            row_counts[table] = int(info.get("row_count", 0) or 0)
            sample_rows = info.get("sample_rows", [])
            samples[table] = sample_rows[:SAMPLE_ROWS_INFO]
            types = info.get("column_types", {})
            for col in columns:
                qualified = f"{table}.{col}"
                if col in types:
                    column_types[qualified] = str(types[col])

        qualified_columns = [
            f"{t}.{c}" for t, cols in columns_by_table.items() for c in cols
        ]

        return {
            "tables": tables,
            "columns": qualified_columns,
            "columns_by_table": columns_by_table,
            "column_types": column_types,
            "row_counts": row_counts,
            "sample_rows": samples,
        }

    async def prompt_expansion_agent(
        self,
        user_prompt: str,
        schema_metadata: Dict[str, Any],
        semantic_layer: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Expand a short/colloquial user prompt into precise business terms
        suitable for NLP table selection.

        Returns an enriched prompt string (not SQL).
        """
        entity_names = []
        glossary_terms = []
        if semantic_layer:
            entity_names = [
                e.get("business_name", e["name"])
                for e in semantic_layer.get("entities", [])
            ]
            glossary_terms = list((semantic_layer.get("term_glossary") or {}).keys())

        system_message = (
            "Rewrite the user request using ONLY schema vocabulary.\n"
            "Replace synonyms with exact entity, glossary, or table terms.\n"
            "Add implied schema tokens (entity, measure, dimension, time).\n"
            "Remove all conversational words.\n"
            "Output a short keyword-style phrase.\n\n"
            f"ENTITIES: {entity_names}\n"
            f"GLOSSARY: {glossary_terms}\n"
            f"TABLES: {schema_metadata['tables']}\n\n"
            "Return ONLY the rewritten phrase."
        )

        messages = [
            {"role": "system", "content": system_message},
            {"role": "user", "content": user_prompt},
        ]

        return await self._call_llm(
            messages,
            max_tokens=200,
            temperature=LLM_TEMPERATURE_DEFAULT,
        )

    async def clarification_detector(
        self,
        user_query: str,
        context: Dict[str, Any],
        semantic_layer: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        """
        Detect if the user query contains ambiguous business meaning that
        would require guessing.

        Returns a clarification question if ambiguity is detected, None otherwise.

        Args:
            user_query: The user's data analysis question
            context: SQL context with schema metadata
            semantic_layer: Optional semantic layer for business context

        Returns:
            Clarification question string, or None if no clarification needed
        """
        # Check if clarification is enabled in config
        if not ConfigManager.get_clarification_enabled():
            logger.debug("Clarification flow disabled in config")
            return None

        schema_metadata = self._build_schema_metadata(context)
        
        # Build semantic context
        entity_names = []
        glossary_terms = {}
        if semantic_layer:
            entity_names = [
                e.get("business_name", e["name"])
                for e in semantic_layer.get("entities", [])
            ]
            glossary_terms = semantic_layer.get("term_glossary") or {}

        # Construct detection prompt
        system_message = (
            "You are an expert at detecting ambiguity in data analysis questions.\n\n"
            "Your task: Determine if the user's question contains ambiguous business terms, "
            "unclear ID codes, or references that cannot be resolved from the provided schema and glossary.\n\n"
            "SCHEMA CONTEXT:\n"
            f"Tables: {schema_metadata['tables']}\n"
            f"Columns: {schema_metadata['columns_by_table']}\n"
            f"Sample data:\n{schema_metadata['sample_rows']}\n\n"
            f"Business entities: {entity_names}\n"
            f"Term glossary: {list(glossary_terms.keys())}\n\n"
            "DETECTION RULES:\n"
            "- If the question references specific IDs, codes, or categories not visible in sample data, flag as ambiguous\n"
            "- If the question uses business terms not in entities or glossary, flag as ambiguous\n"
            "- If column relationships are unclear and would require assumptions, flag as ambiguous\n"
            "- If the question is answerable with available schema/glossary, do NOT flag\n\n"
            "OUTPUT FORMAT:\n"
            "If ambiguous: output ONLY a single clarifying question that would resolve the ambiguity.\n"
            "If clear: output ONLY the word 'CLEAR'.\n\n"
            "Be conservative: only ask when you would genuinely need to GUESS unknown business meaning."
        )

        messages = [
            {"role": "system", "content": system_message},
            {"role": "user", "content": f"Question: {user_query}"},
        ]

        logger.info("Checking for ambiguity in user query...")
        response = await self._call_llm(
            messages,
            max_tokens=300,
            temperature=0.2,  # Low temperature for consistent detection
        )

        response = response.strip()
        
        # If response is "CLEAR" or similar, no clarification needed
        if response.upper().startswith("CLEAR"):
            logger.info("No ambiguity detected - proceeding with query")
            return None
        
        # Otherwise, return the clarification question
        logger.info(f"Ambiguity detected - clarification needed: {response}")
        return response

    async def planner_agent(
        self,
        user_query: str,
        context: Dict[str, Any],
        stream: bool = False,
    ) -> Dict[str, Any]:
        """
        Planner agent that breaks down user queries into actionable steps.

        Analyzes the user's request and creates a structured execution plan.

        Args:
            user_query: The user's data analysis question
            context: SQL context with schema metadata

        Returns:
            Dictionary containing task_type, steps, code requirements, etc.
        """
        schema_metadata = self._build_schema_metadata(context)

        system_message = Template(PLANNER_SYSTEM_PROMPT_TEMPLATE).substitute(
            tables=schema_metadata["tables"],
            columns=schema_metadata["columns"],
            columns_by_table=schema_metadata["columns_by_table"],
            row_counts=schema_metadata["row_counts"],
            dtypes=schema_metadata["column_types"],
            sample=schema_metadata["sample_rows"],
            user_query=user_query,
        )

        messages: List[Dict[str, str]] = [
            {"role": "system", "content": system_message},
            {"role": "user", "content": user_query},
        ]

        logger.info("Calling planner agent...")
        raw_plan_response = await self._call_llm(
            messages,
            max_tokens=DEFAULT_MAX_TOKENS,
            temperature=DEFAULT_TEMPERATURE,
            stream=stream,
        )

        return self._parse_json_response(
            raw_plan_response,
            "plan",
            {
                "task_type": "analysis",
                "steps": ["Analyze the data based on user query"],
                "requires_sql": False,
                "requires_visualization": False,
                "analysis_focus": user_query,
            },
        )

    async def sql_generation_agent(
        self,
        plan: Dict[str, Any],
        user_query: str,
        context: Dict[str, Any],
        stream: bool = False,
    ) -> str:
        """
        SQL generation agent that creates executable SQL.

        Args:
            plan: Execution plan from planner agent
            user_query: Original user query
            context: SQL context with schema metadata

        Returns:
            Executable SQL string
        """
        schema_metadata = self._build_schema_metadata(context)

        system_message = Template(CODE_GENERATION_SYSTEM_PROMPT_TEMPLATE).substitute(
            tables=schema_metadata["tables"],
            columns=schema_metadata["columns"],
            columns_by_table=schema_metadata["columns_by_table"],
            row_counts=schema_metadata["row_counts"],
            dtypes=schema_metadata["column_types"],
            sample=schema_metadata["sample_rows"],
            plan=plan,
        )

        messages: List[Dict[str, str]] = [
            {"role": "system", "content": system_message},
            {"role": "user", "content": f"Generate SQL for: {user_query}"},
        ]

        logger.info("Calling SQL generation agent...")
        generated_sql = await self._call_llm(
            messages,
            max_tokens=CODE_GENERATION_MAX_TOKENS,
            temperature=CODE_GENERATION_TEMPERATURE,
            stream=stream,
        )

        logger.info(f"Generated SQL: {generated_sql}")

        return self._clean_sql_output(generated_sql)

    async def sql_correction_agent(
        self,
        failed_sql: str,
        error_message: str,
        context: Dict[str, Any],
        stream: bool = False,
    ) -> str:
        """
        SQL correction agent that fixes a failed SQL query based on the error.

        Args:
            failed_sql: The SQL query that failed
            error_message: The error message from execution
            context: SQL context with schema metadata

        Returns:
            Corrected SQL string
        """
        schema_metadata = self._build_schema_metadata(context)

        system_message = Template(SQL_CORRECTION_SYSTEM_PROMPT_TEMPLATE).substitute(
            tables=schema_metadata["tables"],
            columns=schema_metadata["columns"],
            columns_by_table=schema_metadata["columns_by_table"],
            row_counts=schema_metadata["row_counts"],
            dtypes=schema_metadata["column_types"],
            sample=schema_metadata["sample_rows"],
            failed_sql=failed_sql,
            error_message=error_message,
        )

        messages: List[Dict[str, str]] = [
            {"role": "system", "content": system_message},
            {
                "role": "user",
                "content": f"Fix this SQL query that failed with error: {error_message}",
            },
        ]

        logger.info(f"Calling SQL correction agent for error: {error_message}")
        corrected_sql = await self._call_llm(
            messages,
            max_tokens=CODE_GENERATION_MAX_TOKENS,
            temperature=CODE_GENERATION_TEMPERATURE,
            stream=stream,
        )

        logger.info(f"Corrected SQL: {corrected_sql}")
        return self._clean_sql_output(corrected_sql)

    async def visualization_agent(
        self,
        query_result: Dict[str, Any],
        plan: Dict[str, Any],
        user_query: str,
        stream: bool = False,
    ) -> Optional[str]:
        """
        Visualization agent that generates interactive charts and tables from query results.
        Uses new PyQtGraph-based interactive visualizations with fallback to Altair.

        Args:
            query_result: Dictionary with 'columns' and 'rows' from SQL execution
            plan: Execution plan with visualization requirements
            user_query: Original user query for context
            stream: Whether to stream the response

        Returns:
            Formatted response with [[CHART_DATA]] blocks for GUI integration, or None if visualization fails
        """
        if "error" in query_result:
            logger.warning("Cannot visualize error result")
            return None

        columns = query_result.get("columns", [])
        rows = query_result.get("rows", [])

        if not rows:
            logger.warning("Cannot visualize empty result set")
            return None

        # Convert to list of dictionaries and DataFrame
        data_records = self._normalize_sql_rows(rows, columns)
        df = pd.DataFrame(data_records)

        # Sanitize DataFrame types for JSON serialization
        df = self._sanitize_dataframe_for_json(df)

        logger.info("Generating interactive visualization data...")
        try:
            # Generate visualization data in new interactive format
            viz_data = self._generate_visualization_data(df, user_query)
            
            # Build response with data blocks for GUI
            response_parts = []
            
            # Add chart data block if available
            if viz_data.get('interactive_data'):
                chart_data = viz_data['interactive_data']
                chart_json = json.dumps(chart_data, default=str)
                response_parts.append(f"[[CHART_DATA_START]]\n{chart_json}\n[[CHART_DATA_END]]")
            
            # Add table data block if available (for large result sets)
            if len(df) > 10:  # Only show table for substantial result sets
                table_data = {
                    'headers': list(df.columns),
                    'rows': df.values.tolist(),
                    'row_count': len(df)
                }
                table_json = json.dumps(table_data, default=str)
                response_parts.append(f"[[TABLE_DATA_START]]\n{table_json}\n[[TABLE_DATA_END]]")
            
            # Add natural language description
            viz_type = viz_data['interactive_data'].get('type', 'chart')
            title = viz_data['interactive_data'].get('title', 'Data Visualization')
            num_series = len(viz_data['interactive_data'].get('series', []))
            
            # Note: Response includes CHART_DATA and TABLE_DATA blocks for GUI extraction
            # GUI will handle widget creation and display from these blocks
            response = "\n\n".join(response_parts)
            logger.info("Visualization data generated successfully")
            return response

        except Exception as e:
            logger.error(f"Visualization generation failed: {str(e)}", exc_info=True)
            # Fallback to old method
            return await self._visualization_agent_legacy(query_result, plan, user_query, stream)

    async def _visualization_agent_legacy(
        self,
        query_result: Dict[str, Any],
        plan: Dict[str, Any],
        user_query: str,
        stream: bool = False,
    ) -> Optional[str]:
        """
        Legacy visualization agent using Altair for backward compatibility.
        Falls back to this method if new interactive visualization generation fails.
        """
        columns = query_result.get("columns", [])
        rows = query_result.get("rows", [])
        
        # Convert to list of dictionaries and DataFrame for Altair
        data_records = self._normalize_sql_rows(rows, columns)
        df = pd.DataFrame(data_records)

        # Sanitize DataFrame types for JSON serialization (Altair / Vega-Lite)
        df = self._sanitize_dataframe_for_json(df)

        # Prepare sample for prompt (first 5 records)
        sample_rows = df.head(5).to_dict(orient="records") if not df.empty else []

        # Determine visualization requirements from plan
        requirements = plan.get("analysis_focus", [])
        if isinstance(requirements, list):
            requirements = ", ".join(requirements)
        requirements = f"{requirements}\n\nUser query: {user_query}"

        system_message = Template(VISUALIZATION_SYSTEM_PROMPT_TEMPLATE).substitute(
            columns=columns,
            sample_rows=sample_rows,
            requirements=requirements,
        )

        messages: List[Dict[str, str]] = [
            {"role": "system", "content": system_message},
            {"role": "user", "content": f"Generate visualization for: {user_query}"},
        ]

        logger.info("Calling legacy Altair visualization agent...")
        try:
            viz_code = await self._call_llm(
                messages,
                max_tokens=VISUALIZATION_MAX_TOKENS,
                temperature=VISUALIZATION_TEMPERATURE,
                stream=stream,
            )

            # Clean code output
            viz_code = self._clean_python_output(viz_code)

            # Execute visualization code
            chart_path = self._execute_visualization_code(viz_code, df)
            return chart_path

        except Exception as e:
            logger.error(f"Legacy visualization generation failed: {str(e)}", exc_info=True)
            return None

    def _execute_visualization_code(
        self, viz_code: str, df: pd.DataFrame
    ) -> Optional[str]:
        """
        Execute generated visualization code and save chart.

        Args:
            viz_code: Python code that creates an Altair chart
            df: pandas DataFrame with the data

        Returns:
            Path to saved chart file, or None if execution fails
        """
        try:
            namespace = {
                "alt": alt,
                "pd": pd,
                "df": df,
            }

            exec(viz_code, namespace)

            chart = namespace.get("chart")
            if chart is None:
                logger.error("Visualization code did not create 'chart' variable")
                return None

            temp_file = tempfile.NamedTemporaryFile(
                mode="wb",
                suffix=".svg",
                dir=VISUALIZATION_TEMP_DIR,
                delete=False,
            )
            temp_path = temp_file.name
            temp_file.close()

            chart.save(temp_path)

            if not os.path.exists(temp_path) or os.path.getsize(temp_path) < 200:
                logger.error("Invalid SVG output")
                return None

            logger.info(f"Chart saved to: {temp_path}")
            return temp_path

        except Exception as e:
            logger.error(
                f"Failed to execute visualization code: {str(e)}", exc_info=True
            )
            return None

    def _generate_visualization_data(
        self, query_result: pd.DataFrame, prompt: str
    ) -> Dict[str, Any]:
        """
        Generate visualization data in both Altair spec (legacy) and PyQtGraph-ready format.

        This method supports backward compatibility with Altair while providing
        new interactive PyQtGraph-ready data structure for enhanced interactivity.

        Args:
            query_result: pandas DataFrame with query results
            prompt: User's visualization request

        Returns:
            Dictionary with both legacy and new formats:
            {
                'altair_spec': {...},  # Legacy Altair spec for backward compatibility
                'interactive_data': {...},  # PyQtGraph-ready data structure
                'raw_data': [...]  # Raw data for table widget
            }
        """
        try:
            # Generate legacy Altair spec (existing behavior)
            columns = list(query_result.columns)
            sample_rows = (
                query_result.head(5).to_dict(orient="records")
                if not query_result.empty
                else []
            )

            requirements = f"{prompt}\n\nDataFrame shape: {query_result.shape}"
            system_message = Template(VISUALIZATION_SYSTEM_PROMPT_TEMPLATE).substitute(
                columns=columns,
                sample_rows=sample_rows,
                requirements=requirements,
            )

            # Build PyQtGraph-ready data structure
            pyqtgraph_data: Dict[str, Any] = {
                "type": self._detect_chart_type_from_prompt(prompt, query_result),
                "title": self._extract_chart_title_from_prompt(prompt),
                "x_label": columns[0] if columns else "Index",
                "y_label": columns[1] if len(columns) > 1 else "Value",
                "series": [],
            }

            # Convert DataFrame to series data for PyQtGraph
            if len(columns) >= 2:
                # Multi-column: create series for each numeric column
                numeric_cols = query_result.select_dtypes(
                    include=["number"]
                ).columns.tolist()

                if numeric_cols:
                    # Use first column as X, rest as Y series
                    x_col = columns[0]
                    x_data_raw = query_result[x_col].tolist()
                    
                    # Convert X data to numeric if it's datetime or string
                    x_data = self._convert_x_data_to_numeric(x_data_raw)
                    # Keep original labels for axis display
                    x_labels = [str(val) for val in x_data_raw]

                    for idx, y_col in enumerate(numeric_cols[:5]):  # Limit to 5 series
                        y_data = query_result[y_col].tolist()
                        pyqtgraph_data["series"].append(
                            {
                                "name": y_col,
                                "x": x_data,
                                "y": y_data,
                                "color": self.DEFAULT_COLORS[
                                    idx % len(self.DEFAULT_COLORS)
                                ]
                                if hasattr(self, "DEFAULT_COLORS")
                                else ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728"][
                                    idx % 4
                                ],
                            }
                        )
                else:
                    # No numeric columns; create a simple series from first two columns
                    x_data_raw = query_result[columns[0]].tolist()
                    x_data = self._convert_x_data_to_numeric(x_data_raw)
                    y_data = query_result[columns[1]].tolist()
                    pyqtgraph_data["series"].append(
                        {
                            "name": columns[1],
                            "x": x_data,
                            "y": y_data,
                            "color": "#1f77b4",
                        }
                    )
            elif len(columns) == 1:
                # Single column: create indexed Y series
                y_data = query_result[columns[0]].tolist()
                pyqtgraph_data["series"].append(
                    {
                        "name": columns[0],
                        "x": list(range(len(y_data))),
                        "y": y_data,
                        "color": "#1f77b4",
                    }
                )

            return {
                "interactive_data": pyqtgraph_data,
                "raw_data": query_result.to_dict("records"),
            }

        except Exception as e:
            logger.error(
                f"Failed to generate visualization data: {str(e)}", exc_info=True
            )
            return {
                "interactive_data": {"type": "error", "series": []},
                "raw_data": [],
            }

    @staticmethod
    def _convert_x_data_to_numeric(x_data: list) -> list:
        """
        Convert X-axis data to numeric values for PyQtGraph rendering.
        
        Handles datetime, string, and numeric data by converting to numeric indices.
        
        Args:
            x_data: List of X-axis values (can be datetime, string, or numeric)
            
        Returns:
            List of numeric values suitable for PyQtGraph plotting
        """
        if not x_data:
            return []
        
        # Check if already numeric
        try:
            numeric_data = [float(val) for val in x_data]
            return numeric_data
        except (ValueError, TypeError):
            # Not purely numeric, convert to indices
            # This handles datetime strings and other non-numeric values
            return list(range(len(x_data)))

    @staticmethod
    def _detect_chart_type_from_prompt(prompt: str, df: pd.DataFrame) -> str:
        """
        Detect appropriate chart type from user prompt and data shape.

        Args:
            prompt: User's visualization request
            df: DataFrame being visualized

        Returns:
            Chart type: 'line', 'bar', 'scatter', 'area', or 'multi-series'
        """
        prompt_lower = prompt.lower()

        # Check for explicit chart type requests
        if any(word in prompt_lower for word in ["scatter", "point", "cloud"]):
            return "scatter"
        elif any(word in prompt_lower for word in ["bar", "column"]):
            return "bar"
        elif any(word in prompt_lower for word in ["area", "filled"]):
            return "area"
        elif any(word in prompt_lower for word in ["line", "trend", "over time"]):
            return "line"

        # Heuristics based on data characteristics
        if len(df) > 100:
            return "line"  # Many points better visualized as line
        elif len(df.select_dtypes(include=["number"]).columns) > 2:
            return "multi-series"
        else:
            return "line"  # Default to line

    @staticmethod
    def _extract_chart_title_from_prompt(prompt: str) -> str:
        """
        Extract or generate a chart title from user prompt.

        Args:
            prompt: User's visualization request

        Returns:
            Chart title string
        """
        # Try to extract title from prompt
        # Look for common patterns like "Chart of X" or "X over time"
        import re

        patterns = [
            r"chart (?:of |for )?(.+?)(?:\s+(?:over|by|grouped|colored)|\?|$)",
            r"visualiz(?:e|ation) (?:of |for )?(.+?)(?:\?|$)",
            r"plot (?:of |for )?(.+?)(?:\?|$)",
        ]

        for pattern in patterns:
            match = re.search(pattern, prompt, re.IGNORECASE)
            if match:
                return match.group(1).strip()

        # Fallback: use first N words of prompt as title
        words = prompt.split()[:5]
        return " ".join(words).rstrip("?")

    async def analysis_agent(
        self,
        user_query: str,
        context: Dict[str, Any],
        plan: Optional[Dict[str, Any]] = None,
        code_output: Optional[Any] = None,
        stream_callback: Optional[Callable[[str], None]] = None,
    ) -> str:
        """
        Analysis agent that provides insights and interprets results.

        Can work standalone or interpret outputs from code execution.

        Args:
            user_query: Original user query
            context: SQL context with schema metadata
            plan: Optional execution plan for context
            code_output: Optional output from code execution

        Returns:
            Human-readable analysis of the results
        """
        # Check if result is too simple for analysis
        if code_output and self._is_low_signal(code_output):
            return "The query returned limited data; no significant analytical insights are available."

        schema_metadata = self._build_schema_metadata(context)

        # Minimal schema info - focus on what's relevant
        context_parts = [
            f"Available tables: {', '.join(schema_metadata['tables'])}",
        ]

        if plan:
            plan_summary = {
                "task_type": plan.get("task_type"),
                "objective": plan.get("objective"),
                "analysis_focus": plan.get("analysis_focus"),
            }
            context_parts.append(f"\nExecution Plan: {plan_summary}")

        # Provide structured result summary instead of raw data
        if code_output is not None:
            summary = self._summarize_query_result(code_output)
            context_parts.append(
                f"\nQuery Result Summary: {json.dumps(summary, indent=2)}"
            )

        system_message = Template(ANALYSIS_SYSTEM_PROMPT_TEMPLATE).substitute(
            context="\n".join(context_parts)
        )

        # Append mode-aware audience instructions
        mode = ConfigManager.get_interaction_mode()
        if mode == "cxo":
            system_message += (
                "\n\nAUDIENCE MODE: CxO (Executive)\n"
                "- Write for an executive audience.\n"
                "- Lead with the headline insight in the first sentence.\n"
                "- Use plain, non-technical language throughout.\n"
                "- Omit all technical details, SQL references, table names, and column names.\n"
                "- Focus on business impact, trends, and actionable takeaways.\n"
                "- Keep the response brief (80\u2013120 words).\n"
                "- Use bullet points for key metrics if helpful."
            )
        else:
            system_message += (
                "\n\nAUDIENCE MODE: Analyst\n"
                "- Write for a data-literate analyst.\n"
                "- Include specific column names, metrics, and methodology notes where relevant.\n"
                "- Detailed breakdowns and caveats are welcome.\n"
                "- Keep length moderate (120\u2013180 words)."
            )

        # Limit to 6 messages (3 exchanges) to keep context manageable
        messages: List[Dict[str, str]] = [
            {"role": "system", "content": system_message},
        ]

        # Add conversation history
        for msg in self.conversation_history[-6:]:
            messages.append({"role": msg["role"], "content": msg["content"]})

        messages.append({"role": "user", "content": user_query})

        logger.info("Calling analysis agent...")
        analysis = await self._call_llm(
            messages,
            max_tokens=ANALYSIS_MAX_TOKENS,
            temperature=ANALYSIS_TEMPERATURE,
            stream=True,
            stream_callback=stream_callback,
        )
        analysis = analysis.strip()

        # Update conversation history
        self.conversation_history.append({"role": "user", "content": user_query})
        self.conversation_history.append({"role": "assistant", "content": analysis})

        return analysis

    def set_project_context(self, project_id: Optional[str] = None) -> None:
        """
        Set the project context for memory service.
        
        Args:
            project_id: Project identifier for scoping query memory
        """
        if project_id:
            # Load memory retention config
            config = ConfigManager.load_config()
            retention_config = config.get("memory_retention", {})
            
            self.memory_service = UnifiedMemoryService(
                project_id=project_id,
                retention_policy=retention_config.get("policy", "keep_all"),
                rolling_n=retention_config.get("rolling_n", 100),
                ttl_days=retention_config.get("ttl_days", 90),
                global_index_enabled=True
            )
            logger.info(f"Memory service initialized for project: {project_id}")
        else:
            self.memory_service = None
            logger.info("Memory service disabled (no project context)")

    async def execute_query(
        self,
        user_query: str,
        context: Dict[str, Any],
        status_callback: Optional[Callable[[str], None]] = None,
        stream_callback: Optional[Callable[[str], None]] = None,
    ) -> str:
        """
        Main orchestration method that coordinates all agents.

        This is the primary entry point for executing user queries.

        Args:
            user_query: The user's data analysis question
            context: SQL context with schema metadata

        Returns:
            Formatted response with results and analysis, or a clarification question

        Raises:
            Exception: Logs error and returns error message
        """
        # Track execution metadata for memory service
        execution_start = pd.Timestamp.now()
        generated_sql = None
        execution_success = False
        execution_metadata = {}
        result_summary = None
        error_message = None
        normalized_prompt = user_query  # Will be updated if prompt expansion is used
        
        try:
            logger.info(f"Starting execute_query with query: {user_query}")
            mode = ConfigManager.get_interaction_mode()
            logger.info(f"Interaction mode: {mode}")

            if status_callback:
                status_callback("Checking for clarification...")

            # Step 0: Check for clarification needs (pre-SQL stage)
            semantic_layer = context.get("semantic_layer")
            clarification_question = await self.clarification_detector(
                user_query, context, semantic_layer
            )
            
            if clarification_question:
                # Return clarification question with special marker
                logger.info("Returning clarification question to user")
                return f"[[CLARIFICATION_NEEDED]]\n{clarification_question}"

            if status_callback:
                status_callback("Planning the request...")

            # Step 1: Plan the task
            plan = await self.planner_agent(user_query, context, stream=True)
            logger.info(f"Generated plan: {plan}")

            if (
                self._query_requests_visualization(user_query)
                or plan.get("task_type") == "visualization"
            ):
                plan["requires_visualization"] = True
                if plan.get("requires_sql") is False:
                    plan["requires_sql"] = True

            query_result = None
            chart_path = None

            # Step 2: Generate and execute SQL if needed
            requires_sql = plan.get("requires_sql")
            if requires_sql is None:
                requires_sql = plan.get("requires_code", False)

            if requires_sql:
                if status_callback:
                    status_callback("Generating SQL...")
                logger.info("SQL generation required")
                generated_sql = await self.sql_generation_agent(
                    plan, user_query, context, stream=True
                )
                if status_callback:
                    status_callback("Executing query...")
                query_result = self._execute_sql_query(
                    generated_sql, context, status_callback
                )

                # Retry loop: if SQL fails, use correction agent (up to 2 attempts)
                current_sql = generated_sql
                for attempt in range(1, 3):
                    if not (query_result and "error" in query_result):
                        break
                    error_msg = query_result["error"]
                    logger.warning(
                        f"SQL execution failed (attempt {attempt}/2), "
                        f"invoking correction agent: {error_msg}"
                    )
                    try:
                        if status_callback:
                            status_callback("Correcting SQL...")
                        corrected_sql = await self.sql_correction_agent(
                            current_sql, error_msg, context, stream=True
                        )
                        if status_callback:
                            status_callback("Re-executing corrected query...")
                        query_result = self._execute_sql_query(
                            corrected_sql, context, status_callback
                        )
                        current_sql = corrected_sql
                    except Exception as e:
                        logger.error(f"SQL correction agent failed: {e}")
                        break

                # Step 3: Generate visualization if needed
                requires_viz = plan.get("requires_visualization", False)
                if requires_viz and query_result and "error" not in query_result:
                    if status_callback:
                        status_callback("Generating visualization...")
                    logger.info("Visualization generation required")
                    chart_path = await self.visualization_agent(
                        query_result, plan, user_query, stream=True
                    )
                else:
                    chart_path = None
            else:
                logger.info("SQL generation not required")
                chart_path = None

            # Step 4: Get analysis
            if status_callback:
                status_callback("Analyzing results...")
            logger.info("Getting analysis from analysis agent")
            analysis = await self.analysis_agent(
                user_query,
                context,
                plan,
                query_result,
                stream_callback=stream_callback,
            )

            # Track execution success
            execution_success = True
            if query_result and "data" in query_result:
                execution_metadata["row_count"] = len(query_result["data"])
            execution_metadata["execution_time_seconds"] = (
                pd.Timestamp.now() - execution_start
            ).total_seconds()
            result_summary = analysis[:200] if analysis else None  # Store brief summary
            
            # Step 5: Format response based on mode
            if status_callback:
                status_callback("Formatting response...")
            
            response = None
            if mode == "cxo":
                response = self._format_cxo_response(analysis, None)
            elif chart_path:
                # If visualization was generated, combine it with analysis
                response = chart_path
                if analysis:
                    # Append analysis after visualization data blocks
                    response = f"{response}\n\n### Analysis:\n{analysis}"
            else:
                # No visualization, format with analysis only
                response = self._format_response(query_result, analysis, None)
            
            # Store in memory service if available
            if self.memory_service:
                try:
                    model_name = self._get_current_model()
                    self.memory_service.store_query(
                        user_prompt=user_query,
                        normalized_prompt=normalized_prompt,
                        generated_sql=generated_sql,
                        execution_success=execution_success,
                        execution_metadata=execution_metadata,
                        model_provider=self.api_provider,
                        model_name=model_name,
                        result_summary=result_summary,
                        error_message=error_message
                    )
                    logger.info("Query stored in memory service")
                except Exception as mem_err:
                    logger.warning(f"Failed to store query in memory: {mem_err}")
            
            return response

        except Exception as e:
            logger.error(f"Error in execute_query: {str(e)}", exc_info=True)
            error_message = str(e)
            
            # Store failed query in memory if available
            if self.memory_service:
                try:
                    model_name = self._get_current_model()
                    self.memory_service.store_query(
                        user_prompt=user_query,
                        normalized_prompt=normalized_prompt,
                        generated_sql=generated_sql,
                        execution_success=False,
                        execution_metadata=execution_metadata,
                        model_provider=self.api_provider,
                        model_name=model_name,
                        result_summary=None,
                        error_message=error_message
                    )
                except Exception as mem_err:
                    logger.warning(f"Failed to store failed query in memory: {mem_err}")
            
            return f"Error processing query: {str(e)}\n\nPlease try rephrasing your question."

    def _execute_sql_query(
        self,
        sql: str,
        context: Dict[str, Any],
        progress_callback: Optional[Callable[[str], None]] = None,
    ) -> Dict[str, Any]:
        """
        Execute generated SQL safely using SQLAlchemy Core.

        For multi-database contexts, automatically resolves which connection
        to use based on the table references in the SQL and strips the
        alias prefixes so that the SQL runs against the real table names.

        Args:
            sql: SQL query to execute
            context: SQL context with connection details

        Returns:
            Dictionary with columns, rows, and optional truncation info
        """
        is_safe, error_msg = validate_sql_security(sql)
        if not is_safe:
            logger.warning(f"SQL security violation: {error_msg}")
            return {"error": error_msg}

        if context.get("source_type") == "multi_database":
            # Determine target connection from table references in SQL
            alias = self._resolve_connection_alias(sql, context)
            if alias is None:
                return {
                    "error": (
                        "Could not determine which database to query. "
                        "Ensure the SQL references tables using their qualified names "
                        "(alias__table)."
                    )
                }
            sub_context = context["connections"].get(alias)
            if sub_context is None:
                return {"error": f"Unknown database alias: {alias}"}
            db_type = sub_context["db_type"]
            credentials = sub_context["credentials"]
            # Strip alias prefix from SQL so it runs on the real tables
            sql = self._strip_alias_prefix(sql, alias)
        else:
            db_type = context.get("db_type")
            credentials = context.get("credentials", {})

        connector = DatabaseConnector()
        success, message = connector.connect(db_type, credentials)
        if not success:
            return {"error": message}

        try:
            from sqlalchemy import text

            result = connector.connection.execute(text(sql))
            columns = list(result.keys())
            rows: List[Dict[str, Any]] = []
            truncated = False

            last_reported = 0

            while True:
                chunk = result.fetchmany(DB_READ_CHUNK_SIZE)
                if not chunk:
                    break
                normalized_chunk = self._normalize_sql_rows(chunk, columns)
                rows.extend(normalized_chunk)
                if progress_callback and len(rows) - last_reported >= DB_READ_CHUNK_SIZE:
                    last_reported = len(rows)
                    progress_callback(f"Fetched {len(rows):,} rows...")
                if len(rows) >= DB_MAX_ROWS_IN_MEMORY:
                    rows = rows[:DB_MAX_ROWS_IN_MEMORY]
                    truncated = True
                    break

            payload: Dict[str, Any] = {"columns": columns, "rows": rows}
            if truncated:
                payload["truncated"] = True
                if progress_callback:
                    progress_callback(
                        f"Result truncated to {DB_MAX_ROWS_IN_MEMORY:,} rows."
                    )
            return payload
        except Exception as e:
            logger.error(f"SQL execution error: {str(e)}", exc_info=True)
            return {"error": f"SQL execution error: {str(e)}"}
        finally:
            connector.close()

    def _resolve_connection_alias(
        self, sql: str, context: Dict[str, Any]
    ) -> Optional[str]:
        """
        Determine which database connection a SQL query targets.

        Inspects the SQL for qualified table names (``alias__table``) and
        returns the alias with the most matches.  Falls back to the first
        connection if no qualified references are found.
        """
        table_to_conn = context.get("table_to_connection", {})
        alias_hits: Dict[str, int] = {}

        sql_upper = sql.upper()
        for qualified_name, alias in table_to_conn.items():
            if qualified_name.upper() in sql_upper:
                alias_hits[alias] = alias_hits.get(alias, 0) + 1

        if alias_hits:
            # Return the alias with the highest number of table hits
            return max(alias_hits, key=alias_hits.get)  # type: ignore[arg-type]

        # Fallback: try matching unqualified table names through connections
        connections = context.get("connections", {})
        for alias, sub_ctx in connections.items():
            for table in sub_ctx.get("tables", []):
                if table.upper() in sql_upper:
                    return alias

        # Last resort: return first connection alias
        if connections:
            return next(iter(connections))
        return None

    @staticmethod
    def _sanitize_dataframe_for_json(df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert DataFrame column types that are not JSON-serializable
        (e.g. ``Decimal``, ``numpy.int64``, ``bytes``) into native Python
        types so that Altair / Vega-Lite serialization does not raise
        ``TypeError``.
        """
        import decimal

        for col in df.columns:
            sample = df[col].dropna().head(1)
            if sample.empty:
                continue
            val = sample.iloc[0]
            if isinstance(val, decimal.Decimal):
                df[col] = df[col].apply(
                    lambda v: float(v) if isinstance(v, decimal.Decimal) else v
                )
            elif isinstance(val, bytes):
                df[col] = df[col].apply(
                    lambda v: (
                        v.decode("utf-8", errors="replace")
                        if isinstance(v, bytes)
                        else v
                    )
                )
            elif hasattr(val, "item"):
                # numpy scalar → native Python type
                df[col] = df[col].apply(lambda v: v.item() if hasattr(v, "item") else v)
            # Attempt datetime conversion for date-like columns
            if "date" in col.lower() or "time" in col.lower():
                try:
                    df[col] = pd.to_datetime(df[col])
                except Exception:
                    pass
        return df

    @staticmethod
    def _strip_alias_prefix(sql: str, alias: str) -> str:
        """
        Remove the ``alias__`` prefix from all table references in *sql*
        so it can run directly against the target database.
        """
        import re

        # Replace alias__tablename with just tablename (case-insensitive)
        return re.sub(
            rf"\b{re.escape(alias)}__(\w+)",
            r"\1",
            sql,
            flags=re.IGNORECASE,
        )

    # ------------------------------------------------------------------
    # SQL result normalization → JSON-safe primitives
    # ------------------------------------------------------------------

    @staticmethod
    def _json_safe_value(v: Any) -> Any:
        """
        Convert DB / SQLAlchemy values into JSON-serializable primitives.

        Ensures only: dict, list, str, int, float, bool, None
        """
        if v is None:
            return None

        # Fast path: already safe
        if isinstance(v, (str, int, float, bool)):
            return v

        # Decimal → float
        try:
            import decimal

            if isinstance(v, decimal.Decimal):
                return float(v)
        except Exception:
            pass

        # datetime / date / time → ISO string
        try:
            import datetime

            if isinstance(v, (datetime.datetime, datetime.date, datetime.time)):
                return v.isoformat()
        except Exception:
            pass

        # UUID → str
        try:
            import uuid

            if isinstance(v, uuid.UUID):
                return str(v)
        except Exception:
            pass

        # bytes → utf-8 string
        if isinstance(v, (bytes, bytearray, memoryview)):
            try:
                return bytes(v).decode("utf-8", "replace")
            except Exception:
                return str(v)

        # numpy scalar → native Python
        try:
            import numpy as np

            if isinstance(v, np.generic):
                return v.item()
        except Exception:
            pass

        # Fallback
        return str(v)

    @classmethod
    def _normalize_sql_rows(
        cls, rows: List[Any], columns: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Normalize SQLAlchemy rows / tuples / dicts into JSON-safe dict rows.
        """
        normalized: List[Dict[str, Any]] = []

        for r in rows:
            # SQLAlchemy Row → mapping
            if hasattr(r, "_mapping"):
                r = dict(r._mapping)

            # tuple → dict via columns
            elif not isinstance(r, dict):
                r = dict(zip(columns, r))

            safe_row = {k: cls._json_safe_value(v) for k, v in r.items()}
            normalized.append(safe_row)

        return normalized

    @staticmethod
    def _query_requests_visualization(user_query: str) -> bool:
        """Heuristic to force visualization when the user explicitly asks for it."""
        query = user_query.lower()
        keywords = [
            "graph",
            "chart",
            "plot",
            "visualize",
            "visualisation",
            "visualization",
            "trend",
            "over time",
            "distribution",
            "compare",
            "correlation",
            "relationship",
            "histogram",
            "scatter",
        ]
        return any(keyword in query for keyword in keywords)

    @staticmethod
    def _summarize_query_result(result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate statistical summary of query results for LLM context.

        Creates aggregated metadata about columns, distributions, and patterns
        instead of passing raw rows to the analysis agent.

        Args:
            result: Query result dictionary with 'columns' and 'rows'

        Returns:
            Dictionary with row count, column profiles, and sample data
        """
        if not result or "columns" not in result:
            return {"type": "none"}

        columns = result.get("columns", [])
        rows = result.get("rows", [])

        if not rows:
            return {"type": "empty", "columns": columns}

        summary = {
            "row_count": len(rows),
            "columns": columns,
            "truncated": bool(result.get("truncated")),
        }

        # Column profiling for insights
        normalized_rows = AIAgent._normalize_sql_rows(rows, columns)

        col_profiles = {}
        for col in columns:
            values = [r.get(col) for r in normalized_rows]
            numeric_vals = [v for v in values if isinstance(v, (int, float))]
            unique_vals = set(str(v) for v in values if v is not None)

            col_profiles[col] = {
                "unique_count": len(unique_vals),
                "sample_values": list(unique_vals)[:5],
            }

            if numeric_vals:
                col_profiles[col].update(
                    {
                        "min": min(numeric_vals),
                        "max": max(numeric_vals),
                        "mean": round(sum(numeric_vals) / len(numeric_vals), 2),
                    }
                )

        summary["column_profiles"] = col_profiles

        # Only include sample rows for small datasets
        if len(normalized_rows) <= 50:
            summary["sample_rows"] = normalized_rows[:5]

        return summary

    @staticmethod
    def _is_low_signal(result: Dict[str, Any]) -> bool:
        """
        Detect if query result has insufficient data for meaningful analysis.

        Args:
            result: Query result dictionary

        Returns:
            True if result is trivial or has low signal
        """
        if not result or "rows" not in result:
            return True
        if "error" in result:
            return True
        rows = result.get("rows", [])
        if len(rows) <= 3:
            return True
        columns = result.get("columns", [])
        if len(columns) == 1 and len(rows) == 1:
            return True
        return False

    def _format_cxo_response(
        self, analysis: str, chart_path: Optional[str] = None
    ) -> str:
        """
        Format a CxO-friendly response: insight + optional chart, no SQL or raw data.

        Args:
            analysis: Text analysis from the analyzer agent
            chart_path: Optional path to generated chart

        Returns:
            Formatted response string for executive audience
        """
        parts = []
        if chart_path:
            parts.append(f"![Chart]({chart_path})")
            parts.append("")
        if analysis:
            parts.append(analysis)
        return "\n".join(parts) if parts else "No insights available for this query."

    def _format_response(
        self, code_result: Any, analysis: str, chart_path: Optional[str] = None
    ) -> str:
        """
        Format the final response with code execution results, visualizations, and analysis.

        Args:
            code_result: Output from code execution
            analysis: Text analysis from the analyzer
            chart_path: Optional path to generated chart

        Returns:
            Formatted response string
        """
        response_parts = []

        # Add visualization if available
        if chart_path:
            response_parts.append("")
            response_parts.append("### Visualization:")
            response_parts.append("")
            response_parts.append(f"![Generated Visualization]({chart_path})")
            response_parts.append("")
        elif code_result is not None:
            response_parts.append("")
            response_parts.append("### Result:")
            response_parts.append("")
            response_parts.append(self._format_query_result(code_result))
            response_parts.append("")

        # Always add analysis
        response_parts.append("### Analysis:")
        response_parts.append(analysis)

        return "\n".join(response_parts)

    @staticmethod
    def _format_query_result(result: Any) -> str:
        """
        Format code execution result into readable string.

        Args:
            result: The result object to format

        Returns:
            Formatted string representation
        """
        if isinstance(result, dict):
            if "error" in result:
                return str(result["error"])
            if "columns" in result and "rows" in result:
                columns = result.get("columns") or []
                rows = result.get("rows") or []
                if not rows:
                    return "No rows returned."
                if rows and isinstance(rows[0], dict):
                    table_md = tabulate(rows, headers="keys", tablefmt="github")
                else:
                    table_md = tabulate(rows, headers=columns, tablefmt="github")
                if result.get("truncated"):
                    return f"{table_md}\n\n_Results truncated._"
                return table_md

            parts = [f"**{key}:** {value}" for key, value in result.items()]
            return "\n".join(parts)

        if isinstance(result, str):
            return result

        return str(result)

    @staticmethod
    def _compact_code_output_for_prompt(result: Any) -> str:
        """Compact query results for LLM context without flooding tokens."""

        def trim_text(text: str, max_chars: int) -> str:
            if len(text) <= max_chars:
                return text
            head = text[: max_chars - 40]
            return f"{head}\n... [truncated {len(text) - len(head)} chars]"

        if isinstance(result, dict):
            if "error" in result:
                return f"Error: {result['error']}"

            if "columns" in result and "rows" in result:
                columns = result.get("columns") or []
                rows = result.get("rows") or []

                if rows and isinstance(rows[0], dict):
                    sample_rows = rows[:5]
                else:
                    sample_rows = [dict(zip(columns, row)) for row in rows[:5]]

                summary = {
                    "columns": columns,
                    "rows_returned": len(rows),
                    "truncated": bool(result.get("truncated")),
                    "sample_rows": sample_rows,
                }
                return trim_text(
                    json.dumps(summary, ensure_ascii=True),
                    ANALYSIS_CONTEXT_RESULT_MAX_CHARS,
                )

            return trim_text(
                json.dumps(result, ensure_ascii=True), ANALYSIS_CONTEXT_RESULT_MAX_CHARS
            )

        if isinstance(result, str):
            return trim_text(result, ANALYSIS_CONTEXT_RESULT_MAX_CHARS)

        return trim_text(str(result), ANALYSIS_CONTEXT_RESULT_MAX_CHARS)

    @staticmethod
    def _clean_sql_output(sql: str) -> str:
        """Remove markdown formatting from generated SQL output."""
        cleaned = sql.strip()

        if cleaned.startswith("```sql"):
            cleaned = cleaned[6:]
        if cleaned.startswith("```"):
            cleaned = cleaned[3:]
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]

        cleaned = cleaned.strip()
        if cleaned.endswith(";"):
            cleaned = cleaned[:-1]
        return cleaned.strip()

    @staticmethod
    def _clean_python_output(code: str) -> str:
        """Remove markdown formatting from generated Python output."""
        cleaned = code.strip()

        if cleaned.startswith("```python"):
            cleaned = cleaned[9:]
        elif cleaned.startswith("```"):
            cleaned = cleaned[3:]
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]

        return cleaned.strip()

    @staticmethod
    def _parse_json_response(
        response: str, response_type: str, fallback: Any
    ) -> Dict[str, Any]:
        """
        Parse JSON response from LLM with fallback.

        Args:
            response: Raw response from LLM
            response_type: Type of response for logging
            fallback: Fallback value if parsing fails

        Returns:
            Parsed JSON as dictionary
        """
        try:
            # Remove markdown code blocks if present
            if response.startswith("```"):
                response = response.split("```")[1]
                if response.startswith("json"):
                    response = response[4:]

            return json.loads(response.strip())
        except Exception as e:
            logger.warning(
                f"Failed to parse {response_type} JSON response: {str(e)}. Using fallback."
            )
            return fallback
