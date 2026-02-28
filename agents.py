"""
Multi-agent system for AI-powered data analysis.

This module provides an orchestrated agent system that breaks down data analysis
queries into manageable tasks, generates SQL, creates visualizations, and provides insights.
"""

import json
import os
import tempfile
from string import Template
from typing import Dict, Any, List, Optional, Tuple
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


class AIAgent:
    """
    Multi-agent system for data analysis with planner, SQL generator, visualizer, and analyzer.

    Coordinates four specialized agents:
    - Planner: Breaks down queries into execution plans
    - SQL Generator: Creates executable SQL queries
    - Visualizer: Generates Altair visualizations from data
    - Analyzer: Provides human-readable insights
    """

    def __init__(self, api_provider: Optional[str] = None):
        """
        Initialize the AI Agent.

        Args:
            api_provider: Which LLM provider to use ('openai' or 'claude').
                         If None, uses DEFAULT_API from config.

        Raises:
            ValueError: If provider is unknown or API key is not configured
        """
        self.conversation_history: List[ChatCompletionMessageParam] = []

        # Determine provider at initialization time (read default lazily)
        chosen_default = ConfigManager.get_default_api()
        self.api_provider = (api_provider or chosen_default).lower()

        logger.info(f"Initializing AIAgent with provider: {self.api_provider}")

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
            self._local_llm_model = config.get("local_llm_model", LOCAL_LLM_DEFAULT_MODEL)
            self._server_starting = False  # True while background auto-start is in progress

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
                                    self._local_llm_url = get_hosted_url(port=hosted_port)
                                    logger.info(f"Hosted server started: {msg}")
                                else:
                                    logger.warning(f"Failed to auto-start hosted server: {msg}")
                        except Exception as e:
                            logger.warning(f"Could not auto-start hosted model server: {e}")
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

    async def _call_llm(
        self,
        messages: List[Dict[str, str]],
        max_tokens: int = DEFAULT_MAX_TOKENS,
        temperature: float = DEFAULT_TEMPERATURE,
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
                return self._call_claude(messages, max_tokens, temperature)
            elif self.api_provider == "local":
                return await self._call_local(messages, max_tokens, temperature)
            else:  # openai
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

        response = self.client.messages.create(
            model=CLAUDE_MODEL,
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
        response = await self.client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=messages,
            max_tokens=max_tokens,
            temperature=temperature,
        )
        return response.choices[0].message.content or ""

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
            raise RuntimeError(
                f"Unexpected response format from local LLM: {e}"
            )

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

    async def planner_agent(
        self, user_query: str, context: Dict[str, Any]
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
            messages, max_tokens=DEFAULT_MAX_TOKENS, temperature=DEFAULT_TEMPERATURE
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
        self, plan: Dict[str, Any], user_query: str, context: Dict[str, Any]
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
        )

        logger.info(f"Generated SQL: {generated_sql}")

        return self._clean_sql_output(generated_sql)

    async def visualization_agent(
        self,
        query_result: Dict[str, Any],
        plan: Dict[str, Any],
        user_query: str,
    ) -> Optional[str]:
        """
        Visualization agent that generates Altair charts from query results.

        Args:
            query_result: Dictionary with 'columns' and 'rows' from SQL execution
            plan: Execution plan with visualization requirements
            user_query: Original user query for context

        Returns:
            Path to saved chart image, or None if visualization fails
        """
        if "error" in query_result:
            logger.warning("Cannot visualize error result")
            return None

        columns = query_result.get("columns", [])
        rows = query_result.get("rows", [])

        if not rows:
            logger.warning("Cannot visualize empty result set")
            return None

        # Convert to list of dictionaries and DataFrame for Altair
        data_records = [dict(zip(columns, row)) for row in rows]
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

        logger.info("Calling visualization agent...")
        try:
            viz_code = await self._call_llm(
                messages,
                max_tokens=VISUALIZATION_MAX_TOKENS,
                temperature=VISUALIZATION_TEMPERATURE,
            )

            # Clean code output
            viz_code = self._clean_python_output(viz_code)

            # Execute visualization code
            chart_path = self._execute_visualization_code(viz_code, df)
            return chart_path

        except Exception as e:
            logger.error(f"Visualization generation failed: {str(e)}", exc_info=True)
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

    async def analysis_agent(
        self,
        user_query: str,
        context: Dict[str, Any],
        plan: Optional[Dict[str, Any]] = None,
        code_output: Optional[Any] = None,
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
            messages, max_tokens=ANALYSIS_MAX_TOKENS, temperature=ANALYSIS_TEMPERATURE
        )
        analysis = analysis.strip()

        # Update conversation history
        self.conversation_history.append({"role": "user", "content": user_query})
        self.conversation_history.append({"role": "assistant", "content": analysis})

        return analysis

    async def execute_query(self, user_query: str, context: Dict[str, Any]) -> str:
        """
        Main orchestration method that coordinates all agents.

        This is the primary entry point for executing user queries.

        Args:
            user_query: The user's data analysis question
            context: SQL context with schema metadata

        Returns:
            Formatted response with results and analysis

        Raises:
            Exception: Logs error and returns error message
        """
        try:
            logger.info(f"Starting execute_query with query: {user_query}")
            mode = ConfigManager.get_interaction_mode()
            logger.info(f"Interaction mode: {mode}")

            # Step 1: Plan the task
            plan = await self.planner_agent(user_query, context)
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
                logger.info("SQL generation required")
                generated_sql = await self.sql_generation_agent(
                    plan, user_query, context
                )
                query_result = self._execute_sql_query(generated_sql, context)

                # Check for SQL execution errors and stop pipeline
                if query_result and "error" in query_result:
                    logger.error(f"SQL execution failed: {query_result['error']}")
                    return f"### Error\n\n{query_result['error']}\n\nPlease try rephrasing your question or check your query."

                # Step 3: Generate visualization if needed
                requires_viz = plan.get("requires_visualization", False)
                if requires_viz and query_result and "error" not in query_result:
                    logger.info("Visualization generation required")
                    chart_path = await self.visualization_agent(
                        query_result, plan, user_query
                    )
            else:
                logger.info("SQL generation not required")

            # Step 4: Get analysis
            logger.info("Getting analysis from analysis agent")
            analysis = await self.analysis_agent(
                user_query, context, plan, query_result
            )

            # Step 5: Format response based on mode
            if mode == "cxo":
                return self._format_cxo_response(analysis, chart_path)
            else:
                return self._format_response(query_result, analysis, chart_path)

        except Exception as e:
            logger.error(f"Error in execute_query: {str(e)}", exc_info=True)
            return f"Error processing query: {str(e)}\n\nPlease try rephrasing your question."

    def _execute_sql_query(self, sql: str, context: Dict[str, Any]) -> Dict[str, Any]:
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
            rows: List[Tuple[Any, ...]] = []
            truncated = False

            while True:
                chunk = result.fetchmany(DB_READ_CHUNK_SIZE)
                if not chunk:
                    break
                rows.extend(chunk)
                if len(rows) >= DB_MAX_ROWS_IN_MEMORY:
                    rows = rows[:DB_MAX_ROWS_IN_MEMORY]
                    truncated = True
                    break

            payload: Dict[str, Any] = {"columns": columns, "rows": rows}
            if truncated:
                payload["truncated"] = True
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
                    lambda v: v.decode("utf-8", errors="replace")
                    if isinstance(v, bytes) else v
                )
            elif hasattr(val, "item"):
                # numpy scalar → native Python type
                df[col] = df[col].apply(
                    lambda v: v.item() if hasattr(v, "item") else v
                )
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
            rf'\b{re.escape(alias)}__(\w+)',
            r'\1',
            sql,
            flags=re.IGNORECASE,
        )

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
        col_profiles = {}

        if rows and isinstance(rows[0], (list, tuple)):
            # Convert to column arrays
            cols_data = list(zip(*rows))
            for col, values in zip(columns, cols_data):
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
        if len(rows) <= 50:
            if isinstance(rows[0], (list, tuple)):
                summary["sample_rows"] = [dict(zip(columns, row)) for row in rows[:5]]
            else:
                summary["sample_rows"] = rows[:5]

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
