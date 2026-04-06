"""Agent pipeline mixin — async agent step methods for the AI Data Workspace."""

from __future__ import annotations

import json
import re
import tempfile
import os
from string import Template
from typing import TYPE_CHECKING, Dict, Any, List, Optional, Tuple, Callable

import altair as alt
import pandas as pd

from core.config import ConfigManager
from core.logger import get_logger
from core.constants import (
    LLM_MAX_TOKENS_DEFAULT,
    LLM_MAX_TOKENS_CODE,
    LLM_MAX_TOKENS_ANALYSIS,
    LLM_TEMPERATURE_DEFAULT,
    LLM_TEMPERATURE_CODE,
    LLM_TEMPERATURE_ANALYSIS,
)
from agents.prompts import (
    PLANNER_SYSTEM_PROMPT_TEMPLATE,
    CODE_GENERATION_SYSTEM_PROMPT_TEMPLATE,
    ANALYST_INSIGHT_PROMPT_TEMPLATE,
    AUDIENCE_TRANSLATION_PROMPT_TEMPLATE,
    VISUALIZATION_SYSTEM_PROMPT_TEMPLATE,
    SQL_CORRECTION_SYSTEM_PROMPT_TEMPLATE,
)
from agents.schema_utils import (
    build_schema_metadata,
    build_semantic_layer_prompt_context,
)
from agents import formatters as _fmt

if TYPE_CHECKING:

    class _AgentHostBase:
        """Structural type describing attributes the mixin expects from its host class (AIAgent).

        This exists solely so that Pylance/mypy can resolve attribute access within
        ``AgentPipelineMixin`` methods.  At runtime the base is ``object``.
        """

        visualization_code_cache: Dict[str, str]
        conversation_history: list

        async def _call_llm(
            self,
            messages: List[Dict[str, str]],
            max_tokens: int = ...,
            temperature: float = ...,
            stream: bool = ...,
            stream_callback: Optional[Callable[[str], None]] = ...,
        ) -> str: ...
        @staticmethod
        def _build_schema_metadata(context: Dict[str, Any]) -> Dict[str, Any]: ...
        @staticmethod
        def _build_semantic_layer_prompt_context(
            semantic_layer: Optional[Dict[str, Any]] = ...,
        ) -> str: ...
        @staticmethod
        def _clean_sql_output(sql: str) -> str: ...
        @staticmethod
        def _clean_python_output(code: str) -> str: ...
        @staticmethod
        def _parse_json_response(
            raw: str, response_type: str = ..., fallback: Any = ...
        ) -> Any: ...
        @staticmethod
        def _normalize_sql_rows(
            rows: Any, columns: List[str]
        ) -> List[Dict[str, Any]]: ...
        @staticmethod
        def _sanitize_dataframe_for_json(df: pd.DataFrame) -> pd.DataFrame: ...
        @staticmethod
        def _is_low_signal(result: Dict[str, Any]) -> bool: ...
        @staticmethod
        def _summarize_query_result(result: Dict[str, Any]) -> Dict[str, Any]: ...
        @staticmethod
        def _query_requests_visualization(user_query: str) -> bool: ...
        def _generate_visualization_cache_key(
            self, columns: List[str], column_types: Optional[Dict[str, str]] = ...
        ) -> str: ...
        def _execute_visualization_code(
            self, viz_code: str, df: pd.DataFrame
        ) -> Optional[str]: ...

else:
    _AgentHostBase = object

# Visualization temp directory (mirrors definition in agents.agent)
VISUALIZATION_TEMP_DIR = os.path.join(tempfile.gettempdir(), "ai_data_workspace_charts")

logger = get_logger(__name__)

# Alias constants kept for compatibility with existing code
DEFAULT_MAX_TOKENS = LLM_MAX_TOKENS_DEFAULT
CODE_GENERATION_MAX_TOKENS = LLM_MAX_TOKENS_CODE
ANALYSIS_MAX_TOKENS = LLM_MAX_TOKENS_ANALYSIS
DEFAULT_TEMPERATURE = LLM_TEMPERATURE_DEFAULT
CODE_GENERATION_TEMPERATURE = LLM_TEMPERATURE_CODE
ANALYSIS_TEMPERATURE = LLM_TEMPERATURE_ANALYSIS

# Visualization configuration
VISUALIZATION_MAX_TOKENS = 800
VISUALIZATION_TEMPERATURE = 0.3


class AgentPipelineMixin(_AgentHostBase):
    """Mixin providing all async agent pipeline step methods.

    Expects the host class (``AIAgent``) to provide the attributes and methods
    declared in ``_AgentHostBase``.  At runtime ``_AgentHostBase`` is just
    ``object``; the full type stub is only visible to type-checkers.
    """

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
        interaction_mode: Optional[str] = None,
    ) -> Optional[str]:
        """
        Detect if the user query contains ambiguous business meaning that
        would require guessing.

        Returns a clarification question if ambiguity is detected, None otherwise.

        Args:
            user_query: The user's data analysis question
            context: SQL context with schema metadata
            semantic_layer: Optional semantic layer for business context
            interaction_mode: Optional explicit interaction mode ('cxo' or 'analyst')

        Returns:
            Clarification question string, or None if no clarification needed
        """
        # Check if clarification is enabled in config
        if not ConfigManager.get_clarification_enabled():
            logger.debug("Clarification flow disabled in config")
            return None

        mode = interaction_mode or ConfigManager.get_interaction_mode() or "analyst"
        mode = mode.strip().lower()
        if mode not in ("cxo", "analyst"):
            mode = "analyst"

        schema_metadata = self._build_schema_metadata(context)

        # Fast-path: when most query terms are already grounded in schema/semantic
        # vocabulary, skip clarification to avoid unnecessary follow-up questions.
        if self._can_infer_query_meaning(user_query, schema_metadata, semantic_layer):
            logger.info(
                "Clarification skipped: query meaning is inferable from context"
            )
            return None

        # Build semantic context
        entity_names = []
        glossary_terms = {}
        if semantic_layer:
            entity_names = [
                e.get("business_name", e["name"])
                for e in semantic_layer.get("entities", [])
            ]
            glossary_terms = semantic_layer.get("term_glossary") or {}

        mode_specific_rules = ""
        if mode == "cxo":
            mode_specific_rules = (
                "MODE-SPECIFIC CLARIFICATION STYLE (CxO):\n"
                "- Ask in plain business language only\n"
                "- NEVER ask for table names, column names, schema fields, or SQL terms\n"
                "- Prefer one concise choice-style business question when possible (for example: customer vs supplier, order date vs ship date)\n"
                "- Keep the question non-technical and decision-oriented\n"
            )
        else:
            mode_specific_rules = (
                "MODE-SPECIFIC CLARIFICATION STYLE (Analyst):\n"
                "- Technical precision is allowed when needed\n"
                "- Asking for exact field names, table names, IDs, or codes is acceptable when it materially impacts SQL correctness\n"
                "- Keep the clarification concise and specific\n"
            )

        # Construct detection prompt
        system_message = (
            "You are an expert at detecting ambiguity in data analysis questions.\n\n"
            "Your task: Determine if the user's question contains ambiguous business terms, "
            "unclear ID codes, or references that cannot be resolved from the provided schema and glossary.\n\n"
            f"INTERACTION MODE: {mode}\n"
            f"{mode_specific_rules}\n"
            "SCHEMA CONTEXT:\n"
            f"Tables: {schema_metadata['tables']}\n"
            f"Columns: {schema_metadata['columns_by_table']}\n"
            f"Sample data:\n{schema_metadata['sample_rows']}\n\n"
            f"Business entities: {entity_names}\n"
            f"Term glossary: {list(glossary_terms.keys())}\n\n"
            "DETECTION RULES:\n"
            "- If the question is reasonably inferable from schema + glossary + common business conventions, mark CLEAR\n"
            "- Ask a clarification ONLY when missing detail would materially change the SQL result\n"
            "- If the question references specific IDs, codes, or categories not visible in sample data, flag as ambiguous\n"
            "- If the question uses business terms not in entities or glossary AND those terms are not inferable from context, flag as ambiguous\n"
            "- If column relationships are unclear and would require assumptions that could change output, flag as ambiguous\n"
            "- If the question is answerable with available schema/glossary, do NOT flag\n\n"
            "OUTPUT FORMAT:\n"
            "If ambiguous: output ONLY a single clarifying question that would resolve the ambiguity.\n"
            "If clear: output ONLY the word 'CLEAR'.\n\n"
            "Be highly conservative: prefer CLEAR whenever a reasonable interpretation exists."
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

        response = self._normalize_clarification_detector_response(response)

        # If response is "CLEAR" or similar, no clarification needed
        if response is None:
            logger.info("No ambiguity detected - proceeding with query")
            return None

        # Otherwise, return the clarification question
        logger.info(f"Ambiguity detected - clarification needed: {response}")
        return response

    @staticmethod
    def _normalize_clarification_detector_response(response: str) -> Optional[str]:
        """Normalize detector output and guard against prompt-instruction echoes."""
        cleaned = (response or "").strip()
        if not cleaned:
            return None

        # Explicit CLEAR/clear-like outputs.
        if cleaned.upper().startswith("CLEAR"):
            return None

        # Some models echo the detector instruction prefix verbatim.
        cleaned = re.sub(r"^\s*if\s+ambiguous\s*:\s*", "", cleaned, flags=re.I)

        lower = cleaned.lower()
        instruction_echo_markers = (
            "output only a single clarifying question",
            "if clear: output only the word",
            "be highly conservative: prefer clear",
        )
        if any(marker in lower for marker in instruction_echo_markers):
            logger.warning(
                "Clarification detector returned instruction text; treating as CLEAR"
            )
            return None

        return cleaned

    @staticmethod
    def _can_infer_query_meaning(
        user_query: str,
        schema_metadata: Dict[str, Any],
        semantic_layer: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Heuristic gate to suppress unnecessary clarification requests."""

        stop_words = {
            "show",
            "list",
            "give",
            "find",
            "get",
            "me",
            "the",
            "a",
            "an",
            "for",
            "of",
            "in",
            "on",
            "to",
            "from",
            "with",
            "and",
            "or",
            "by",
            "is",
            "are",
            "was",
            "were",
            "last",
            "this",
            "that",
            "top",
            "best",
            "worst",
        }

        known_terms: set[str] = set()

        for table in schema_metadata.get("tables", []):
            parts = re.split(r"[^a-z0-9]+", str(table).lower())
            known_terms.update(p for p in parts if p)

        for qualified_col in schema_metadata.get("columns", []):
            parts = re.split(r"[^a-z0-9]+", str(qualified_col).lower())
            known_terms.update(p for p in parts if p)

        if semantic_layer:
            for entity in semantic_layer.get("entities", []):
                for key in ("name", "business_name"):
                    value = entity.get(key)
                    if value:
                        parts = re.split(r"[^a-z0-9]+", str(value).lower())
                        known_terms.update(p for p in parts if p)
            for term in (semantic_layer.get("term_glossary") or {}).keys():
                parts = re.split(r"[^a-z0-9]+", str(term).lower())
                known_terms.update(p for p in parts if p)

        tokens = [
            tok
            for tok in re.findall(r"[a-zA-Z][a-zA-Z0-9_]{2,}", user_query.lower())
            if tok not in stop_words
        ]

        if not tokens:
            return True

        unknown = [tok for tok in tokens if tok not in known_terms]
        unknown_ratio = len(unknown) / max(len(tokens), 1)

        # If most business terms map to known schema/semantic tokens, proceed.
        return unknown_ratio <= 0.4

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
        semantic_prompt_context = self._build_semantic_layer_prompt_context(
            context.get("semantic_layer")
        )

        system_message = Template(PLANNER_SYSTEM_PROMPT_TEMPLATE).substitute(
            tables=schema_metadata["tables"],
            columns=schema_metadata["columns"],
            columns_by_table=schema_metadata["columns_by_table"],
            row_counts=schema_metadata["row_counts"],
            dtypes=schema_metadata["column_types"],
            sample=schema_metadata["sample_rows"],
            user_query=user_query,
        )
        if semantic_prompt_context:
            system_message = f"{system_message}{semantic_prompt_context}"

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

    def _normalize_execution_plan(
        self, plan: Dict[str, Any], user_query: str
    ) -> Dict[str, Any]:
        """
        Reconcile planner flags so execution decisions are internally consistent.

        The planner prompt emits both `expected_result_type` and
        `requires_visualization`, but execution historically only used
        `requires_visualization`. This normalization ensures chart-intent from
        either field is respected before downstream orchestration.
        """
        normalized = dict(plan or {})
        task_type = str(normalized.get("task_type", "")).strip().lower()
        expected_result_type = (
            str(normalized.get("expected_result_type", "")).strip().lower()
        )

        requires_viz = bool(normalized.get("requires_visualization", False))
        requires_sql = normalized.get("requires_sql")

        query_requests_viz = self._query_requests_visualization(user_query)

        # Any explicit chart-like signal should force visualization.
        if (
            query_requests_viz
            or task_type == "visualization"
            or expected_result_type == "chart"
        ):
            requires_viz = True

        # Visualization generally requires tabular data from SQL first.
        if requires_viz and requires_sql is False:
            requires_sql = True

        normalized["requires_visualization"] = requires_viz
        normalized["requires_sql"] = requires_sql

        # If visualization is now required but result type was not specified,
        # default to chart so metadata reflects execution intent.
        if requires_viz and expected_result_type in {"", "unknown"}:
            normalized["expected_result_type"] = "chart"

        return normalized

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
        semantic_prompt_context = self._build_semantic_layer_prompt_context(
            context.get("semantic_layer")
        )

        system_message = Template(CODE_GENERATION_SYSTEM_PROMPT_TEMPLATE).substitute(
            tables=schema_metadata["tables"],
            columns=schema_metadata["columns"],
            columns_by_table=schema_metadata["columns_by_table"],
            row_counts=schema_metadata["row_counts"],
            dtypes=schema_metadata["column_types"],
            sample=schema_metadata["sample_rows"],
            plan=plan,
        )
        if semantic_prompt_context:
            system_message = (
                f"{system_message}{semantic_prompt_context}\n"
                "Follow the semantic layer when mapping business concepts to SQL tables, columns, and joins."
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
        semantic_prompt_context = self._build_semantic_layer_prompt_context(
            context.get("semantic_layer")
        )

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
        if semantic_prompt_context:
            system_message = (
                f"{system_message}{semantic_prompt_context}\n"
                "Preserve the original intent by correcting the query to match the semantic layer's business mappings."
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
        plan: Optional[Dict[str, Any]],
        user_query: str,
        stream: bool = False,
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Visualization agent that generates Altair charts from query results.

        Args:
            query_result: Dictionary with 'columns' and 'rows' from SQL execution
            plan: Execution plan with visualization requirements
            user_query: Original user query for context

        Returns:
            Tuple of (chart_path, visualization_code), or (None, None) if visualization fails
        """
        if "error" in query_result:
            logger.warning("Cannot visualize error result")
            return None, None

        columns = query_result.get("columns", [])
        rows = query_result.get("rows", [])

        if not rows:
            logger.warning("Cannot visualize empty result set")
            return None, None

        # Convert to list of dictionaries and DataFrame for Altair
        data_records = self._normalize_sql_rows(rows, columns)
        df = pd.DataFrame(data_records)

        # Sanitize DataFrame types for JSON serialization (Altair / Vega-Lite)
        df = self._sanitize_dataframe_for_json(df)

        # Prepare sample for prompt (first 5 records)
        sample_rows = df.head(5).to_dict(orient="records") if not df.empty else []

        # Check visualization code cache first
        cache_key = self._generate_visualization_cache_key(columns)
        cached_viz_code = self.visualization_code_cache.get(cache_key)

        if cached_viz_code:
            logger.info(f"VISUALIZATION CACHE HIT: Reusing code for columns: {columns}")
            try:
                # Try executing cached code
                chart_path = self._execute_visualization_code(cached_viz_code, df)
                if chart_path:
                    logger.info("Cached visualization code executed successfully")
                    return chart_path, cached_viz_code
                else:
                    logger.warning(
                        "Cached visualization code execution failed, regenerating..."
                    )
            except Exception as e:
                logger.warning(
                    f"Cached visualization code failed: {e}, regenerating..."
                )

        # Cache miss or cached code failed - generate new code
        logger.info("VISUALIZATION CACHE MISS: Generating new visualization code")

        # Determine visualization requirements from plan
        plan_obj = plan or {}
        requirements = plan_obj.get("analysis_focus", [])
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
                stream=stream,
            )

            # Clean code output
            viz_code = self._clean_python_output(viz_code)

            # Store in cache
            self.visualization_code_cache[cache_key] = viz_code
            logger.info(f"Stored visualization code in cache (key: {cache_key[:8]}...)")

            # Execute visualization code
            chart_path = self._execute_visualization_code(viz_code, df)
            return chart_path, viz_code

        except Exception as e:
            logger.error(f"Visualization generation failed: {str(e)}", exc_info=True)
            return None, None

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
            _ALLOWED_MODULES = frozenset({
                "math", "datetime", "decimal", "fractions", "statistics",
                "collections", "itertools", "functools", "operator",
                "string", "re", "json", "copy", "textwrap",
                "altair", "pandas", "numpy",
            })

            def _safe_import(name, globals=None, locals=None, fromlist=(), level=0):
                if level != 0:
                    raise ImportError(f"Relative imports are not allowed")
                top = name.split(".")[0]
                if top not in _ALLOWED_MODULES:
                    raise ImportError(
                        f"Import of '{name}' is not allowed in visualization code"
                    )
                return __import__(name, globals, locals, fromlist, level)

            _builtins_src = __builtins__ if isinstance(__builtins__, dict) else vars(__builtins__)
            _safe_builtins = {
                k: _builtins_src[k]
                for k in (
                    "True", "False", "None",
                    "abs", "all", "any", "bin", "bool", "bytes", "chr",
                    "dict", "divmod", "enumerate", "filter", "float",
                    "format", "frozenset", "hasattr", "hash", "hex",
                    "int", "isinstance", "issubclass", "iter", "len",
                    "list", "map", "max", "min", "next", "oct", "ord",
                    "pow", "print", "range", "repr", "reversed", "round",
                    "set", "slice", "sorted", "str", "sum", "tuple",
                    "type", "zip",
                )
                if k in _builtins_src
            }
            _safe_builtins["__import__"] = _safe_import

            namespace = {
                "__builtins__": _safe_builtins,
                "alt": alt,
                "pd": pd,
                "df": df,
            }

            exec(viz_code, namespace)  # nosec B102 — restricted builtins, LLM-generated Altair charts only

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

        table_generated = self._has_table_output(code_output)
        graph_generated = bool(plan and plan.get("requires_visualization"))

        # Build analysis context focused on execution intent and outputs.
        context_parts = [f"User question: {user_query}"]

        if plan:
            plan_summary = {
                "task_type": plan.get("task_type"),
                "objective": plan.get("objective"),
                "analysis_focus": plan.get("analysis_focus"),
            }
            context_parts.append(
                f"Execution Plan: {json.dumps(plan_summary, ensure_ascii=True)}"
            )

        if code_output is not None:
            summary = self._summarize_query_result(code_output)
            context_parts.append(
                f"Query Result Summary: {json.dumps(summary, ensure_ascii=True)}"
            )

        analyst_findings = await self._extract_analyst_insights(
            context_text="\n".join(context_parts),
            user_query=user_query,
            graph_generated=graph_generated,
            table_generated=table_generated,
        )

        audience_mode = self._resolve_audience_mode()
        analysis = await self._translate_for_audience(
            audience_mode=audience_mode,
            analyst_findings=analyst_findings,
            stream_callback=stream_callback,
        )
        analysis = analysis.strip()

        # Update conversation history
        self.conversation_history.append({"role": "user", "content": user_query})
        self.conversation_history.append({"role": "assistant", "content": analysis})

        return analysis

    @staticmethod
    def _has_table_output(code_output: Optional[Any]) -> bool:
        """Return True when tabular query output exists and is non-empty."""
        if not isinstance(code_output, dict):
            return False
        if "error" in code_output:
            return False
        rows = code_output.get("rows")
        return bool(rows)

    async def _extract_analyst_insights(
        self,
        context_text: str,
        user_query: str,
        graph_generated: bool,
        table_generated: bool,
    ) -> Dict[str, Any]:
        """Stage 1: Extract structured analyst insights as strict JSON."""
        system_message = Template(ANALYST_INSIGHT_PROMPT_TEMPLATE).substitute(
            context=context_text,
            graph_generated=str(graph_generated).lower(),
            table_generated=str(table_generated).lower(),
        )
        messages: List[Dict[str, str]] = [
            {"role": "system", "content": system_message},
            {"role": "user", "content": user_query},
        ]

        logger.info("Calling analyst insight extraction stage...")
        raw_response = await self._call_llm(
            messages,
            max_tokens=ANALYSIS_MAX_TOKENS,
            temperature=0.2,
            stream=False,
        )

        parsed = self._parse_analyst_json(
            raw_response, graph_generated, table_generated
        )
        if parsed is not None:
            return parsed

        logger.warning("Stage 1 JSON parse failed, retrying once with repair prompt")
        repaired = await self._repair_analyst_json(
            raw_response, graph_generated, table_generated
        )
        parsed = self._parse_analyst_json(repaired, graph_generated, table_generated)
        if parsed is not None:
            return parsed

        logger.warning("Stage 1 JSON repair failed, using fallback analyst structure")
        fallback_chart = (
            "The chart communicates a comparison or trend in the returned data."
            if graph_generated
            else None
        )
        fallback_table = (
            "The table presents grouped or ranked summary values from the returned data."
            if table_generated
            else None
        )
        return {
            "headline": "The result indicates a measurable pattern in the requested data.",
            "key_insights": [
                "The output contains directional evidence that supports a primary trend.",
                "Key values show a meaningful difference across compared groups or periods.",
                "The strongest and weakest outcomes are identifiable from the summary.",
            ],
            "chart_interpretation": fallback_chart,
            "table_interpretation": fallback_table,
            "limitations": ["Insights are limited to the returned result summary."],
        }

    async def _repair_analyst_json(
        self, raw_response: str, graph_generated: bool, table_generated: bool
    ) -> str:
        """Ask the LLM once to repair non-compliant Stage 1 JSON."""
        system_message = (
            "You are a strict JSON repair utility. "
            "Rewrite the input into valid JSON only, with keys: "
            "headline, key_insights, chart_interpretation, table_interpretation, limitations. "
            "Do not add extra keys or commentary."
        )
        user_message = (
            "Repair this into strict JSON. "
            f"graph_generated={str(graph_generated).lower()}, "
            f"table_generated={str(table_generated).lower()}.\n\n"
            f"INPUT:\n{raw_response}"
        )
        messages: List[Dict[str, str]] = [
            {"role": "system", "content": system_message},
            {"role": "user", "content": user_message},
        ]
        return await self._call_llm(
            messages,
            max_tokens=ANALYSIS_MAX_TOKENS,
            temperature=0.0,
            stream=False,
        )

    async def _translate_for_audience(
        self,
        audience_mode: str,
        analyst_findings: Dict[str, Any],
        stream_callback: Optional[Callable[[str], None]] = None,
    ) -> str:
        """Stage 2: Translate structured findings for the active audience mode."""
        analyst_json = json.dumps(analyst_findings, ensure_ascii=True)
        system_message = Template(AUDIENCE_TRANSLATION_PROMPT_TEMPLATE).substitute(
            audience_mode=audience_mode,
            analyst_json=analyst_json,
        )

        messages: List[Dict[str, str]] = [
            {"role": "system", "content": system_message},
            {
                "role": "user",
                "content": "Translate the analyst findings for the selected audience mode.",
            },
        ]

        logger.info(f"Calling audience translation stage for mode: {audience_mode}")
        translated = await self._call_llm(
            messages,
            max_tokens=ANALYSIS_MAX_TOKENS,
            temperature=ANALYSIS_TEMPERATURE,
            stream=True,
            stream_callback=stream_callback,
        )
        return self._normalize_audience_markdown(translated)

    @staticmethod
    def _normalize_audience_markdown(text: str) -> str:
        """Repair common inline section formatting issues in stage 2 markdown output."""
        content = (text or "").strip()
        if not content:
            return ""

        # If the model already produced markdown headings, keep output as-is.
        if re.search(
            r"(?im)^\s*#{1,6}\s*(Headline Insight|Key Patterns and Insights|Business Implications|Suggested Actions)\b",
            content,
        ):
            return content

        section_re = re.compile(
            r"(?is)(?:\*\*)?\s*(Headline Insight|Key Patterns and Insights|Business Implications|Suggested Actions)\s*:\s*"
        )
        matches = list(section_re.finditer(content))
        if len(matches) < 2:
            return content

        sections: Dict[str, str] = {}
        for idx, match in enumerate(matches):
            name = match.group(1)
            start = match.end()
            end = matches[idx + 1].start() if idx + 1 < len(matches) else len(content)
            sections[name] = content[start:end].strip()

        def _normalize_bullets(block: str) -> List[str]:
            raw = (block or "").strip()
            if not raw:
                return []

            if "\n- " in raw or raw.startswith("-"):
                lines = [line.strip().lstrip("-").strip() for line in raw.splitlines()]
                return [line for line in lines if line]

            cleaned = re.sub(r"\s+", " ", raw).strip()

            parts = [
                part.strip(" -\t")
                for part in re.split(r"\s+-\s+", cleaned)
                if part.strip(" -\t")
            ]
            if len(parts) > 1:
                return parts

            return [cleaned]

        heading_order = [
            "Headline Insight",
            "Key Patterns and Insights",
            "Business Implications",
            "Suggested Actions",
        ]
        output_blocks: List[str] = []
        for heading in heading_order:
            section_text = sections.get(heading, "").strip()
            if not section_text:
                continue

            output_blocks.append(f"### {heading}")
            if heading == "Headline Insight":
                output_blocks.append(re.sub(r"\s+", " ", section_text).strip())
                continue

            for bullet in _normalize_bullets(section_text):
                output_blocks.append(f"- {bullet}")

        if not output_blocks:
            return content

        return "\n\n".join(output_blocks)

    @staticmethod
    def _resolve_audience_mode() -> str:
        """Map interaction mode config to Stage 2 audience label."""
        mode = (ConfigManager.get_interaction_mode() or "").strip().lower()
        if mode == "cxo":
            return "EXECUTIVE (CxO)"
        if mode == "analyst":
            return "ANALYST"
        return "DEFAULT"

    @staticmethod
    def _parse_analyst_json(
        response: str, graph_generated: bool, table_generated: bool
    ) -> Optional[Dict[str, Any]]:
        """Parse and normalize Stage 1 JSON output; return None on failure."""
        text = (response or "").strip()
        if not text:
            return None

        if "```" in text:
            fence_match = re.search(
                r"```(?:json)?\s*(.*?)\s*```", text, re.DOTALL | re.IGNORECASE
            )
            if fence_match:
                text = fence_match.group(1).strip()

        try:
            payload = json.loads(text)
        except Exception:
            obj_match = re.search(r"\{.*\}", text, re.DOTALL)
            if not obj_match:
                return None
            try:
                payload = json.loads(obj_match.group(0))
            except Exception:
                return None

        if not isinstance(payload, dict):
            return None

        headline = str(payload.get("headline", "")).strip()
        if not headline:
            return None
        headline = headline.splitlines()[0].strip()
        if headline and headline[-1] not in ".!?":
            headline = f"{headline}."

        key_insights = payload.get("key_insights", [])
        if not isinstance(key_insights, list):
            key_insights = []
        key_insights = [str(item).strip() for item in key_insights if str(item).strip()]
        if len(key_insights) < 3:
            key_insights.extend(
                [
                    "The returned data highlights meaningful directional differences.",
                    "The strongest values are concentrated in a subset of the result.",
                    "Observed patterns should be interpreted within the available result scope.",
                ]
            )
        key_insights = key_insights[:5]

        chart_interpretation = payload.get("chart_interpretation")
        table_interpretation = payload.get("table_interpretation")

        if not graph_generated:
            chart_interpretation = None
        elif chart_interpretation is not None:
            chart_interpretation = str(chart_interpretation).strip() or None

        if not table_generated:
            table_interpretation = None
        elif table_interpretation is not None:
            table_interpretation = str(table_interpretation).strip() or None

        limitations = payload.get("limitations", [])
        if not isinstance(limitations, list):
            limitations = []
        limitations = [str(item).strip() for item in limitations if str(item).strip()]

        return {
            "headline": headline,
            "key_insights": key_insights,
            "chart_interpretation": chart_interpretation,
            "table_interpretation": table_interpretation,
            "limitations": limitations,
        }
