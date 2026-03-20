"""
Multi-agent system for AI-powered data analysis.

This module provides an orchestrated agent system that breaks down data analysis
queries into manageable tasks, generates SQL, creates visualizations, and provides insights.
"""

import json
import os
import random
import re
import tempfile
from typing import Dict, Any, List, Optional, Tuple, Callable
from pathlib import Path

from anthropic import Anthropic
from openai import AsyncOpenAI
from openai.types.chat import ChatCompletionMessageParam
from tabulate import tabulate
import altair as alt
import pandas as pd

from core.config import ConfigManager
from db.connector import DatabaseConnector
from core.logger import get_logger
from core.security import validate_sql_security
from memory.query_memory import UnifiedMemoryService
from core.constants import (
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
from agents.llm_client import LLMClient
from agents.pipeline import AgentPipelineMixin
from agents.schema_utils import build_schema_metadata, build_semantic_layer_prompt_context
from agents import formatters as _fmt

logger = get_logger(__name__)

# LLM configuration aliases
DEFAULT_MAX_TOKENS = LLM_MAX_TOKENS_DEFAULT
CODE_GENERATION_MAX_TOKENS = LLM_MAX_TOKENS_CODE
ANALYSIS_MAX_TOKENS = LLM_MAX_TOKENS_ANALYSIS
DEFAULT_TEMPERATURE = LLM_TEMPERATURE_DEFAULT
CODE_GENERATION_TEMPERATURE = LLM_TEMPERATURE_CODE
ANALYSIS_TEMPERATURE = LLM_TEMPERATURE_ANALYSIS

# Supported LLM models
CLAUDE_MODEL = LLM_MODELS.get("claude")
OPENAI_MODEL = LLM_MODELS.get("openai")
LOCAL_MODEL = LLM_MODELS.get("local")

# Visualization configuration
VISUALIZATION_MAX_TOKENS = 800
VISUALIZATION_TEMPERATURE = 0.3
VISUALIZATION_TEMP_DIR = os.path.join(tempfile.gettempdir(), "ai_data_workspace_charts")
ANALYSIS_CONTEXT_RESULT_MAX_CHARS = 2000


class AIAgent(AgentPipelineMixin):
    """
    Multi-agent system for data analysis with planner, SQL generator, visualizer, and analyzer.

    Coordinates four specialized agents:
    - Planner: Breaks down queries into execution plans
    - SQL Generator: Creates executable SQL queries
    - Visualizer: Generates Altair visualizations from data
    - Analyzer: Provides human-readable insights
    """

    def __init__(
        self, api_provider: Optional[str] = None, session_model: Optional[str] = None
    ):
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

        # Initialize visualization code cache (maps data structure to generated Altair code)
        self.visualization_code_cache: Dict[str, str] = {}

        # Ensure temp directory exists
        Path(VISUALIZATION_TEMP_DIR).mkdir(exist_ok=True)

        # Compose the LLMClient for provider calls
        self._llm = LLMClient(
            api_provider=self.api_provider,
            client=self.client,
            resolve_model_fn=self._resolve_model,
            local_llm_url=getattr(self, "_local_llm_url", None),
            local_llm_model=getattr(self, "_local_llm_model", None),
            server_starting=getattr(self, "_server_starting", False),
        )

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
            logger.debug(
                f"Using provider default for {self.api_provider}: {provider_default}"
            )
            return provider_default

        # Tier 3: System fallback
        from core.constants import LLM_MODELS

        fallback = LLM_MODELS.get(self.api_provider, "")
        logger.debug(f"Using system fallback for {self.api_provider}: {fallback}")
        return fallback

    def _get_current_model(self) -> str:
        """
        Get the current model name being used for this agent session.

        Returns:
            The resolved model ID string
        """
        return self._resolve_model()

    def _generate_visualization_cache_key(
        self, columns: List[str], column_types: Optional[Dict[str, str]] = None
    ) -> str:
        """
        Generate a cache key for visualization code based on data structure.

        The key is derived from the column names and their types, ensuring
        that similar data structures reuse the same visualization code.

        Args:
            columns: List of column names
            column_types: Optional mapping of column names to their types

        Returns:
            A hash-based cache key string
        """
        import hashlib

        # Build a canonical representation of the data structure
        if not column_types:
            column_types = {}

        structure_parts = []
        for col in sorted(columns):
            col_type = column_types.get(col, "unknown")
            structure_parts.append(f"{col}:{col_type}")

        structure_str = "|".join(structure_parts)
        cache_key = hashlib.md5(structure_str.encode()).hexdigest()

        logger.debug(
            f"Generated visualization cache key: {cache_key} for columns: {columns}"
        )
        return cache_key

    async def _call_llm(
        self,
        messages: List[Dict[str, str]],
        max_tokens: int = DEFAULT_MAX_TOKENS,
        temperature: float = DEFAULT_TEMPERATURE,
        stream: bool = False,
        stream_callback: Optional[Callable[[str], None]] = None,
    ) -> str:
        """Delegate to the LLMClient instance."""
        return await self._llm._call_llm(
            messages, max_tokens, temperature, stream, stream_callback
        )

    @staticmethod
    def _build_schema_metadata(context: Dict[str, Any]) -> Dict[str, Any]:
        """Build schema metadata for prompt context."""
        return build_schema_metadata(context)

    @staticmethod
    def _build_semantic_layer_prompt_context(
        semantic_layer: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Serialize the most useful semantic-layer hints for prompt grounding."""
        return build_semantic_layer_prompt_context(semantic_layer)

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
                global_index_enabled=True,
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
        generated_viz_code = None  # Track generated visualization code
        max_sql_correction_attempts = 4
        sql_correction_attempts_used = 0

        try:
            logger.info(f"Starting execute_query with query: {user_query}")
            mode = ConfigManager.get_interaction_mode()
            logger.info(f"Interaction mode: {mode}")
            clarification_already_provided = bool(
                context.get("_skip_clarification", False)
            )

            # Step 0: Check memory cache for similar queries first.
            # This avoids unnecessary clarification/prompt expansion work on cache hits.
            if self.memory_service and status_callback:
                status_callback("Checking memory cache...")

            cache_hit = None
            query_requires_viz = self._query_requests_visualization(user_query)
            if self.memory_service:
                try:
                    similar_queries = self.memory_service.search_similar_queries(
                        prompt=user_query,
                        limit=10 if query_requires_viz else 3,
                        project_scoped=True,
                        similarity_threshold=0.75,  # Lower threshold for candidates
                    )

                    # Check for high-confidence cache hit (successful queries only).
                    # If this query asks for a chart, prefer records that already have
                    # generated visualization code in memory.
                    fallback_hit = None
                    for result in similar_queries:
                        if (
                            result.similarity_score >= 0.85  # High confidence threshold
                            and result.record.execution_success
                            and result.record.generated_sql
                        ):
                            if query_requires_viz and result.record.generated_viz_code:
                                cache_hit = result
                                break
                            if fallback_hit is None:
                                fallback_hit = result

                    if cache_hit is None:
                        cache_hit = fallback_hit

                    if cache_hit:
                        logger.info(
                            f"CACHE HIT: similarity={cache_hit.similarity_score:.3f}, "
                            f"prompt='{cache_hit.record.user_prompt}', "
                            f"has_viz_code={'yes' if bool(cache_hit.record.generated_viz_code) else 'no'}"
                        )

                    if not cache_hit and similar_queries:
                        logger.info(
                            f"CACHE MISS: highest similarity={similar_queries[0].similarity_score:.3f} "
                            f"(threshold=0.85)"
                        )
                    elif not cache_hit:
                        logger.info("CACHE MISS: no similar queries found")

                except Exception as cache_err:
                    logger.warning(f"Cache lookup failed: {cache_err}")

            # If cache hit found, re-execute the cached SQL
            if cache_hit:
                if status_callback:
                    status_callback("Using cached query...")

                try:
                    # Re-execute the cached SQL query
                    generated_sql = cache_hit.record.generated_sql
                    if not generated_sql:
                        logger.warning(
                            "Cache hit has no SQL, falling back to normal flow"
                        )
                        raise ValueError("No SQL in cached record")

                    logger.info(f"Re-executing cached SQL: {generated_sql}")

                    query_result = self._execute_sql_query(
                        generated_sql, context, status_callback
                    )

                    # Apply the same SQL correction loop on cache hits.
                    current_sql = generated_sql
                    while (
                        query_result
                        and "error" in query_result
                        and sql_correction_attempts_used < max_sql_correction_attempts
                    ):
                        sql_correction_attempts_used += 1
                        error_msg = query_result["error"]
                        logger.warning(
                            "Cached SQL execution failed "
                            f"(attempt {sql_correction_attempts_used}/{max_sql_correction_attempts}), "
                            f"invoking correction agent: {error_msg}"
                        )
                        if status_callback:
                            status_callback("Correcting cached SQL...")
                        corrected_sql = await self.sql_correction_agent(
                            current_sql, error_msg, context, stream=True
                        )
                        if status_callback:
                            status_callback("Re-executing corrected cached query...")
                        query_result = self._execute_sql_query(
                            corrected_sql, context, status_callback
                        )
                        current_sql = corrected_sql

                    # Persist final SQL used after correction retries.
                    generated_sql = current_sql

                    # If cached SQL still fails after correction, fall back to
                    # normal planning/generation flow instead of returning an
                    # empty/low-value cache response.
                    if query_result and "error" in query_result:
                        raise RuntimeError(
                            f"Cached SQL remained invalid after correction: {query_result['error']}"
                        )

                    execution_success = True
                    execution_metadata = query_result.get("metadata", {})
                    execution_metadata["cache_hit"] = True
                    execution_metadata["cache_similarity"] = cache_hit.similarity_score
                    execution_metadata["original_prompt"] = cache_hit.record.user_prompt

                    # Write-through update: persist corrected/working SQL back
                    # to the cached record so future cache hits use the fixed SQL.
                    if self.memory_service and cache_hit.record.record_id:
                        try:
                            self.memory_service.update_cached_sql(
                                record_id=cache_hit.record.record_id,
                                generated_sql=generated_sql,
                                execution_success=True,
                                error_message=None,
                            )
                        except Exception as cache_update_err:
                            logger.warning(
                                f"Failed to update cached SQL record: {cache_update_err}"
                            )

                    # Check if visualization is needed
                    chart_path = None
                    requires_viz = query_requires_viz
                    if requires_viz and query_result and "error" not in query_result:
                        generated_viz_code = cache_hit.record.generated_viz_code
                        if generated_viz_code:
                            # Hydrate in-memory viz cache from persisted query memory.
                            # This avoids an LLM round-trip for repeated visualizations.
                            viz_columns = query_result.get("columns", [])
                            viz_cache_key = self._generate_visualization_cache_key(
                                viz_columns
                            )
                            self.visualization_code_cache[viz_cache_key] = (
                                generated_viz_code
                            )
                            logger.info(
                                "Visualization code loaded from query memory index"
                            )
                        if status_callback:
                            status_callback("Generating visualization...")
                        logger.info(
                            "Visualization generation required for cached query"
                        )
                        chart_path, generated_viz_code = await self.visualization_agent(
                            query_result, None, user_query, stream=True
                        )

                    # Generate analysis and format response
                    if status_callback:
                        status_callback("Analyzing results...")

                    cached_plan = {
                        "task_type": "analysis",
                        "requires_visualization": bool(chart_path),
                    }
                    analysis = await self.analysis_agent(
                        user_query,
                        context,
                        plan=cached_plan,
                        code_output=query_result,
                        stream_callback=stream_callback,
                    )

                    response = None
                    if mode == "cxo":
                        response = self._format_cxo_response(
                            analysis,
                            chart_path,
                            query_result,
                            generated_sql=generated_sql,
                        )
                    else:
                        response = self._format_response(
                            query_result,
                            analysis,
                            chart_path,
                            generated_sql=generated_sql,
                        )

                    # Store cache hit as a new record
                    if self.memory_service:
                        try:
                            model_name = self._get_current_model()
                            self.memory_service.store_query(
                                user_prompt=user_query,
                                normalized_prompt=user_query,
                                generated_sql=generated_sql,
                                generated_viz_code=generated_viz_code,
                                execution_success=True,
                                execution_metadata=execution_metadata,
                                model_provider=self.api_provider,
                                model_name=model_name,
                                result_summary="Cache hit",
                                error_message=None,
                            )
                        except Exception as mem_err:
                            logger.warning(f"Failed to store cache hit: {mem_err}")

                    return response

                except Exception as cache_exec_err:
                    logger.warning(
                        f"Cache hit execution failed, falling back to normal flow: {cache_exec_err}"
                    )
                    # Fall through to normal execution flow

            if status_callback:
                status_callback("Checking for clarification...")

            # Step 0.5: Check for clarification needs (pre-SQL stage)
            clarification_question = None
            if clarification_already_provided:
                logger.info(
                    "Skipping clarification detector: user already provided follow-up context"
                )
            else:
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
            plan = self._normalize_execution_plan(plan, user_query)
            logger.info(f"Generated plan: {plan}")

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
                while (
                    query_result
                    and "error" in query_result
                    and sql_correction_attempts_used < max_sql_correction_attempts
                ):
                    sql_correction_attempts_used += 1
                    error_msg = query_result["error"]
                    logger.warning(
                        f"SQL execution failed (attempt {sql_correction_attempts_used}/{max_sql_correction_attempts}), "
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

                # Persist the final SQL that was actually executed after retries.
                generated_sql = current_sql

                # Step 3: Generate visualization if needed
                requires_viz = plan.get("requires_visualization", False)
                if requires_viz and query_result and "error" not in query_result:
                    if status_callback:
                        status_callback("Generating visualization...")
                    logger.info("Visualization generation required")
                    chart_path, generated_viz_code = await self.visualization_agent(
                        query_result, plan, user_query, stream=True
                    )
            else:
                logger.info("SQL generation not required")

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
            execution_metadata["cache_hit"] = False  # Mark as normal execution
            result_summary = analysis[:200] if analysis else None  # Store brief summary

            # Step 5: Format response based on mode
            if status_callback:
                status_callback("Formatting response...")

            response = None
            if mode == "cxo":
                response = self._format_cxo_response(
                    analysis,
                    chart_path,
                    query_result,
                    generated_sql=generated_sql,
                )
            else:
                response = self._format_response(
                    query_result, analysis, chart_path, generated_sql=generated_sql
                )

            # Store in memory service if available
            if self.memory_service:
                try:
                    model_name = self._get_current_model()
                    self.memory_service.store_query(
                        user_prompt=user_query,
                        normalized_prompt=normalized_prompt,
                        generated_sql=generated_sql,
                        generated_viz_code=generated_viz_code,
                        execution_success=execution_success,
                        execution_metadata=execution_metadata,
                        model_provider=self.api_provider,
                        model_name=model_name,
                        result_summary=result_summary,
                        error_message=error_message,
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
                        generated_viz_code=generated_viz_code,
                        execution_success=False,
                        execution_metadata=execution_metadata,
                        model_provider=self.api_provider,
                        model_name=model_name,
                        result_summary=None,
                        error_message=error_message,
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
                if (
                    progress_callback
                    and len(rows) - last_reported >= DB_READ_CHUNK_SIZE
                ):
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
        if not rows:
            return True
        return False

    def _format_cxo_response(
        self,
        analysis: str,
        chart_path: Optional[str] = None,
        query_result: Optional[Dict[str, Any]] = None,
        generated_sql: Optional[str] = None,
    ) -> str:
        """
        Format a CxO-friendly response with optional chart and SQL.

        Args:
            analysis: Text analysis from the analyzer agent
            chart_path: Optional path to generated chart
            generated_sql: Optional SQL query that was executed

        Returns:
            Formatted response string for executive audience
        """
        parts = []
        if ConfigManager.get_show_sql_in_responses() and generated_sql:
            parts.append("### Generated SQL:")
            parts.append("")
            parts.append(f"```sql\n{self._clean_sql_output(generated_sql)}\n```")
            parts.append("")
        if chart_path:
            parts.append("")
            parts.append(f"![Chart]({chart_path})")
            parts.append("")
        preview = self._format_cxo_table_preview(query_result)
        if preview:
            parts.append("### Executive Snapshot")
            parts.append("")
            parts.append(preview)
            parts.append("")
        if analysis:
            parts.append(analysis)
        return "\n".join(parts) if parts else "No insights available for this query."

    @staticmethod
    def _format_cxo_table_preview(result: Optional[Dict[str, Any]]) -> Optional[str]:
        """Render a compact table preview for ranked/top-N style CxO outputs."""
        MAX_COLUMNS = 5
        MAX_ROWS = 15

        # TODO: Define helper function that drops id columns if name/label columns exist
        if not isinstance(result, dict) or result.get("error"):
            return None

        columns = (result.get("columns") or [])[:MAX_COLUMNS]
        rows = (result.get("rows") or [])[:MAX_ROWS]

        if not columns or not rows:
            return None

        try:
            headers = "keys" if isinstance(rows[0], dict) else columns
            return tabulate(rows, headers=headers, tablefmt="github")

        except (ValueError, TypeError):
            return None

    def _format_response(
        self,
        code_result: Any,
        analysis: str,
        chart_path: Optional[str] = None,
        generated_sql: Optional[str] = None,
    ) -> str:
        """
        Format the final response with code execution results, visualizations, and analysis.

        Args:
            code_result: Output from code execution
            analysis: Text analysis from the analyzer
            chart_path: Optional path to generated chart
            generated_sql: Optional SQL query that was executed

        Returns:
            Formatted response string
        """
        response_parts = []

        # Include generated SQL when SQL visibility is enabled.
        if ConfigManager.get_show_sql_in_responses() and generated_sql:
            response_parts.append("### Generated SQL:")
            response_parts.append("")
            response_parts.append(
                f"```sql\n{self._clean_sql_output(generated_sql)}\n```"
            )

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

        # Strip common lead-in labels the model may add.
        cleaned = re.sub(
            r"^\s*corrected\s+sql\s*:\s*", "", cleaned, flags=re.IGNORECASE
        )
        cleaned = re.sub(r"^\s*sql\s*:\s*", "", cleaned, flags=re.IGNORECASE)

        # If a fenced block exists anywhere, extract that block content.
        if "```" in cleaned:
            fence_match = re.search(
                r"```(?:sql)?\s*(.*?)\s*```", cleaned, flags=re.IGNORECASE | re.DOTALL
            )
            if fence_match:
                cleaned = fence_match.group(1).strip()

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
