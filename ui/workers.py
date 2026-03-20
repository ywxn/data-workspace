"""Background worker threads for query execution."""

import asyncio
from typing import Optional, Dict, Any, List
from PySide6.QtCore import Signal, QThread
from core.config import ConfigManager
from core.logger import get_logger
from agents import AIAgent
from db.connector import DatabaseConnector
from db.nlp import NLPTableSelector
from db.processing import load_data

logger = get_logger(__name__)


class QueryWorker(QThread):
    """Worker thread to handle long-running queries without blocking UI"""

    result_signal = Signal(str)
    error_signal = Signal(str)
    clarification_signal = Signal(str)  # New signal for clarification requests
    progress_signal = Signal(str)
    stream_signal = Signal(str)

    def __init__(
        self,
        query: str,
        data_context: Dict[str, Any],
        clarification_context: Optional[str] = None,
        project_id: Optional[str] = None,
    ):
        super().__init__()
        self.query = query
        self.data_context = data_context
        self.clarification_context = (
            clarification_context  # Previous clarification answer if any
        )
        self.agent = AIAgent()
        # Initialize memory service with project context
        if project_id:
            self.agent.set_project_context(project_id)

    def run(self):
        try:
            logger.debug(
                f"QueryWorker starting execution for query: {self.query[:100]}..."
            )

            def _emit_status(message: str) -> None:
                if message:
                    self.progress_signal.emit(message)

            def _emit_stream(chunk: str) -> None:
                if chunk:
                    self.stream_signal.emit(chunk)

            # If we have clarification context, enrich the query
            effective_query = self.query
            if self.clarification_context:
                effective_query = (
                    f"{self.query}\n\nAdditional context: {self.clarification_context}"
                )
                logger.info(f"Query enriched with clarification context")

            # CxO mode: run NLP table selection first, then build context
            if self.data_context.get("cxo_mode"):
                cached_cxo_context = self.data_context.get("_cxo_selected_context")
                base_query = (self.query or "").strip().lower()

                if self._has_usable_cxo_context(cached_cxo_context):
                    logger.info(
                        "CxO mode: reusing existing chat context without NLP table reselection"
                    )
                    effective_context = cached_cxo_context
                else:
                    if cached_cxo_context:
                        logger.info(
                            "CxO mode: cached chat context has no selected tables, rerunning NLP selection"
                        )
                    # In CxO mode, table selection should always be based on the
                    # initial user prompt, not clarification follow-up text.
                    effective_context = self._build_cxo_context(self.query)
                    if effective_context is not None:
                        # Persist selected context for clarification round-trips.
                        self.data_context["_cxo_selected_context"] = effective_context
                        self.data_context["_cxo_selected_prompt"] = base_query
                if effective_context is None:
                    self.error_signal.emit(
                        "Could not identify relevant tables for your question. "
                        "Please try rephrasing with more specific terms."
                    )
                    return
            else:
                effective_context = self.data_context

            # Clarification follow-up should continue execution, not ask for
            # another ambiguity question for the same intent.
            effective_context = dict(effective_context)
            effective_context["_skip_clarification"] = bool(self.clarification_context)

            # Run async agent methods in a new event loop
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            # Use the new orchestrated execute_query method
            tables = effective_context.get("tables", [])
            logger.debug(f"Executing query asynchronously (tables: {tables})")
            result = loop.run_until_complete(
                self.agent.execute_query(
                    effective_query,
                    effective_context,
                    status_callback=_emit_status,
                    stream_callback=_emit_stream,
                )
            )

            # Check if result is a clarification request
            if result.startswith("[[CLARIFICATION_NEEDED]]"):
                clarification_text = result.replace(
                    "[[CLARIFICATION_NEEDED]]", ""
                ).strip()
                logger.debug("Clarification requested; emitting clarification signal")
                self.clarification_signal.emit(clarification_text)
            else:
                logger.info(
                    f"Query execution completed successfully (result length: {len(result)} chars)"
                )
                self.result_signal.emit(result)

            # Prevent "Task was destroyed but it is pending" warnings from
            # async generators used by streaming clients.
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}", exc_info=True)
            self.error_signal.emit(f"Error: {str(e)}")

    def _has_high_confidence_cache_hit(self, query: str) -> bool:
        """Check whether query memory already has a reusable high-confidence SQL hit."""
        if not self.agent.memory_service:
            return False

        try:
            similar_queries = self.agent.memory_service.search_similar_queries(
                prompt=query,
                limit=3,
                project_scoped=True,
                similarity_threshold=0.75,
            )
            for result in similar_queries:
                if (
                    result.similarity_score >= 0.85
                    and result.record.execution_success
                    and result.record.generated_sql
                ):
                    return True
        except Exception as cache_err:
            logger.warning(f"CxO mode: cache pre-check failed: {cache_err}")

        return False

    @staticmethod
    def _has_usable_cxo_context(context: Optional[Dict[str, Any]]) -> bool:
        """Return True when CxO context has selected tables ready for SQL planning."""
        if not isinstance(context, dict):
            return False
        tables = context.get("tables")
        return isinstance(tables, list) and len(tables) > 0

    def _build_cxo_context(self, query: str) -> Optional[Dict[str, Any]]:
        """
        In CxO mode, connect to the database, run NLP table selection on the
        user's prompt, collect table info for the selected tables, and return
        a complete data context ready for execute_query.

        Args:
            query: The user query (potentially enriched with clarification)
        """
        from db.processing import _collect_table_info

        db_type = self.data_context["db_type"]
        credentials = self.data_context["credentials"]
        semantic_layer = self.data_context.get("semantic_layer")

        # If query memory has a high-confidence SQL hit, first try to reuse an
        # existing selected-table context; otherwise run NLP once to seed context
        # for stable follow-up questions in the same chat.
        has_cache_hit = self._has_high_confidence_cache_hit(query)
        if has_cache_hit:
            logger.info("CxO mode: cache hit detected, attempting context reuse")
            cached_context = self.data_context.get("_cxo_selected_context")
            if self._has_usable_cxo_context(cached_context):
                logger.info("CxO mode: cache hit using existing selected-table context")
                return cached_context
            logger.info(
                "CxO mode: cache hit has no selected-table context; running NLP selection to seed follow-up context"
            )

        logger.info(f"CxO mode: connecting to {db_type} for NLP table selection...")
        connector = DatabaseConnector()
        success, message = connector.connect(db_type, credentials)
        if not success:
            logger.error(f"CxO mode: DB connection failed: {message}")
            connector.close()
            return None

        try:
            # --- Expand prompt via LLM middleman if enabled ---
            effective_prompt = query
            if ConfigManager.get_prompt_expansion_enabled():
                logger.info(
                    "CxO mode: prompt expansion enabled — attempting LLM expansion"
                )
                try:
                    all_tables = self.data_context.get("all_tables", [])
                    schema_meta = {"tables": all_tables}
                    exp_agent = AIAgent()
                    exp_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(exp_loop)
                    try:
                        expanded = exp_loop.run_until_complete(
                            exp_agent.prompt_expansion_agent(
                                query, schema_meta, semantic_layer
                            )
                        )
                    finally:
                        exp_loop.run_until_complete(exp_loop.shutdown_asyncgens())
                        exp_loop.close()
                    if expanded and expanded.strip():
                        logger.info(f"CxO prompt expanded: {expanded[:200]}")
                        effective_prompt = expanded
                    else:
                        logger.warning(
                            "CxO prompt expansion returned empty result, using original"
                        )
                except Exception as e:
                    logger.warning(f"CxO prompt expansion failed, using original: {e}")

            # Run NLP table selector
            selector = NLPTableSelector(
                connector,
                semantic_layer=semantic_layer or {},
            )
            result = selector.select_tables(effective_prompt, top_k=5)

            if result.status == "no_match" or not result.tables:
                logger.warning("CxO mode: NLP found no matching tables")
                connector.close()
                return None

            # Use all selected + top candidates
            selected_tables = result.tables[:]
            if result.top_candidates:
                selected_tables = list(
                    dict.fromkeys(result.tables + result.top_candidates)
                )

            logger.info(f"CxO mode: NLP selected tables: {selected_tables}")

            # Collect table info for the selected tables
            table_info: Dict[str, Any] = {}
            for table_name in selected_tables:
                info, _skipped = _collect_table_info(connector, table_name)
                table_info[table_name] = info

            context = {
                "source_type": "database",
                "db_type": db_type,
                "credentials": credentials,
                "tables": selected_tables,
                "table_info": table_info,
                "semantic_layer": semantic_layer,
            }

            logger.info(f"CxO mode: built context with {len(selected_tables)} tables")
            return context

        except Exception as e:
            logger.error(f"CxO mode: NLP table selection failed: {e}", exc_info=True)
            return None
        finally:
            connector.close()
