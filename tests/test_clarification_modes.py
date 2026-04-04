"""Tests for mode-aware clarification prompt behavior."""

import asyncio
from typing import Any, Dict, List, Optional
from unittest.mock import patch

from agents.pipeline import AgentPipelineMixin


class _DummyPipelineHost(AgentPipelineMixin):
    """Minimal host to exercise clarification detector behavior."""

    def __init__(self) -> None:
        self.last_messages: Optional[List[Dict[str, str]]] = None

    async def _call_llm(
        self,
        messages,
        max_tokens=300,
        temperature=0.2,
        stream=False,
        stream_callback=None,
    ):
        self.last_messages = messages
        return "CLEAR"

    @staticmethod
    def _build_schema_metadata(context: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "tables": ["sales", "customers"],
            "columns_by_table": {
                "sales": ["id", "customer_id", "amount", "order_date"],
                "customers": ["id", "name", "segment"],
            },
            "sample_rows": "sales(id=1, amount=100)",
            "columns": [
                "sales.id",
                "sales.customer_id",
                "sales.amount",
                "sales.order_date",
                "customers.id",
                "customers.name",
                "customers.segment",
            ],
        }


def _run(coro):
    return asyncio.run(coro)


@patch("agents.pipeline.ConfigManager.get_clarification_enabled", return_value=True)
@patch("agents.pipeline.AgentPipelineMixin._can_infer_query_meaning", return_value=False)
def test_clarification_prompt_is_non_technical_in_cxo(
    _mock_infer,
    _mock_enabled,
):
    host = _DummyPipelineHost()

    result = _run(
        host.clarification_detector(
            user_query="Show growth for our top clients",
            context={},
            semantic_layer=None,
            interaction_mode="cxo",
        )
    )

    assert result is None
    assert host.last_messages is not None
    system_prompt = host.last_messages[0]["content"]
    assert "INTERACTION MODE: cxo" in system_prompt
    assert "NEVER ask for table names, column names" in system_prompt
    assert "plain business language" in system_prompt


@patch("agents.pipeline.ConfigManager.get_clarification_enabled", return_value=True)
@patch("agents.pipeline.AgentPipelineMixin._can_infer_query_meaning", return_value=False)
def test_clarification_prompt_allows_precision_in_analyst(
    _mock_infer,
    _mock_enabled,
):
    host = _DummyPipelineHost()

    result = _run(
        host.clarification_detector(
            user_query="Compare conversion by channel for Q1",
            context={},
            semantic_layer=None,
            interaction_mode="analyst",
        )
    )

    assert result is None
    assert host.last_messages is not None
    system_prompt = host.last_messages[0]["content"]
    assert "INTERACTION MODE: analyst" in system_prompt
    assert "Technical precision is allowed" in system_prompt
    assert "Asking for exact field names, table names" in system_prompt
