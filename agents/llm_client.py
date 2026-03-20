"""LLM client abstraction for calling different providers."""
import asyncio
import random
from typing import Dict, List, Optional, Callable

from core.logger import get_logger
from core.constants import (
    LLM_MAX_TOKENS_DEFAULT,
    LLM_TEMPERATURE_DEFAULT,
    LOCAL_LLM_DEFAULT_URL,
    LOCAL_LLM_DEFAULT_MODEL,
    LOCAL_LLM_REQUEST_TIMEOUT,
)

logger = get_logger(__name__)


class LLMClient:
    """Thin wrapper around Anthropic / OpenAI / local-LLM HTTP calls."""

    def __init__(
        self,
        api_provider: str,
        client,
        resolve_model_fn: Callable[[], str],
        local_llm_url: Optional[str] = None,
        local_llm_model: Optional[str] = None,
        server_starting: bool = False,
    ):
        self.api_provider = api_provider
        self.client = client
        self.resolve_model_fn = resolve_model_fn
        self._local_llm_url = local_llm_url
        self._local_llm_model = local_llm_model
        self._server_starting = server_starting

    async def _call_llm(
        self,
        messages: List[Dict[str, str]],
        max_tokens: int = LLM_MAX_TOKENS_DEFAULT,
        temperature: float = LLM_TEMPERATURE_DEFAULT,
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
        import asyncio

        max_rate_limit_retries = 3

        for retry_index in range(max_rate_limit_retries + 1):
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
                should_retry = self._is_rate_limit_error(e)
                last_attempt = retry_index >= max_rate_limit_retries

                if not should_retry or last_attempt:
                    logger.error(f"LLM call failed: {str(e)}")
                    raise RuntimeError(f"Failed to call {self.api_provider}: {str(e)}")

                delay_seconds = self._get_rate_limit_backoff_seconds(e, retry_index)
                logger.warning(
                    "Rate limit encountered from %s; retrying in %.2fs "
                    "(attempt %d/%d)",
                    self.api_provider,
                    delay_seconds,
                    retry_index + 1,
                    max_rate_limit_retries,
                )
                await asyncio.sleep(delay_seconds)

        raise RuntimeError(
            f"Failed to call {self.api_provider}: exceeded retry attempts"
        )

    @staticmethod
    def _is_rate_limit_error(error: Exception) -> bool:
        """Return True when an exception indicates provider/API rate limiting."""
        status_code = getattr(error, "status_code", None)
        if status_code == 429:
            return True

        response = getattr(error, "response", None)
        if response is not None and getattr(response, "status_code", None) == 429:
            return True

        error_name = type(error).__name__.lower()
        error_text = str(error).lower()
        return "ratelimit" in error_name or "rate limit" in error_text

    @staticmethod
    def _get_rate_limit_backoff_seconds(error: Exception, retry_index: int) -> float:
        """Compute retry delay honoring Retry-After when available."""
        retry_after = None

        response = getattr(error, "response", None)
        headers = getattr(response, "headers", None)
        if headers:
            retry_after_raw = headers.get("retry-after") or headers.get("Retry-After")
            if retry_after_raw is not None:
                try:
                    retry_after = float(str(retry_after_raw).strip())
                except (TypeError, ValueError):
                    retry_after = None

        if retry_after is not None:
            return max(0.5, min(retry_after, 60.0))

        # Exponential backoff with small jitter to reduce synchronized retries.
        base_delay = min(2**retry_index, 30)
        jitter = random.uniform(0.0, 0.5)
        return float(base_delay) + jitter

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

        model_id = self.resolve_model_fn()
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
        model_id = self.resolve_model_fn()
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
        model_id = self.resolve_model_fn()
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
