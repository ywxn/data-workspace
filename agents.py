"""
Multi-agent system for AI-powered data analysis.

This module provides an orchestrated agent system that breaks down data analysis
queries into manageable tasks, generates code, and provides insights.
"""

import pandas as pd
from openai import AsyncOpenAI
from anthropic import Anthropic
from openai.types.chat import ChatCompletionMessageParam
from typing import Dict, Any, List, Optional, Tuple
import tempfile
import os
import json
import re
import altair as alt
import numpy as np

from config import ConfigManager
from logger import get_logger
from security_validators import validate_code_security, get_security_violations
from constants import (
    LLM_MAX_TOKENS_DEFAULT,
    LLM_MAX_TOKENS_CODE,
    LLM_MAX_TOKENS_ANALYSIS,
    LLM_TEMPERATURE_DEFAULT,
    LLM_TEMPERATURE_CODE,
    LLM_TEMPERATURE_ANALYSIS,
    LLM_MODELS,
)

# Load API keys from configuration
OPENAI_API_KEY = ConfigManager.get_api_key("openai")
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


class AIAgent:
    """
    Multi-agent system for data analysis with planner, code generator, and analyzer.

    Coordinates three specialized agents:
    - Planner: Breaks down queries into execution plans
    - Code Generator: Creates executable pandas code
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
        openai_key = ConfigManager.get_api_key("openai")
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
        else:
            raise ValueError(
                f"Unknown API provider: {self.api_provider}. Use 'openai' or 'claude'"
            )

        self.execution_context: Dict[str, Any] = {}

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

    @staticmethod
    def _get_column_names(df: pd.DataFrame) -> List[str]:
        """Get column names from a pandas DataFrame."""
        return df.columns.tolist()

    @staticmethod
    def _get_sample_data(df: pd.DataFrame, n: int = 5) -> List[Dict[str, Any]]:
        """Get sample data from a pandas DataFrame."""
        return df.head(n).to_dict(orient="records")

    @staticmethod
    def _get_dataframe_info(df: pd.DataFrame) -> Dict[str, Any]:
        """
        Get comprehensive DataFrame information for context.

        Returns:
            Dictionary with columns, dtypes, shape, null counts, and sample data
        """
        return {
            "columns": df.columns.tolist(),
            "dtypes": df.dtypes.astype(str).to_dict(),
            "shape": df.shape,
            "null_counts": df.isnull().sum().to_dict(),
            "sample": df.head(3).to_dict(orient="records"),
        }

    async def planner_agent(self, user_query: str, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Planner agent that breaks down user queries into actionable steps.

        Analyzes the user's request and creates a structured execution plan.

        Args:
            user_query: The user's data analysis question
            df: The DataFrame to analyze

        Returns:
            Dictionary containing task_type, steps, code requirements, etc.
        """
        dataframe_metadata = self._get_dataframe_info(df)

        system_message = f"""You are a data analysis planner. Given a user query and DataFrame info, create a clear execution plan.

DataFrame Info:
- Columns: {dataframe_metadata['columns']}
- Shape: {dataframe_metadata['shape']} (rows, columns)
- Data types: {dataframe_metadata['dtypes']}
- Sample: {dataframe_metadata['sample']}

Analyze the user's request and respond with a JSON plan containing:
1. "task_type": one of ["analysis", "code_generation", "visualization", "summary", "transformation"]
2. "steps": list of specific steps needed
3. "requires_code": boolean indicating if code generation is needed
4. "analysis_focus": specific aspects to analyze
5. "requires_visualization": boolean indicating if visualizations are necessary

Return ONLY valid JSON, no markdown or explanations."""

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
                "requires_code": False,
                "analysis_focus": user_query,
            },
        )

    async def code_generation_agent(
        self, plan: Dict[str, Any], user_query: str, df: pd.DataFrame
    ) -> str:
        """
        Code generation agent that creates executable pandas/Python code.

        Args:
            plan: Execution plan from planner agent
            user_query: Original user query
            df: DataFrame to operate on

        Returns:
            Executable Python code string
        """
        dataframe_metadata = self._get_dataframe_info(df)

        system_message = f"""You are an expert Python code generator specializing in pandas data analysis.

DataFrame Info:
- Columns: {dataframe_metadata['columns']}
- Data types: {dataframe_metadata['dtypes']}
- Shape: {dataframe_metadata['shape']}
- Sample data: {dataframe_metadata['sample']}

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

        messages: List[Dict[str, str]] = [
            {"role": "system", "content": system_message},
            {"role": "user", "content": f"Generate code for: {user_query}"},
        ]

        logger.info("Calling code generation agent...")
        generated_code = await self._call_llm(
            messages,
            max_tokens=CODE_GENERATION_MAX_TOKENS,
            temperature=CODE_GENERATION_TEMPERATURE,
        )

        return self._clean_code_markdown(generated_code)

    async def analysis_agent(
        self,
        user_query: str,
        df: pd.DataFrame,
        plan: Optional[Dict[str, Any]] = None,
        code_output: Optional[Any] = None,
    ) -> str:
        """
        Analysis agent that provides insights and interprets results.

        Can work standalone or interpret outputs from code execution.

        Args:
            user_query: Original user query
            df: DataFrame being analyzed
            plan: Optional execution plan for context
            code_output: Optional output from code execution

        Returns:
            Human-readable analysis of the results
        """
        dataframe_metadata = self._get_dataframe_info(df)

        context_parts = [
            "DataFrame Info:",
            f"- Columns: {dataframe_metadata['columns']}",
            f"- Shape: {dataframe_metadata['shape']} (rows, columns)",
            f"- Data types: {dataframe_metadata['dtypes']}",
            f"- Sample: {dataframe_metadata['sample']}",
        ]

        if plan:
            context_parts.append(f"\nExecution Plan: {plan}")

        if code_output is not None:
            context_parts.append(f"\nCode Execution Result: {code_output}")

        system_message = f"""You are a thoughtful data analyst who explains findings in detail so non-technical people understand.

{chr(10).join(context_parts)}

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

    async def execute_query(self, user_query: str, df: pd.DataFrame) -> str:
        """
        Main orchestration method that coordinates all agents.

        This is the primary entry point for executing user queries.

        Args:
            user_query: The user's data analysis question
            df: The DataFrame to analyze

        Returns:
            Formatted response with results and analysis

        Raises:
            Exception: Logs error and returns error message
        """
        try:
            logger.info(f"Starting execute_query with query: {user_query}")

            # Step 1: Plan the task
            plan = await self.planner_agent(user_query, df)
            logger.info(f"Generated plan: {plan}")

            code_execution_result = None

            # Step 2: Generate and execute code if needed
            if plan.get("requires_code", False):
                logger.info("Code generation required")
                generated_code = await self.code_generation_agent(plan, user_query, df)
                code_execution_result = self._execute_generated_code(generated_code, df)
            else:
                logger.info("Code generation not required")

            # Step 3: Get analysis
            logger.info("Getting analysis from analysis agent")
            analysis = await self.analysis_agent(
                user_query, df, plan, code_execution_result
            )

            # Step 4: Format response
            return self._format_response(code_execution_result, analysis)

        except Exception as e:
            logger.error(f"Error in execute_query: {str(e)}", exc_info=True)
            return f"Error processing query: {str(e)}\n\nPlease try rephrasing your question."

    def _execute_generated_code(self, code: str, df: pd.DataFrame) -> Any:
        """
        Execute generated code safely with security validation.

        Args:
            code: Python code to execute
            df: DataFrame available to the code

        Returns:
            The 'result' variable from code execution, or error message
        """
        # Validate code safety
        is_safe, error_msg = validate_code_security(code)
        if not is_safe:
            logger.warning(f"Code security violation: {error_msg}")
            return f"Security violation detected: {error_msg}"

        try:
            logger.info("Executing generated code")

            # Setup execution environment
            local_vars = {
                "df": df,
                "pd": pd,
                "result": None,
                "tempfile": tempfile,
                "os": os,
                "alt": alt,
                "altair": alt,
            }

            global_vars = {
                "pd": pd,
                "df": df,
                "tempfile": tempfile,
                "os": os,
                "alt": alt,
                "altair": alt,
            }
            
            logger.info(f"Generated code:\n{code}")

            exec(code, global_vars, local_vars)
            code_result = local_vars.get("result")

            logger.info("Code execution successful")
            return code_result

        except Exception as e:
            error_msg = f"Code execution error: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return error_msg

    def _format_response(self, code_result: Any, analysis: str) -> str:
        """
        Format the final response with code execution results and analysis.

        Args:
            code_result: Output from code execution
            analysis: Text analysis from the analyzer

        Returns:
            Formatted response string
        """
        response_parts = []

        if code_result is not None:
            response_parts.append("### Result:")
            response_parts.append(self._format_code_result(code_result))
            response_parts.append("")

        response_parts.append("### Analysis:")
        response_parts.append(analysis)

        return "\n".join(response_parts)

    @staticmethod
    def _format_code_result(result: Any) -> str:
        """
        Format code execution result into readable string.

        Args:
            result: The result object to format

        Returns:
            Formatted string representation
        """
        if isinstance(result, dict):
            # Handle dictionary results (e.g., with image paths)
            parts = []
            for key, value in result.items():
                if key in ["image_path", "plot_path", "image_file"] and isinstance(
                    value, str
                ):
                    parts.append(f"![Generated Visualization]({value})")
                elif key == "message":
                    parts.append(str(value))
                else:
                    parts.append(f"**{key}:** {value}")
            return "\n".join(parts)
        elif isinstance(result, str):
            return result
        elif isinstance(result, pd.DataFrame):
            return result.to_markdown() or str(result)
        else:
            return str(result)

    @staticmethod
    def _clean_code_markdown(code: str) -> str:
        """
        Remove markdown formatting from generated code.

        Args:
            code: Code string possibly wrapped in markdown code blocks

        Returns:
            Clean Python code
        """
        code = code.strip()

        if code.startswith("```python"):
            code = code[9:]
        if code.startswith("```"):
            code = code[3:]
        if code.endswith("```"):
            code = code[:-3]

        return code.strip()

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
