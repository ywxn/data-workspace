"""
Security validation utilities for code execution.

Provides validators to prevent malicious code patterns
and ensure safe execution of generated code.
"""

import re
from typing import Tuple, List, NamedTuple, Dict, Optional


class SecurityRule(NamedTuple):
    """Represents a security rule pattern and its violation message."""

    pattern: str
    message: str
    ignore_case: bool = True


# Define security rules as data, not in code
FILE_OPERATION_RULES = [
    SecurityRule(
        r"\.to_csv\s*\(",
        "DataFrame.to_csv() is forbidden - return data as string instead",
    ),
    SecurityRule(
        r"\.to_excel\s*\(",
        "DataFrame.to_excel() is forbidden - return data as string instead",
    ),
    SecurityRule(
        r"\.to_json\s*\(",
        "DataFrame.to_json() is forbidden - return data as JSON-compatible dict instead",
    ),
    SecurityRule(
        r"\.to_parquet\s*\(",
        "DataFrame.to_parquet() is forbidden - return data as dict instead",
    ),
    SecurityRule(r"\.to_sql\s*\(", "DataFrame.to_sql() is forbidden"),
    SecurityRule(r"\.to_pickle\s*\(", "DataFrame.to_pickle() is forbidden"),
    SecurityRule(r"\.to_hdf\s*\(", "DataFrame.to_hdf() is forbidden"),
    SecurityRule(
        r"open\s*\(", "open() for writing files is forbidden - use tempfile only"
    ),
]

SHELL_COMMAND_RULES = [
    SecurityRule(r"os\.system", "os.system() is forbidden"),
    SecurityRule(r"subprocess", "subprocess module is forbidden"),
    SecurityRule(r"os\.popen", "os.popen() is forbidden"),
    SecurityRule(r"os\.execv", "os.execv() is forbidden"),
]

DANGEROUS_EVAL_RULES = [
    SecurityRule(r"\beval\s*\(", "eval() is forbidden"),
    SecurityRule(r"\bexec\s*\(", "exec() is forbidden"),
    SecurityRule(
        r"__import__", "__import__() is forbidden except in controlled contexts"
    ),
]

PATH_TRAVERSAL_RULES = [
    SecurityRule(r"os\.chdir", "os.chdir() is forbidden"),
    SecurityRule(
        r"pathlib\.Path\s*\(.*\)", "Use only tempfile - absolute paths forbidden"
    ),
]

# All rules combined
ALL_SECURITY_RULES = (
    FILE_OPERATION_RULES
    + SHELL_COMMAND_RULES
    + DANGEROUS_EVAL_RULES
    + PATH_TRAVERSAL_RULES
)

SQL_INJECTION_RULES = [
    SecurityRule(r";\s*\S+", "Multiple SQL statements are not allowed"),
    SecurityRule(r"--", "SQL line comments are not allowed"),
    SecurityRule(r"/\*", "SQL block comments are not allowed"),
    SecurityRule(r"\bunion\b\s+\bselect\b", "UNION SELECT is not allowed"),
    SecurityRule(r"\bor\s+1\s*=\s*1\b", "Tautology-based SQL injection detected"),
    SecurityRule(r"\bxp_cmdshell\b", "xp_cmdshell is forbidden"),
    SecurityRule(r"\bexec\b", "EXEC is forbidden in ad-hoc queries"),
]

SQL_DYNAMIC_RULES = [
    SecurityRule(r"\{\s*\w+\s*\}", "Dynamic SQL formatting detected"),
    SecurityRule(r"%\([a-zA-Z0-9_]+\)s", "Percent-style SQL formatting detected"),
]

ALL_SQL_RULES = SQL_INJECTION_RULES + SQL_DYNAMIC_RULES


def validate_code_security(code: str) -> Tuple[bool, str]:
    """
    Validate generated code for security violations.

    Checks the code against a list of dangerous patterns.

    Args:
        code: Python code string to validate

    Returns:
        Tuple of (is_safe: bool, error_message: str)
        If is_safe is True, error_message is empty string.
        If is_safe is False, error_message contains the violation details.
    """
    for rule in ALL_SECURITY_RULES:
        flags = re.IGNORECASE if rule.ignore_case else 0
        if re.search(rule.pattern, code, flags):
            return False, f"Security violation: {rule.message}"

    return True, ""


def get_security_violations(code: str) -> List[str]:
    """
    Get all security violations found in the code.

    Useful for detailed error reporting.

    Args:
        code: Python code string to check

    Returns:
        List of violation messages (empty if code is safe)
    """
    violations = []

    for rule in ALL_SECURITY_RULES:
        flags = re.IGNORECASE if rule.ignore_case else 0
        if re.search(rule.pattern, code, flags):
            violations.append(f"- {rule.message}")

    return violations


def validate_sql_security(
    query: str, params: Optional[Dict[str, object]] = None
) -> Tuple[bool, str]:
    """
    Validate SQL query string for injection and unsafe patterns.

    Args:
        query: SQL query string
        params: Optional query parameters used for parameterization

    Returns:
        Tuple of (is_safe: bool, error_message: str)
    """
    for rule in ALL_SQL_RULES:
        flags = re.IGNORECASE if rule.ignore_case else 0
        if re.search(rule.pattern, query, flags):
            return False, f"SQL security violation: {rule.message}"

    if re.search(r"\bwhere\b", query, re.IGNORECASE):
        has_string_literal = bool(re.search(r"'[^']*'", query))
        if has_string_literal and not params:
            return (
                False,
                "SQL security violation: missing parameterization for WHERE clause",
            )

    return True, ""


def get_sql_security_violations(
    query: str, params: Optional[Dict[str, object]] = None
) -> List[str]:
    """
    Get SQL security violations found in the query.

    Args:
        query: SQL query string
        params: Optional query parameters used for parameterization

    Returns:
        List of violation messages (empty if safe)
    """
    violations: List[str] = []

    for rule in ALL_SQL_RULES:
        flags = re.IGNORECASE if rule.ignore_case else 0
        if re.search(rule.pattern, query, flags):
            violations.append(f"- {rule.message}")

    if re.search(r"\bwhere\b", query, re.IGNORECASE):
        has_string_literal = bool(re.search(r"'[^']*'", query))
        if has_string_literal and not params:
            violations.append("- Missing parameterization for WHERE clause")

    return violations
