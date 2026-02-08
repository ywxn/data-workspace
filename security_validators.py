"""
Security validation utilities for code execution.

Provides validators to prevent malicious code patterns
and ensure safe execution of generated code.
"""

import re
from typing import Tuple, List, NamedTuple


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
