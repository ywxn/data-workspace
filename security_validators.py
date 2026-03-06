"""
Hardened security validation utilities for AI-generated Python and SQL code.

Focus:
- Prevent OS command execution
- Prevent dynamic evaluation
- Enforce strict SQL safety (SELECT-only, parameterized-only)
- Detect common and advanced SQL injection patterns
"""

import re
from typing import Tuple, List, NamedTuple, Dict, Optional, Iterable


# ============================================================
# Core Rule Model
# ============================================================


class SecurityRule(NamedTuple):
    pattern: str
    message: str
    ignore_case: bool = True


# ============================================================
# Python Execution Rules (Minimal but High-Impact)
# ============================================================

SHELL_COMMAND_RULES = [
    SecurityRule(r"\bos\.system\s*\(", "os.system() is forbidden"),
    SecurityRule(
        r"\bsubprocess\.(run|popen|call|check_output|Popen)\s*\(",
        "subprocess execution is forbidden",
    ),
    SecurityRule(r"\bos\.popen\s*\(", "os.popen() is forbidden"),
]

DANGEROUS_EVAL_RULES = [
    SecurityRule(r"\beval\s*\(", "eval() is forbidden"),
    SecurityRule(r"\bexec\s*\(", "exec() is forbidden"),
    SecurityRule(r"\bcompile\s*\(", "compile() is forbidden"),
    SecurityRule(r"\b__import__\s*\(", "__import__() is forbidden"),
    SecurityRule(
        r"\bimport\s+(os|subprocess|sys)\b", "Importing system modules is forbidden"
    ),
]

DESTRUCTIVE_FS_RULES = [
    SecurityRule(r"\bshutil\.rmtree\s*\(", "shutil.rmtree() is forbidden"),
    SecurityRule(r"\bos\.remove\s*\(", "os.remove() is forbidden"),
    SecurityRule(r"\bos\.unlink\s*\(", "os.unlink() is forbidden"),
]

ALL_SECURITY_RULES: Iterable[SecurityRule] = (
    SHELL_COMMAND_RULES + DANGEROUS_EVAL_RULES + DESTRUCTIVE_FS_RULES
)


# ============================================================
# SQL Security Rules
# ============================================================

# Hard deny dangerous SQL keywords (DDL/DML/system access)
FORBIDDEN_SQL_KEYWORDS = [
    "insert",
    "update",
    "delete",
    "drop",
    "alter",
    "truncate",
    "create",
    "grant",
    "revoke",
    "exec",
    "execute",
    "xp_cmdshell",
]

# Injection patterns
SQL_INJECTION_RULES = [
    SecurityRule(r";\s*\S+", "Stacked SQL statements are not allowed"),
    SecurityRule(r"\bunion\s+select\b", "UNION SELECT injection detected"),
    SecurityRule(r"\bor\s+1\s*=\s*1\b", "Tautology injection detected"),
    SecurityRule(r"\band\s+1\s*=\s*1\b", "Tautology injection detected"),
]

ALL_SQL_RULES = SQL_INJECTION_RULES


# ============================================================
# Utility Helpers
# ============================================================


def _normalize_sql(query: str) -> str:
    """Normalize SQL for analysis."""
    # Remove excessive whitespace
    query = re.sub(r"\s+", " ", query.strip())
    return query


def _contains_forbidden_keywords(query: str) -> Optional[str]:
    """Check for forbidden SQL keywords."""
    lowered = query.lower()
    for keyword in FORBIDDEN_SQL_KEYWORDS:
        if re.search(rf"\b{keyword}\b", lowered):
            return f"Forbidden SQL keyword detected: {keyword.upper()}"
    return None


def _validate_parameterization(
    query: str, params: Optional[Dict[str, object]]
) -> Optional[str]:
    """
    Enforce parameterized queries using named placeholders.

    Acceptable placeholder styles:
        :name
        %(name)s

    Reject:
        direct interpolation
        no params when placeholders present
    """

    named_placeholders = re.findall(r":([a-zA-Z_][a-zA-Z0-9_]*)", query)
    percent_placeholders = re.findall(r"%\(([a-zA-Z_][a-zA-Z0-9_]*)\)s", query)

    placeholders = set(named_placeholders + percent_placeholders)

    if placeholders:
        if not params:
            return "Query contains placeholders but no parameters provided"

        missing = placeholders - set(params.keys())
        if missing:
            return f"Missing SQL parameters: {', '.join(sorted(missing))}"

    else:
        # No placeholders — must not contain raw string literals
        if re.search(r"'[^']*'", query):
            return "Literal string values detected — use parameterized queries"

    return None


def _enforce_select_only(query: str) -> Optional[str]:
    """
    Enforce strict SELECT-only queries.
    """
    lowered = query.lower().strip()

    if not lowered.startswith("select"):
        return "Only SELECT statements are allowed"

    if ";" in lowered[:-1]:
        return "Multiple SQL statements are not allowed"

    return None


# ============================================================
# Public Validation Functions
# ============================================================


def validate_code_security(code: str) -> Tuple[bool, str]:
    """
    Validate Python code against dangerous execution patterns.
    """
    for rule in ALL_SECURITY_RULES:
        flags = re.IGNORECASE if rule.ignore_case else 0
        if re.search(rule.pattern, code, flags):
            return False, f"Security violation: {rule.message}"

    return True, ""


def get_security_violations(code: str) -> List[str]:
    """
    Return all Python security violations.
    """
    violations: List[str] = []

    for rule in ALL_SECURITY_RULES:
        flags = re.IGNORECASE if rule.ignore_case else 0
        if re.search(rule.pattern, code, flags):
            violations.append(f"- {rule.message}")

    return violations


def validate_sql_security(
    query: str, params: Optional[Dict[str, object]] = None
) -> Tuple[bool, str]:
    """
    Validate SQL query for injection, dynamic construction,
    and enforce SELECT-only parameterized policy.
    """

    if not query or not query.strip():
        return False, "Empty SQL query"

    query = _normalize_sql(query)

    # 1. Pattern-based injection detection
    for rule in ALL_SQL_RULES:
        flags = re.IGNORECASE if rule.ignore_case else 0
        if re.search(rule.pattern, query, flags):
            return False, f"SQL security violation: {rule.message}"

    # 2. Enforce SELECT-only
    select_error = _enforce_select_only(query)
    if select_error:
        return False, select_error

    # 3. Block dangerous keywords
    keyword_error = _contains_forbidden_keywords(query)
    if keyword_error:
        return False, keyword_error

    # 4. Enforce parameterization correctness
    param_error = _validate_parameterization(query, params)
    if param_error:
        return False, param_error

    return True, ""


def get_sql_security_violations(
    query: str, params: Optional[Dict[str, object]] = None
) -> List[str]:
    """
    Return all SQL security violations found.
    """
    violations: List[str] = []

    is_safe, message = validate_sql_security(query, params)
    if not is_safe:
        violations.append(f"- {message}")

    return violations
