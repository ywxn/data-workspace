import markdown
import re

from config import ConfigManager
from constants import DARK_THEME_STYLESHEET, LIGHT_THEME_STYLESHEET


def _extract_markdown_theme_colors(stylesheet: str, fallback: dict) -> dict:
    """Extract markdown SQL color values from theme QSS metadata comments."""

    def _get_value(key: str) -> str:
        pattern = rf"/\*\s*{re.escape(key)}\s*:\s*([^*]+?)\s*\*/"
        match = re.search(pattern, stylesheet, flags=re.IGNORECASE)
        return match.group(1).strip() if match else fallback.get(key, "")

    return {
        "code_bg": _get_value("markdown-sql-code-bg"),
        "code_fg": _get_value("markdown-sql-code-fg"),
        "inline_bg": _get_value("markdown-inline-code-bg"),
        "text_color": _get_value("markdown-text-color"),
    }


def markdown_to_html(md: str) -> str:
    """
    Convert GitHub Flavored Markdown to styled HTML.

    Includes support for:
    - Tables with styling
    - Fenced code blocks
    - Lists

    Args:
        md: Markdown text string

    Returns:
        HTML string with embedded CSS for table styling
    """
    # Convert markdown to HTML with extensions for GitHub-flavored markdown
    html_content = markdown.markdown(
        md, extensions=["tables", "fenced_code", "sane_lists"]
    )

    config = ConfigManager.load_config()
    theme = config.get("theme", "system")

    dark_fallback = {
        "code_bg": "#3a3a3a",
        "code_fg": "#ffffff",
        "inline_bg": "#3a3a3a",
        "text_color": "#f8fafc",
    }
    light_fallback = {
        "code_bg": "#f6f8fa",
        "code_fg": "#1f2328",
        "inline_bg": "#eef2f7",
        "text_color": "#1f2328",
    }

    if theme == "dark":
        colors = _extract_markdown_theme_colors(DARK_THEME_STYLESHEET, dark_fallback)
    elif theme == "light":
        colors = _extract_markdown_theme_colors(LIGHT_THEME_STYLESHEET, light_fallback)
    else:
        # System mode has no dedicated QSS; use the light theme metadata.
        colors = _extract_markdown_theme_colors(LIGHT_THEME_STYLESHEET, light_fallback)

    code_bg = colors["code_bg"]
    code_fg = colors["code_fg"]
    inline_bg = colors["inline_bg"]
    text_color = colors["text_color"]
    # Add CSS styling for tables and other elements
    styled_html = f"""
<style>
    body {{
        color: {text_color};
    }}
    table {{
        border-collapse: collapse;
        margin: 10px 0;
        width: 100%;
    }}
    th, td {{
        border: 1px solid #888;
        padding: 6px 12px;
        text-align: left;
    }}
    th {{
        background: #4a4a4a;
        color: #ffffff;
        font-weight: bold;
    }}
    tr:nth-child(even) {{
        background: #2a2a2a;
    }}
    tr:hover {{
        background: #3a3a3a;
    }}
    code {{
        background: {inline_bg};
        color: {code_fg};
        padding: 2px 4px;
        border-radius: 3px;
        font-family: 'Consolas', 'Monaco', monospace;
    }}
    pre {{
        background: {code_bg};
        color: {code_fg} !important;
        border: 1px solid #d0d7de;
        padding: 10px;
        border-radius: 5px;
        overflow-x: auto;
        white-space: pre;
    }}
    pre code {{
        background: none;
        color: {code_fg} !important;
        padding: 0;
    }}
    pre code * {{
        color: {code_fg} !important;
    }}
</style>
{html_content}
"""

    return styled_html
