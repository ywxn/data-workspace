import markdown
import re

from core.config import ConfigManager
from core.constants import DARK_THEME_STYLESHEET, LIGHT_THEME_STYLESHEET

_DARK_FALLBACK = {
    "code_bg": "#3a3a3a",
    "code_fg": "#ffffff",
    "inline_bg": "#3a3a3a",
    "text_color": "#f8fafc",
}
_LIGHT_FALLBACK = {
    "code_bg": "#f6f8fa",
    "code_fg": "#1f2328",
    "inline_bg": "#eef2f7",
    "text_color": "#1f2328",
}

# Module-level cache for the <style> block (invalidated on theme change).
_style_cache: dict = {"theme": None, "html": ""}


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


def _resolve_theme_colors(theme: str) -> dict:
    if theme == "dark":
        return _extract_markdown_theme_colors(DARK_THEME_STYLESHEET, _DARK_FALLBACK)
    elif theme == "light":
        return _extract_markdown_theme_colors(LIGHT_THEME_STYLESHEET, _LIGHT_FALLBACK)
    else:
        return _extract_markdown_theme_colors(LIGHT_THEME_STYLESHEET, _LIGHT_FALLBACK)


def _build_style_block(colors: dict) -> str:
    code_bg = colors["code_bg"]
    code_fg = colors["code_fg"]
    inline_bg = colors["inline_bg"]
    text_color = colors["text_color"]
    return f"""<style>
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
</style>"""


def invalidate_markdown_style_cache() -> None:
    """Clear the cached style block (call after a theme change)."""
    _style_cache["theme"] = None


def markdown_style_html() -> str:
    """Return the ``<style>`` block for the current theme, cached."""
    config = ConfigManager.load_config()
    theme = config.get("theme", "system")
    if _style_cache["theme"] == theme:
        return _style_cache["html"]
    colors = _resolve_theme_colors(theme)
    style = _build_style_block(colors)
    _style_cache["theme"] = theme
    _style_cache["html"] = style
    return style


def markdown_body_to_html(md: str) -> str:
    """Convert markdown text to an HTML fragment (no ``<style>`` wrapper)."""
    return markdown.markdown(
        md, extensions=["tables", "fenced_code", "sane_lists"]
    )


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
    return markdown_style_html() + "\n" + markdown_body_to_html(md)
