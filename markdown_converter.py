import markdown


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

    # Add CSS styling for tables and other elements
    styled_html = f"""
<style>
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
        background: #2a2a2a;
        padding: 2px 4px;
        border-radius: 3px;
        font-family: 'Consolas', 'Monaco', monospace;
    }}
    pre {{
        background: #2a2a2a;
        padding: 10px;
        border-radius: 5px;
        overflow-x: auto;
    }}
    pre code {{
        background: none;
        padding: 0;
    }}
</style>
{html_content}
"""

    return styled_html
