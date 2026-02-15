"""
Markdown to HTML conversion utilities.

Provides robust markdown-to-HTML conversion with fallback to regex-based
conversion if the markdown library is not available. Handles images,
code blocks, tables, and other common markdown elements.
"""

import re
import os
import base64
from typing import Optional, List, Tuple, Dict
from constants import (
    SUPPORTED_IMAGE_FORMATS,
    MARKDOWN_CSS_TABLE_STYLE,
    MARKDOWN_CSS_TABLE_HEADER,
    MARKDOWN_CSS_TABLE_CELL,
    MARKDOWN_CSS_TABLE_CELL_ALT,
    MARKDOWN_CSS_CODE_BLOCK,
)
from logger import get_logger

logger = get_logger(__name__)

try:
    import markdown

    MARKDOWN_LIBRARY_AVAILABLE = True
except ImportError:
    MARKDOWN_LIBRARY_AVAILABLE = False


def convert_image_path_to_base64(image_path: str) -> str:
    """
    Convert a local image file path to base64 data URL.

    Args:
        image_path: Path to image file (may include file:// prefix)

    Returns:
        Data URL or original path if conversion fails
    """
    try:  # If already a data URL, return as-is
        if image_path.startswith("data:"):
            return image_path
        # Remove file:// URL scheme if present
        if image_path.startswith("file:///"):
            image_path = image_path[8:]
            if len(image_path) > 2 and image_path[0] == "/" and image_path[2] == ":":
                image_path = image_path[1:]
        elif image_path.startswith("file://"):
            image_path = image_path[7:]

        image_path = os.path.normpath(image_path)

        if not os.path.isfile(image_path):
            logger.warning(f"Image file not found: {image_path}")
            return image_path

        # Determine MIME type from extension
        ext = os.path.splitext(image_path)[1].lower()
        mime_type = SUPPORTED_IMAGE_FORMATS.get(ext, "image/png")

        # Read and encode as base64
        with open(image_path, "rb") as f:
            image_data = base64.b64encode(f.read()).decode("utf-8")

        return f"data:{mime_type};base64,{image_data}"
    except Exception as e:
        logger.warning(f"Error converting image to data URL: {e}")
        return image_path


def convert_image_paths_to_base64(html: str) -> str:
    """
    Convert all image src file paths in HTML to base64 data URLs.

    Args:
        html: HTML string with img tags

    Returns:
        HTML with image paths converted to data URLs
    """

    def replace_img_src(match):
        full_tag = match.group(0)
        src = match.group(1)
        data_url = convert_image_path_to_base64(src)
        return full_tag.replace(src, data_url)

    # Match img tags with src attributes (handles both quote styles)
    html = re.sub(
        r'<img\s+[^>]*src=["\']([^"\']+)["\']',
        replace_img_src,
        html,
        flags=re.IGNORECASE,
    )
    return html


def plaintext_image_source_to_html(text: str) -> str:
    # TODO: Make this less shitty
    """
    Convert plaintext image paths in text to HTML img tags with base64 sources.

    Args:
        text: Text containing plaintext image paths

    Returns:
        Text with image paths replaced by HTML img tags
    """

    def replace_plain_image(match):
        image_path = match.group(0)
        data_url = convert_image_path_to_base64(image_path)
        return f'<img src="{data_url}" alt="Image" style="max-width: 100%; height: auto;" />'

    # Match lines that look like image file paths (e.g., ending with .png, .jpg)
    return re.sub(
        r"(?<!src=[\"'])(?:file://)?(?:[A-Za-z]:\\)?[\w./\\-]+\.(?:png|jpg|jpeg|gif|svg|webp)\b",
        replace_plain_image,
        text,
        flags=re.IGNORECASE,
    )


def escape_html(text: str) -> str:
    """
    Escape HTML special characters.

    Args:
        text: Text to escape

    Returns:
        HTML-escaped text
    """
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def extract_code_blocks(text: str) -> Tuple[str, List[str]]:
    """
    Extract code blocks from markdown before processing other elements.

    Args:
        text: Markdown text

    Returns:
        Tuple of (text with code block placeholders, list of code contents)
    """
    code_blocks = []

    def save_code_block(match):
        code = match.group(2) if match.lastindex >= 2 else match.group(1)
        code_blocks.append(code)
        return f"___CODE_BLOCK_{len(code_blocks)-1}___"

    # Extract fenced code blocks
    text = re.sub(r"```(?:\w+)?\n([\s\S]*?)```", save_code_block, text)
    text = re.sub(r"```([\s\S]*?)```", save_code_block, text)

    return text, code_blocks


def replace_images(text: str) -> str:
    """
    Replace markdown image syntax with HTML img tags and convert paths to base64.

    Args:
        text: Text containing markdown images

    Returns:
        Text with HTML image tags
    """

    def replace_image(match):
        alt_text = match.group(1)
        image_path = match.group(2)
        data_url = convert_image_path_to_base64(image_path)
        return f'<img src="{data_url}" alt="{alt_text}" style="max-width: 100%; height: auto;" />'

    return re.sub(r"!\[([^\]]*)\]\(([^)]+)\)", replace_image, text)


def replace_headers(text: str) -> str:
    """Replace markdown headers with HTML header tags."""
    text = re.sub(r"^#### (.*?)$", r"<h4>\1</h4>", text, flags=re.MULTILINE)
    text = re.sub(r"^### (.*?)$", r"<h3>\1</h3>", text, flags=re.MULTILINE)
    text = re.sub(r"^## (.*?)$", r"<h2>\1</h2>", text, flags=re.MULTILINE)
    text = re.sub(r"^# (.*?)$", r"<h1>\1</h1>", text, flags=re.MULTILINE)
    return text


def replace_text_formatting(text: str) -> str:
    """Replace markdown text formatting (bold, italic, code, links)."""
    # Bold (**text**)
    text = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", text)

    # Italic (*text*)
    text = re.sub(r"\*([^*]+)\*", r"<em>\1</em>", text)

    # Inline code (`code`)
    text = re.sub(r"`([^`]+)`", r"<code>\1</code>", text)

    # Links ([text](url))
    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r'<a href="\2">\1</a>', text)

    return text


def replace_tables(text: str) -> str:
    """
    Replace markdown tables with HTML table tags.

    Supports tables with headers and separator rows.
    """

    def convert_table(match):
        table_text = match.group(0)
        lines = table_text.strip().split("\n")

        # Extract headers
        headers = [h.strip() for h in lines[0].strip().split("|") if h.strip()]

        # Build HTML table
        html = f'<table style="{MARKDOWN_CSS_TABLE_STYLE}">\n<thead>\n<tr style="{MARKDOWN_CSS_TABLE_HEADER}">\n'

        for header in headers:
            html += f'<th style="{MARKDOWN_CSS_TABLE_CELL}font-weight: bold;">{header}</th>\n'

        html += "</tr>\n</thead>\n<tbody>\n"

        # Add rows (skip header and separator)
        for idx, line in enumerate(lines[2:]):
            cells = [c.strip() for c in line.strip().split("|") if c.strip()]
            bg_color = "#ffffff" if idx % 2 == 0 else MARKDOWN_CSS_TABLE_CELL_ALT
            html += f'<tr style="background-color: {bg_color};">\n'

            for cell in cells:
                html += f'<td style="{MARKDOWN_CSS_TABLE_CELL}">{cell}</td>\n'

            html += "</tr>\n"

        html += "</tbody>\n</table>\n"
        return html

    # Match table pattern
    return re.sub(r"\|[^\n]+\|(\n\|[\s\-|]+\|)?(\n\|[^\n]+\|)+", convert_table, text)


def replace_lists(text: str) -> str:
    """Replace markdown list syntax (unordered and ordered) with HTML lists."""
    lines = text.split("\n")
    in_list = False
    result_lines = []

    for line in lines:
        stripped = line.strip()

        # Unordered list item
        if re.match(r"^[*-]\s+", stripped):
            if not in_list:
                result_lines.append("<ul>")
                in_list = True
            item_text = re.sub(r"^[*-]\s+", "", stripped)
            result_lines.append(f"<li>{item_text}</li>")

        # Ordered list item
        elif re.match(r"^\d+\.\s+", stripped):
            if not in_list:
                result_lines.append("<ol>")
                in_list = "ol"
            item_text = re.sub(r"^\d+\.\s+", "", stripped)
            result_lines.append(f"<li>{item_text}</li>")

        # Regular line
        else:
            if in_list:
                result_lines.append("</ol>" if in_list == "ol" else "</ul>")
                in_list = False
            result_lines.append(line)

    # Close any remaining open list
    if in_list:
        result_lines.append("</ol>" if in_list == "ol" else "</ul>")

    return "\n".join(result_lines)


def restore_code_blocks(text: str, code_blocks: List[str]) -> str:
    """
    Restore code blocks with proper HTML formatting.

    Args:
        text: Text with code block placeholders
        code_blocks: List of original code block contents

    Returns:
        Text with actual code blocks restored
    """
    for i, code in enumerate(code_blocks):
        formatted_code = f'<pre style="{MARKDOWN_CSS_CODE_BLOCK}"><code>{escape_html(code)}</code></pre>'
        text = text.replace(f"___CODE_BLOCK_{i}___", formatted_code)

    return text


def add_paragraph_tags(text: str) -> str:
    """
    Wrap non-empty lines that aren't already HTML in paragraph tags.

    Args:
        text: HTML text

    Returns:
        Text with paragraph tags added
    """
    lines = text.split("\n")
    result_lines = []

    for line in lines:
        stripped = line.strip()

        # Check if line should be wrapped in <p> tag
        if (
            stripped
            and not re.match(r"^<[^>]+>.*</[^>]+>$", stripped)
            and not re.match(r"^<(h[1-6]|pre|ul|ol|li|img)", stripped)
            and not "<img" in stripped
        ):
            result_lines.append(f"<p>{line}</p>")
        else:
            result_lines.append(line)

    return "\n".join(result_lines)


def markdown_to_html(text: str) -> str:
    """
    Convert markdown text to HTML using python-markdown library with regex fallback.

    Handles: headers, bold, italic, code, links, images, tables, lists, code blocks.

    Args:
        text: Markdown text to convert

    Returns:
        HTML string
    """
    if not text:
        return ""

    # Try using python-markdown library first (more robust)
    if MARKDOWN_LIBRARY_AVAILABLE:
        try:
            logger.info("Using python-markdown library for conversion")
            html = markdown.markdown(text, extensions=["tables", "fenced_code"])
            html = plaintext_image_source_to_html(html)
            return convert_image_paths_to_base64(html)
        except Exception as e:
            logger.warning(
                f"Markdown library conversion failed, falling back to regex: {e}"
            )

    # Fallback: regex-based conversion
    logger.info("Using regex-based markdown conversion")

    # Step 1: Extract code blocks before other processing
    text, code_blocks = extract_code_blocks(text)

    # Step 2: Escape HTML in remaining text
    text = escape_html(text)

    # Step 3: Process markdown elements in order
    text = replace_images(text)
    text = plaintext_image_source_to_html(text)
    text = replace_headers(text)
    text = replace_text_formatting(text)
    text = replace_tables(text)
    text = replace_lists(text)

    # Step 4: Restore code blocks
    text = restore_code_blocks(text, code_blocks)

    # Step 5: Add paragraph tags and convert image paths to base64
    text = add_paragraph_tags(text)
    text = convert_image_paths_to_base64(text)

    return text
