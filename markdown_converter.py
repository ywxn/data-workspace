import markdown

# TODO: Table rendering in Qt text widgets, since they don't natively support GitHub-Flavored Markdown tables.
def markdown_to_html(md: str) -> str:
    return markdown.markdown(
        md,
        extensions=["tables", "fenced_code", "sane_lists"]
    )