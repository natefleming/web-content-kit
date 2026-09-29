"""The page file format shared by both steps.

Every page is one Markdown file with a small front-matter header naming its source::

    ---
    title: "Returns policy"
    source_url: "https://example.com/help/returns"
    crawled_at: "2026-09-29T16:15:52+00:00"
    ---

    # Returns policy

    Source: https://example.com/help/returns

    <page content>

Values are written as JSON strings, which are also valid YAML, so any tool can read them.
"""

import json
import re

FRONT_MATTER_PATTERN = re.compile(r"\A---\n(.*?)\n---\n?", re.DOTALL)
SOURCE_LINE_PATTERN = re.compile(r"^Source: \S+\s*$", re.MULTILINE)


def build_markdown_file(title: str, url: str, crawled_at: str, body: str) -> str:
    """Full text of a page file: front matter, title heading, source line, then the content."""
    header = "\n".join(
        [
            "---",
            f"title: {json.dumps(title, ensure_ascii=False)}",
            f"source_url: {json.dumps(url)}",
            f"crawled_at: {json.dumps(crawled_at)}",
            "---",
        ]
    )
    return f"{header}\n\n# {title}\n\nSource: {url}\n\n{body.strip()}\n"


def parse_front_matter(text: str) -> tuple[dict[str, str], str]:
    """Split a page file into its front-matter values and its Markdown body."""
    match = FRONT_MATTER_PATTERN.match(text)
    if not match:
        return {}, text
    metadata: dict[str, str] = {}
    for line in match.group(1).splitlines():
        key, sep, raw_value = line.partition(":")
        if not sep:
            continue
        raw_value = raw_value.strip()
        try:
            metadata[key.strip()] = str(json.loads(raw_value))
        except json.JSONDecodeError:
            metadata[key.strip()] = raw_value.strip("\"'")
    return metadata, text[match.end() :]


def strip_source_line(markdown: str) -> str:
    """Remove the ``Source: <url>`` line (metadata, not content) from a page body."""
    return SOURCE_LINE_PATTERN.sub("", markdown, count=1)
