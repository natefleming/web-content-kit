"""Split a Markdown page into search-ready chunks (a common best practice for clean Markdown).

1. Split on headings (#, ##, ###) so a chunk never mixes unrelated sections.
2. Split long sections further on paragraphs, then lines, sentences and words, keeping a
   small overlap so no sentence loses its context at a boundary.
3. Put the page title and section path at the top of every chunk, so the chunk still makes
   sense on its own when an agent retrieves it.
"""

from typing import TypedDict

from langchain_text_splitters import MarkdownHeaderTextSplitter, RecursiveCharacterTextSplitter

from web_content_kit.documents import strip_source_line

# Rule of thumb: one token is about four characters of English text.
CHARS_PER_TOKEN: int = 4
# Chunks shorter than this carry too little meaning to be worth indexing.
MIN_CHUNK_CHARS: int = 40


class Chunk(TypedDict):
    """One search-ready passage of a page."""

    chunk_index: int
    section: str | None
    chunk_text: str
    token_estimate: int


def chunk_markdown(
    title: str | None,
    markdown: str | None,
    chunk_size_tokens: int = 512,
    chunk_overlap_tokens: int = 64,
) -> list[Chunk]:
    """Split one Markdown page into overlapping, heading-aware chunks."""
    if not markdown or not markdown.strip():
        return []
    markdown = strip_source_line(markdown)

    header_splitter = MarkdownHeaderTextSplitter(
        headers_to_split_on=[("#", "h1"), ("##", "h2"), ("###", "h3")],
        strip_headers=True,
    )
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size_tokens * CHARS_PER_TOKEN,
        chunk_overlap=chunk_overlap_tokens * CHARS_PER_TOKEN,
        separators=["\n\n", "\n", ". ", " ", ""],
    )

    chunks: list[Chunk] = []
    for section in header_splitter.split_text(markdown):
        headings = [section.metadata[k] for k in ("h1", "h2", "h3") if section.metadata.get(k)]
        # The page title usually repeats as the first H1; don't show it twice.
        if title and headings and headings[0].strip() == title.strip():
            headings = headings[1:]
        section_path = " > ".join(headings)
        for piece in text_splitter.split_text(section.page_content):
            piece = piece.strip()
            if len(piece) < MIN_CHUNK_CHARS:
                continue
            context = [line for line in (title, section_path) if line]
            chunk_text = "\n".join(context + ["", piece]) if context else piece
            chunks.append(
                Chunk(
                    chunk_index=len(chunks),
                    section=section_path or None,
                    chunk_text=chunk_text,
                    token_estimate=max(1, len(chunk_text) // CHARS_PER_TOKEN),
                )
            )
    return chunks
