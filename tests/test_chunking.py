from web_content_kit.chunking import chunk_markdown

PAGE = """# Returns policy

Source: https://example.com/returns

Most items can be returned within 90 days of purchase with a receipt.

## Electronics

""" + " ".join(["Electronics must be returned within 30 days in the original packaging."] * 80)


def test_chunks_carry_title_and_section_context() -> None:
    chunks = chunk_markdown("Returns policy", PAGE, chunk_size_tokens=128, chunk_overlap_tokens=16)
    assert len(chunks) > 2
    assert [c["chunk_index"] for c in chunks] == list(range(len(chunks)))
    assert all(c["chunk_text"].startswith("Returns policy") for c in chunks)
    assert chunks[0]["section"] is None  # the title heading is not repeated as a section
    assert all(c["section"] == "Electronics" for c in chunks[1:])
    assert all(c["token_estimate"] <= 128 + 20 for c in chunks)
    assert not any("Source: https://" in c["chunk_text"] for c in chunks)


def test_empty_page_has_no_chunks() -> None:
    assert chunk_markdown("Empty", "") == []
    assert chunk_markdown("Empty", "   \n") == []
