from web_content_kit.documents import build_markdown_file, parse_front_matter, strip_source_line


def test_front_matter_round_trip() -> None:
    text = build_markdown_file('Returns: "90 days"', "https://example.com/returns", "2026-09-29T00:00:00+00:00", "Body")
    metadata, body = parse_front_matter(text)
    assert metadata == {
        "title": 'Returns: "90 days"',
        "source_url": "https://example.com/returns",
        "crawled_at": "2026-09-29T00:00:00+00:00",
    }
    assert body.lstrip().startswith('# Returns: "90 days"')
    assert "Source: https://example.com/returns" not in strip_source_line(body)


def test_file_without_front_matter() -> None:
    assert parse_front_matter("# Just markdown") == ({}, "# Just markdown")
