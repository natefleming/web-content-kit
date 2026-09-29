import re

from web_content_kit.crawler import (
    CrawlSettings,
    extract_links,
    is_sitemap,
    normalize_url,
    should_follow,
    url_to_relative_path,
)


def test_normalize_url_drops_tracking_and_fragment_but_keeps_trailing_slash() -> None:
    assert normalize_url("HTTPS://Example.com/Help/?utm_source=x&b=2&a=1#top") == "https://example.com/Help/?a=1&b=2"
    assert normalize_url("https://example.com") == "https://example.com/"


def test_relative_path_is_descriptive_and_deterministic() -> None:
    assert url_to_relative_path("https://docs.example.com/help/returns") == "docs.example.com/help-returns.md"
    assert url_to_relative_path("https://example.com/") == "example.com/index.md"
    with_query = url_to_relative_path("https://example.com/search?q=tv")
    assert with_query.startswith("example.com/search-") and with_query == url_to_relative_path(
        "https://example.com/search?q=tv"
    )
    assert len(url_to_relative_path("https://example.com/" + "a/" * 200)) < 150


def test_is_sitemap() -> None:
    assert is_sitemap("https://example.com/sitemap.xml")
    assert is_sitemap("https://example.com/sitemap-pages.xml.gz")
    assert not is_sitemap("https://example.com/feed.xml")


def test_should_follow_rules() -> None:
    settings = CrawlSettings(
        seed_urls=["https://example.com/"],
        content_path="/tmp/c",
        staging_path="/tmp/s",
        include_pattern=re.compile("/help/"),
        exclude_pattern=re.compile("/help/private"),
    )
    assert should_follow("https://www.example.com/help/returns", "example.com", settings)
    assert not should_follow("https://other.com/help/returns", "example.com", settings)
    assert not should_follow("https://example.com/about", "example.com", settings)
    assert not should_follow("https://example.com/help/private/x", "example.com", settings)
    assert not should_follow("https://example.com/help/guide.pdf", "example.com", settings)


def test_extract_links_makes_links_absolute() -> None:
    html = '<a href="/a#x">A</a><a href="b?utm_source=z">B</a><a href="mailto:x@y.z">M</a>'
    assert extract_links(html, "https://example.com/dir/page") == [
        "https://example.com/a",
        "https://example.com/dir/b",
    ]
