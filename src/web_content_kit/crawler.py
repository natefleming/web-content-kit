"""Step 1: crawl websites and save every page as a clean Markdown file.

* URLs are normalized (tracking parameters and ``#fragments`` removed) so a page isn't
  collected twice.
* Politeness: each website gets at most one request every ``request_delay_seconds``, and
  ``robots.txt`` rules are respected.
* Cleaning: ``trafilatura`` keeps the main content and headings, and drops menus, ads
  and footers.
* File names come from the web address (e.g. ``docs.example.com/help-returns.md``), so they
  are easy to recognise and a re-crawl overwrites the same file instead of duplicating it.
"""

import gzip
import hashlib
import json
import logging
import re
import threading
import time
import urllib.robotparser
import xml.etree.ElementTree as ET
from collections import Counter, deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

import lxml.html
import requests
import trafilatura
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from web_content_kit.documents import build_markdown_file

log = logging.getLogger("web_content_kit.crawler")

TRACKING_PARAMS: frozenset[str] = frozenset(
    {"utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "gclid", "fbclid", "mc_cid", "mc_eid"}
)
SKIP_EXTENSIONS: tuple[str, ...] = (
    ".pdf",
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".svg",
    ".webp",
    ".ico",
    ".css",
    ".js",
    ".json",
    ".zip",
    ".gz",
    ".tar",
    ".mp3",
    ".mp4",
    ".mov",
    ".avi",
    ".woff",
    ".woff2",
    ".ttf",
    ".xml",
    ".rss",
)
MAX_PAGE_BYTES: int = 10 * 1024 * 1024  # skip anything larger than 10 MB
REQUEST_TIMEOUT_SECONDS: tuple[float, float] = (10.0, 30.0)  # (connect, read)
PROGRESS_EVERY: int = 25  # log a progress line every N pages


@dataclass(frozen=True)
class CrawlSettings:
    """Everything the crawler needs to know for one run."""

    seed_urls: list[str]
    content_path: str  # folder for the clean .md pages (the Genie volume)
    staging_path: str  # folder for run logs and optional raw HTML
    max_pages: int = 100
    max_depth: int = 1
    stay_on_domain: bool = True
    include_pattern: re.Pattern[str] | None = None
    exclude_pattern: re.Pattern[str] | None = None
    concurrency: int = 16
    request_delay_seconds: float = 0.5
    respect_robots_txt: bool = True
    min_words: int = 50
    keep_raw_html: bool = False
    user_agent: str = "DatabricksWebContentCollector/1.0"


@dataclass
class PageOutcome:
    """What happened to one URL."""

    url: str
    depth: int
    seed_site: str
    status: str = "failed"  # saved | duplicate | too_short | skipped | failed
    detail: str = ""
    file_path: str | None = None
    title: str | None = None
    words: int = 0
    links: tuple[str, ...] = field(default=(), repr=False)


# ---------------------------------------------------------------------------
# URL helpers
# ---------------------------------------------------------------------------
def normalize_url(url: str) -> str:
    """Canonical form of a URL: lower-case host, no #fragment, no tracking params.

    The path is kept exactly as-is (including any trailing slash): some websites treat
    ``/page`` and ``/page/`` differently.
    """
    parts = urlsplit(url.strip())
    query = urlencode(sorted((k, v) for k, v in parse_qsl(parts.query) if k.lower() not in TRACKING_PARAMS))
    return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), parts.path or "/", query, ""))


def site_of(url: str) -> str:
    """Website name used for 'stay on same website' checks (ignores a leading 'www.')."""
    host = urlsplit(url).netloc.lower()
    return host.removeprefix("www.")


def is_sitemap(url: str) -> bool:
    """True when the URL looks like a sitemap file."""
    path = urlsplit(url).path.lower()
    return path.endswith((".xml", ".xml.gz")) and "sitemap" in path


def url_to_relative_path(url: str) -> str:
    """Descriptive, deterministic file path for a page, e.g. ``docs.example.com/help-returns.md``."""
    parts = urlsplit(url)
    slug = re.sub(r"[^A-Za-z0-9]+", "-", parts.path).strip("-").lower() or "index"
    if parts.query:
        slug += "-" + hashlib.sha1(parts.query.encode()).hexdigest()[:8]
    if len(slug) > 120:  # keep names readable and within file-system limits
        slug = slug[:100].rstrip("-") + "-" + hashlib.sha1(slug.encode()).hexdigest()[:8]
    host = re.sub(r"[^A-Za-z0-9.\-]+", "-", parts.netloc.lower())
    return f"{host}/{slug}.md"


def should_follow(link: str, seed_site: str, settings: CrawlSettings) -> bool:
    """Apply the crawl rules (same website, include/exclude patterns, file types)."""
    if urlsplit(link).path.lower().endswith(SKIP_EXTENSIONS):
        return False
    if settings.stay_on_domain and site_of(link) != seed_site:
        return False
    if settings.include_pattern and not settings.include_pattern.search(link):
        return False
    return not (settings.exclude_pattern and settings.exclude_pattern.search(link))


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------
def make_session(user_agent: str, pool_size: int) -> requests.Session:
    """HTTP session that retries temporary errors (429 / 5xx) with back-off."""
    retry = Retry(
        total=3,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=pool_size, pool_maxsize=pool_size)
    session = requests.Session()
    session.headers.update({"User-Agent": user_agent, "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.5"})
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def decode_html(response: requests.Response) -> str:
    """Page text with the right character encoding.

    When the server doesn't say which encoding it uses, ``requests`` falls back to Latin-1,
    which garbles characters such as "é" or "—". Most sites use UTF-8, so try that first and
    only guess when it doesn't fit.
    """
    if "charset" in response.headers.get("Content-Type", "").lower():
        return response.text
    try:
        return response.content.decode("utf-8")
    except UnicodeDecodeError:
        response.encoding = response.apparent_encoding
        return response.text


class PolitenessGate:
    """Makes sure each website gets at most one request every ``delay_seconds``."""

    def __init__(self, delay_seconds: float) -> None:
        self._delay = delay_seconds
        self._next_allowed: dict[str, float] = {}
        self._lock = threading.Lock()

    def wait(self, url: str) -> None:
        host = urlsplit(url).netloc
        with self._lock:
            now = time.monotonic()
            slot = max(now, self._next_allowed.get(host, now))
            self._next_allowed[host] = slot + self._delay
        if slot > now:
            time.sleep(slot - now)


class RobotsCache:
    """Downloads each website's robots.txt once and answers 'may we fetch this URL?'."""

    def __init__(self, session: requests.Session, user_agent: str) -> None:
        self._session = session
        self._user_agent = user_agent
        self._parsers: dict[str, urllib.robotparser.RobotFileParser | None] = {}
        self._lock = threading.Lock()

    def allowed(self, url: str) -> bool:
        parts = urlsplit(url)
        root = f"{parts.scheme}://{parts.netloc}"
        with self._lock:
            if root not in self._parsers:
                self._parsers[root] = self._load(root)
            parser = self._parsers[root]
        return parser is None or parser.can_fetch(self._user_agent, url)

    def _load(self, root: str) -> urllib.robotparser.RobotFileParser | None:
        try:
            response = self._session.get(f"{root}/robots.txt", timeout=REQUEST_TIMEOUT_SECONDS)
        except requests.RequestException:
            return None  # robots.txt unreachable: treat as allowed
        if response.status_code >= 400:
            return None
        parser = urllib.robotparser.RobotFileParser()
        parser.parse(response.text.splitlines())
        return parser


def expand_sitemap(session: requests.Session, url: str, limit: int, seen: set[str] | None = None) -> list[str]:
    """Return the page URLs listed in a sitemap (follows nested sitemap indexes)."""
    seen = seen if seen is not None else set()
    if url in seen or len(seen) > 200:
        return []
    seen.add(url)
    try:
        response = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
    except requests.RequestException as error:
        log.warning("Could not read sitemap %s: %s", url, error)
        return []
    content = response.content
    if content[:2] == b"\x1f\x8b":  # gzip-compressed sitemap
        content = gzip.decompress(content)
    try:
        root = ET.fromstring(content)
    except ET.ParseError as error:
        log.warning("Sitemap %s is not valid XML: %s", url, error)
        return []
    locations = [el.text.strip() for el in root.iter() if el.tag.endswith("loc") and el.text]
    if root.tag.endswith("sitemapindex"):
        pages: list[str] = []
        for child in locations:
            if len(pages) >= limit:
                break
            pages.extend(expand_sitemap(session, child, limit - len(pages), seen))
        return pages
    return locations[:limit]


# ---------------------------------------------------------------------------
# HTML helpers
# ---------------------------------------------------------------------------
def extract_links(html: str, base_url: str) -> list[str]:
    """All http(s) links on a page, made absolute and normalized."""
    try:
        document = lxml.html.fromstring(html)
    except (ValueError, lxml.etree.ParserError):
        return []
    links: list[str] = []
    for href in document.xpath("//a/@href"):
        absolute = urljoin(base_url, str(href).strip())
        if absolute.startswith(("http://", "https://")):
            links.append(normalize_url(absolute))
    return links


def extract_page(html: str, url: str) -> tuple[str, str]:
    """Main content of a page as Markdown, plus its title."""
    markdown = (
        trafilatura.extract(
            html,
            url=url,
            output_format="markdown",
            include_tables=True,
            include_links=False,
            include_images=False,
            include_comments=False,
            favor_precision=True,
        )
        or ""
    )
    metadata = trafilatura.extract_metadata(html, default_url=url)
    title = (metadata.title if metadata and metadata.title else "") or urlsplit(url).path.strip("/") or url
    return markdown, title.strip()


# ---------------------------------------------------------------------------
# Crawler
# ---------------------------------------------------------------------------
class Crawler:
    """Breadth-first crawler: seeds first, then the links found on them, up to ``max_depth``.

    Pages are downloaded in parallel and each one is cleaned and written to the content
    folder as soon as it arrives. A page that fails never stops the run.
    """

    def __init__(self, settings: CrawlSettings, run_id: str) -> None:
        self.settings = settings
        self.run_id = run_id
        self.session = make_session(settings.user_agent, pool_size=settings.concurrency * 2)
        self.gate = PolitenessGate(settings.request_delay_seconds)
        self.robots = RobotsCache(self.session, settings.user_agent) if settings.respect_robots_txt else None
        self.raw_html_dir = Path(settings.staging_path) / "runs" / run_id / "raw_html"
        self._content_hashes: set[str] = set()
        self._hash_lock = threading.Lock()

    def process(self, url: str, depth: int, seed_site: str) -> PageOutcome:
        """Download, clean and save one page. Never raises: problems become a 'failed' outcome."""
        outcome = PageOutcome(url=url, depth=depth, seed_site=seed_site)
        try:
            if self.robots and not self.robots.allowed(url):
                outcome.status, outcome.detail = "skipped", "blocked by robots.txt"
                return outcome
            self.gate.wait(url)
            response = self.session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
            if response.status_code != 200:
                outcome.detail = f"HTTP {response.status_code}"
                return outcome
            content_type = response.headers.get("Content-Type", "")
            if "html" not in content_type.lower():
                outcome.status, outcome.detail = "skipped", f"not HTML ({content_type or '?'})"
                return outcome
            if len(response.content) > MAX_PAGE_BYTES:
                outcome.status, outcome.detail = "skipped", "page larger than 10 MB"
                return outcome

            html = decode_html(response)
            final_url = normalize_url(response.url)
            if depth < self.settings.max_depth:
                outcome.links = tuple(extract_links(html, final_url))

            markdown, title = extract_page(html, final_url)
            outcome.title = title
            outcome.words = len(markdown.split())
            if outcome.words < self.settings.min_words:
                outcome.status, outcome.detail = "too_short", f"{outcome.words} words"
                return outcome

            content_hash = hashlib.sha256(markdown.encode()).hexdigest()
            with self._hash_lock:
                duplicate = content_hash in self._content_hashes
                self._content_hashes.add(content_hash)
            if duplicate:
                outcome.status, outcome.detail = "duplicate", "same content as another page"
                return outcome

            relative_path = url_to_relative_path(final_url)
            target = Path(self.settings.content_path) / relative_path
            target.parent.mkdir(parents=True, exist_ok=True)
            crawled_at = datetime.now(UTC).isoformat(timespec="seconds")
            target.write_text(build_markdown_file(title, final_url, crawled_at, markdown), encoding="utf-8")
            if self.settings.keep_raw_html:
                raw_target = self.raw_html_dir / relative_path.replace(".md", ".html")
                raw_target.parent.mkdir(parents=True, exist_ok=True)
                raw_target.write_text(html, encoding="utf-8")

            outcome.status, outcome.file_path = "saved", str(target)
            return outcome
        except Exception as error:  # noqa: BLE001 - a single bad page must never stop the whole crawl
            outcome.detail = f"{type(error).__name__}: {error}"[:500]
            return outcome

    def _seed_queue(self) -> list[tuple[str, int, str]]:
        """Starting URLs: the seeds themselves, or every page listed in a sitemap seed."""
        seeds: list[tuple[str, int, str]] = []
        for seed in self.settings.seed_urls:
            if is_sitemap(seed):
                pages = expand_sitemap(self.session, seed, self.settings.max_pages)
                log.info("Sitemap %s lists %d pages", seed, len(pages))
                for page in pages:
                    url = normalize_url(page)
                    if should_follow(url, site_of(url), self.settings):
                        seeds.append((url, 0, site_of(url)))
            else:
                url = normalize_url(seed)
                seeds.append((url, 0, site_of(url)))
        return seeds

    def run(self) -> list[PageOutcome]:
        """Crawl every seed, following links breadth-first until a limit is reached."""
        queue: deque[tuple[str, int, str]] = deque()
        scheduled: set[str] = set()

        def enqueue(url: str, depth: int, seed_site: str) -> None:
            if url not in scheduled and len(scheduled) < self.settings.max_pages:
                scheduled.add(url)
                queue.append((url, depth, seed_site))

        for item in self._seed_queue():
            enqueue(*item)

        outcomes: list[PageOutcome] = []
        started = time.monotonic()
        with ThreadPoolExecutor(max_workers=self.settings.concurrency) as pool:
            in_flight: set[Future[PageOutcome]] = set()
            while queue or in_flight:
                while queue and len(in_flight) < self.settings.concurrency * 2:
                    in_flight.add(pool.submit(self.process, *queue.popleft()))
                done, in_flight = wait(in_flight, return_when=FIRST_COMPLETED)
                for future in done:
                    outcome = future.result()
                    outcomes.append(outcome)
                    if outcome.status == "failed":
                        log.warning("Failed  %s (%s)", outcome.url, outcome.detail)
                    for link in outcome.links:
                        if should_follow(link, outcome.seed_site, self.settings):
                            enqueue(link, outcome.depth + 1, outcome.seed_site)
                    if len(outcomes) % PROGRESS_EVERY == 0:
                        saved = sum(o.status == "saved" for o in outcomes)
                        rate = len(outcomes) / max(time.monotonic() - started, 1e-6)
                        log.info(
                            "Progress: %d processed, %d saved, %d queued, %.1f pages/sec",
                            len(outcomes),
                            saved,
                            len(queue),
                            rate,
                        )
        self.session.close()
        log.info("Crawl finished in %.0f seconds: %d URLs processed.", time.monotonic() - started, len(outcomes))
        return outcomes


def new_run_id() -> str:
    """UTC timestamp used to name a run's log folder."""
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def write_run_log(outcomes: list[PageOutcome], staging_path: str, run_id: str) -> Path:
    """Save ``manifest.jsonl`` (every URL) and ``failures.jsonl`` under ``runs/<run_id>/``."""
    run_dir = Path(staging_path) / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    with open(run_dir / "manifest.jsonl", "w", encoding="utf-8") as manifest:
        for outcome in outcomes:
            record = {k: v for k, v in asdict(outcome).items() if k not in ("links", "seed_site")}
            manifest.write(json.dumps({**record, "run_id": run_id}, ensure_ascii=False) + "\n")
    with open(run_dir / "failures.jsonl", "w", encoding="utf-8") as failures:
        for outcome in outcomes:
            if outcome.status == "failed":
                failures.write(json.dumps({"url": outcome.url, "detail": outcome.detail}) + "\n")
    log.info("Run log written to %s", run_dir)
    return run_dir


def count_statuses(outcomes: list[PageOutcome]) -> Counter[str]:
    """How many URLs ended in each status (saved, failed, ...)."""
    return Counter(outcome.status for outcome in outcomes)
