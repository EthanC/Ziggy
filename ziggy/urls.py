"""URL normalization, host scope checks, and static URL extraction."""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from contextlib import suppress
from dataclasses import dataclass
from html.parser import HTMLParser
from urllib.parse import SplitResult, parse_qsl, urljoin, urlsplit, urlunsplit

_HTTP_SCHEMES = {"http", "https"}
_LINK_SPLIT_RE = re.compile(r",(?=\s*<)")
_DNS_LABEL_RE = re.compile(r"^[a-z0-9-]+$")
_MAX_DNS_NAME = 253
_MAX_DNS_LABEL = 63
_MAX_PORT = 65_535
_DEFAULT_PORTS = {"http": 80, "https": 443}
DEFAULT_MAX_QUERY_VARIANTS_PER_BASE = 20
_SENSITIVE_QUERY_KEYS = {
    "access_token",
    "api_key",
    "apikey",
    "auth",
    "authorization",
    "code",
    "credential",
    "csrf",
    "error_key",
    "errorkey",
    "id_token",
    "jwt",
    "password",
    "recaptcha",
    "refresh_token",
    "session",
    "session_id",
    "sessionid",
    "sig",
    "signature",
    "token",
}
_SENSITIVE_QUERY_PREFIXES = ("x-amz-", "x-goog-")
_SENSITIVE_QUERY_SUFFIXES = ("secret", "signature", "token")


class UrlError(ValueError):
    """Raised when a URL cannot be normalized safely."""


def normalize_host(value: str) -> str:
    """Normalize a bare DNS host with IDNA and lowercase conversion."""
    if not value or value != value.strip():
        raise UrlError("host must be nonempty and contain no surrounding whitespace")
    if any(character in value for character in "/?#@[]:"):
        raise UrlError("host must not contain URL syntax or a port")
    host = value.rstrip(".").lower()
    if not host:
        raise UrlError("host must not be the DNS root")
    try:
        ascii_host = host.encode("idna").decode("ascii")
    except UnicodeError as error:
        raise UrlError("host cannot be IDNA-normalized") from error
    if len(ascii_host) > _MAX_DNS_NAME or any(
        not label
        or len(label) > _MAX_DNS_LABEL
        or _DNS_LABEL_RE.fullmatch(label) is None
        or label.startswith("-")
        or label.endswith("-")
        for label in ascii_host.split(".")
    ):
        raise UrlError("host has an invalid DNS label")
    return ascii_host


def _remove_dot_segments(path: str) -> str:
    absolute = path.startswith("/")
    output = [""] if absolute else []
    segments = path.split("/")[1:] if absolute else path.split("/")
    for segment in segments:
        if segment == ".":
            continue
        if segment == "..":
            minimum = 1 if absolute else 0
            if len(output) > minimum:
                output.pop()
            continue
        output.append(segment)
    if path.endswith(("/.", "/..")):
        output.append("")
    return "/".join(output) or "/"


def normalize_url(value: str, *, base: str | None = None) -> str:
    """Resolve and normalize one absolute HTTP or HTTPS URL."""
    if not isinstance(value, str) or not value:
        raise UrlError("URL must be a nonempty string")
    candidate = urljoin(base, value) if base is not None else value
    try:
        parts = urlsplit(candidate)
        port = parts.port
    except ValueError as error:
        raise UrlError("URL authority is invalid") from error
    scheme = parts.scheme.lower()
    if scheme not in _HTTP_SCHEMES or not parts.netloc or parts.hostname is None:
        raise UrlError("URL must be absolute HTTP or HTTPS")
    if parts.username is not None or parts.password is not None:
        raise UrlError("URL credentials are not allowed")
    host = _normalize_url_host(parts.hostname)
    if port is not None and not 1 <= port <= _MAX_PORT:
        raise UrlError("URL port is outside 1-65535")
    default_port = port == _DEFAULT_PORTS[scheme]
    netloc = host if port is None or default_port else f"{host}:{port}"
    normalized = SplitResult(
        scheme=scheme,
        netloc=netloc,
        path=_remove_dot_segments(parts.path or "/"),
        query=parts.query,
        fragment="",
    )
    return urlunsplit(normalized)


def query_base_url(url: str) -> str | None:
    """Return a normalized URL without its query, or None when queryless."""
    parts = urlsplit(url)
    if not parts.query:
        return None
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


def sensitive_query_key(url: str) -> str | None:
    """Return the first credential-like or ephemeral query key in a URL."""
    for key, _value in parse_qsl(urlsplit(url).query, keep_blank_values=True):
        normalized = key.casefold()
        if (
            normalized in _SENSITIVE_QUERY_KEYS
            or normalized.startswith(_SENSITIVE_QUERY_PREFIXES)
            or normalized.endswith(_SENSITIVE_QUERY_SUFFIXES)
        ):
            return key
    return None


def _normalize_url_host(value: str) -> str:
    if ":" in value:
        return f"[{value.lower()}]"
    return normalize_host(value)


def url_in_scope(
    url: str, configured_host: str, *, include_subdomains: bool = False
) -> bool:
    """Check a normalized URL against a configured host boundary."""
    try:
        host_value = urlsplit(normalize_url(url)).hostname
        if host_value is None:
            return False
        host = host_value.lower().rstrip(".")
        scope = normalize_host(configured_host)
    except ValueError:
        return False
    return host == scope or (include_subdomains and host.endswith(f".{scope}"))


class _PageParser(HTMLParser):
    def __init__(self, response_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.response_url = response_url
        self.base_url = response_url
        self.urls: list[str] = []
        self._base_seen = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = {name.lower(): value for name, value in attrs}
        href = attributes.get("href")
        if tag.lower() == "base" and not self._base_seen and href:
            self._base_seen = True
            with suppress(UrlError):
                self.base_url = normalize_url(href, base=self.response_url)
            return
        target: str | None = None
        if tag.lower() in {"a", "area"} or (
            tag.lower() == "link"
            and "canonical" in (attributes.get("rel") or "").lower().split()
        ):
            target = attributes.get("href")
        if target:
            self.urls.append(target)


def extract_html_urls(body: str, response_url: str) -> tuple[str, ...]:
    """Extract normalized navigable and canonical links from HTML."""
    parser = _PageParser(normalize_url(response_url))
    parser.feed(body)
    parser.close()
    result: dict[str, None] = {}
    for value in parser.urls:
        try:
            result[normalize_url(value, base=parser.base_url)] = None
        except UrlError:
            continue
    return tuple(result)


def extract_http_link_urls(
    header_values: list[str], response_url: str
) -> tuple[str, ...]:
    """Extract navigable targets from HTTP Link header values."""
    result: dict[str, None] = {}
    for header in header_values:
        for raw_item in _LINK_SPLIT_RE.split(header):
            item = raw_item.strip()
            if not item.startswith("<") or ">" not in item:
                continue
            target = item[1 : item.index(">")]
            try:
                result[normalize_url(target, base=response_url)] = None
            except UrlError:
                continue
    return tuple(result)


@dataclass(frozen=True, slots=True)
class SitemapContents:
    """Normalized URLs found in one sitemap document."""

    pages: tuple[str, ...]
    sitemaps: tuple[str, ...]


def extract_sitemap(body: bytes, response_url: str) -> SitemapContents:
    """Parse a URL set or sitemap index without resolving external entities."""
    try:
        root = ET.fromstring(body)  # noqa: S314 - caller enforces the byte limit.
    except ET.ParseError as error:
        raise UrlError("invalid sitemap XML") from error
    kind = root.tag.rsplit("}", 1)[-1].lower()
    if kind not in {"urlset", "sitemapindex"}:
        raise UrlError("XML document is not a sitemap")
    values: dict[str, None] = {}
    entry_kind = "url" if kind == "urlset" else "sitemap"
    for entry in root:
        if entry.tag.rsplit("}", 1)[-1].lower() != entry_kind:
            continue
        for element in entry:
            if element.tag.rsplit("}", 1)[-1].lower() != "loc" or not element.text:
                continue
            try:
                values[normalize_url(element.text.strip(), base=response_url)] = None
            except UrlError:
                continue
    if kind == "urlset":
        return SitemapContents(tuple(values), ())
    return SitemapContents((), tuple(values))
