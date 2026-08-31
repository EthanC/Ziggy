from __future__ import annotations

import pytest

import ziggy.urls as urls_module
from ziggy.urls import (
    SitemapContents,
    UrlError,
    extract_html_urls,
    extract_http_link_urls,
    extract_sitemap,
    normalize_host,
    normalize_url,
    query_base_url,
    sensitive_query_key,
    url_in_scope,
)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("Example.COM", "example.com"),
        ("example.com.", "example.com"),
        ("BÜCHER.example", "xn--bcher-kva.example"),
        ("a-b.example", "a-b.example"),
    ],
)
def test_normalize_host(value, expected):
    assert normalize_host(value) == expected


@pytest.mark.parametrize(
    "value",
    [
        "",
        " example.com",
        "example.com ",
        ".",
        "example.com/path",
        "example.com:443",
        "user@example.com",
        "[::1]",
        "bad..example",
        "-bad.example",
        "bad-.example",
        f"{'a' * 64}.example",
        ".".join(["a" * 63] * 5),
        "\ud800.example",
    ],
)
def test_normalize_host_rejects_invalid_hosts(value):
    with pytest.raises(UrlError):
        normalize_host(value)


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("https://example.com/path", None),
        ("https://example.com/path?a=1", "https://example.com/path"),
        ("https://example.com:8443/path?a=1", "https://example.com:8443/path"),
    ],
)
def test_query_base_url(url, expected):
    assert query_base_url(url) == expected


@pytest.mark.parametrize(
    "url",
    [
        "https://example.com/?loginToken=secret",
        "https://example.com/?ACCESS_TOKEN=secret",
        "https://example.com/?access%5Ftoken=secret",
        "https://example.com/?recaptcha=",
        "https://example.com/?client_secret=secret",
        "https://example.com/?x-amz-signature=secret",
        "https://example.com/?x-goog-credential=secret",
    ],
)
def test_sensitive_query_key_detects_credentials(url):
    assert sensitive_query_key(url) is not None


@pytest.mark.parametrize(
    "url",
    [
        "https://example.com/",
        "https://example.com/?page=1&sort=token",
        "https://example.com/?state=Ohio&monkey=1",
    ],
)
def test_sensitive_query_key_allows_navigation_parameters(url):
    assert sensitive_query_key(url) is None


@pytest.mark.parametrize(
    ("value", "base", "expected"),
    [
        (
            "HTTPS://Example.COM:443/a/./b/../c?x=1#frag",
            None,
            "https://example.com/a/c?x=1",
        ),
        ("http://Example.COM:80", None, "http://example.com/"),
        ("http://Example.COM:8080/a", None, "http://example.com:8080/a"),
        ("../next", "https://example.com/a/b/", "https://example.com/a/next"),
        ("?page=2#top", "https://example.com/a", "https://example.com/a?page=2"),
        (
            "//CHILD.example.com/x",
            "https://example.com/",
            "https://child.example.com/x",
        ),
        ("https://BÜCHER.example/", None, "https://xn--bcher-kva.example/"),
        ("http://[2001:DB8::1]:80/a", None, "http://[2001:db8::1]/a"),
        ("https://example.com/a//b/", None, "https://example.com/a//b/"),
        ("https://example.com/a/.", None, "https://example.com/a/"),
        ("https://example.com/a/b/..", None, "https://example.com/a/"),
        ("https://example.com/../../a", None, "https://example.com/a"),
        ("https://example.com/a//../b", None, "https://example.com/a/b"),
        ("https://example.com/a/..//b", None, "https://example.com//b"),
        ("https://example.com/a//b/../../c", None, "https://example.com/a/c"),
        ("https://example.com/a//b/../c", None, "https://example.com/a//c"),
        ("https://example.com/a//.", None, "https://example.com/a//"),
        ("https://example.com/a/%2e%2e/b", None, "https://example.com/a/%2e%2e/b"),
    ],
)
def test_normalize_url(value, base, expected):
    assert normalize_url(value, base=base) == expected


@pytest.mark.parametrize(
    "value",
    [
        "",
        1,
        "/relative",
        "ftp://example.com/file",
        "https:///missing-host",
        "https://user@example.com/",
        "https://user:pass@example.com/",
        "https://example.com:0/",
        "https://example.com:65536/",
        "https://example.com:not-a-port/",
        "https://[bad/",
        "https://bad host/",
        "https://bad..example/",
    ],
)
def test_normalize_url_rejects_invalid_urls(value):
    with pytest.raises(UrlError):
        normalize_url(value)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("url", "include_subdomains", "expected"),
    [
        ("https://abc.example.com/", False, True),
        ("https://ABC.EXAMPLE.COM./", False, True),
        ("https://child.abc.example.com/", False, False),
        ("https://child.abc.example.com/", True, True),
        ("https://deep.child.abc.example.com/", True, True),
        ("https://example.com/", True, False),
        ("https://sibling.example.com/", True, False),
        ("https://notabc.example.com/", True, False),
        ("https://abc.example.com.evil.test/", True, False),
    ],
)
def test_url_in_scope_honors_exact_abc_subdomain_boundary(
    url, include_subdomains, expected
):
    assert (
        url_in_scope(
            url,
            "abc.example.com",
            include_subdomains=include_subdomains,
        )
        is expected
    )


@pytest.mark.parametrize(
    ("url", "host"),
    [
        ("not a URL", "abc.example.com"),
        ("https://abc.example.com/", "bad host"),
    ],
)
def test_url_in_scope_returns_false_for_invalid_input(url, host):
    assert not url_in_scope(url, host)


def test_url_in_scope_handles_normalized_url_without_hostname(monkeypatch):
    monkeypatch.setattr(urls_module, "normalize_url", lambda value: "https:///path")

    assert not url_in_scope("https://example.com/", "example.com")


def test_remove_dot_segments_handles_relative_internal_path():
    assert urls_module._remove_dot_segments("a/../b") == "b"  # noqa: SLF001
    assert urls_module._remove_dot_segments("/a//b") == "/a//b"  # noqa: SLF001


def test_extract_html_urls_handles_base_canonical_anchor_area_and_deduplication():
    body = """
        <base href="https://cdn.example.com/root/">
        <base href="https://ignored.example/">
        <a href="page#one">Page</a>
        <AREA HREF="/map">
        <link rel="stylesheet canonical alternate" href="canonical">
        <link rel="stylesheet" href="ignored.css">
        <a href="page#two">duplicate after fragment removal</a>
        <a href="mailto:test@example.com">invalid</a>
        <a>missing</a>
    """

    assert extract_html_urls(body, "https://origin.example/start/index.html") == (
        "https://cdn.example.com/root/page",
        "https://cdn.example.com/map",
        "https://cdn.example.com/root/canonical",
    )


def test_extract_html_urls_ignores_invalid_first_base_but_does_not_use_later_base():
    body = '<base href="ftp://bad.example/"><base href="/good/"><a href="page">'

    assert extract_html_urls(body, "https://origin.example/start/") == (
        "https://origin.example/start/page",
    )


def test_extract_html_urls_rejects_invalid_response_url():
    with pytest.raises(UrlError):
        extract_html_urls("<a href='/'>", "relative")


def test_extract_http_link_urls_handles_multiple_headers_and_malformed_items():
    headers = [
        '</next>; rel="next", <https://EXAMPLE.com/canonical#x>; rel="canonical"',
        "garbage, <../previous>; rel=prev, <mailto:test@example.com>; rel=author",
        "<https://example.com/canonical#other>; rel=duplicate",
        "<unterminated",
    ]

    assert extract_http_link_urls(headers, "https://example.com/dir/page") == (
        "https://example.com/next",
        "https://example.com/canonical",
        "https://example.com/previous",
    )


def test_extract_sitemap_urlset_with_namespaces_and_invalid_locations():
    body = b"""<?xml version="1.0"?>
    <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
      <url><loc>https://Example.com/a#fragment</loc></url>
      <url><loc>/relative</loc></url>
      <url><loc>mailto:test@example.com</loc></url>
      <url><loc> https://example.com/a </loc></url>
      <url><loc /></url>
    </urlset>
    """

    assert extract_sitemap(body, "https://example.com/sitemap.xml") == SitemapContents(
        pages=("https://example.com/a", "https://example.com/relative"),
        sitemaps=(),
    )


def test_extract_sitemap_index_without_namespace():
    body = b"""
    <sitemapindex>
      <sitemap><loc>sitemaps/one.xml</loc></sitemap>
      <sitemap><loc>https://other.example/two.xml</loc></sitemap>
    </sitemapindex>
    """

    assert extract_sitemap(body, "https://example.com/root.xml") == SitemapContents(
        pages=(),
        sitemaps=(
            "https://example.com/sitemaps/one.xml",
            "https://other.example/two.xml",
        ),
    )


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        (
            b"""
            <urlset xmlns:image="http://www.google.com/schemas/sitemap-image/1.1">
              <url><loc>/page</loc><image:image><image:loc>/image</image:loc></image:image></url>
              <url><metadata><loc>/nested</loc></metadata></url>
              <loc>/root</loc><wrapper><url><loc>/wrapped</loc></url></wrapper>
            </urlset>
            """,
            SitemapContents(("https://example.com/page",), ()),
        ),
        (
            b"""
            <sitemapindex>
              <sitemap><loc>/child.xml</loc><metadata><loc>/nested.xml</loc></metadata></sitemap>
              <loc>/root.xml</loc><wrapper><sitemap><loc>/wrapped.xml</loc></sitemap></wrapper>
            </sitemapindex>
            """,
            SitemapContents((), ("https://example.com/child.xml",)),
        ),
    ],
)
def test_extract_sitemap_uses_only_direct_entry_locations(body, expected):
    assert extract_sitemap(body, "https://example.com/sitemap.xml") == expected


@pytest.mark.parametrize(
    ("body", "message"),
    [
        (b"<urlset>", "invalid sitemap XML"),
        (b"<rss><loc>https://example.com/</loc></rss>", "not a sitemap"),
    ],
)
def test_extract_sitemap_rejects_malformed_or_wrong_xml(body, message):
    with pytest.raises(UrlError, match=message):
        extract_sitemap(body, "https://example.com/sitemap.xml")
