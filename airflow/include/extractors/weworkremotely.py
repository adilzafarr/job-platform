"""We Work Remotely — public RSS feeds (https://weworkremotely.com/remote-job-rss-feed)

WWR's JSON API (weworkremotely.com/api) is a token-gated *posting* API for
partners; it cannot list the board. The public read mechanism is RSS: one
feed per category, no pagination, no date filter. The ETag it sends changes
on every request even for identical bodies (If-None-Match always gets 200),
so change detection uses a SHA-256 per feed instead.

If any feed changed, all feeds of the run are written, so each committed WWR
partition is a complete snapshot of the configured feeds.

RSS items are converted to dicts that keep the element names exactly as they
appear in the feed (`pubDate`, `expires_at`, `media:content`, ...). Elements
with attributes become `{"@attr": ..., "#text": ...}`; repeated elements
become lists. No field is renamed or reinterpreted.
"""

from __future__ import annotations

import io
from typing import Any
from xml.etree.ElementTree import Element, ParseError

from defusedxml import DefusedXmlException
from defusedxml import ElementTree as SafeET

from include.errors import EmptyResponseError, ResponseFormatError, SchemaError
from include.extractors.base import ExtractionResult, FetchedPage, JobSource, ParsedPage
from include.extractors.http import HttpResponse

_WELL_KNOWN_NS = {
    "http://search.yahoo.com/mrss": "media",
    "http://search.yahoo.com/mrss/": "media",
    "http://purl.org/dc/elements/1.1/": "dc",
}


class WeWorkRemotelyExtractor(JobSource):
    name = "weworkremotely"
    mode = "snapshot"
    id_field = "guid"
    required_fields = ("guid", "title", "pubDate")
    payload_format = "xml"

    def source_job_id(self, record: dict[str, Any]) -> str | None:
        # guid is the job permalink; fall back to link if a feed ever omits it.
        for key in ("guid", "link"):
            value = record.get(key)
            if isinstance(value, dict):
                value = value.get("#text")
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None

    def parse_page(self, response: HttpResponse, context: dict[str, Any]) -> ParsedPage:
        ctype = response.content_type.lower()
        if "html" in ctype:
            raise ResponseFormatError(f"Expected RSS from {response.url} but got {ctype!r}")
        try:
            namespaces = _collect_namespaces(response.body)
            root = SafeET.fromstring(response.body)
        except (ParseError, DefusedXmlException) as exc:
            raise ResponseFormatError(f"Invalid XML from {response.url}: {exc}") from exc

        channel = root.find("channel") if root.tag == "rss" else None
        if channel is None:
            raise SchemaError(f"WWR feed {response.url} is not RSS 2.0 (root <{root.tag}>, no <channel>)")

        items = [_element_to_dict(item, namespaces) for item in channel.findall("item")]
        meta = {
            "feed": context.get("feed"),
            "channel_title": channel.findtext("title"),
            "ttl": channel.findtext("ttl"),
        }
        return ParsedPage(records=items, meta=meta)

    def extract(self, incremental_state: dict[str, Any], *, full_refresh: bool = False) -> ExtractionResult:
        cfg = self.settings.weworkremotely
        previous_hashes: dict[str, str] = incremental_state.get("feed_sha256") or {}
        pages: list[FetchedPage] = []
        hashes: dict[str, str] = {}
        warnings: list[str] = []

        for feed in cfg.feeds:
            url = f"{cfg.base_url.rstrip('/')}/{feed}.rss"
            response = self.http.get(
                url,
                headers={"Accept": "application/rss+xml, application/xml;q=0.9"},
                min_interval=cfg.request_interval_seconds,
            )
            context = {"page": len(pages) + 1, "feed": feed}
            parsed = self.parse_page(response, context)
            if not parsed.records:
                warnings.append(f"WWR feed {feed!r} is empty")
            hashes[feed] = self.body_sha256(response)
            pages.append(FetchedPage(response=response, context=context, record_count=len(parsed.records)))

        changed = sorted(f for f in hashes if hashes[f] != previous_hashes.get(f))
        notes = {"feeds": list(cfg.feeds), "changed_feeds": changed, "full_refresh": full_refresh}

        if not full_refresh and not changed:
            return ExtractionResult(
                status="not_modified",
                pages=pages,
                state_after=dict(incremental_state),
                notes={**notes, "reason": "all_feed_hashes_unchanged"},
                warnings=warnings,
            )
        if sum(p.record_count for p in pages) == 0:
            raise EmptyResponseError(f"All {len(pages)} WWR feeds returned zero items")

        return ExtractionResult(
            status="fetched",
            pages=pages,
            state_after={"feed_sha256": hashes},
            notes=notes,
            warnings=warnings,
        )


def _collect_namespaces(body: bytes) -> dict[str, str]:
    namespaces = dict(_WELL_KNOWN_NS)
    for _event, (prefix, uri) in SafeET.iterparse(io.BytesIO(body), events=("start-ns",)):
        if prefix:
            namespaces[uri] = prefix
    return namespaces


def _qualified_name(tag: str, namespaces: dict[str, str]) -> str:
    if tag.startswith("{"):
        uri, _, local = tag[1:].partition("}")
        prefix = namespaces.get(uri)
        return f"{prefix}:{local}" if prefix else tag
    return tag


def _element_to_dict(element: Element, namespaces: dict[str, str]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for child in element:
        key = _qualified_name(child.tag, namespaces)
        value = _element_value(child, namespaces)
        if key in result:
            existing = result[key]
            result[key] = existing + [value] if isinstance(existing, list) else [existing, value]
        else:
            result[key] = value
    return result


def _element_value(element: Element, namespaces: dict[str, str]) -> Any:
    text = element.text if element.text is not None else ""
    if not element.attrib and len(element) == 0:
        return text
    value: dict[str, Any] = {f"@{_qualified_name(k, namespaces)}": v for k, v in element.attrib.items()}
    if len(element):
        value.update(_element_to_dict(element, namespaces))
    if text.strip():
        value["#text"] = text
    return value
