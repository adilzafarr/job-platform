"""Source-specific extractors behind the common `JobSource` contract."""

from __future__ import annotations

from include.extractors.base import ExtractionResult, FetchedPage, JobSource, ParsedPage
from include.extractors.himalayas import HimalayasExtractor
from include.extractors.remoteok import RemoteOKExtractor
from include.extractors.remotive import RemotiveExtractor
from include.extractors.weworkremotely import WeWorkRemotelyExtractor

EXTRACTORS: dict[str, type[JobSource]] = {
    cls.name: cls
    for cls in (HimalayasExtractor, RemoteOKExtractor, RemotiveExtractor, WeWorkRemotelyExtractor)
}

SOURCE_NAMES: tuple[str, ...] = tuple(EXTRACTORS)


def get_extractor_class(source: str) -> type[JobSource]:
    try:
        return EXTRACTORS[source]
    except KeyError:
        raise ValueError(f"Unknown source {source!r}; expected one of {sorted(EXTRACTORS)}") from None


__all__ = [
    "EXTRACTORS",
    "SOURCE_NAMES",
    "ExtractionResult",
    "FetchedPage",
    "JobSource",
    "ParsedPage",
    "get_extractor_class",
]
