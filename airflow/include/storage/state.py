"""Per-source ingestion state, stored as one JSON object per source.

    ingestion/state/<source>.json

The generic fields (status, last attempted/successful run) are shared by all
sources. `incremental` holds only what that provider supports:

    himalayas       last_seen_published_at, last_cursor, sweep_high_watermark
    remoteok        content_sha256, last_updated
    remotive        last_modified, etag, content_sha256
    weworkremotely  feed_sha256 {feed: sha256}

`version` increments only when a run commits successfully. A Bronze manifest
records the version it was built on, so a commit can be replayed safely after
a crash (roll forward) without regressing state that a later run already
advanced. There is one writer per source at a time (the DAG runs with
`max_active_runs=1`, and a source's tasks run sequentially), so plain
read-modify-write of a single atomically written object is sufficient.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any

from include.storage.bronze import dumps_json, state_key
from include.storage.object_storage import ObjectNotFoundError, ObjectStorage


@dataclass
class SourceState:
    source: str
    version: int = 0
    status: str = "never_run"  # running | succeeded | not_modified | failed
    last_attempted_run_id: str | None = None
    last_attempted_at: str | None = None
    last_successful_run_id: str | None = None
    last_successful_at: str | None = None
    last_bronze_run_id: str | None = None
    last_error: dict[str, Any] | None = None
    incremental: dict[str, Any] = field(default_factory=dict)
    updated_at: str | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SourceState":
        known = {f for f in cls.__dataclass_fields__}
        return cls(**{k: v for k, v in data.items() if k in known})


class StateStore:
    def __init__(self, storage: ObjectStorage) -> None:
        self._storage = storage

    def load(self, source: str) -> SourceState:
        try:
            raw = self._storage.get_bytes(state_key(source))
        except ObjectNotFoundError:
            return SourceState(source=source)
        return SourceState.from_dict(json.loads(raw))

    def save(self, state: SourceState) -> None:
        state.updated_at = datetime.now(UTC).isoformat()
        self._storage.put_bytes(state_key(state.source), dumps_json(asdict(state)), content_type="application/json")
