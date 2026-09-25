"""Object storage abstraction for the lake (MinIO locally, S3 later).

The pipeline only needs a handful of primitives: put/get whole objects, check
existence, list and delete by prefix. Both implementations give atomic
whole-object writes: an S3 PUT is atomic, and local writes go through a
temp file + rename. Readers therefore never see a half-written file, which
the idempotency design relies on.

Moving from MinIO to AWS S3 is a configuration change: unset
`LAKE_S3_ENDPOINT_URL` and let boto3 use its default credential chain.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Protocol

from include.config import StorageSettings


class ObjectNotFoundError(KeyError):
    pass


class ObjectStorage(Protocol):
    def put_bytes(self, key: str, data: bytes, *, content_type: str | None = None) -> None: ...
    def get_bytes(self, key: str) -> bytes: ...
    def exists(self, key: str) -> bool: ...
    def list_keys(self, prefix: str) -> list[str]: ...
    def delete_prefix(self, prefix: str) -> int: ...
    def uri(self, key: str) -> str: ...
    def ensure_bucket(self) -> None: ...


class S3ObjectStorage:
    def __init__(self, settings: StorageSettings, *, client=None) -> None:
        self.bucket = settings.bucket
        self._settings = settings
        if client is None:
            import boto3
            from botocore.config import Config

            client = boto3.client(
                "s3",
                endpoint_url=settings.endpoint_url,
                region_name=settings.region,
                aws_access_key_id=settings.access_key_id,
                aws_secret_access_key=settings.secret_access_key,
                config=Config(
                    retries={"max_attempts": 5, "mode": "standard"},
                    # MinIO needs path-style addressing; harmless on AWS.
                    s3={"addressing_style": "path"} if settings.endpoint_url else {},
                    connect_timeout=10,
                    read_timeout=60,
                ),
            )
        self._client = client

    def put_bytes(self, key: str, data: bytes, *, content_type: str | None = None) -> None:
        extra = {"ContentType": content_type} if content_type else {}
        self._client.put_object(Bucket=self.bucket, Key=key, Body=data, **extra)

    def get_bytes(self, key: str) -> bytes:
        try:
            return self._client.get_object(Bucket=self.bucket, Key=key)["Body"].read()
        except self._client.exceptions.NoSuchKey as exc:
            raise ObjectNotFoundError(key) from exc

    def exists(self, key: str) -> bool:
        from botocore.exceptions import ClientError

        try:
            self._client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
                return False
            raise

    def list_keys(self, prefix: str) -> list[str]:
        keys: list[str] = []
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            keys.extend(obj["Key"] for obj in page.get("Contents", []))
        return keys

    def delete_prefix(self, prefix: str) -> int:
        if not prefix or not prefix.endswith("/"):
            raise ValueError(f"Refusing to delete non-directory prefix {prefix!r}")
        keys = self.list_keys(prefix)
        for start in range(0, len(keys), 1000):
            batch = [{"Key": k} for k in keys[start : start + 1000]]
            self._client.delete_objects(Bucket=self.bucket, Delete={"Objects": batch, "Quiet": True})
        return len(keys)

    def uri(self, key: str) -> str:
        return f"s3://{self.bucket}/{key}"

    def ensure_bucket(self) -> None:
        from botocore.exceptions import ClientError

        try:
            self._client.head_bucket(Bucket=self.bucket)
        except ClientError:
            self._client.create_bucket(Bucket=self.bucket)


class LocalObjectStorage:
    """Filesystem-backed storage with the same semantics (tests, offline dev)."""

    def __init__(self, root: str | Path, bucket: str = "lake") -> None:
        self.bucket = bucket
        self.root = Path(root) / bucket

    def _path(self, key: str) -> Path:
        path = (self.root / key).resolve()
        if self.root.resolve() not in path.parents and path != self.root.resolve():
            raise ValueError(f"Key escapes storage root: {key!r}")
        return path

    def put_bytes(self, key: str, data: bytes, *, content_type: str | None = None) -> None:
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".tmp-")
        try:
            with os.fdopen(fd, "wb") as fh:
                fh.write(data)
            os.replace(tmp, path)
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise

    def get_bytes(self, key: str) -> bytes:
        path = self._path(key)
        if not path.is_file():
            raise ObjectNotFoundError(key)
        return path.read_bytes()

    def exists(self, key: str) -> bool:
        return self._path(key).is_file()

    def list_keys(self, prefix: str) -> list[str]:
        if not self.root.exists():
            return []
        keys = []
        for path in self.root.rglob("*"):
            if path.is_file() and not path.name.startswith(".tmp-"):
                key = path.relative_to(self.root).as_posix()
                if key.startswith(prefix):
                    keys.append(key)
        return sorted(keys)

    def delete_prefix(self, prefix: str) -> int:
        if not prefix or not prefix.endswith("/"):
            raise ValueError(f"Refusing to delete non-directory prefix {prefix!r}")
        keys = self.list_keys(prefix)
        for key in keys:
            self._path(key).unlink(missing_ok=True)
        return len(keys)

    def uri(self, key: str) -> str:
        path = self._path(key).as_posix()
        return path + "/" if key.endswith("/") else path

    def ensure_bucket(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)


def storage_from_settings(settings: StorageSettings) -> ObjectStorage:
    if settings.backend == "s3":
        return S3ObjectStorage(settings)
    if settings.backend == "local":
        return LocalObjectStorage(settings.local_root, settings.bucket)
    raise ValueError(f"Unknown LAKE_STORAGE_BACKEND {settings.backend!r} (expected 's3' or 'local')")
