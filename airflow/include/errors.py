"""Error taxonomy for ingestion.

`retryable` tells the orchestration layer whether a task-level retry could
plausibly succeed. Permanent errors (bad request, bad credentials, data that
fails validation) are surfaced immediately instead of being retried into a
request storm.
"""

from __future__ import annotations


class IngestionError(Exception):
    """Base class for all ingestion failures."""

    retryable: bool = True


class PermanentIngestionError(IngestionError):
    """A failure that will not resolve by retrying the same task."""

    retryable = False


# --- HTTP ---------------------------------------------------------------------


class HttpError(IngestionError):
    def __init__(self, message: str, *, url: str, status: int | None = None):
        super().__init__(message)
        self.url = url
        self.status = status


class TransientHttpError(HttpError):
    """Timeouts, connection failures and 5xx responses."""


class RateLimitedError(TransientHttpError):
    """HTTP 429, or a Retry-After too long to wait out inside one task."""


class PermanentHttpError(HttpError):
    """4xx responses other than 429: a configuration or request problem."""

    retryable = False


# --- Response content ---------------------------------------------------------


class ResponseFormatError(IngestionError):
    """The body could not be parsed (invalid JSON/XML, unexpected content type).

    Retryable: the usual cause is an intermediary (CDN challenge, error page)
    serving HTML with a 200 status, which is often temporary.
    """


class SchemaError(ResponseFormatError):
    """The body parsed but the expected top-level structure is missing."""


class EmptyResponseError(ResponseFormatError):
    """A structurally valid response with zero records where some are expected."""


class InvalidCursorError(PermanentHttpError):
    """The provider rejected a stored pagination cursor."""


# --- Pipeline -----------------------------------------------------------------


class ValidationError(PermanentIngestionError):
    """Extracted data failed quality checks. Retrying re-validates the same data."""


class PersistenceError(IngestionError):
    """Bronze output could not be written or read back."""
