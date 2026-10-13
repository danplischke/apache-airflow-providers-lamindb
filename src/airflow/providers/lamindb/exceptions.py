from __future__ import annotations

from typing import Any

from airflow.sdk.exceptions import AirflowException, AirflowFailException


class LaminDBApiError(AirflowException):
    """Raised when the LaminHub REST API returns an error response."""

    def __init__(self, message: str, *, http_status_code: int | None = None, detail: Any = None) -> None:
        super().__init__(message)
        self.http_status_code = http_status_code
        self.detail = detail


class LaminDBVersionError(AirflowFailException, ValueError):
    """Raised for lamindb older than the provider supports; tasks fail without retries."""
