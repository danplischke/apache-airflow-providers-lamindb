from __future__ import annotations

from typing import Any

from airflow.sdk.exceptions import AirflowException, AirflowFailException


class LaminDBApiError(AirflowException):
    """
    Raised when the LaminHub REST API returns an error or malformed response.

    ``transient`` is true when the request may succeed if repeated later: network errors, rate
    limiting (429) and server errors (5xx).
    """

    def __init__(
        self,
        message: str,
        *,
        http_status_code: int | None = None,
        detail: Any = None,
        transient: bool = False,
    ) -> None:
        super().__init__(message)
        self.http_status_code = http_status_code
        self.detail = detail
        self.transient = transient


class LaminDBFlowWarning(UserWarning):
    """A DAG uses LaminDB flow runs in a way that changed meaning; see the message."""


class LaminDBVersionError(AirflowFailException, ValueError):
    """Raised for lamindb older than the provider supports; tasks fail without retries."""
