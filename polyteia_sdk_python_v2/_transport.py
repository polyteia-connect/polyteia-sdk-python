"""Core transport for the Polyteia API.

The Polyteia API is an RPC API. Every procedure is reached as
``POST /rpc/<router>/<procedure>``. The request body wraps the input under a
``json`` key; the response wraps the output under ``json`` with a ``meta``
sidecar the server uses to describe rich types (e.g. dates)::

    request:  {"json": <input>}
    response: {"json": <output>, "meta": [...]}

Authentication: a personal access key is exchanged for a short-lived,
organization-scoped session token (see ``get_org_access_token``). That token is
sent as ``Authorization: Bearer <token>`` on every subsequent call.
"""

import time
from typing import Any, Optional
import requests

# Base URL of the Polyteia API. Override per call with the ``API_URL`` argument.
DEFAULT_API_URL = "https://app.polyteia.com"

# Network timeout (seconds) applied to every request unless overridden.
DEFAULT_TIMEOUT = 60

# Transient failures (5xx, connection errors) are retried with exponential
# backoff. 429 is also retried. 4xx (other than 429) are not — they are
# deterministic client errors.
_MAX_RETRIES = 3
_RETRY_BACKOFF_SECONDS = 1.0
_RETRY_STATUS = {429, 500, 502, 503, 504}


class PolyteiaAPIError(Exception):
    """Raised when an API call fails.

    Attributes:
        status_code: HTTP status code, if the request completed.
        code: The API error code (e.g. ``"NOT_FOUND"``), if present in the body.
        body: The parsed error body (dict) or raw text.
    """

    def __init__(
        self,
        message: str,
        *,
        status_code: Optional[int] = None,
        code: Optional[str] = None,
        body: Any = None,
    ):
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.body = body


def _normalize_base_url(api_url: str) -> str:
    """Return ``api_url`` with a scheme and no trailing slash.

    A base URL supplied without a scheme (e.g. ``"api.example.com"``) defaults
    to HTTPS.
    """
    url = (api_url or "").strip().rstrip("/")
    if not url:
        raise PolyteiaAPIError("API_URL is empty")
    if "://" not in url:
        url = f"https://{url}"
    return url


def _headers(access_token: Optional[str]) -> dict:
    headers = {"Content-Type": "application/json"}
    if access_token:
        headers["Authorization"] = f"Bearer {access_token}"
    return headers


def _post_with_retry(url, *, headers, json_body, timeout, ctx, files=None, data=None):
    """POST with exponential-backoff retry on transient failures (5xx / 429 /
    connection errors). Deterministic 4xx responses are returned immediately for
    the caller to surface. Raises PolyteiaAPIError only if every attempt fails
    with a transport error."""
    last_exc = None
    for attempt in range(_MAX_RETRIES):
        try:
            response = requests.post(
                url, headers=headers, json=json_body, files=files, data=data, timeout=timeout
            )
        except requests.RequestException as exc:
            last_exc = exc
            if attempt < _MAX_RETRIES - 1:
                time.sleep(_RETRY_BACKOFF_SECONDS * (2 ** attempt))
                continue
            raise PolyteiaAPIError(f"{ctx} failed: request error: {exc}") from exc

        if response.status_code in _RETRY_STATUS and attempt < _MAX_RETRIES - 1:
            time.sleep(_RETRY_BACKOFF_SECONDS * (2 ** attempt))
            continue
        return response
    # Unreachable, but keep type-checkers happy.
    raise PolyteiaAPIError(f"{ctx} failed after retries", body=str(last_exc))


# oRPC ``meta`` type codes. The server sends these on responses and expects
# them on requests to describe rich types that JSON cannot represent natively.
_META_TYPE_DATE = 1


def rpc_call(
    router: str,
    procedure: str,
    input: Optional[dict] = None,
    *,
    access_token: Optional[str] = None,
    API_URL: str = DEFAULT_API_URL,
    context: Optional[str] = None,
    timeout: int = DEFAULT_TIMEOUT,
    date_fields: Optional[list] = None,
) -> Any:
    """Call an RPC procedure and return its unwrapped result.

    Args:
        router: Router key, e.g. ``"insight"``, ``"dataset"``, ``"report"``.
        procedure: Procedure name, e.g. ``"listInsights"``, ``"createDataset"``.
            Nested procedures use ``/`` (e.g. ``"personalAccessKey/exchange"``).
        input: The procedure input object.
        access_token: Session token. Omit only for the token exchange, which
            must be sent unauthenticated.
        API_URL: Base URL of the Polyteia API.
        context: Human-readable label used in error messages.
        timeout: Request timeout in seconds.
        date_fields: Top-level input keys whose (ISO-string) values are Dates.
            The server's validator rejects a bare date string unless the request
            carries a ``meta`` entry marking the field as a Date; this emits that
            entry for each named key present and non-null in ``input``.

    Returns:
        The value carried under the response ``json`` key.

    Raises:
        PolyteiaAPIError: on a non-success status, a transport failure, or a
            response body that is not valid JSON.
    """
    ctx = context or f"{router}.{procedure}"
    url = f"{_normalize_base_url(API_URL)}/rpc/{router}/{procedure}"
    payload_in = input if input is not None else {}
    body = {"json": payload_in}

    if date_fields:
        meta = [
            [_META_TYPE_DATE, key]
            for key in date_fields
            if payload_in.get(key) is not None
        ]
        if meta:
            body["meta"] = meta

    response = _post_with_retry(url, headers=_headers(access_token), json_body=body, timeout=timeout, ctx=ctx)

    if response.status_code not in (200, 201):
        _raise_for_response(response, ctx)

    try:
        payload = response.json()
    except ValueError:
        raise PolyteiaAPIError(
            f"{ctx} failed: response was not valid JSON:\n{response.text}",
            status_code=response.status_code,
        )

    if isinstance(payload, dict) and "json" in payload:
        return payload["json"]
    return payload


def rest_post(
    path: str,
    *,
    access_token: Optional[str] = None,
    json_body: Optional[dict] = None,
    files: Optional[list] = None,
    data: Optional[dict] = None,
    extra_headers: Optional[dict] = None,
    API_URL: str = DEFAULT_API_URL,
    context: Optional[str] = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> "requests.Response":
    """POST to a hand-rolled REST route (e.g. multipart file upload).

    Not all endpoints are RPC procedures: dataset data upload and report-image
    upload are plain REST routes that take multipart form data. This helper
    posts to ``{API_URL}{path}`` and returns the raw response for the caller to
    interpret (some routes return JSON, some return no body).

    Args:
        path: Route path beginning with ``/`` (e.g. ``"/api/upload/dataset/ds_1"``).
        access_token: Session token for the ``Authorization`` header.
        json_body: JSON body (mutually exclusive with ``files``/``data``).
        files: ``requests`` files argument for multipart uploads.
        data: Form fields for multipart uploads.
        extra_headers: Additional headers to merge in.
        API_URL: Base URL of the Polyteia API.
        context: Label for error messages.
        timeout: Request timeout in seconds.

    Raises:
        PolyteiaAPIError: on a non-success status or transport failure.
    """
    ctx = context or f"POST {path}"
    url = f"{_normalize_base_url(API_URL)}{path}"

    headers = {}
    if access_token:
        headers["Authorization"] = f"Bearer {access_token}"
    if json_body is not None:
        headers["Content-Type"] = "application/json"
    if extra_headers:
        headers.update(extra_headers)

    response = _post_with_retry(
        url, headers=headers, json_body=json_body, files=files, data=data,
        timeout=timeout, ctx=ctx,
    )

    if response.status_code not in (200, 201, 204):
        _raise_for_response(response, ctx)
    return response


def _extract_error_code(body: Any) -> Optional[str]:
    """Pull an error code out of a parsed error body, if present."""
    if not isinstance(body, dict):
        return None
    if isinstance(body.get("code"), str):
        return body["code"]
    inner = body.get("json")
    if isinstance(inner, dict) and isinstance(inner.get("code"), str):
        return inner["code"]
    return None


def _raise_for_response(response: "requests.Response", ctx: str) -> None:
    """Build and raise a PolyteiaAPIError from a failed response."""
    try:
        body = response.json()
    except ValueError:
        body = response.text

    raise PolyteiaAPIError(
        f"{ctx} failed (HTTP {response.status_code}):\n{body}",
        status_code=response.status_code,
        code=_extract_error_code(body),
        body=body,
    )
