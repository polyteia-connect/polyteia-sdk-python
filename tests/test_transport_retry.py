"""
Tests for the retry behaviour in polyteia_sdk_python_v2._transport.

The upload case is the one that matters: a retry that re-posts a consumed
stream stores an empty file, which looks like success.
"""
import io

import pytest
import requests

from polyteia_sdk_python_v2 import _transport as t
from polyteia_sdk_python_v2 import PolyteiaAPIError


class Response:
    def __init__(self, status_code, headers=None):
        self.status_code = status_code
        self.headers = headers or {}
        self.text = ""


@pytest.fixture(autouse=True)
def no_sleeping(monkeypatch):
    """Record the backoff instead of waiting for it."""
    slept = []
    monkeypatch.setattr(t.time, "sleep", slept.append)
    return slept


def responder(monkeypatch, statuses, *, on_post=None):
    """Answer successive POSTs with `statuses`; record what was sent."""
    sent = []

    def post(url, headers=None, json=None, files=None, data=None, timeout=None):
        if on_post is not None:
            on_post(files)
        sent.append({"files": files, "json": json})
        status = statuses[min(len(sent) - 1, len(statuses) - 1)]
        if isinstance(status, Exception):
            raise status
        return Response(*status) if isinstance(status, tuple) else Response(status)

    monkeypatch.setattr(t.requests, "post", post)
    return sent


def call(files=None, **kw):
    return t._post_with_retry("http://api/x", headers={}, json_body={"a": 1},
                              timeout=1, ctx="ctx", files=files, **kw)


# --- uploads ---------------------------------------------------------------

def consume(files):
    """What requests does to a file payload: reads it to the end."""
    for payload in t._file_objects(files):
        payload.read()


def test_a_retried_upload_sends_the_whole_body_again(monkeypatch):
    handle = io.BytesIO(b"payload")
    sent = responder(monkeypatch, [503, 503, 200], on_post=consume)

    response = call(files={"file": handle})

    assert response.status_code == 200
    assert len(sent) == 3
    # Without the rewind the second and third attempts upload zero bytes and
    # the server stores a truncated dataset.
    assert handle.getvalue() == b"payload"
    assert handle.tell() == len(b"payload"), "the last attempt read the full body"


def test_the_tuple_form_of_files_is_rewound_too(monkeypatch):
    handle = io.BytesIO(b"parquet")
    files = [("file", ("data.parquet", handle, "application/vnd.apache.parquet"))]
    responder(monkeypatch, [503, 200], on_post=consume)

    assert call(files=files).status_code == 200
    assert handle.tell() == len(b"parquet")


def test_a_non_rewindable_body_raises_instead_of_truncating(monkeypatch):
    class Unseekable(io.BytesIO):
        def seek(self, *a):
            raise OSError("not seekable")

    responder(monkeypatch, [503, 200], on_post=consume)

    with pytest.raises(PolyteiaAPIError, match="not rewindable"):
        call(files={"file": Unseekable(b"payload")})


def test_bytes_and_strings_need_no_rewinding(monkeypatch):
    sent = responder(monkeypatch, [503, 200])

    assert call(files={"a": b"bytes", "b": "text"}).status_code == 200
    assert len(sent) == 2


def test_a_request_without_files_still_retries(monkeypatch):
    sent = responder(monkeypatch, [500, 200])

    assert call().status_code == 200
    assert len(sent) == 2


# --- backoff ---------------------------------------------------------------

def test_retry_after_on_a_429_is_honoured(monkeypatch, no_sleeping):
    responder(monkeypatch, [(429, {"Retry-After": "30"}), 200])

    call()

    # Retrying into the same rate limit after 1s burns the remaining attempts.
    assert no_sleeping == [30.0]


def test_a_retry_after_shorter_than_the_backoff_does_not_shorten_it(monkeypatch, no_sleeping):
    responder(monkeypatch, [(429, {"Retry-After": "0"}), 200])

    call()

    assert no_sleeping == [1.0]


def test_an_absurd_retry_after_is_capped(monkeypatch, no_sleeping):
    responder(monkeypatch, [(503, {"Retry-After": "86400"}), 200])

    call()

    assert no_sleeping == [t._MAX_RETRY_AFTER_SECONDS]


def test_a_date_form_retry_after_falls_back_to_the_backoff(monkeypatch, no_sleeping):
    responder(monkeypatch, [(429, {"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"}), 200])

    call()

    assert no_sleeping == [1.0]


def test_the_backoff_is_exponential(monkeypatch, no_sleeping):
    responder(monkeypatch, [500, 500, 200])

    call()

    assert no_sleeping == [1.0, 2.0]


# --- what is and is not retried -------------------------------------------

@pytest.mark.parametrize("status", sorted(t._RETRY_STATUS))
def test_every_retryable_status_is_retried(monkeypatch, status):
    sent = responder(monkeypatch, [status, 200])

    assert call().status_code == 200
    assert len(sent) == 2


@pytest.mark.parametrize("status", [400, 401, 403, 404, 422])
def test_a_client_error_is_returned_without_retrying(monkeypatch, status):
    sent = responder(monkeypatch, [status])

    assert call().status_code == status
    assert len(sent) == 1, "a deterministic 4xx will not succeed on a second try"


def test_the_last_retryable_response_is_returned_for_the_caller_to_surface(monkeypatch):
    sent = responder(monkeypatch, [503])

    assert call().status_code == 503
    assert len(sent) == t._MAX_RETRIES


def test_a_transport_error_raises_after_the_last_attempt(monkeypatch):
    sent = responder(monkeypatch, [requests.ConnectionError("refused")])

    with pytest.raises(PolyteiaAPIError, match="request error"):
        call()
    assert len(sent) == t._MAX_RETRIES


def test_a_transport_error_that_clears_is_not_fatal(monkeypatch):
    responder(monkeypatch, [requests.ConnectionError("refused"), 200])

    assert call().status_code == 200
