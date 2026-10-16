"""
Tests for partial dataset uploads: write modes, partitioning and ingest status.

An API without write modes ignores `?mode=` and replaces the whole dataset, so
a partial upload must be refused before any data is sent to one.
"""
import pyarrow as pa
import pytest

from polyteia_sdk_python_v2 import api_utils as api
from polyteia_sdk_python_v2 import PolyteiaAPIError

TABLE = pa.table({"year": [2025], "id": [1]})


class Response:
    def __init__(self, body):
        self._body = body

    def json(self):
        return self._body


class Posts(list):
    """Every rest_post made, plus how the probe answers."""
    probe_status = 400


@pytest.fixture
def posts(monkeypatch):
    sent = Posts()

    def rest_post(path, *, params=None, files=None, **kwargs):
        sent.append({"path": path, "params": params, "files": files})
        if params == {"mode": "__probe__"}:
            status = sent.probe_status
            body = ({"error": "mode must be one of: replace, replace_partitions, upsert"}
                    if status == 400 else {"error": "Expected multipart/form-data"})
            raise PolyteiaAPIError("refused", status_code=status, body=body)
        return Response({"ok": True, "ingestId": "ing_1"})

    monkeypatch.setattr(api, "rest_post", rest_post)
    return sent


def test_without_a_mode_the_upload_is_unchanged(posts):
    result = api.upload_file("ds_1", TABLE, "tok")

    assert len(posts) == 1
    assert posts[0]["params"] is None
    assert result["ingestId"] == "ing_1"


def test_an_upsert_repeats_key_per_column(posts):
    api.upload_file("ds_1", TABLE, "tok", mode="upsert", key_columns=["year", "id"])

    assert posts[-1]["params"] == [("mode", "upsert"), ("key", "year"), ("key", "id")]
    assert posts[-1]["files"] is not None


def test_a_partial_upload_probes_first_without_data(posts):
    api.upload_file("ds_1", TABLE, "tok", mode="replace_partitions")

    assert [p["params"] for p in posts] == [{"mode": "__probe__"},
                                            [("mode", "replace_partitions")]]
    assert posts[0]["files"] is None


def test_an_api_without_write_modes_gets_nothing(posts):
    posts.probe_status = 415

    with pytest.raises(PolyteiaAPIError, match="Nothing was uploaded"):
        api.upload_file("ds_1", TABLE, "tok", mode="replace_partitions")

    assert len(posts) == 1


def test_an_unknown_mode_is_refused_before_calling(posts):
    with pytest.raises(PolyteiaAPIError, match="mode"):
        api.upload_file("ds_1", TABLE, "tok", mode="append")

    assert posts == []


@pytest.fixture
def statuses(monkeypatch):
    answers = []
    monkeypatch.setattr(api, "rpc_call", lambda *a, **kw: answers.pop(0))
    monkeypatch.setattr(api.time, "sleep", lambda s: None)
    return answers


def test_waiting_returns_once_completed(statuses):
    statuses.extend([{"status": "pending"}, {"status": "running"}, {"status": "completed"}])

    assert api.wait_for_dataset_ingest("ds_1", "ing_1", "tok")["status"] == "completed"
    assert statuses == []


def test_a_failed_ingest_raises_its_message(statuses):
    statuses.extend([{"status": "running"},
                     {"status": "failed", "errorMessage": "upsert keys must be unique"}])

    with pytest.raises(PolyteiaAPIError, match="upsert keys must be unique"):
        api.wait_for_dataset_ingest("ds_1", "ing_1", "tok")


def test_waiting_gives_up_after_the_timeout(statuses):
    statuses.extend([{"status": "running"}])

    with pytest.raises(PolyteiaAPIError, match="still running"):
        api.wait_for_dataset_ingest("ds_1", "ing_1", "tok", timeout=0)


def test_partitioning_is_set_and_removed(monkeypatch):
    sent = []
    monkeypatch.setattr(api, "rpc_call",
                        lambda router, proc, params, **kw: sent.append((proc, params)) or {})

    api.set_dataset_partitioning("ds_1", ("year", "region"), "tok")
    api.set_dataset_partitioning("ds_1", None, "tok")

    assert sent == [("setDatasetPartitioning", {"id": "ds_1", "columns": ["year", "region"]}),
                    ("setDatasetPartitioning", {"id": "ds_1", "columns": None})]


def test_an_unpartitioned_dataset_reads_as_none(monkeypatch):
    monkeypatch.setattr(api, "get_dataset_by_id", lambda *a: {"id": "ds_1"})

    assert api.get_dataset_partitioning("ds_1", "tok") is None
