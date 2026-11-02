"""
The dataset source under both API names: the current API calls it
``sourceNote``, the older one ``source``, and the SDK has to work against both.
"""
import pytest

from polyteia_sdk_python_v2 import api_utils as api


class Calls(list):
    """Every rpc_call made; `current` is what getDatasetById answers."""
    current: dict = {}


@pytest.fixture
def calls(monkeypatch):
    sent = Calls()

    def rpc_call(router, method, params, **kwargs):
        sent.append((method, params))
        if method == "getDatasetById":
            return sent.current
        return {"id": "ds_1"}

    monkeypatch.setattr(api, "rpc_call", rpc_call)
    return sent


def test_create_sends_both_names(calls):
    api.create_dataset("sol_1", "Name", "", "Formulare", "slug", "tok", organization_id="org_1")
    method, params = calls[-1]
    assert method == "createDataset"
    assert params["source"] == params["sourceNote"] == "Formulare"


def test_update_mirrors_either_name(calls):
    api.update_dataset("ds_1", "tok", source="A")
    assert calls[-1][1]["source"] == calls[-1][1]["sourceNote"] == "A"
    api.update_dataset("ds_1", "tok", sourceNote="B")
    assert calls[-1][1]["source"] == calls[-1][1]["sourceNote"] == "B"


def test_update_with_both_names_uses_source_note(calls):
    api.update_dataset("ds_1", "tok", source="A", sourceNote="B")
    assert calls[-1][1]["source"] == calls[-1][1]["sourceNote"] == "B"


def test_update_without_source_sends_neither(calls):
    api.update_dataset("ds_1", "tok", name="N")
    assert "source" not in calls[-1][1] and "sourceNote" not in calls[-1][1]


@pytest.mark.parametrize("current", [
    {"name": "N", "slug": "s", "source": "Meso"},
    {"name": "N", "slug": "s", "sourceNote": "Meso", "sourceType": "manual"},
])
def test_source_timestamp_keeps_the_source_on_either_server(calls, current):
    calls.current = current
    api.update_dataset_source_timestamp("ds_1", "2026-11-02", "tok")
    method, params = calls[-1]
    assert method == "updateDataset"
    assert params["source"] == params["sourceNote"] == "Meso"
    assert params["dataAsOf"] == "2026-11-02T00:00:00.000Z"
