"""
Tests for re-publishing a report view and for who can open one.

A view serves a snapshot of the report's structure, insight definitions and
filters, so new data reaches readers on its own and only a change to the report
itself needs a sync. These pin the payload each API version expects, and that
setting the workspace audience does not disturb the named viewers - a freshly
created view is visible to nobody, which is invisible in the UI and looks
exactly like a report that failed to roll out.
"""
import pytest

from polyteia_sdk_python import api_utils as v0
from polyteia_sdk_python_v2 import api_utils as v2


# --- the current API ------------------------------------------------------

class Calls(list):
    def __init__(self):
        super().__init__()
        self.answers = {}


@pytest.fixture
def calls(monkeypatch):
    recorded = Calls()

    def rpc_call(namespace, procedure, params, access_token=None, API_URL=None,
                 context=None, **kwargs):
        recorded.append({"namespace": namespace, "procedure": procedure,
                         "params": params})
        return recorded.answers.get(procedure, {})

    monkeypatch.setattr(v2, "rpc_call", rpc_call)
    return recorded


def test_sync_names_the_view_by_id(calls):
    v2.sync_report_view("rptv_1", "token", API_URL="http://api")

    assert calls[0]["namespace"] == "reportView"
    assert calls[0]["procedure"] == "syncReportView"
    assert calls[0]["params"] == {"id": "rptv_1"}


def test_the_audience_goes_through_manage_viewers(calls):
    # updateReportView rejects the field, so this is the procedure that takes it.
    v2.set_report_view_workspace_audience("rptv_1", "everybody", "token",
                                          API_URL="http://api")

    assert calls[0]["procedure"] == "manageReportViewViewers"
    assert calls[0]["params"]["workspaceAudience"] == "everybody"


def test_setting_the_audience_keeps_the_named_viewers(calls):
    # Empty add/remove lists: the call changes the audience only. Anything else
    # here would drop the groups a view was shared with.
    v2.set_report_view_workspace_audience("rptv_1", "everybody", "token",
                                          API_URL="http://api")

    assert calls[0]["params"]["add"] == {"members": [], "groups": []}
    assert calls[0]["params"]["remove"] == {"members": [], "groups": []}


# --- the older API --------------------------------------------------------

@pytest.fixture
def posted(monkeypatch):
    sent = {}

    class Response:
        status_code = 200
        headers = {"Content-Type": "application/json"}

        @staticmethod
        def json():
            return {"data": {"id": "rptv_1"}}

    def post(url, headers=None, json=None, **kwargs):
        sent.update(url=url, headers=headers, payload=json)
        return Response()

    monkeypatch.setattr(v0.requests, "post", post)
    return sent


def test_the_older_api_syncs_by_command(posted):
    v0.sync_report_view("rptv_1", "token", API_URL="http://api")

    assert posted["payload"] == {"command": "sync_report_view",
                                 "params": {"id": "rptv_1"}}
    assert posted["url"] == "http://api/api"
