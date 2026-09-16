"""
Tests for resolving an insight by slug.

The lookup used to scan `list_insights`, which returns an incomplete random
subset under load and gives a client no way to tell. A slug that exists could
therefore be reported absent — and a caller that reads "absent" as "not created
yet" then creates a duplicate, or drops the widget from a report. These pin
that the resolution is a single API call, that it goes through `getInsight`
(the route every deployed API version serves), and that a miss is a real 404.
"""
import pytest

from polyteia_sdk_python_v2 import api_utils as api
from polyteia_sdk_python_v2 import PolyteiaAPIError


class Calls(list):
    """Every rpc_call made, plus the answers to give, keyed by procedure."""

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
        answer = recorded.answers.get(procedure)
        if answer is None:
            raise PolyteiaAPIError(f"unexpected call to {procedure}", status_code=404)
        if isinstance(answer, Exception):
            raise answer
        return answer

    monkeypatch.setattr(api, "rpc_call", rpc_call)
    return recorded


def test_the_slug_is_resolved_by_the_api(calls):
    calls.answers["getInsight"] = {"id": "ins_1", "slug": "headcount"}

    got = api.get_insight_by_slug("sol_1", "headcount", "token", API_URL="http://api")

    assert got["id"] == "ins_1"


def test_it_does_not_list_the_solutions_insights(calls):
    # The point of the change: listing is what returned incomplete subsets.
    calls.answers["getInsight"] = {"id": "ins_1"}

    api.get_insight_by_slug("sol_1", "headcount", "token", API_URL="http://api")

    assert [c["procedure"] for c in calls] == ["getInsight"]


def test_it_uses_the_slug_form_of_getInsight(calls):
    # getInsight takes {id} or {solutionId, slug}; the slug form is what every
    # deployed API version serves, unlike a dedicated slug procedure.
    calls.answers["getInsight"] = {"id": "ins_1"}

    api.get_insight_by_slug("sol_1", "headcount", "token", API_URL="http://api")

    assert calls[0]["params"] == {"solutionId": "sol_1", "slug": "headcount"}
    assert "id" not in calls[0]["params"]


def test_an_absent_slug_raises_with_a_status(calls):
    # The scan raised without one, so a caller could not tell a real absence
    # from a transport failure.
    calls.answers["getInsight"] = PolyteiaAPIError("not found", status_code=404)

    with pytest.raises(PolyteiaAPIError) as excinfo:
        api.get_insight_by_slug("sol_1", "absent", "token", API_URL="http://api")

    assert excinfo.value.status_code == 404


def test_a_server_failure_is_not_reported_as_absence(calls):
    calls.answers["getInsight"] = PolyteiaAPIError("boom", status_code=503)

    with pytest.raises(PolyteiaAPIError) as excinfo:
        api.get_insight_by_slug("sol_1", "headcount", "token", API_URL="http://api")

    assert excinfo.value.status_code == 503


def test_a_forbidden_insight_is_a_403_not_an_absence(calls):
    # Under the scan an insight the caller could not view was simply missing
    # from the list, so it looked absent and a create followed.
    calls.answers["getInsight"] = PolyteiaAPIError("forbidden", status_code=403)

    with pytest.raises(PolyteiaAPIError) as excinfo:
        api.get_insight_by_slug("sol_1", "headcount", "token", API_URL="http://api")

    assert excinfo.value.status_code == 403
