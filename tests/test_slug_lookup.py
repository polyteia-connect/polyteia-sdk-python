"""
Tests for resolving an insight by slug.

The lookup used to scan `list_insights`, which returns an incomplete random
subset under load and gives a client no way to tell. A slug that exists could
therefore be reported absent — and a caller that reads "absent" as "not created
yet" then creates a duplicate, or drops the widget from a report. These pin
that the resolution is a single API call and that a miss is a real 404.
"""
import pytest

from polyteia_sdk_python_v2 import api_utils as api
from polyteia_sdk_python_v2 import PolyteiaAPIError


@pytest.fixture
def calls(monkeypatch):
    """Record every rpc_call; answer with whatever the test queued."""
    class Calls(list):
        """The recorded calls, with the answers to give keyed by procedure."""
        answers: dict = {}

    recorded = Calls()
    answers = recorded.answers = {}

    def rpc_call(namespace, procedure, params, access_token=None, API_URL=None,
                 context=None, **kwargs):
        recorded.append({"namespace": namespace, "procedure": procedure,
                         "params": params, "context": context})
        if procedure in answers:
            answer = answers[procedure]
            if isinstance(answer, Exception):
                raise answer
            return answer
        raise PolyteiaAPIError(f"unexpected call to {procedure}", status_code=404)

    monkeypatch.setattr(api, "rpc_call", rpc_call)
    return recorded


def test_the_slug_is_resolved_by_the_api(calls):
    calls.answers["getInsightBySolutionSlug"] = {"id": "ins_1", "slug": "headcount"}

    got = api.get_insight_by_slug("sol_1", "headcount", "token", API_URL="http://api")

    assert got["id"] == "ins_1"


def test_it_does_not_list_the_solutions_insights(calls):
    # The point of the change: listing is what returned incomplete subsets.
    calls.answers["getInsightBySolutionSlug"] = {"id": "ins_1"}

    api.get_insight_by_slug("sol_1", "headcount", "token", API_URL="http://api")

    assert [c["procedure"] for c in calls] == ["getInsightBySolutionSlug"]


def test_the_solution_and_slug_are_sent(calls):
    calls.answers["getInsightBySolutionSlug"] = {"id": "ins_1"}

    api.get_insight_by_slug("sol_1", "headcount", "token", API_URL="http://api")

    assert calls[0]["params"] == {"solutionId": "sol_1", "slug": "headcount"}


def test_an_absent_slug_raises_with_a_status(calls):
    # The scan raised without one, so a caller could not tell a real absence
    # from a transport failure.
    calls.answers["getInsightBySolutionSlug"] = PolyteiaAPIError("not found",
                                                                 status_code=404)

    with pytest.raises(PolyteiaAPIError) as excinfo:
        api.get_insight_by_slug("sol_1", "absent", "token", API_URL="http://api")

    assert excinfo.value.status_code == 404


def test_a_server_failure_is_not_reported_as_absence(calls):
    calls.answers["getInsightBySolutionSlug"] = PolyteiaAPIError("boom",
                                                                 status_code=503)

    with pytest.raises(PolyteiaAPIError) as excinfo:
        api.get_insight_by_slug("sol_1", "headcount", "token", API_URL="http://api")

    assert excinfo.value.status_code == 503


def test_it_matches_the_dataset_lookup_it_mirrors(calls):
    calls.answers["getDatasetBySolutionSlug"] = {"id": "ds_1"}
    calls.answers["getInsightBySolutionSlug"] = {"id": "ins_1"}

    api.get_dataset_by_slug("sol_1", "people", "token", API_URL="http://api")
    api.get_insight_by_slug("sol_1", "headcount", "token", API_URL="http://api")

    assert calls[0]["params"] == calls[1]["params"] | {"slug": "people"}
