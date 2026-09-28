"""
Tests for adding someone to a workspace and listing workspaces as org admin.

The API renamed the add's `kind` to `role` and answered the old shape with a
400 that the provisioning helpers swallowed — the PAK user then never became an
org member and every org-scoped exchange was refused. These pin the shape sent.
"""
import pytest

from polyteia_sdk_python_v2 import api_utils as api
from polyteia_sdk_python_v2 import PolyteiaAPIError


class Calls(list):
    """Every rpc_call made, plus the one answer to give."""
    answer = None


@pytest.fixture
def calls(monkeypatch):
    recorded = Calls()

    def rpc_call(namespace, procedure, params, access_token=None, API_URL=None,
                 context=None, **kwargs):
        recorded.append({"namespace": namespace, "procedure": procedure,
                         "params": params})
        return recorded.answer

    monkeypatch.setattr(api, "rpc_call", rpc_call)
    return recorded


def test_the_add_sends_a_role_not_a_kind(calls):
    calls.answer = {"outcome": "added", "memberId": "mem_1", "joined": True}

    result = api.invite_to_workspace("ws_1", "a@b.de", "tok")

    assert calls[0]["procedure"] == "workspace/inviteToWorkspace"
    assert calls[0]["params"] == {"workspaceId": "ws_1", "email": "a@b.de",
                                  "role": "admin"}
    assert result["memberId"] == "mem_1"


@pytest.mark.parametrize("kind, role", [("workspaceAdmin", "admin"),
                                        ("workspaceMember", "member"),
                                        ("workspaceGuest", "guest")])
def test_the_old_kind_still_works(calls, kind, role):
    api.invite_to_workspace("ws_1", "a@b.de", "tok", kind=kind)

    assert calls[0]["params"]["role"] == role


def test_an_unknown_kind_is_refused_before_calling(calls):
    with pytest.raises(PolyteiaAPIError, match="kind"):
        api.invite_to_workspace("ws_1", "a@b.de", "tok", kind="owner")

    assert calls == []


def test_the_admin_listing_leaves_out_archived_workspaces(calls):
    calls.answer = [{"id": "ws_1", "name": "A", "deletedAt": None},
                    {"id": "ws_2", "name": "A", "deletedAt": "2026-09-28T10:00:00Z"}]

    assert [w["id"] for w in api.list_org_workspaces("org_1", "tok")] == ["ws_1"]
    assert calls[0]["namespace"] == "enterprise"
    assert calls[0]["procedure"] == "workspace/listWorkspacesByOrganization"


def test_the_admin_listing_can_include_archived_workspaces(calls):
    calls.answer = [{"id": "ws_2", "name": "A", "deletedAt": "2026-09-28T10:00:00Z"}]

    assert len(api.list_org_workspaces("org_1", "tok", include_archived=True)) == 1
