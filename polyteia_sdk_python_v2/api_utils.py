"""Client functions for the Polyteia API.

Function names and signatures mirror the previous major version of this SDK so
that existing code needs minimal changes. Most functions take ``access_token``
and ``API_URL`` as trailing arguments.

Behavioural differences from the previous major version:
- Single-resource getters return the resource object directly (the previous
  version wrapped it as ``{"data": ...}``).
- Dataset data upload is a single call (``upload_file(dataset_id, table, ...)``)
  rather than a separate token-generation step followed by an upload.
"""

import io
from typing import List, Optional

import pyarrow as pa
import pyarrow.parquet as pq

from ._transport import (
    rpc_call,
    rest_post,
    PolyteiaAPIError,
    DEFAULT_API_URL,
    DEFAULT_TIMEOUT,
)

# ---------------------------------------------------------------------------
# Response helper (compatibility)
# ---------------------------------------------------------------------------


def handle_api_response(
    response,
    *,
    context: str = "API call",
    expected_status_codes: tuple = (200, 201),
    required_keys: Optional[tuple] = None,
) -> dict:
    """Validate an HTTP response and return its parsed JSON body.

    Retained for compatibility with code written against the previous major
    version. New code should rely on the exceptions raised by the client
    functions directly.

    Raises:
        PolyteiaAPIError: on an unexpected status, non-JSON body, or a missing
            required key.
    """
    content_type = response.headers.get("Content-Type", "")
    if "application/json" not in content_type:
        if response.status_code in expected_status_codes:
            return {}
        raise PolyteiaAPIError(
            f"{context} failed (HTTP {response.status_code}):\n{response.text}",
            status_code=response.status_code,
        )

    try:
        json_response = response.json()
    except ValueError:
        raise PolyteiaAPIError(
            f"{context} failed: invalid JSON response:\n{response.text}",
            status_code=response.status_code,
        )

    if response.status_code not in expected_status_codes:
        raise PolyteiaAPIError(
            f"{context} failed (HTTP {response.status_code}):\n{json_response}",
            status_code=response.status_code,
            body=json_response,
        )

    if required_keys:
        current = json_response
        for key in required_keys:
            if not isinstance(current, dict) or key not in current:
                raise PolyteiaAPIError(
                    f"{context} failed: missing key '{key}' in response:\n{json_response}",
                    status_code=response.status_code,
                    body=json_response,
                )
            current = current[key]

    return json_response


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------


def get_org_access_token(org_slug: Optional[str] = None, PAK: str = "", API_URL: str = DEFAULT_API_URL, org_id: Optional[str] = None) -> str:
    """Exchange a personal access key for an organization-scoped session token.

    The organization is identified by its slug. The returned token is scoped to
    that organization and is short-lived; re-exchange when it expires. Send it as
    ``Authorization: Bearer <token>`` on subsequent calls. The exchange itself is
    sent unauthenticated.

    Args:
        org_slug: Slug of the organization to scope the session to. For
            compatibility, the first positional argument (and the ``org_id``
            keyword) are also accepted and treated as the slug.
        PAK: A personal access key.
        API_URL: Base URL of the Polyteia API.
        org_id: Deprecated alias for ``org_slug`` (accepted for compatibility;
            its value is used as the slug).

    Returns:
        The session token.
    """
    slug = org_slug if org_slug is not None else org_id
    if not slug:
        raise PolyteiaAPIError("get_org_access_token requires an organization slug")
    result = rpc_call(
        "auth", "personalAccessKey/exchange",
        {"key": PAK, "organizationSlug": slug},
        access_token=None, API_URL=API_URL, context="Exchange personal access key",
    )
    if not isinstance(result, dict) or "token" not in result:
        raise PolyteiaAPIError("Token exchange returned no token")
    return result["token"]


def get_user_access_token(PAK: str, API_URL: str = DEFAULT_API_URL) -> str:
    """Exchange a PAK for an UNSCOPED (user-level) session token.

    Needed for accepting organization invitations: a token scoped to some other
    org is refused with "Sign out of the current organization before accepting".
    """
    result = rpc_call(
        "auth", "personalAccessKey/exchange", {"key": PAK},
        access_token=None, API_URL=API_URL, context="Exchange personal access key",
    )
    if not isinstance(result, dict) or "token" not in result:
        raise PolyteiaAPIError("Token exchange returned no token")
    return result["token"]


def accept_org_invitation(invitation_id: str, access_token: str,
                          API_URL: str = DEFAULT_API_URL) -> dict:
    """Accept an organization or workspace invitation.

    ``access_token`` must be an unscoped session (see get_user_access_token).
    This is a REST route, not RPC.
    """
    import requests
    from ._transport import _normalize_base_url

    url = f"{_normalize_base_url(API_URL)}/api/auth/organization/invitation/accept"
    try:
        resp = requests.post(
            url, headers={"Authorization": f"Bearer {access_token}",
                          "Content-Type": "application/json"},
            json={"invitationId": invitation_id}, timeout=DEFAULT_TIMEOUT)
    except requests.RequestException as exc:
        raise PolyteiaAPIError(f"Accept invitation failed: {exc}") from exc
    if resp.status_code != 200:
        raise PolyteiaAPIError(
            f"Accept invitation failed (HTTP {resp.status_code}):\n{resp.text}",
            status_code=resp.status_code)
    return resp.json()


def invite_to_organization(org_id: str, email: str, access_token: str,
                           API_URL: str = DEFAULT_API_URL) -> dict:
    """Invite someone to an organization as org-admin (cockpit, system-admin).

    Grants admin only, not membership — membership comes from a workspace
    invitation (see invite_to_workspace).
    """
    return rpc_call(
        "cockpit", "admin/invite", {"organizationId": org_id, "email": email},
        access_token=access_token, API_URL=API_URL, context="Invite to organization",
    )


def invite_to_workspace(workspace_id: str, email: str, access_token: str,
                        kind: str = "workspaceAdmin",
                        API_URL: str = DEFAULT_API_URL) -> dict:
    """Invite someone to a workspace. Accepting this grants org membership."""
    return rpc_call(
        "enterprise", "workspace/inviteToWorkspace",
        {"workspaceId": workspace_id, "email": email, "kind": kind},
        access_token=access_token, API_URL=API_URL, context="Invite to workspace",
    )


def delete_org(org_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Delete an organization (cockpit, system-admin)."""
    return rpc_call(
        "cockpit", "organization/delete", {"organizationId": org_id},
        access_token=access_token, API_URL=API_URL, context="Delete organization",
    )


# ---------------------------------------------------------------------------
# Datasets
# ---------------------------------------------------------------------------


def create_dataset(
    solution_id: str, name: str, description: str, source: str, slug: str,
    access_token: str, documentation: Optional[dict] = None,
    organization_id: Optional[str] = None, API_URL: str = DEFAULT_API_URL,
) -> str:
    """Create a dataset and return its id."""
    params = {
        "name": name,
        "solutionId": solution_id,
        "description": description,
        "source": source,
        "documentation": documentation or {},
    }
    if slug:
        params["slug"] = slug
    # organizationId is required; resolve it from the solution when not given.
    if not organization_id:
        organization_id = get_solution(solution_id, access_token, API_URL)["organizationId"]
    params["organizationId"] = organization_id
    dataset = rpc_call(
        "dataset", "createDataset", params,
        access_token=access_token, API_URL=API_URL, context="Create dataset",
    )
    return dataset["id"]


def update_dataset(ds_id: str, access_token: str, API_URL: str = DEFAULT_API_URL, **kwargs) -> dict:
    """Update a dataset's mutable fields (``name``, ``slug``, ``description``,
    ``source``, ``dataAsOf``, ``documentation``). Returns the updated dataset.

    ``dataAsOf`` is required by the API (nullable); it defaults to ``None`` if
    not supplied. When set, pass an ISO-8601 string.
    """
    params = {"id": ds_id, "dataAsOf": None}
    params.update(kwargs)
    return rpc_call(
        "dataset", "updateDataset", params,
        access_token=access_token, API_URL=API_URL, context="Update dataset",
        date_fields=["dataAsOf"],
    )


def get_dataset_by_id(dataset_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Get a dataset by id."""
    return rpc_call(
        "dataset", "getDatasetById", {"id": dataset_id},
        access_token=access_token, API_URL=API_URL, context="Get dataset by id",
    )


def get_dataset_by_slug(solution_id: str, slug: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Get a dataset by slug within a solution."""
    return rpc_call(
        "dataset", "getDatasetBySolutionSlug", {"solutionId": solution_id, "slug": slug},
        access_token=access_token, API_URL=API_URL, context="Get dataset by slug",
    )


def list_datasets(solution_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> list:
    """List all datasets in a solution."""
    return rpc_call(
        "dataset", "listDatasets", {"solutionId": solution_id},
        access_token=access_token, API_URL=API_URL, context="List datasets",
    )


def get_all_datasets_in_sol(sol_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> List[dict]:
    """Return every dataset object in a solution."""
    return list_datasets(sol_id, access_token, API_URL)


# Map the previous version's generic resource type names to the per-type list
# procedures. The API lists resources per type within a solution.
_RESOURCE_LISTERS = {
    "dataset": ("dataset", "listDatasets"),
    "insight": ("insight", "listInsights"),
    "report": ("report", "listReports"),
    "tag": ("tag", "listTags"),
}


def list_resources(
    container_id: str, access_token: str, ressource_type: str = "dataset",
    API_URL: str = DEFAULT_API_URL, **_ignored,
) -> list:
    """List resources of a given type in a solution.

    ``container_id`` is the solution id. ``ressource_type`` is one of
    ``dataset``, ``insight``, ``report``, ``tag``. Extra keyword arguments from
    the previous version's paginated signature (``page_nr``, ``page_size``,
    ``permission``) are accepted and ignored — the API returns the full list.
    """
    try:
        router, proc = _RESOURCE_LISTERS[ressource_type]
    except KeyError:
        raise PolyteiaAPIError(f"Unsupported resource type: {ressource_type!r}")
    return rpc_call(
        router, proc, {"solutionId": container_id},
        access_token=access_token, API_URL=API_URL, context=f"List {ressource_type}s",
    )


def list_resources_recursive(
    container_id: str, access_token: str, ressource_type: str = "dataset",
    API_URL: str = DEFAULT_API_URL, **_ignored,
) -> List[str]:
    """Return the ids of all resources of a type in a solution.

    The API is not paginated for these lists, so this returns every id in one
    call. Retained for compatibility with the previous version's name.
    """
    resources = list_resources(container_id, access_token, ressource_type, API_URL)
    return [r["id"] for r in resources if isinstance(r, dict) and "id" in r]


def get_or_create_dataset(
    solution_id: str, name: str, description: str, source: str, slug: str,
    access_token: str, documentation: Optional[dict] = None, API_URL: str = DEFAULT_API_URL,
) -> str:
    """Return the id of the dataset with ``slug``, creating it if absent."""
    try:
        return get_dataset_by_slug(solution_id, slug, access_token, API_URL)["id"]
    except PolyteiaAPIError:
        return create_dataset(
            solution_id, name, description, source, slug, access_token,
            documentation=documentation, API_URL=API_URL,
        )


def delete_dataset(ds_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> None:
    """Delete a dataset."""
    rpc_call(
        "dataset", "deleteDataset", {"id": ds_id},
        access_token=access_token, API_URL=API_URL, context="Delete dataset",
    )


def update_dataset_source_timestamp(dataset_id: str, source_timestamp: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Set a dataset's ``dataAsOf`` source timestamp (ISO-8601 string). Returns
    the updated dataset.

    ``updateDataset`` is a full-replacement update: it requires ``name``,
    ``slug``, ``description``, ``source`` and ``documentation`` alongside the
    changed field. This reads the current dataset and re-sends those values.
    """
    # Accept a bare "YYYY-MM-DD" and expand to a full ISO timestamp, which the
    # API's date validator requires.
    parts = source_timestamp.split("-")
    if len(parts) == 3 and all(p.isdigit() for p in parts):
        source_timestamp = source_timestamp + "T00:00:00.000Z"

    current = get_dataset_by_id(dataset_id, access_token, API_URL)
    return update_dataset(
        dataset_id, access_token, API_URL=API_URL,
        name=current["name"], slug=current["slug"],
        description=current.get("description") or "",
        source=current["source"], documentation=current.get("documentation") or {},
        dataAsOf=source_timestamp,
    )


def update_dataset_metadata(ds_id: str, columns: list, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Update a dataset's column metadata. Returns the updated dataset."""
    return rpc_call(
        "dataset", "updateDatasetColumns", {"id": ds_id, "columns": columns},
        access_token=access_token, API_URL=API_URL, context="Update dataset metadata",
    )


def get_dataset_metadata_cols(ds_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> list:
    """Return a dataset's column metadata."""
    return get_dataset_by_id(ds_id, access_token, API_URL).get("columns", [])


# ---------------------------------------------------------------------------
# Dataset data: upload / download
# ---------------------------------------------------------------------------


def upload_file(dataset_id: str, df: pa.Table, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Upload an Arrow table to a dataset as its data.

    NOTE: the previous major version required a separately generated upload
    token as the first argument. The Polyteia API now accepts a direct
    multipart upload, so this takes the ``dataset_id`` instead. The Arrow table
    is written to Parquet in memory and posted as multipart form data.
    """
    buffer = io.BytesIO()
    pq.write_table(df, buffer)
    buffer.seek(0)
    files = [("file", ("data.parquet", buffer, "application/vnd.apache.parquet"))]
    response = rest_post(
        f"/api/upload/dataset/{dataset_id}",
        access_token=access_token, files=files, API_URL=API_URL,
        context="Upload dataset data",
    )
    try:
        return response.json()
    except ValueError:
        return {}


def generate_upload_token(ds_id: str, content_type: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> str:
    """Deprecated. The Polyteia API no longer uses upload tokens; data is
    uploaded directly via :func:`upload_file`. Returns ``ds_id`` so that
    two-step call sites (``token = generate_upload_token(...); upload_file(token, ...)``)
    continue to resolve to the dataset id until they are updated."""
    return ds_id


def generate_download_token(ds_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> str:
    """Return a presigned download URL for a dataset's data (Parquet)."""
    result = rpc_call(
        "dataset", "exportDatasetData", {"id": ds_id, "format": "parquet"},
        access_token=access_token, API_URL=API_URL, context="Export dataset data",
    )
    return result["url"]


def download_file_to_arrow(ds_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> pa.Table:
    """Download a dataset's data and return it as an Arrow table."""
    import requests

    url = generate_download_token(ds_id, access_token, API_URL)
    resp = requests.get(url, timeout=DEFAULT_TIMEOUT)
    if resp.status_code != 200:
        raise PolyteiaAPIError(
            f"Download dataset data failed (HTTP {resp.status_code})",
            status_code=resp.status_code,
        )
    return pq.read_table(io.BytesIO(resp.content))


# ---------------------------------------------------------------------------
# Insights
# ---------------------------------------------------------------------------


def create_insight(insight_body: dict, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Create an insight. ``insight_body`` holds ``name``, ``description``,
    ``query``, ``config``, ``solutionId``, ``organizationId``."""
    return rpc_call(
        "insight", "createInsight", insight_body,
        access_token=access_token, API_URL=API_URL, context="Create insight",
    )


def update_insight(insight_id: str, insight_body: dict, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Update an insight's fields. Returns the updated insight."""
    params = {"id": insight_id, **insight_body}
    return rpc_call(
        "insight", "updateInsight", params,
        access_token=access_token, API_URL=API_URL, context="Update insight",
    )


def get_insight(insight_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Get an insight by id."""
    return rpc_call(
        "insight", "getInsight", {"id": insight_id},
        access_token=access_token, API_URL=API_URL, context="Get insight",
    )


def list_insights(solution_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> list:
    """List all insights in a solution."""
    return rpc_call(
        "insight", "listInsights", {"solutionId": solution_id},
        access_token=access_token, API_URL=API_URL, context="List insights",
    )


def get_insight_by_slug(solution_id: str, slug: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Get an insight by its slug within a solution."""
    for insight in list_insights(solution_id, access_token, API_URL):
        if insight.get("slug") == slug or insight.get("name") == slug:
            return insight
    raise PolyteiaAPIError(f"No insight with slug '{slug}' found in solution")


def delete_insight(insight_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> None:
    """Delete an insight."""
    rpc_call(
        "insight", "deleteInsight", {"id": insight_id},
        access_token=access_token, API_URL=API_URL, context="Delete insight",
    )


# ---------------------------------------------------------------------------
# Reports
# ---------------------------------------------------------------------------


def create_report(report_body: dict, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Create a report. ``report_body`` holds ``name``, ``description``,
    ``solutionId``, ``organizationId``."""
    return rpc_call(
        "report", "createReport", report_body,
        access_token=access_token, API_URL=API_URL, context="Create report",
    )


def update_report(report_id: str, access_token: str, API_URL: str = DEFAULT_API_URL, **kwargs) -> dict:
    """Update a report's fields (``name``, ``description``, ``structure``,
    ``filters``, ``metadata``). Returns the updated report."""
    params = {"id": report_id, **kwargs}
    return rpc_call(
        "report", "updateReport", params,
        access_token=access_token, API_URL=API_URL, context="Update report",
    )


def get_report(report_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Get a report by id."""
    return rpc_call(
        "report", "getReport", {"id": report_id},
        access_token=access_token, API_URL=API_URL, context="Get report",
    )


def list_reports(solution_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> list:
    """List all reports in a solution."""
    return rpc_call(
        "report", "listReports", {"solutionId": solution_id},
        access_token=access_token, API_URL=API_URL, context="List reports",
    )


def delete_report(report_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> None:
    """Delete a report."""
    rpc_call(
        "report", "deleteReport", {"id": report_id},
        access_token=access_token, API_URL=API_URL, context="Delete report",
    )


def extract_insights_from_structure(structure) -> set:
    """Walk a report ``structure`` and return the set of embedded insight ids.

    The structure is a list of page objects; each page holds
    ``content.editorState``, a list of rich-text blocks. Blocks of type
    ``widget`` embed an insight via ``widgetData.insightId``.
    """
    insight_ids: set = set()

    def _walk_blocks(blocks):
        if not isinstance(blocks, list):
            return
        for block in blocks:
            if not isinstance(block, dict):
                continue
            if block.get("type") == "widget":
                insight_id = (block.get("widgetData") or {}).get("insightId")
                if insight_id:
                    insight_ids.add(insight_id)
            _walk_blocks(block.get("children"))

    pages = structure if isinstance(structure, list) else [structure]
    for page in pages:
        if isinstance(page, dict):
            _walk_blocks((page.get("content") or {}).get("editorState"))
    return insight_ids


# ---------------------------------------------------------------------------
# Report images
# ---------------------------------------------------------------------------


def get_image_upload_token(report_id: str, access_token: str, content_type: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Deprecated. Report images are uploaded directly via
    :func:`upload_local_file`; no token step is required. Returns a mapping
    that satisfies the previous two-step call shape (``{"upload_url", "token"}``)
    so existing call sites resolve, with the report id carried through."""
    return {"upload_url": f"/api/report/{report_id}/image", "token": report_id, "report_id": report_id}


def upload_local_file(upload_url: str, upload_token: str, local_path: str, content_type: str, access_token: Optional[str] = None, API_URL: str = DEFAULT_API_URL) -> dict:
    """Upload a local image file to a report.

    ``upload_token`` carries the report id (see :func:`get_image_upload_token`).
    """
    report_id = upload_token
    with open(local_path, "rb") as fh:
        files = [("file", (local_path.split("/")[-1], fh, content_type))]
        response = rest_post(
            f"/api/report/{report_id}/image",
            access_token=access_token, files=files, API_URL=API_URL,
            context="Upload report image",
        )
    try:
        return response.json()
    except ValueError:
        return {}


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------


def create_tag(org_id: Optional[str], name: str, description: str, access_token: str, color: str = "#1F009D", solution_id: Optional[str] = None, API_URL: str = DEFAULT_API_URL) -> str:
    """Create a tag in a solution and return its id.

    Both ``solution_id`` and an organization id are required by the API. If
    ``org_id`` is not given, it is resolved from the solution.
    """
    if not solution_id:
        raise PolyteiaAPIError("create_tag requires a solution_id")
    if not org_id:
        org_id = get_solution(solution_id, access_token, API_URL)["organizationId"]
    params = {"name": name, "description": description, "color": color,
              "organizationId": org_id, "solutionId": solution_id}
    tag = rpc_call(
        "tag", "createTag", params,
        access_token=access_token, API_URL=API_URL, context="Create tag",
    )
    return tag["id"]


def get_tag_by_id(tag_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Get a tag by id."""
    return rpc_call(
        "tag", "getTag", {"id": tag_id},
        access_token=access_token, API_URL=API_URL, context="Get tag",
    )


def list_tags(solution_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> list:
    """List all tags in a solution."""
    return rpc_call(
        "tag", "listTags", {"solutionId": solution_id},
        access_token=access_token, API_URL=API_URL, context="List tags",
    )


def delete_tag(tag_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> None:
    """Delete a tag."""
    rpc_call(
        "tag", "deleteTag", {"id": tag_id},
        access_token=access_token, API_URL=API_URL, context="Delete tag",
    )


def add_tag_to_ressource(tag_id: str, ressource_id: str, access_token: str, solution_id: Optional[str] = None, API_URL: str = DEFAULT_API_URL) -> dict:
    """Assign a tag to a resource."""
    params = {"tagId": tag_id, "resourceId": ressource_id}
    if solution_id:
        params["solutionId"] = solution_id
    return rpc_call(
        "tag", "assignTag", params,
        access_token=access_token, API_URL=API_URL, context="Assign tag",
    )


def remove_tag_from_ressource(tag_id: str, ressource_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> None:
    """Remove a tag assignment from a resource."""
    rpc_call(
        "tag", "removeTagAssignment", {"tagId": tag_id, "resourceId": ressource_id},
        access_token=access_token, API_URL=API_URL, context="Remove tag assignment",
    )


def list_tags_for_resource(ressource_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> list:
    """List the tags assigned to a resource."""
    return rpc_call(
        "tag", "listTagsForResource", {"resourceId": ressource_id},
        access_token=access_token, API_URL=API_URL, context="List tags for resource",
    )


# ---------------------------------------------------------------------------
# Organizations, workspaces, solutions, groups
# ---------------------------------------------------------------------------


def get_organisation(org_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Get an organization by id."""
    return rpc_call(
        "organization", "getOrganizationById", {"id": org_id},
        access_token=access_token, API_URL=API_URL, context="Get organization",
    )


def list_my_organizations(access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Return the current user's organizations (memberOf/adminOf) and identity."""
    return rpc_call(
        "auth", "listMyOrganizations", {},
        access_token=access_token, API_URL=API_URL, context="List my organizations",
    )


def get_my_member_id(org_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> Optional[str]:
    """Return the current user's organization member id for ``org_id`` (or None)."""
    me = list_my_organizations(access_token, API_URL)
    for o in me.get("memberOf", []):
        if o.get("id") == org_id:
            return o.get("memberId")
    return None


def list_org_members(org_id: str, access_token: str, include_deactivated: bool = False, API_URL: str = DEFAULT_API_URL) -> list:
    """List an organization's members. Requires an organization-admin session.

    Each row is ``{id, role, createdAt, deactivatedAt, licensedAt, user}`` where
    ``user`` carries ``{id, name, email, ...}`` and ``id`` is the member id that
    membership calls take.

    NOTE: only rows whose role is ``member`` or ``guest`` are returned —
    organization ADMINS are excluded. A user who holds only an admin row does not
    appear here, and has no member id to grant workspace or solution access with.
    """
    return rpc_call(
        "auth", "organizationAdmin/listMembers",
        {"organizationId": org_id, "includeDeactivated": include_deactivated},
        access_token=access_token, API_URL=API_URL, context="List organization members",
    )


def find_member_id_by_email(org_id: str, email: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> Optional[str]:
    """The organization member id whose user has ``email``, or ``None``.

    Compared case-insensitively. Searches only members and guests, so see the
    caveat on ``list_org_members`` about organization admins.
    """
    wanted = (email or "").strip().lower()
    for member in list_org_members(org_id, access_token, API_URL=API_URL) or []:
        user = member.get("user") or {}
        if (user.get("email") or "").strip().lower() == wanted:
            return member.get("id")
    return None


def create_workspace(org_id: str, name: str, description: str, access_token: str, add_self_as_admin: bool = True, API_URL: str = DEFAULT_API_URL) -> str:
    """Create a workspace and return its id.

    Workspace creation lives on the enterprise surface and requires org-admin.
    A newly created workspace has no members, so creating solutions/groups in it
    would be denied. By default this also makes the caller a workspace admin
    (via the enterprise admin surface, which an org-admin may call) so that
    downstream solution/group creation works. Pass ``add_self_as_admin=False``
    to skip that step.
    """
    ws = rpc_call(
        "enterprise", "workspace/createWorkspace",
        {"organizationId": org_id, "name": name, "description": description},
        access_token=access_token, API_URL=API_URL, context="Create workspace",
    )
    ws_id = ws["id"]
    if add_self_as_admin:
        member_id = get_my_member_id(org_id, access_token, API_URL)
        if member_id:
            add_workspace_admin(ws_id, member_id, access_token, API_URL=API_URL)
    return ws_id


def add_workspace_admin(ws_id: str, member_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Make an organization member a workspace admin (enterprise surface;
    callable by an org-admin)."""
    return rpc_call(
        "enterprise", "workspace/addWorkspaceAdmin",
        {"workspaceId": ws_id, "memberId": member_id},
        access_token=access_token, API_URL=API_URL, context="Add workspace admin",
    )


def list_workspaces(org_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> list:
    """List all workspaces in an organization."""
    return rpc_call(
        "workspace", "listWorkspacesByOrganization", {"organizationId": org_id},
        access_token=access_token, API_URL=API_URL, context="List workspaces",
    )


def create_solution(workspace_id: str, name: str, description: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> str:
    """Create a solution and return its id."""
    sol = rpc_call(
        "solution", "createSolution",
        {"workspaceId": workspace_id, "name": name, "description": description},
        access_token=access_token, API_URL=API_URL, context="Create solution",
    )
    return sol["id"]


def get_solution(solution_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Get a solution by id."""
    return rpc_call(
        "solution", "getSolutionById", {"id": solution_id},
        access_token=access_token, API_URL=API_URL, context="Get solution",
    )


def list_solutions(workspace_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> list:
    """List all solutions in a workspace."""
    return rpc_call(
        "solution", "listSolutionsByWorkspace", {"workspaceId": workspace_id},
        access_token=access_token, API_URL=API_URL, context="List solutions",
    )


def update_solution_doc(solution_id: str, access_token: str, doc: dict, API_URL: str = DEFAULT_API_URL) -> dict:
    """Update a solution's documentation. Returns the updated solution."""
    return rpc_call(
        "solution", "updateSolutionDocumentation", {"id": solution_id, "documentation": doc},
        access_token=access_token, API_URL=API_URL, context="Update solution documentation",
    )


def delete_solution(solution_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> None:
    """Archive (delete) a solution."""
    rpc_call(
        "solution", "deleteSolution", {"id": solution_id},
        access_token=access_token, API_URL=API_URL, context="Delete solution",
    )


# ---------------------------------------------------------------------------
# Public properties
#
# Workspaces, solutions and groups each carry a ``publicProperties`` bag: an
# arbitrary key-value mapping (values may be nested JSON) exposed to the
# platform's expression-evaluation context.
#
# All three setters REPLACE the bag wholesale rather than merging into it, so a
# caller that wants to change one key must send every key it wishes to keep. To
# amend rather than define, read the current bag first: it is returned on the
# resource itself by ``get_solution``, ``list_workspaces`` and ``list_groups``.
#
# Permission: the same "edit details" right as the resource's other edits (for
# a workspace or solution, workspace-admin).
# ---------------------------------------------------------------------------


def set_workspace_public_properties(
    workspace_id: str,
    public_properties: dict,
    access_token: str,
    API_URL: str = DEFAULT_API_URL,
) -> dict:
    """Replace a workspace's public properties. Returns the updated workspace.

    ``public_properties`` replaces the stored bag in full; keys absent from it
    are removed.
    """
    return rpc_call(
        "workspace", "setWorkspacePublicProperties",
        {"id": workspace_id, "publicProperties": public_properties},
        access_token=access_token, API_URL=API_URL,
        context="Set workspace public properties",
    )


def set_solution_public_properties(
    solution_id: str,
    public_properties: dict,
    access_token: str,
    API_URL: str = DEFAULT_API_URL,
) -> dict:
    """Replace a solution's public properties. Returns the updated solution.

    ``public_properties`` replaces the stored bag in full; keys absent from it
    are removed.
    """
    return rpc_call(
        "solution", "setSolutionPublicProperties",
        {"id": solution_id, "publicProperties": public_properties},
        access_token=access_token, API_URL=API_URL,
        context="Set solution public properties",
    )


def set_group_public_properties(
    group_id: str,
    public_properties: dict,
    access_token: str,
    API_URL: str = DEFAULT_API_URL,
) -> dict:
    """Replace a group's public properties. Returns the updated group.

    ``public_properties`` replaces the stored bag in full; keys absent from it
    are removed.
    """
    return rpc_call(
        "group", "setGroupPublicProperties",
        {"id": group_id, "publicProperties": public_properties},
        access_token=access_token, API_URL=API_URL,
        context="Set group public properties",
    )


def create_group(workspace_id: str, name: str, description: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> str:
    """Create a group and return its id."""
    group = rpc_call(
        "group", "createGroup",
        {"workspaceId": workspace_id, "name": name, "description": description},
        access_token=access_token, API_URL=API_URL, context="Create group",
    )
    return group["id"]


def list_groups(workspace_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> list:
    """List all groups in a workspace."""
    return rpc_call(
        "group", "listGroupsByWorkspace", {"workspaceId": workspace_id},
        access_token=access_token, API_URL=API_URL, context="List groups",
    )


def add_user_to_group(group_id: str, member_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Add a member to a group."""
    return rpc_call(
        "group", "addGroupMember", {"groupId": group_id, "memberId": member_id},
        access_token=access_token, API_URL=API_URL, context="Add group member",
    )


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------


def execute_sql(
    sql: str, datasets: Optional[List] = None, access_token: str = "",
    API_URL: str = DEFAULT_API_URL, args: Optional[List] = None,
    named_args: Optional[dict] = None, timeout: int = DEFAULT_TIMEOUT,
    solution_id: Optional[str] = None,
) -> pa.Table:
    """Run an ad-hoc SQL query and return the result as an Arrow table.

    Reference datasets by bare id (``FROM 'ds_abc'``), not with the ``{{...}}``
    braces used in insight SQL. ``datasets`` is ignored, kept for compatibility.
    """
    import requests
    from ._transport import _normalize_base_url

    if not solution_id:
        raise PolyteiaAPIError("execute_sql requires solution_id")

    body = {"query": sql, "solution_id": solution_id}
    if args is not None:
        body["args"] = args
    if named_args is not None:
        body["named_args"] = named_args

    url = f"{_normalize_base_url(API_URL)}/api/query"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    try:
        resp = requests.post(url, headers=headers, json=body, timeout=timeout)
    except requests.RequestException as exc:
        raise PolyteiaAPIError(f"Execute SQL failed: request error: {exc}") from exc
    if resp.status_code != 200:
        raise PolyteiaAPIError(
            f"Execute SQL failed (HTTP {resp.status_code}):\n{resp.text}",
            status_code=resp.status_code,
        )
    reader = pa.ipc.open_stream(io.BytesIO(resp.content))
    return reader.read_all()


# ---------------------------------------------------------------------------
# Report views
# ---------------------------------------------------------------------------


def get_report_view(report_view_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Get a report view by id."""
    return rpc_call(
        "reportView", "getReportView", {"id": report_view_id},
        access_token=access_token, API_URL=API_URL, context="Get report view",
    )


def list_report_views(report_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> list:
    """List the report views created from a report."""
    return rpc_call(
        "reportView", "listReportViews", {"reportId": report_id},
        access_token=access_token, API_URL=API_URL, context="List report views",
    )


def create_report_view(report_id: str, name: str, selection: dict, access_token: str, description: Optional[str] = None, API_URL: str = DEFAULT_API_URL) -> dict:
    """Create a report view over a selection of a report's pages/groups/sections.

    ``selection`` is ``{"pageIds": [...], "groupIds": [...], "sectionIds": [...]}``.
    """
    params = {"reportId": report_id, "name": name, "selection": selection}
    if description is not None:
        params["description"] = description
    return rpc_call(
        "reportView", "createReportView", params,
        access_token=access_token, API_URL=API_URL, context="Create report view",
    )


def delete_report_view(report_view_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> None:
    """Delete a report view."""
    rpc_call(
        "reportView", "deleteReportView", {"id": report_view_id},
        access_token=access_token, API_URL=API_URL, context="Delete report view",
    )


# ---------------------------------------------------------------------------
# Organizations: lifecycle & settings
# ---------------------------------------------------------------------------


def get_public_organisation(org_slug: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> Optional[dict]:
    """Get the public representation of an organization by slug (or ``None``)."""
    return rpc_call(
        "organization", "getPublicOrganization", {"organizationSlug": org_slug},
        access_token=access_token, API_URL=API_URL, context="Get public organization",
    )


def create_org(name: str, description: str, slug: str, access_token: str, no_seats: int = 0, enabled_dpa: bool = True, API_URL: str = DEFAULT_API_URL) -> str:
    """Create an organization and return its id.

    NOTE: organization creation is a system-admin operation. ``access_token``
    must be a session for a system administrator; an ordinary org-scoped token
    (from a personal access key) will be rejected. ``description``/``enabled_dpa``
    are accepted for signature compatibility; the API takes ``fullName`` and a
    ``licenses`` seat count.
    """
    org = rpc_call(
        "cockpit", "organization/create",
        {"slug": slug, "name": name, "fullName": description or "", "licenses": no_seats},
        access_token=access_token, API_URL=API_URL, context="Create organization",
    )
    return org["id"]


def invite_user_to_org(org_id: str, access_token: str, email: str, role: str = "member", API_URL: str = DEFAULT_API_URL) -> dict:
    """Invite a user to an organization as a member.

    Uses the organization-admin surface (``access_token`` must be an admin
    session for the organization). ``role`` is accepted for compatibility;
    invitations are created with the member role.
    """
    return rpc_call(
        "auth", "organizationAdmin/inviteMember",
        {"organizationId": org_id, "email": email},
        access_token=access_token, API_URL=API_URL, context="Invite user to organization",
    )


def create_group(workspace_id: str, name: str, description: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> str:
    """Create a group in a workspace and return its id.

    NOTE: groups are workspace-scoped. The first argument is a workspace id
    (the previous major version took an organization id here).
    """
    group = rpc_call(
        "group", "createGroup",
        {"workspaceId": workspace_id, "name": name, "description": description},
        access_token=access_token, API_URL=API_URL, context="Create group",
    )
    return group["id"]


def add_group_to_workspace(ws_id: str, group_id: str, access_token: str, role: str = "member", API_URL: str = DEFAULT_API_URL) -> None:
    """No-op: groups are created within a workspace and belong to it by
    construction, so there is no separate "add group to workspace" step. Kept so
    existing setup code that calls it continues to run. ``role`` is ignored.
    """
    return None


# ---------------------------------------------------------------------------
# Membership & roles
#
# Membership is managed per resource type. A "member id" identifies an
# organization member (resolve it from the org's member list). Workspaces and
# groups take a single member; solution access distinguishes owners from
# collaborators (members + groups).
# ---------------------------------------------------------------------------


def add_user_to_workspace(workspace_id: str, member_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Add a member to a workspace."""
    return rpc_call(
        "workspace", "addWorkspaceMember", {"workspaceId": workspace_id, "memberId": member_id},
        access_token=access_token, API_URL=API_URL, context="Add workspace member",
    )


def remove_user_from_workspace(workspace_id: str, member_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Remove a member from a workspace."""
    return rpc_call(
        "workspace", "removeWorkspaceMember", {"workspaceId": workspace_id, "memberId": member_id},
        access_token=access_token, API_URL=API_URL, context="Remove workspace member",
    )


def list_workspace_members(workspace_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> list:
    """List a workspace's members."""
    return rpc_call(
        "workspace", "listWorkspaceMembers", {"id": workspace_id},
        access_token=access_token, API_URL=API_URL, context="List workspace members",
    )


def add_user_to_solution(solution_id: str, member_id: str, access_token: str, as_owner: bool = False, API_URL: str = DEFAULT_API_URL) -> dict:
    """Grant a member access to a solution (as collaborator by default, or owner)."""
    if as_owner:
        return rpc_call(
            "solution", "manageSolutionOwners",
            {"solutionId": solution_id, "add": [member_id], "remove": []},
            access_token=access_token, API_URL=API_URL, context="Add solution owner",
        )
    return rpc_call(
        "solution", "manageSolutionCollaborators",
        {"solutionId": solution_id, "add": {"members": [member_id], "groups": []},
         "remove": {"members": [], "groups": []}},
        access_token=access_token, API_URL=API_URL, context="Add solution collaborator",
    )


def remove_user_from_solution(solution_id: str, member_id: str, access_token: str, as_owner: bool = False, API_URL: str = DEFAULT_API_URL) -> dict:
    """Revoke a member's access to a solution."""
    if as_owner:
        return rpc_call(
            "solution", "manageSolutionOwners",
            {"solutionId": solution_id, "add": [], "remove": [member_id]},
            access_token=access_token, API_URL=API_URL, context="Remove solution owner",
        )
    return rpc_call(
        "solution", "manageSolutionCollaborators",
        {"solutionId": solution_id, "add": {"members": [], "groups": []},
         "remove": {"members": [member_id], "groups": []}},
        access_token=access_token, API_URL=API_URL, context="Remove solution collaborator",
    )


def add_group_to_solution(sol_id: str, group_id: str, access_token: str, role: str = "member", API_URL: str = DEFAULT_API_URL) -> dict:
    """Grant a group collaborator access to a solution. ``role`` is accepted for
    signature compatibility; solution collaborators have a single access level."""
    return rpc_call(
        "solution", "manageSolutionCollaborators",
        {"solutionId": sol_id, "add": {"members": [], "groups": [group_id]},
         "remove": {"members": [], "groups": []}},
        access_token=access_token, API_URL=API_URL, context="Add solution collaborator group",
    )


def remove_group_from_solution(sol_id: str, group_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Revoke a group's collaborator access to a solution."""
    return rpc_call(
        "solution", "manageSolutionCollaborators",
        {"solutionId": sol_id, "add": {"members": [], "groups": []},
         "remove": {"members": [], "groups": [group_id]}},
        access_token=access_token, API_URL=API_URL, context="Remove solution collaborator group",
    )


def remove_group_member(group_id: str, member_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Remove a member from a group."""
    return rpc_call(
        "group", "removeGroupMember", {"groupId": group_id, "memberId": member_id},
        access_token=access_token, API_URL=API_URL, context="Remove group member",
    )


def list_group_members(group_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> list:
    """List a group's members."""
    return rpc_call(
        "group", "listGroupMembers", {"id": group_id},
        access_token=access_token, API_URL=API_URL, context="List group members",
    )


def delete_group(group_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> None:
    """Delete a group."""
    rpc_call(
        "group", "deleteGroup", {"id": group_id},
        access_token=access_token, API_URL=API_URL, context="Delete group",
    )


# ---------------------------------------------------------------------------
# Permission checks
# ---------------------------------------------------------------------------


def check_permission(permission: str, resource_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Check whether the current session has ``permission`` on a resource.

    ``permission`` is a resource-specific access relation, e.g.
    ``"can_view_solution"``, ``"can_edit_solution_details"``,
    ``"can_create_dataset"`` for a solution, or ``"can_view_workspace"`` for a
    workspace. The relation must be valid for the resource's type.

    Returns ``{"allowed": bool, "permission": ..., "resourceId": ...}``.
    """
    return rpc_call(
        "permission", "checkPermission", {"permission": permission, "resourceId": resource_id},
        access_token=access_token, API_URL=API_URL, context="Check permission",
    )


def check_permission_bulk(permissions: List[dict], access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Check several permissions at once. Each entry is
    ``{"permission": ..., "resourceId": ...}``."""
    return rpc_call(
        "permission", "checkPermissionBulk", {"permissions": permissions},
        access_token=access_token, API_URL=API_URL, context="Check permission (bulk)",
    )


# ---------------------------------------------------------------------------
# Solution access
# ---------------------------------------------------------------------------


def list_solution_access(solution_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """List a solution's owners and collaborators (members + groups)."""
    return rpc_call(
        "solution", "listSolutionAccess", {"id": solution_id},
        access_token=access_token, API_URL=API_URL, context="List solution access",
    )


# ---------------------------------------------------------------------------
# Datasets: stats, duplicate, export, prune, clear
# ---------------------------------------------------------------------------


def get_dataset_stats(ds_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Return a dataset's row count and column physical types."""
    return rpc_call(
        "dataset", "getDatasetStats", {"id": ds_id},
        access_token=access_token, API_URL=API_URL, context="Get dataset stats",
    )


def duplicate_dataset(ds_id: str, name: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Clone a dataset (including its data) under a new name."""
    return rpc_call(
        "dataset", "duplicateDataset", {"id": ds_id, "name": name},
        access_token=access_token, API_URL=API_URL, context="Duplicate dataset",
    )


def export_dataset_data(ds_id: str, access_token: str, format: str = "parquet", API_URL: str = DEFAULT_API_URL) -> str:
    """Export a dataset's data and return a presigned download URL.

    ``format`` is one of ``csv``, ``json``, ``parquet``, ``xlsx``.
    """
    result = rpc_call(
        "dataset", "exportDatasetData", {"id": ds_id, "format": format},
        access_token=access_token, API_URL=API_URL, context="Export dataset data",
    )
    return result["url"]


def schedule_dataset_prune(ds_id: str, schedule: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Set a recurring auto-clear schedule for a dataset.

    ``schedule`` is one of ``daily``, ``weekly``, ``monthly``.
    """
    return rpc_call(
        "dataset", "scheduleDatasetPrune", {"id": ds_id, "schedule": schedule},
        access_token=access_token, API_URL=API_URL, context="Schedule dataset prune",
    )


def remove_dataset_prune_schedule(ds_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Remove a dataset's auto-clear schedule."""
    return rpc_call(
        "dataset", "removeDatasetPruneSchedule", {"id": ds_id},
        access_token=access_token, API_URL=API_URL, context="Remove dataset prune schedule",
    )


def delete_dataset_data(ds_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Clear a dataset's data (keeps the dataset). Note: the input key is
    ``datasetId``."""
    return rpc_call(
        "dataset", "deleteDatasetData", {"datasetId": ds_id},
        access_token=access_token, API_URL=API_URL, context="Delete dataset data",
    )


def set_dataset_lock(ds_id: str, locked: bool, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Lock or unlock a dataset."""
    return rpc_call(
        "dataset", "setDatasetLock", {"id": ds_id, "locked": locked},
        access_token=access_token, API_URL=API_URL, context="Set dataset lock",
    )


# ---------------------------------------------------------------------------
# Insight / report data fetching
# ---------------------------------------------------------------------------


def get_data_for_insight(insight_id: str, access_token: str, filters: Optional[dict] = None, API_URL: str = DEFAULT_API_URL) -> dict:
    """Run a saved insight's stored query server-side and return ``{columns, rows}``.

    ``filters`` is an optional mapping keyed by the insight's variable names.
    """
    params = {"id": insight_id}
    if filters is not None:
        params["filters"] = filters
    return rpc_call(
        "insight", "getDataForInsight", params,
        access_token=access_token, API_URL=API_URL, context="Get data for insight",
    )


def set_insight_lock(insight_id: str, locked: bool, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Lock or unlock an insight."""
    return rpc_call(
        "insight", "setInsightLock", {"id": insight_id, "locked": locked},
        access_token=access_token, API_URL=API_URL, context="Set insight lock",
    )


def get_report_insight_data(report_id: str, insight_id: str, access_token: str, selections: Optional[dict] = None, API_URL: str = DEFAULT_API_URL) -> dict:
    """Fetch an embedded insight's result within a report, returning
    ``{columns, rows}``. ``selections`` maps filter ids to selected values."""
    params = {"reportId": report_id, "insightId": insight_id}
    if selections is not None:
        params["selections"] = selections
    return rpc_call(
        "report", "getInsightData", params,
        access_token=access_token, API_URL=API_URL, context="Get report insight data",
    )


def set_report_lock(report_id: str, locked: bool, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Lock or unlock a report."""
    return rpc_call(
        "report", "setReportLock", {"id": report_id, "locked": locked},
        access_token=access_token, API_URL=API_URL, context="Set report lock",
    )


def duplicate_report(report_id: str, name: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Duplicate a report under a new name."""
    return rpc_call(
        "report", "duplicateReport", {"id": report_id, "name": name},
        access_token=access_token, API_URL=API_URL, context="Duplicate report",
    )


# ---------------------------------------------------------------------------
# Report view: update, sync, sharing
# ---------------------------------------------------------------------------


def update_report_view(report_view_id: str, access_token: str, API_URL: str = DEFAULT_API_URL, **kwargs) -> dict:
    """Update a report view's ``name``, ``description``, and/or ``selection``.

    ``selection`` is ``{"pageIds": [...], "groupIds": [...], "sectionIds": [...]}``.
    """
    params = {"id": report_view_id, **kwargs}
    return rpc_call(
        "reportView", "updateReportView", params,
        access_token=access_token, API_URL=API_URL, context="Update report view",
    )


def sync_report_view(report_view_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Re-publish a report view (rebuild its snapshot from the current report)."""
    return rpc_call(
        "reportView", "syncReportView", {"id": report_view_id},
        access_token=access_token, API_URL=API_URL, context="Sync report view",
    )


def set_report_view_public_access(report_view_id: str, visibility: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Set a report view's visibility. ``visibility`` is ``internal`` or ``public``."""
    return rpc_call(
        "reportView", "setReportViewPublicAccess", {"id": report_view_id, "visibility": visibility},
        access_token=access_token, API_URL=API_URL, context="Set report view public access",
    )


def list_report_view_viewers(report_view_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """List the members and groups granted viewer access to a report view."""
    return rpc_call(
        "reportView", "listReportViewViewers", {"reportViewId": report_view_id},
        access_token=access_token, API_URL=API_URL, context="List report view viewers",
    )


def manage_report_view_viewers(
    report_view_id: str, access_token: str,
    add_members: Optional[List[str]] = None, add_groups: Optional[List[str]] = None,
    remove_members: Optional[List[str]] = None, remove_groups: Optional[List[str]] = None,
    API_URL: str = DEFAULT_API_URL,
) -> dict:
    """Add and/or remove report-view viewers (members and groups)."""
    return rpc_call(
        "reportView", "manageReportViewViewers",
        {
            "reportViewId": report_view_id,
            "add": {"members": add_members or [], "groups": add_groups or []},
            "remove": {"members": remove_members or [], "groups": remove_groups or []},
        },
        access_token=access_token, API_URL=API_URL, context="Manage report view viewers",
    )


# ---------------------------------------------------------------------------
# Member permission checks (organization-member scope)
# ---------------------------------------------------------------------------


def check_member_permission(permission: str, resource_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Check a permission for the current organization member (see
    :func:`check_permission` for how ``permission`` relations work)."""
    return rpc_call(
        "permission", "checkMemberPermission", {"permission": permission, "resourceId": resource_id},
        access_token=access_token, API_URL=API_URL, context="Check member permission",
    )


def check_member_permission_bulk(permissions: List[dict], access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Batch permission check for the current organization member. Each entry is
    ``{"permission": ..., "resourceId": ...}``."""
    return rpc_call(
        "permission", "checkMemberPermissionBulk", {"permissions": permissions},
        access_token=access_token, API_URL=API_URL, context="Check member permission (bulk)",
    )


# ---------------------------------------------------------------------------
# Forms
#
# A "form" is authored (draft), then published to create a "released form" that
# submitters fill in. Submissions are made against a released form.
# ---------------------------------------------------------------------------


def create_form(
    solution_id: str, organization_id: str, name: str, access_token: str,
    description: Optional[str] = None, draft_schema: Optional[dict] = None,
    attributes: Optional[dict] = None, documentation: Optional[dict] = None,
    metadata: Optional[dict] = None, API_URL: str = DEFAULT_API_URL,
) -> dict:
    """Create a form (draft)."""
    params = {
        "solutionId": solution_id,
        "organizationId": organization_id,
        "name": name,
        "description": description,
        "draftSchema": draft_schema,
        "attributes": attributes or {},
        "documentation": documentation or {},
    }
    if metadata is not None:
        params["metadata"] = metadata
    return rpc_call(
        "form", "createForm", params,
        access_token=access_token, API_URL=API_URL, context="Create form",
    )


def update_form(form_id: str, name: str, access_token: str, API_URL: str = DEFAULT_API_URL, **kwargs) -> dict:
    """Update a form. Requires ``name``; other fields (``description``,
    ``draftSchema``, ``attributes``, ``documentation``, ``metadata``) may be
    passed as keyword arguments."""
    params = {
        "id": form_id, "name": name,
        "description": kwargs.get("description"),
        "draftSchema": kwargs.get("draftSchema"),
        "attributes": kwargs.get("attributes", {}),
        "documentation": kwargs.get("documentation", {}),
    }
    if "metadata" in kwargs:
        params["metadata"] = kwargs["metadata"]
    return rpc_call(
        "form", "updateForm", params,
        access_token=access_token, API_URL=API_URL, context="Update form",
    )


def list_forms(solution_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> list:
    """List all forms in a solution."""
    return rpc_call(
        "form", "listForms", {"solutionId": solution_id},
        access_token=access_token, API_URL=API_URL, context="List forms",
    )


def get_form(form_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Get a form by id."""
    return rpc_call(
        "form", "getForm", {"id": form_id},
        access_token=access_token, API_URL=API_URL, context="Get form",
    )


def delete_form(form_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> None:
    """Delete a form."""
    rpc_call(
        "form", "deleteForm", {"id": form_id},
        access_token=access_token, API_URL=API_URL, context="Delete form",
    )


def publish_form(form_id: str, schema_version_name: str, access_token: str, schema_version_description: Optional[str] = None, API_URL: str = DEFAULT_API_URL) -> dict:
    """Publish a form, creating a released form and a schema version."""
    params = {"id": form_id, "schemaVersionName": schema_version_name}
    if schema_version_description is not None:
        params["schemaVersionDescription"] = schema_version_description
    return rpc_call(
        "form", "publishForm", params,
        access_token=access_token, API_URL=API_URL, context="Publish form",
    )


def attach_dataset_to_form(form_id: str, dataset_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Attach a dataset to a form as its submission target."""
    return rpc_call(
        "form", "attachDataset", {"formId": form_id, "datasetId": dataset_id},
        access_token=access_token, API_URL=API_URL, context="Attach dataset to form",
    )


def get_latest_released_form(form_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Get the latest released form for a form."""
    return rpc_call(
        "form", "getLatestReleasedForm", {"formId": form_id},
        access_token=access_token, API_URL=API_URL, context="Get latest released form",
    )


def get_released_form(released_form_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Get a released form by id."""
    return rpc_call(
        "form", "getReleasedForm", {"releasedFormId": released_form_id},
        access_token=access_token, API_URL=API_URL, context="Get released form",
    )


def manage_form_submitters(
    released_form_id: str, access_token: str,
    add_members: Optional[List[str]] = None, add_emails: Optional[List[str]] = None, add_groups: Optional[List[str]] = None,
    remove_members: Optional[List[str]] = None, remove_emails: Optional[List[str]] = None, remove_groups: Optional[List[str]] = None,
    API_URL: str = DEFAULT_API_URL,
) -> dict:
    """Add and/or remove submitters (members, emails, groups) on a released form."""
    return rpc_call(
        "form", "manageSubmitters",
        {
            "releasedFormId": released_form_id,
            "add": {"members": add_members or [], "emails": add_emails or [], "groups": add_groups or []},
            "remove": {"members": remove_members or [], "emails": remove_emails or [], "groups": remove_groups or []},
        },
        access_token=access_token, API_URL=API_URL, context="Manage form submitters",
    )


def list_form_submitters(released_form_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """List a released form's submitters."""
    return rpc_call(
        "form", "listSubmitters", {"releasedFormId": released_form_id},
        access_token=access_token, API_URL=API_URL, context="List form submitters",
    )


def get_submission(submission_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Get a form submission by id."""
    return rpc_call(
        "form", "getSubmission", {"submissionId": submission_id},
        access_token=access_token, API_URL=API_URL, context="Get submission",
    )


def get_submission_asset_url(submission_id: str, path: str, access_token: str, download: bool = False, API_URL: str = DEFAULT_API_URL) -> dict:
    """Get a presigned URL for a file uploaded in a submission."""
    return rpc_call(
        "form", "getSubmissionAssetUrl", {"submissionId": submission_id, "path": path, "download": download},
        access_token=access_token, API_URL=API_URL, context="Get submission asset URL",
    )


# ---------------------------------------------------------------------------
# DPA (Verzeichnis von Verarbeitungstätigkeiten)
# ---------------------------------------------------------------------------


def list_dpa_activities(solution_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> list:
    """List the processing activities recorded for a solution."""
    return rpc_call(
        "dpa", "listActivities", {"solutionId": solution_id},
        access_token=access_token, API_URL=API_URL, context="List DPA activities",
    )


def create_dpa_activity(
    solution_id: str, data_type: str, person_group: str, purpose: str,
    access_token: str, safety_measures: Optional[str] = None,
    data_category: str = "none", API_URL: str = DEFAULT_API_URL,
) -> dict:
    """``data_category``: "none" | "art9" | "art10" | "both"."""
    return rpc_call(
        "dpa", "createActivity",
        {"solutionId": solution_id, "dataType": data_type,
         "personGroup": person_group, "purpose": purpose,
         "safetyMeasures": safety_measures, "dataCategory": data_category},
        access_token=access_token, API_URL=API_URL, context="Create DPA activity",
    )


def update_dpa_activity(
    solution_id: str, activity_id: str, data_type: str, person_group: str,
    purpose: str, access_token: str, safety_measures: Optional[str] = None,
    data_category: str = "none", API_URL: str = DEFAULT_API_URL,
) -> dict:
    return rpc_call(
        "dpa", "updateActivity",
        {"solutionId": solution_id, "id": activity_id, "dataType": data_type,
         "personGroup": person_group, "purpose": purpose,
         "safetyMeasures": safety_measures, "dataCategory": data_category},
        access_token=access_token, API_URL=API_URL, context="Update DPA activity",
    )


def delete_dpa_activity(solution_id: str, activity_id: str, access_token: str,
                        API_URL: str = DEFAULT_API_URL) -> dict:
    return rpc_call(
        "dpa", "deleteActivity", {"solutionId": solution_id, "id": activity_id},
        access_token=access_token, API_URL=API_URL, context="Delete DPA activity",
    )


def toggle_dpa_activity(solution_id: str, activity_id: str, access_token: str,
                        API_URL: str = DEFAULT_API_URL) -> dict:
    """Flip an activity between active and inactive."""
    return rpc_call(
        "dpa", "toggleActivity", {"solutionId": solution_id, "id": activity_id},
        access_token=access_token, API_URL=API_URL, context="Toggle DPA activity",
    )


def set_dpa_activity_lock(solution_id: str, activity_id: str, locked: bool,
                          access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Locked entries cannot be edited, deleted or toggled until unlocked."""
    return rpc_call(
        "dpa", "setActivityLock",
        {"solutionId": solution_id, "id": activity_id, "locked": locked},
        access_token=access_token, API_URL=API_URL, context="Set DPA activity lock",
    )


def record_dpa_acceptance(solution_id: str, access_token: str,
                          context: Optional[str] = None,
                          API_URL: str = DEFAULT_API_URL) -> dict:
    """Record that the current user accepted the DPA terms for a solution."""
    params = {"solutionId": solution_id}
    if context is not None:
        params["context"] = context
    return rpc_call(
        "dpa", "recordAcceptance", params,
        access_token=access_token, API_URL=API_URL, context="Record DPA acceptance",
    )


def get_my_dpa_acceptance_status(solution_id: str, access_token: str,
                                 API_URL: str = DEFAULT_API_URL) -> dict:
    """Has the current user acknowledged the DPA terms for this solution?"""
    return rpc_call(
        "dpa", "getMyAcceptanceStatus", {"solutionId": solution_id},
        access_token=access_token, API_URL=API_URL, context="Get DPA acceptance status",
    )
