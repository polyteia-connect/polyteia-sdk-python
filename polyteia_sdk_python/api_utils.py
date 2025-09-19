import io
from pathlib import Path
from typing import Optional, List
import time as timer
import requests
import pyarrow as pa
import pyarrow.parquet as pq

DEFAULT_API_URL = "https://app.polyteia.com"


def handle_api_response(response, *, context: str = "API call", expected_status_codes: tuple = (200, 201), required_keys: Optional[tuple] = None) -> dict:
    """
    Validates an HTTP response from the API.

    Args:
        response (requests.Response): The response object returned by requests.
        context (str): A human-readable context for the operation (e.g. "Create dataset").
        expected_status_codes (tuple): Tuple of acceptable HTTP status codes.
        required_keys (tuple): Nested keys to check existence in the JSON response.

    Returns:
        dict: Parsed JSON response if validation passes, or an empty dict for non-JSON responses.

    Raises:
        Exception: If status code is unexpected, response isn't JSON, or required keys are missing.
    """
    content_type = response.headers.get("Content-Type", "")

    # Case: Non-JSON response (e.g. file upload with 204 or plain text)
    if "application/json" not in content_type:
        if response.status_code in expected_status_codes:
            return {}  # Acceptable non-JSON success
        raise Exception(f"{context} failed (HTTP {response.status_code}):\n{response.text}")

    # Case: Valid JSON response expected
    try:
        json_response = response.json()
    except ValueError:
        raise Exception(f"{context} failed: Invalid JSON response:\n{response.text}")

    if response.status_code not in expected_status_codes:
        raise Exception(f"{context} failed (HTTP {response.status_code}):\n{json_response}")

    if required_keys:
        current = json_response
        for key in required_keys:
            if key not in current:
                raise Exception(f"{context} failed: Missing key '{key}' in response:\n{json_response}")
            current = current[key]

    return json_response


def get_org_access_token(org_id: str, PAK: str, API_URL: str = DEFAULT_API_URL) -> str:
    """
    Get access token for organization.
    """

    token_payload = {
            "organization_id": org_id,
        }

    # Get access token
    token_response = requests.put(
        f"{API_URL}/auth/pak/token",
        headers={"Authorization": f"Bearer {PAK}", "Content-Type": "application/json"}, 
        json=token_payload
    )

    json_response = handle_api_response(token_response, context=f"Get org access token for {org_id}", required_keys=("token",))
    return json_response["token"]


def get_org_id_by_slug(slug: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> str:
    """
    Retrieve an organization ID by its slug.

    Args:
        slug (str): The unique slug of the organization.
        access_token (str): Bearer token for API access.
        API_URL (str, optional): Base URL for the API.

    Returns:
        str: The organization's unique ID.

    Note:
        This is a placeholder function. Logic needs to be implemented based on API support.
    """
    raise NotImplementedError("get_org_id_by_slug is not yet implemented.")


def update_dataset(ds_id: str, access_token: str, API_URL: str = DEFAULT_API_URL, **kwargs) -> dict:
    """
    Update a dataset's properties.
    
    Args:
        ds_id (str): Dataset ID
        access_token (str): Bearer token for authentication
        API_URL (str, optional): API endpoint. Defaults to DEFAULT_API_URL.
        **kwargs: Additional dataset properties to update
        
    Returns:
        dict: API response
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    current_dataset = get_dataset_by_id(ds_id, access_token, API_URL)["data"]
    
    params = {
        "id": ds_id,
        "name": current_dataset["name"],
        "solution_id": current_dataset["solution_id"],
        "description": current_dataset["description"],
        "source": current_dataset["source"],
        "slug": current_dataset["slug"],
    }
    
    if "documentation" in current_dataset.keys():
        params["documentation"] = current_dataset["documentation"]
    
    updated_params = {**params, **kwargs}
    
    payload = {
        "command": "update_dataset",
        "params": updated_params
    }
    
    update_response = requests.post(f"{API_URL}/api", headers=headers, json=payload)
    return handle_api_response(update_response, context="Update dataset")


def create_dataset(solution_id: str, name: str, description: str, source: str, slug: str, access_token: str, documentation: Optional[dict] = None, API_URL: str = DEFAULT_API_URL) -> str:
    """
    Create a dataset.
    """
    headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    dataset_payload: dict = {
        "command": "create_dataset",
        "params": {
                "name": name,
                "solution_id": solution_id,
                "description": description,
                "source": source,
                "slug": slug,
                    }
        }

    if documentation:
        dataset_payload["params"]["documentation"] = documentation
    
    # print(dataset_payload)

    response = requests.post(
            f"{API_URL}/api",
            headers=headers,
            json=dataset_payload
        )


    json_response = handle_api_response(response, context="Create dataset", required_keys=("data", "id"))
    return json_response["data"]["id"]


def generate_upload_token(ds_id: str, content_type: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> str:
    """
    Generate an upload token.
    """
    headers = { "Authorization": f"Bearer {access_token}",
                'Content-Type': 'application/json' }
    payload = {
        "command": "generate_dataset_upload_token",
        "params": {
            "id": ds_id,
            "content_type": content_type
        }
    }

    response = requests.post(
        f"{API_URL}/api",
        headers=headers,
        json=payload
    )

    json_response = handle_api_response(response, context="Generate upload token", required_keys=("data", "token"))
    return json_response["data"]["token"]


def upload_file(upload_token: str, df: pa.Table, access_token: str, API_URL: str = DEFAULT_API_URL) -> None:
    """
    Upload a file to the dataset. Input must be convertible to pyarrow.Table.
    """

    # Write the Arrow table to a Parquet file in a BytesIO buffer
    buffer = io.BytesIO()
    pq.write_table(df, buffer)

    buffer.seek(0)
    headers = {
        "Authorization": f"Bearer {access_token}",
        "X-Upload-Token": upload_token
    }
    payload: dict[str, str] = {}

    files = [
        ('file', ('filename', buffer, 'application/octet-stream'))
    ]

    response = requests.post(
        f"{API_URL}/upload",
        headers=headers,
        data=payload,
        files=files
    )

    handle_api_response(response, context="Upload file")


def create_insight(insight_body: dict, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """
    Create an insight.
    """
    headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    payload = {
        "command": "create_insight",
            "params": insight_body
        }
    
    response = requests.post(
            f"{API_URL}/api",
            headers=headers,
            json=payload
        )
    
    json_response = handle_api_response(response, context="Create insight")
    return json_response


def update_insight(insight_id: str, insight_body: dict, access_token: str, API_URL: str = DEFAULT_API_URL) -> None:
    """
    Update an insight.
    """
    headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    # Make the params explicit here
    
    payload = {
        "command": "update_insight",
            "params": {
                "id": insight_id,
                **insight_body
            }
        }
    
    response = requests.post(
            f"{API_URL}/api",
            headers=headers,
            json=payload
        )

    handle_api_response(response, context="Update insight")


def get_or_create_dataset(
                        solution_id: str,
                        name: str,
                        description: str,
                        source: str,
                        slug: str,
                        access_token: str,
                        documentation: Optional[dict] = None,
                        API_URL: str = DEFAULT_API_URL
                    ) -> str:

    try:
        ds = get_dataset_by_slug(solution_id, slug, access_token, API_URL)
        ds_id = ds["data"]["id"]
    except Exception:
        ds_id = create_dataset(solution_id, name, description, source, slug, access_token, documentation = documentation, API_URL = API_URL)
    
    return ds_id
    

def list_resources(
                container_id: str,
                access_token: str,
                ressource_type: str = "dataset",
                page_nr: int = 1,
                page_size: int = 100,
                permission: str = "can_edit",
                API_URL: str = DEFAULT_API_URL
            ) -> dict:
    
    headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    payload = {
        "query": "list_resources",
            "params": {
                "page": page_nr,
                "size": page_size,
                "resource_type": ressource_type,
                "permission": permission,
                "filters": [{
                    "container_id": container_id
                }],
                "search": "",
                "tags": []
            }
        }
    
    response = requests.post(
            f"{API_URL}/api",
            headers=headers,
            json=payload
        )
    
    json_response = handle_api_response(response, context="List resources")
    return json_response

def list_resources_recursive(
                            container_id: str,
                            access_token: str,
                            ressource_type: str = "dataset",
                            permission: str = "can_edit",
                            API_URL: str = DEFAULT_API_URL
                        ) -> List[str]:

    page_nr = 1
    page_size = 100
    resources = []
    
    while True:
        response = list_resources(container_id, access_token, ressource_type, page_nr, page_size, permission, API_URL)
        resources.extend(response["data"]["items"])
        page_nr += 1
        timer.sleep(0.2)
        if (response["data"]["page"] * page_size) >= response["data"]["total"]:
            break
    
    return resources


def get_dataset_by_id(dataset_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:

    headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    payload = {
        "query": "get_dataset",
            "params": {
                "id": dataset_id
            }
        }
    
    response = requests.post(
            f"{API_URL}/api",
            headers=headers,
            json=payload
        )
    
    json_response = handle_api_response(response, context="Get dataset by id")
    return json_response

def get_dataset_by_slug(solution_id: str, slug: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:

    headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    payload = {
        "query": "get_dataset",
            "params": {
                "solution_id": solution_id,
                "slug": slug
            }
        }
    
    response = requests.post(
            f"{API_URL}/api",
            headers=headers,
            json=payload
        )
    
    json_response = handle_api_response(response, context="Get dataset by slug")
    return json_response


def get_all_datasets_in_sol(sol_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> List[dict]:

    ds_ids = list_resources_recursive(sol_id, access_token, API_URL = API_URL)
    datasets = [get_dataset_by_id(ds_id, access_token, API_URL) for ds_id in ds_ids]
    datasets = [ds["data"] for ds in datasets]
    return datasets


def create_tag(org_id: str, name: str, description: str, access_token: str, color: str = "#1F009D", API_URL: str = DEFAULT_API_URL) -> str:

    headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    payload = {
        "command": "create_tag",
            "params": {
                "organization_id": org_id,
                "name": name,
                "description": description,
                "color": color
            }
        }
    
    response = requests.post(
            f"{API_URL}/api",
            headers=headers,
            json=payload
        )
    
    json_response = handle_api_response(response, context="Create tag", required_keys=("data", "id"))
    return json_response["data"]["id"]


def search_tags(org_id: str, access_token: str, search: str, page: int = 1, size: int = 100, API_URL: str = DEFAULT_API_URL) -> List[dict]:
    
    headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    payload = {
        "query": "list_tags",
            "params": {
                "organization_id": org_id,
                "search": search,
                "page": page,
                "size": size
            }
        }
    
    response = requests.post(
            f"{API_URL}/api",
            headers=headers,
            json=payload
        )
    
    json_response = handle_api_response(response, context="Search tags", required_keys=("data", "items"))
    return json_response["data"]["items"]


def add_tag_to_ressource(tag_id: str, ressource_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> None:
    
    headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    payload = {
        "command": "add_tag_to_resource",
            "params": {
                "tag_id": tag_id,
                "resource_id": ressource_id
            }
        }
    
    response = requests.post(
            f"{API_URL}/api",
            headers=headers,
            json=payload
        )
    
    handle_api_response(response, context="Add tag to resource")
    

def get_insight(insight_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    
    headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    payload = {
        "query": "get_insight",
            "params": {
                "id": insight_id
            }
        }
    
    response = requests.post(
            f"{API_URL}/api",
            headers=headers,
            json=payload
        )
    
    json_response = handle_api_response(response, context="Get insight")
    return json_response


def get_insight_by_slug(solution_id: str, slug: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:

    headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    payload = {
        "query": "get_insight",
            "params": {
                "solution_id": solution_id,
                "slug": slug
            }
        }
    
    response = requests.post(
            f"{API_URL}/api",
            headers=headers,
            json=payload
        )
    
    json_response = handle_api_response(response, context="Get insight by slug")
    return json_response

def find_insight_by_kpi_id(kpi_id: str, solution_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    
    """
    This only works with KKS-specific insight ids and names
    """
    all_insights = list_resources_recursive(container_id=solution_id, access_token=access_token, ressource_type="insight", permission="can_edit", API_URL = API_URL)
    for insight_id in all_insights:
        insight = get_insight(insight_id, access_token, API_URL)
        insight_kpi_id = insight["data"]["name"].split(" - ")[0]
        if insight_kpi_id == kpi_id:
            return insight

    raise Exception(f"Insight with KPI id {kpi_id} not found in solution {solution_id}")


def create_or_update_insight(insight_body: dict, solution_id: str, kpi_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> str:
    
    """
    This only works with KKS-specific insight ids and names
    """
    try:
        insight = find_insight_by_kpi_id(kpi_id, solution_id, access_token, API_URL)
        insight_id = insight["data"]["id"]
        update_insight(insight_id, insight_body, access_token, API_URL)
        return insight_id
    except Exception:
        insight_id = create_insight(insight_body, access_token, API_URL)
        #return insight_id["data"]["id"]

        ###CONFIRM
        return insight_id["data"]["id"] if isinstance(insight_id, dict) else insight_id
        

def delete_insight(insight_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> None:
    
    headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    payload = {
        "command": "delete_insight",
            "params": {
                "id": insight_id
            }
        }
    
    response = requests.post(
            f"{API_URL}/api",
            headers=headers,
            json=payload
        )
    
    handle_api_response(response, context="Delete insight")


def delete_dataset(ds_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> None:
    
    headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    payload = {
        "command": "delete_dataset",
            "params": {
                "id": ds_id
            }
        }
    
    response = requests.post(
            f"{API_URL}/api",
            headers=headers,
            json=payload
        )
    
    handle_api_response(response, context="Delete dataset")

def delete_report(report_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> None:
    
    headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    payload = {
        "command": "delete_report",
            "params": {
                "id": report_id
            }
        }
    
    response = requests.post(
            f"{API_URL}/api",
            headers=headers,
            json=payload
        )
    
    handle_api_response(response, context="Delete report")
        

def list_tags(org_id: str, access_token: str, page: int = 1, size: int = 100, search: str = "", API_URL: str = DEFAULT_API_URL) -> dict:
    
    headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    payload = {
        "query": "list_tags",
            "params": {
                "organization_id": org_id,
                "page": page,
                "size": size,
                "search": search
            }
        }
    
    response = requests.post(
            f"{API_URL}/api",
            headers=headers,
            json=payload
        )
    
    json_response = handle_api_response(response, context="List tags")
    return json_response

def list_tags_recursive(org_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> List[str]:
    page_nr = 1
    page_size = 100
    tags = []
    
    while True:
        response = list_tags(org_id, access_token, page_nr, page_size, API_URL = API_URL)
        tags.extend(response["data"]["items"])
        page_nr += 1
        timer.sleep(0.2)
        if (response["data"]["page"] * page_size) >= response["data"]["total"]:
            break
    
    return tags


def delete_tag(tag_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> None:
    
    headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    payload = {
        "command": "delete_tag",
            "params": {
                "id": tag_id
            }
        }
    
    response = requests.post(
            f"{API_URL}/api",
            headers=headers,
            json=payload
        )
    
    handle_api_response(response, context="Delete tag")


def get_organisation(org_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    
    headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    payload = {
        "query": "get_organization",
            "params": {
                "id": org_id
            }
        }
    
    response = requests.post(
            f"{API_URL}/api",
            headers=headers,
            json=payload
        )
    
    json_response = handle_api_response(response, context="Get organization", required_keys=("data",))
    return json_response["data"]


def create_org(name: str, description: str, slug: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> str:
    
    headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    payload = {
        "command": "create_organization",
            "params": {
                "name": name,
                "description": description,
                "slug": slug,
                "settings": {
                "seats": 10
            },
            "attributes": {
                "key": "value"
            }
                }
        }
    
    response = requests.post(
            f"{API_URL}/api",
            headers=headers,
            json=payload
        )
    
    json_response = handle_api_response(response, context="Create organization", required_keys=("data", "id"))
    return json_response["data"]["id"]


def invite_user_to_org(org_id: str, access_token: str, email: str, role: str, API_URL: str = DEFAULT_API_URL) -> dict:
    
    headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    payload = {
        "command": "invite_user_to_organization",
            "params": {
                "id": org_id,
                "email": email,
                "role": role,
                "message": "Ich lade Sie zur einer Polyteia-Organisation ein."
            }
        }
    
    response = requests.post(
            f"{API_URL}/api",
            headers=headers,
            json=payload
        )
    
    json_response = handle_api_response(response, context="Invite user to organization")
    return json_response


def create_workspace(org_id: str, name: str, description: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> str:
    
    headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    payload = {
        "command": "create_workspace",
            "params": {
                "organization_id": org_id,
                "name": name,
                "description": description,
                "settings": {
                    "seats": 10
                }
            }
        }
    
    response = requests.post(
            f"{API_URL}/api",
            headers=headers,
            json=payload
        )
    
    json_response = handle_api_response(response, context="Create workspace", required_keys=("data", "id"))
    return json_response["data"]["id"]


def create_solution(workspace_id: str, name: str, description: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> str:
    
    headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    payload = {
        "command": "create_solution",
            "params": {
                "workspace_id": workspace_id,
                "name": name,
                "description": description
            }
        }
    
    response = requests.post(
            f"{API_URL}/api",
            headers=headers,
            json=payload
        )
    
    json_response = handle_api_response(response, context="Create solution", required_keys=("data", "id"))
    return json_response["data"]["id"]


def add_user_to_workspace(workspace_id: str, user_id: str, access_token: str, role: str = "admin", API_URL: str = DEFAULT_API_URL) -> None:
    
    headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    payload = {
        "command": "add_workspace_member",
            "params": {
                "id": workspace_id,
                "user_id": user_id,
                "role": role
            }
        }
    
    response = requests.post(
            f"{API_URL}/api",
            headers=headers,
            json=payload
        )
    
    handle_api_response(response, context="Add user to workspace")
    

def add_user_to_solution(solution_id: str, user_id: str, access_token: str, role: str = "admin", API_URL: str = DEFAULT_API_URL) -> None:
    
    headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    payload = {
        "command": "add_solution_member",
            "params": {
                "id": solution_id,
                "user_id": user_id,
                "role": role
            }
        }
    
    response = requests.post(
            f"{API_URL}/api",
            headers=headers,
            json=payload
        )
    
    handle_api_response(response, context="Add user to solution")


def delete_org(org_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> None:
    """
    Organizations can only be deleted if they contain no other resources and no users.
    """
    
    headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    payload = {
        "command": "delete_organization",
            "params": {
                "id": org_id
            }
        }
    
    response = requests.post(
            f"{API_URL}/api",
            headers=headers,
            json=payload
        )
    
    handle_api_response(response, context="Delete organization")
    

def get_solution(solution_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    
    headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    payload = {
        "query": "get_solution",
            "params": {
                "id": solution_id
            }
        }
    
    response = requests.post(
            f"{API_URL}/api",
            headers=headers,
            json=payload
        )
    
    json_response = handle_api_response(response, context="Get solution", required_keys=("data",))
    return json_response["data"]


def update_solution_doc(solution_id: str, access_token: str, doc: dict, API_URL: str = DEFAULT_API_URL) -> dict:
    
    headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    current_solution = get_solution(solution_id, access_token, API_URL)

    params = {
        "id": solution_id,
        "name": current_solution["name"],
        "description": current_solution["description"],
        "documentation": doc
    }

    payload = {
        "command": "update_solution",
            "params": params
        }
    
    response = requests.post(
            f"{API_URL}/api",
            headers=headers,
            json=payload
        )
    
    json_response = handle_api_response(response, context="Update solution")
    return json_response


def update_dataset_metadata(ds_id: str, columns: dict, access_token: str, API_URL: str = DEFAULT_API_URL) -> None:
    
    headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

    params = {
        "id": ds_id,
        "columns": columns
    }

    payload = {
        "command": "update_dataset_metadata",
            "params": params
        }
    
    response = requests.post(
            f"{API_URL}/api",
            headers=headers,
            json=payload
        )
    
    handle_api_response(response, context="Update dataset metadata")
    

def get_dataset_metadata_cols(ds_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    
    dataset = get_dataset_by_id(ds_id, access_token, API_URL)

    return dataset["data"].get("metadata", {}).get("schema", {}).get("columns", {})

def create_group(org_id: str, name: str, description: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> str:
   
    headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    payload = {
        "command": "create_group",
            "params": {
                "organization_id": org_id,
                "name": name,
                "description": description
            }
        }
    
    response = requests.post(
            f"{API_URL}/api",
            headers=headers,
            json=payload
        )
    
    json_response = handle_api_response(response, context="Create group", required_keys=("data", "id"))
    return json_response["data"]["id"]

def share_dataset_with_group(ds_id: str, group_id: str, role: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> None:
    
    headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    params = {
            "resource_id": ds_id,
            "assignments": [
                {
                    "id": group_id,
                    "role": role
                }
            ],
            "unassignments": []
        }
    
    payload = {
        "command": "bulk_role_update",
            "params": params
        }
    
    response = requests.post(
            f"{API_URL}/api",
            headers=headers,
            json=payload
        )
    
    handle_api_response(response, context="Share dataset with group")


def generate_download_token(ds_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> str:
    """
    Generate a download token using the dataset id.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    params = {
        "sql": "FROM '{{" + ds_id + "}}'",
        "datasets": [ ds_id ],
        "args": [],
        "format": "application/vnd.apache.parquet"
    }

    payload = {
        "command": "generate_query_export_token",
        "params": params
    }

    response = requests.post(f"{API_URL}/api", headers=headers, json=payload)
    
    json_response = handle_api_response(response, context="Generate download token", required_keys=("data", "token"))
    return json_response["data"]["token"]


def download_file_to_arrow(
    download_token: str,
    access_token: str,
    API_URL: str = DEFAULT_API_URL,
    ) -> pa.Table:
    """
    Download a file using the download token and return it as a PyArrow table.

    Args:
        download_token (str): The secure download token.
        access_token (str): Bearer token for authentication.
        API_URL (str): The base API endpoint.

    Returns:
        pyarrow.Table: The downloaded file as a PyArrow table.
    """
    url = f"{API_URL}/download?token={download_token}"
    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        raise Exception(f"Download file failed (HTTP {response.status_code}): {response.text}")

    buffer = io.BytesIO(response.content)

    return pq.read_table(buffer)
    
def list_workspaces(org_id: str, access_token: str, page: int = 1, size: int = 100, search: str = "", API_URL: str = DEFAULT_API_URL) -> dict:
    
    headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    payload = {
        "query": "list_workspaces",
            "params": {
                "organization_id": org_id,
                "page": page,
                "size": size,
                "search": search
            }
        }
    
    response = requests.post(
            f"{API_URL}/api",
            headers=headers,
            json=payload
        )
    
    json_response = handle_api_response(response, context="List workspaces")
    return json_response

def list_solutions(org_id: str, access_token: str, page: int = 1, size: int = 100, search: str = "", API_URL: str = DEFAULT_API_URL) -> dict:
    
    headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    payload = {
        "query": "list_solutions",
            "params": {
                "organization_id": org_id,
                "page": page,
                "size": size,
                "search": search
            }
        }
    
    response = requests.post(
            f"{API_URL}/api",
            headers=headers,
            json=payload
        )
    
    json_response = handle_api_response(response, context="List solutions")
    return json_response

def add_insight_to_report(report_id: str, insight_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Add an insight to a report."""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "command": "add_insight_to_report",
        "params": {
            "insight_id": insight_id,
            "report_id": report_id
        }
    }
    
    response = requests.post(
        f"{API_URL}/api",
        headers=headers,
        json=payload
    )
    
    return handle_api_response(response, context="Add insight to report")

def create_report(report_body: dict, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Create a report and add insights to it."""
    # Get insights from metadata
    insights = report_body.get("metadata", {}).get("insights", [])
    
    # if "metadata" in report_body:
    #     del report_body["metadata"]
    
    # Create the report first
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "command": "create_report",
        "params": report_body
    }
    
    response = requests.post(
        f"{API_URL}/api",
        headers=headers,
        json=payload
    )
    
    report_response = handle_api_response(response, context="Create report")
    report_id = report_response["data"]["id"]
    
    # Add each insight to the report
    for insight_id in insights:
        add_insight_to_report(report_id, insight_id, access_token, API_URL)
    
    return report_response

def delete_solution(solution_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> None:
    
    headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    payload = {
        "command": "delete_solution",
            "params": {
                "id": solution_id
            }
        }
    
    response = requests.post(
            f"{API_URL}/api",
            headers=headers,
            json=payload
        )
    
    handle_api_response(response, context="Delete solution")

def delete_workspace(workspace_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> None:
    
    headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

    payload = {
        "command": "delete_workspace",
            "params": {
                "id": workspace_id
            }
        }   
    
    response = requests.post(
            f"{API_URL}/api",
            headers=headers,
            json=payload
        )   
    
    handle_api_response(response, context="Delete workspace")

def add_group_to_workspace(ws_id: str, group_id: str, role: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> None:
    
    headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    params = {
            "resource_id": ws_id,
            "assignments": [
                {
                    "id": group_id,
                    "role": role
                }
            ],
            "unassignments": []
        }
    
    payload = {
        "command": "bulk_role_update",
            "params": params
        }
    
    response = requests.post(
            f"{API_URL}/api",
            headers=headers,
            json=payload
        )
    
    handle_api_response(response, context="Add group to workspace")

def add_group_to_solution(sol_id: str, group_id: str, role: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> None:
    
    headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    params = {
            "resource_id": sol_id,
            "assignments": [
                {
                    "id": group_id,
                    "role": role
                }
            ],
            "unassignments": []
        }
    
    payload = {
        "command": "bulk_role_update",
            "params": params
        }
    
    response = requests.post(
            f"{API_URL}/api",
            headers=headers,
            json=payload
        )
    
    handle_api_response(response, context="Add group to solution")

def add_user_to_group(group_id: str, user_id: str, role: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> None:
    
    headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    params = {
            "resource_id": group_id,
            "assignments": [
                {
                    "id": user_id,
                    "role": role
                }
            ],
            "unassignments": []
        }
    
    payload = {
        "command": "bulk_role_update",
            "params": params
        }
    
    response = requests.post(
            f"{API_URL}/api",
            headers=headers,
            json=payload
        )

    handle_api_response(response, context="Add user to group")

def check_group(group_id: str, access_token: str, filters: Optional[dict] = None, API_URL: str = DEFAULT_API_URL) -> dict:
    headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    payload: dict = {
        "query": "get_users_or_groups_for_resource",
            "params": {
                "resource_id": group_id
            }
        }

    if filters:
        payload["params"]["filters"] = filters
    
    response = requests.post(
            f"{API_URL}/api",
            headers=headers,
            json=payload
        )

    return handle_api_response(response, context="Check group")

def share_report_with_group(report_id: str, group_id: str, role: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> None:
    
    headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    params = {
        "resource_id": report_id,
        "assignments": [
            {
                "id": group_id,
                "role": role
            }
        ],
        "unassignments": []
    }
    
    payload = {
        "command": "bulk_role_update",
            "params": params
        }
    
    response = requests.post(
            f"{API_URL}/api",
            headers=headers,
            json=payload
        )
    
    handle_api_response(response, context="Share report with group")

def list_org_members(org_id: str, access_token: str, page: int = 1, size: int = 100, search: str = "", filters: Optional[dict] = None,  API_URL: str = DEFAULT_API_URL) -> dict:
    
    headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    payload: dict = {
        "query": "list_organization_members",
            "params": {
                "id": org_id,
                "page": page,
                "size": size,
                "search": search,
                "order": []
            }
        }
    
    if filters:
        payload["params"]["filters"] = filters
    
    response = requests.post(
            f"{API_URL}/api",
            headers=headers,
            json=payload
    )

    return handle_api_response(response, context="List org members")

def get_org_user_by_user_id(org_id: str, user_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    
    headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    payload = {
        "query": "get_organization_member",
            "params": {
                "id": org_id,
                "user_id": user_id
            }
        }
    
    response = requests.post(
            f"{API_URL}/api",
            headers=headers,
            json=payload
        )
    
    return handle_api_response(response, context="Get org user by user id")

def list_groups(org_id: str, access_token: str, page: int = 1, size: int = 100, search: str = "", filters: Optional[dict] = None,  API_URL: str = DEFAULT_API_URL) -> List[dict]:
    
    headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    payload: dict = {
        "query": "list_groups",
            "params": {
                "organization_id": org_id,
                "page": page,
                "size": size,
                "search": search,
                "order": []
            }
        }
    
    if filters:
        payload["params"]["filters"] = filters
    
    response = requests.post(
            f"{API_URL}/api",
            headers=headers,
            json=payload
    )

    return handle_api_response(response, context="List groups")["data"]["items"]

def delete_group(org_id: str, group_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> None:
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "command": "delete_group",
        "params": {
            "organization_id": org_id,
            "id": group_id
        }
    }
    
    response = requests.post(
        f"{API_URL}/api",
        headers=headers,
        json=payload
    )

    handle_api_response(response, context="Delete group")

def get_report(report_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "query": "get_resource",
        "params": {
            "id": report_id
        }
    }
    
    response = requests.post(
        f"{API_URL}/api",
        headers=headers,
        json=payload
    )

    return handle_api_response(response, context="Get report")


def remove_insight_from_report(report_id: str, insight_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Remove an insight from a report."""

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    payload = {
        "command": "remove_insight_from_report",
        "params": {
            "insight_id": insight_id,
            "report_id": report_id
        }
    }
    response = requests.post(
        f"{API_URL}/api",
        headers=headers,
        json=payload,
        timeout=50
    )
    return handle_api_response(response, context="Remove insight from report")


def extract_insights_from_structure(structure: dict | list) -> set:
    """
    Recursively extract all insight IDs from a report structure.

    The structure can be deeply nested with various types of blocks (sections, pages, columns, widgets etc.).
    We're looking for widget blocks that have widgetData.insightId.

    This is a helper function needed for updating reports

    Args:
        structure (Union[dict, list]): Report structure (can be dict or list at top level)

    Returns:
        set: Set of insight IDs found in the structure
    """
    insights = set()

    if isinstance(structure, dict):
        # Check if this is a widget with an insight
        if structure.get("type") == "widget" and "widgetData" in structure:
            insight_id = structure["widgetData"].get("insightId")
            if insight_id:
                insights.add(insight_id)

        # Recursively check all nested structures
        for value in structure.values():
            if isinstance(value, (dict, list)):
                insights.update(extract_insights_from_structure(value))
    
    elif isinstance(structure, list):
        # Recursively check all items
        for item in structure:
            if isinstance(item, (dict, list)):
                insights.update(extract_insights_from_structure(item))

    return insights


def update_report(report_id: str, access_token: str, API_URL: str = DEFAULT_API_URL, **kwargs) -> dict:
    """
    Flexibly update a report via the provided kwargs.

    Args:
        report_id (str): Report ID
        access_token (str): Bearer token for authentication
        API_URL (str): API endpoint
        **kwargs: Additional report properties to update

    Returns:
        dict: API response from the update operation

    Note:
        The function manages insights by:
        1. Getting current insights from metadata
        2. If a new structure is provided, extracts actually used insights from it
        3. Adds/removes insights as needed (API handles metadata updates)
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    current_report = get_report(report_id, access_token, API_URL)["data"]
    current_insights = set(current_report.get("metadata", {}).get("insights", []))

    params = {
        "id": report_id,
        "name": current_report["name"],
        "description": current_report["description"],
        "version": current_report["version"],
        "structure": current_report["structure"]
    }

    if "metadata" in current_report:
        params["metadata"] = current_report["metadata"]

    updated_params = {**params, **kwargs}

    payload = {
        "command": "update_report",
        "params": updated_params
    }

    update_response = requests.post(f"{API_URL}/api", headers=headers, json=payload)
    update_result = handle_api_response(update_response, context="Update report")

    if "structure" in kwargs:
        new_insights = extract_insights_from_structure(kwargs["structure"])

        insights_to_add = new_insights - current_insights
        insights_to_remove = current_insights - new_insights

        for insight_id in insights_to_add:
            try:
                add_insight_to_report(report_id, insight_id, access_token, API_URL)
            except Exception as e:
                print(f"Warning: Failed to add insight {insight_id}: {str(e)}")

        for insight_id in insights_to_remove:
            try:
                remove_insight_from_report(report_id, insight_id, access_token, API_URL)
            except Exception as e:
                print(f"Warning: Failed to remove insight {insight_id}: {str(e)}")

    return update_result


def get_image_upload_token(report_id: str, access_token: str, content_type: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Generate an upload token for images, e.g. logos in reports
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    payload = {
        "command": "generate_report_image_upload_token",
        "params": {"id": report_id, "content_type": content_type},
    }
    response = requests.post(
        f"{API_URL}/api/generate_report_image_upload_token",
        headers=headers,
        json=payload,
        timeout=60,
    )
    r = handle_api_response(response, context="Get image upload token")

    data = r["data"]

    return {
        "upload_url": data.get("upload_url", f"{API_URL}/upload"),
        "token": data["token"],
        "filename": data["filename"],
    }

def upload_local_file(upload_url: str, upload_token: str, local_path: str, content_type: str) -> None:
    """Upload a file from a local storage
    Needs an upload url and upload token generated by get_image_upload_token
    """
    try:
        with open(local_path, "rb") as f:
            files = {"file": (Path(local_path).name, f, content_type)}
            response = requests.post(upload_url, headers={"X-Upload-Token": upload_token}, files=files, timeout=120)
            handle_api_response(response, context="Upload file")
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {local_path}")
    except Exception as e:
        raise Exception(f"Failed to upload file {local_path}: {str(e)}")


def get_report_view(report_view_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> dict:
    """Get a single report view by its ID."""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "query": "get_report_view",
        "params": {
            "id": report_view_id
        }
    }
    
    response = requests.post(
        f"{API_URL}/api",
        headers=headers,
        json=payload
    )
    
    return handle_api_response(response, context="Get report view")


def list_report_views(report_id: str, access_token: str, page: int = 1, size: int = 100, API_URL: str = DEFAULT_API_URL) -> dict:
    """Get an array of report views created from a report by its ID."""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "query": "list_report_views",
        "params": {
            "report_id": report_id,
            "page": page,
            "size": size
        }
    }
    
    response = requests.post(
        f"{API_URL}/api",
        headers=headers,
        json=payload
    )
    
    return handle_api_response(response, context="List report views")