import io
from typing import Optional, List
import time as timer
import requests
import pyarrow.parquet as pq
import polars as pl

DEFAULT_API_URL = "https://dev.polyteia.com"


def hello_world():
    return "Hello, world from gOS-api-sdk!"

def handle_api_response(response, *, context: str = "API call", expected_status_codes: tuple = (200, 201), required_keys: tuple = None) -> dict:
    """
    Validates an HTTP response from the API.

    Args:
        response (requests.Response): The response object returned by requests.
        context (str): A human-readable context for the operation (e.g. "Create dataset").
        expected_status_codes (tuple): Tuple of acceptable HTTP status codes.
        required_keys (tuple): Nested keys to check existence in the JSON response.

    Returns:
        dict: Parsed JSON response if validation passes.

    Raises:
        Exception: If status code is unexpected, response isn't JSON, or required keys are missing.
    """
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



def update_dataset(ds_id: str, access_token: str, API_URL: str = DEFAULT_API_URL, **kwargs) -> None:

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

    handle_api_response(update_response, context="Update dataset")


def create_dataset(solution_id: str, name: str, description: str, source: str, slug: str, access_token: str, documentation: Optional[dict] = None, API_URL: str = DEFAULT_API_URL) -> str:
    """
    Create a dataset.
    """
    headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    dataset_payload = {
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


def upload_file(upload_token: str, df: pl.DataFrame, access_token: str, API_URL: str = DEFAULT_API_URL) -> None:
    """
    Upload a file to the dataset.
    """
    # Convert DataFrame to a PyArrow table
    table = df.to_arrow()

    # Write the Arrow table to a Parquet file in a BytesIO buffer
    buffer = io.BytesIO()
    pq.write_table(table, buffer)

    buffer.seek(0)
    headers = {
        "Authorization": f"Bearer {access_token}",
        "X-Upload-Token": upload_token
    }
    payload = {}

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


def create_insight(insight_body: dict, access_token: str, API_URL: str = DEFAULT_API_URL) -> str:
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
    

def get_org_id(client_id: str, mandant: str, API_URL: str = DEFAULT_API_URL) -> str:
    # use static mapping
    return("Not implemented")


def get_solution_id(org_id: str, API_URL: str = DEFAULT_API_URL) -> str:
    # use static mapping
    return("Not implemented")


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


def invite_user_to_org(org_id: str, access_token: str, email: str = "cloud@polyteia.de", role: str = "admin", API_URL: str = DEFAULT_API_URL) -> None:
    
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
                "message": "I am inviting you to org."
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


def update_solution_doc(solution_id: str, access_token: str, doc: dict, API_URL: str = DEFAULT_API_URL) -> None:
    
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
    #return dataset["data"]["metadata"]["schema"]["columns"]

    ### CONFIRM
    return dataset["data"].get("metadata", {}).get("schema", {}).get("columns", {})


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


def download_file(download_token: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> pl.DataFrame:
    """
    Download a Parquet file using the download token and return it as a Polars DataFrame.
    """
    url = f"{API_URL}/download?token={download_token}"

    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        raise Exception(f"Download file failed (HTTP {response.status_code}): {response.text}")

    buffer = io.BytesIO(response.content)
    df = pl.read_parquet(buffer)
    return df