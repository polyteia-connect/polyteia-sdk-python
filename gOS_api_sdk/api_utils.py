import io
from typing import Optional, List
import time as timer
import requests
import pyarrow.parquet as pq
import polars as pl
from get_env import API_URL, PAK


def hello_world():
    return "Hello, world from gOS-api-sdk!"



def get_org_access_token(org_id: str) -> str:
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

    if token_response.status_code not in [200, 201]:
        raise Exception(f"Failed to get access token for {org_id}: {token_response.text}")

    access_token = token_response.json().get('token')
    return access_token


def update_dataset(ds_id: str, access_token: str, **kwargs) -> None:
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    current_dataset = get_dataset_by_id(ds_id, access_token)["data"]
    
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

    if update_response.status_code not in [200, 201]:
        raise Exception(f"Failed to update dataset: {update_response.text}")

    

def create_dataset(solution_id: str, name: str, description: str, source: str, slug: str, access_token: str, documentation: Optional[dict]= None) -> str:
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

    if response.status_code not in [200, 201]:
        raise Exception(f"Failed to create dataset: {response.text}")
    
    return response.json()["data"]["id"]


def generate_upload_token(ds_id: str, content_type: str, access_token: str) -> str:
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

    if response.status_code not in [200, 201]:
        raise Exception(f"Failed to generate upload token: {response.text}")
    
    return response.json()["data"]["token"]

def upload_file(upload_token: str, df: pl.DataFrame, access_token: str) -> None:
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

    if response.status_code not in [200, 201]:
        raise Exception(f"Failed to upload: {response.text}")

    return None


def create_insight(insight_body: dict, access_token: str) -> str:
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
    
    if response.status_code not in [200, 201]:
        raise Exception(f"Failed to create insight: {response.text}")
    
    return response.json()


def update_insight(insight_id: str, insight_body: dict, access_token: str) -> None:
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

    if response.status_code not in [200, 201]:
        raise Exception(f"Failed to update insight: {response.text}")
    

def get_org_id(client_id: str, mandant: str) -> str:
    # use static mapping
    return("Not implemented")


def get_solution_id(org_id: str) -> str:
    # use static mapping
    return("Not implemented")


def get_or_create_dataset(solution_id: str, name: str, description: str, source: str, slug: str, access_token: str, documentation: Optional[dict]= None) -> str:
    try:
        ds = get_dataset_by_slug(solution_id, slug, access_token)
        ds_id = ds["data"]["id"]
    except Exception:
        ds_id = create_dataset(solution_id, name, description, source, slug, access_token, documentation = documentation)
    
    return ds_id
    

def list_resources(container_id: str, 
                    access_token: str,
                    ressource_type: str = "dataset", 
                    page_nr: int = 1, 
                    page_size: int = 100,
                    permission: str = "can_edit") -> dict:
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
    
    if response.status_code not in [200, 201]:
        raise Exception(f"Failed to list resources: {response.text}")

    return response.json()

def list_resources_recursive(container_id: str, access_token: str, ressource_type: str = "dataset", permission: str = "can_edit") -> List[str]:
    page_nr = 1
    page_size = 100
    resources = []
    
    while True:
        response = list_resources(container_id, access_token, ressource_type, page_nr, page_size, permission)
        resources.extend(response["data"]["items"])
        page_nr += 1
        timer.sleep(0.2)
        if (response["data"]["page"] * page_size) >= response["data"]["total"]:
            break
    
    return resources

def get_dataset_by_id(dataset_id: str, access_token: str) -> dict:
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
    
    if response.status_code not in [200, 201]:
        raise Exception(f"Failed to get dataset: {response.text}")
    
    return response.json()

def get_dataset_by_slug(solution_id: str, slug: str, access_token: str) -> dict:
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
    
    if response.status_code not in [200, 201]:
        raise ValueError(f"Failed to get dataset: {response.text}")
    
    return response.json()

def get_all_datasets_in_sol(sol_id: str, access_token: str) -> List[dict]:
    ds_ids = list_resources_recursive(sol_id, access_token)
    datasets = [get_dataset_by_id(ds_id, access_token) for ds_id in ds_ids]
    datasets = [ds["data"] for ds in datasets]
    return datasets

def create_tag(org_id: str, name: str, description: str, access_token: str, color: str = "#1F009D") -> str:
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
    
    if response.status_code not in [200, 201]:
        raise Exception(f"Failed to create tag: {response.text}")
    
    return response.json()["data"]["id"]

def search_tags(org_id: str, access_token: str, search: str, page: int = 1, size: int = 100) -> List[dict]:
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
    
    if response.status_code not in [200, 201]:
        raise Exception(f"Failed to search tags: {response.text}")
    
    return response.json()["data"]["items"]

def add_tag_to_ressource(tag_id: str, ressource_id: str, access_token: str) -> None:
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
    
    if response.status_code not in [200, 201]:
        raise Exception(f"Failed to add tag to resource: {response.text}")
    
def get_insight(insight_id: str, access_token: str) -> dict:
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
    
    if response.status_code not in [200, 201]:
        raise Exception(f"Failed to get insight: {response.text}")
    
    return response.json()

def find_insight_by_kpi_id(kpi_id: str, solution_id: str, access_token: str) -> dict:
    """
    This only works with KKS-specific insight ids and names
    """
    all_insights = list_resources_recursive(container_id=solution_id, access_token=access_token, ressource_type="insight", permission="can_edit")
    for insight_id in all_insights:
        insight = get_insight(insight_id, access_token)
        insight_kpi_id = insight["data"]["name"].split(" - ")[0]
        if insight_kpi_id == kpi_id:
            return insight

    raise Exception(f"Insight with KPI id {kpi_id} not found in solution {solution_id}")


def create_or_update_insight(insight_body: dict, solution_id: str, kpi_id: str, access_token: str) -> str:
    """
    This only works with KKS-specific insight ids and names
    """
    try:
        insight = find_insight_by_kpi_id(kpi_id, solution_id, access_token)
        insight_id = insight["data"]["id"]
        update_insight(insight_id, insight_body, access_token)
        return insight_id
    except Exception:
        insight_id = create_insight(insight_body, access_token)
        return insight_id["data"]["id"]
        

def delete_insight(insight_id: str, access_token: str) -> None:
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
    
    if response.status_code not in [200, 201]:
        raise Exception(f"Failed to delete insight: {response.text}")

def delete_dataset(ds_id: str, access_token: str) -> None:
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
    
    if response.status_code not in [200, 201]:
        raise Exception(f"Failed to delete dataset: {response.text}")
        
def list_tags(org_id: str, access_token: str, page: int = 1, size: int = 100, search="") -> dict:
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
    
    if response.status_code not in [200, 201]:
        raise Exception(f"Failed to list tags: {response.text}")
    
    return response.json()

def list_tags_recursive(org_id: str, access_token: str) -> List[str]:
    page_nr = 1
    page_size = 100
    tags = []
    
    while True:
        response = list_tags(org_id, access_token, page_nr, page_size)
        tags.extend(response["data"]["items"])
        page_nr += 1
        timer.sleep(0.2)
        if (response["data"]["page"] * page_size) >= response["data"]["total"]:
            break
    
    return tags

def delete_tag(tag_id: str, access_token: str) -> None:
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
    
    if response.status_code not in [200, 201]:
        raise Exception(f"Failed to delete tag: {response.text}")
    
    return response.json()

def get_organisation(org_id: str, access_token: str) -> dict:
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
    
    if response.status_code not in [200, 201]:
        raise Exception(f"Failed to get organization: {response.text}")
    
    return response.json()["data"]

def create_org(name: str, description: str, slug: str, access_token: str) -> str:
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
    
    if response.status_code not in [200, 201]:
        raise Exception(f"Failed to create organization: {response.text}")
    
    # print(response.json())
    
    return response.json()["data"]["id"]

def invite_user_to_org(org_id: str, access_token: str, email: str = "cloud@polyteia.de", role: str = "admin") -> None:
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
    
    if response.status_code not in [200, 201]:
        raise Exception(f"Failed to invite user to organization: {response.text}")
    
    return response.json()

def create_workspace(org_id: str, name: str, description: str, access_token: str) -> str:
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
    
    if response.status_code not in [200, 201]:
        raise Exception(f"Failed to create workspace: {response.text}")
    
    return response.json()["data"]["id"]

def create_solution(workspace_id: str, name: str, description: str, access_token: str) -> str:
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
    
    if response.status_code not in [200, 201]:
        raise Exception(f"Failed to create solution: {response.text}")
    
    return response.json()["data"]["id"]

def add_user_to_workspace(workspace_id: str, user_id: str, access_token: str, role: str = "admin") -> None:
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
    
    if response.status_code not in [200, 201]:
        raise Exception(f"Failed to add user to workspace: {response.text}")
    


def add_user_to_solution(solution_id: str, user_id: str, access_token: str, role: str = "admin") -> None:
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
    
    if response.status_code not in [200, 201]:
        raise Exception(f"Failed to add user to solution: {response.text}")

def delete_org(org_id: str, access_token: str) -> None:
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
    
    if response.status_code not in [200, 201]:
        raise Exception(f"Failed to delete organization: {response.text}")
    

def get_solution(solution_id: str, access_token: str) -> dict:
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
    
    if response.status_code not in [200, 201]:
        raise Exception(f"Failed to get solution: {response.text}")
    
    return response.json()["data"]

def update_solution_doc(solution_id: str, access_token: str, doc: dict) -> None:
    headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    current_solution = get_solution(solution_id, access_token)

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
    
    if response.status_code not in [200, 201]:
        raise Exception(f"Failed to update solution: {response.text}")
    
    return response.json()

def update_dataset_metadata(ds_id: str, columns: dict, access_token: str) -> None:
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
    
    if response.status_code not in [200, 201]:
        raise Exception(f"Failed to update dataset metadata: {response.text}")
    

def get_dataset_metadata_cols(ds_id: str, access_token: str) -> dict:
    dataset = get_dataset_by_id(ds_id, access_token)
    return dataset["data"]["metadata"]["schema"]["columns"]


def share_dataset_with_group(ds_id: str, group_id: str, role: str, access_token: str) -> None:
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
    
    if response.status_code not in [200, 201]:
        raise Exception(f"Failed to share dataset with group: {response.text}")

def generate_download_token(ds_id: str, access_token: str) -> str:
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

    if response.status_code not in [200, 201]:
        raise Exception(f"Failed to generate download token: {response.text}")
    
    download_token = response.json()["data"]["token"]
    return download_token


def download_file(download_token: str, access_token: str) -> pl.DataFrame:
    """
    Download a Parquet file using the download token and return it as a Polars DataFrame.
    """
    url = f"{API_URL}/download?token={download_token}"

    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        raise Exception(f"Failed to download file: {response.text}")
    
    buffer = io.BytesIO(response.content)
    df = pl.read_parquet(buffer)
    return df