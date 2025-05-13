# 📦 gOS API TOOLKIT

A lightweight Python SDK for interacting with the gOS API — designed for seamless data integration, resource management, and automation within the Polyteia platform or similar data environments.

---

## 🚀 Features

* 🔐 Full organization, workspace, user, and role management
* 📊 Dataset operations: create, update, delete, metadata handling, and bulk listing
* 📈 Insight lifecycle support: create, update, retrieve, find-by-KPI, delete
* 🏷️ Comprehensive tagging: create, assign, search, list, delete
* 📁 Upload/download Parquet files via Polars and Arrow
* 🔁 Robust error handling using a shared `handle_api_response` utility
* 🔎 Resource discovery: list datasets, insights, tags, solutions recursively
* ⚙️ Modular and extensible — easily add new API-bound commands

---

## 📦 Installation

### Install via pip

```bash
pip install git+https://github.com/polyteia-de/gOS-api-toolkit.git
```

### Install locally for development

```bash
git clone https://github.com/polyteia-de/gOS-api-toolkit.git
cd gOS-api-toolkit
pip install -e .
```

---

## 📁 Project Structure

```
gos-api-toolkit/
│
├── gos_api_sdk/              # SDK source package
│   ├── __init__.py
│   └── api_utils.py          # Core API functions
│
├── requirements.txt          # Runtime dependencies
├── dev-requirements.txt      # Linting, testing, dev tools
├── setup.py                  # Package metadata/setup
├── pyproject.toml            # Build backend & versioning
├── .gitignore                # Local tests (excluded from Git)
```

---

## 🔧 Requirements

### ⚙️ Runtime Dependencies

The SDK depends on a few core libraries to handle HTTP requests and data serialization:

* `requests` – for communicating with the API
* `pyarrow` – for working with Arrow and Parquet file formats
* `polars` – for efficient DataFrame processing

These dependencies are **automatically installed** when the SDK is installed via pip:

```bash
pip install git+https://github.com/polyteia-de/gOS-api-toolkit.git
```

If needed (e.g. in a minimal environment or container), you can manually install runtime dependencies with:

```bash
pip install -r requirements.txt
```

### 🛠 Install Development Dependencies

To contribute to the SDK or run tests, you’ll need additional tools.

Install them using:

```bash
pip install -r dev-requirements.txt
```

This includes:

* `bump-my-version` – version bumping for releases
* More tools will be added later

These tools ensure the SDK maintains a clean, consistent, and reliable codebase.

---

## 🌐 API URL Configuration

Most SDK functions include an optional `API_URL` parameter with a default value:

```python
API_URL: str = DEFAULT_API_URL
```

By default, all API requests are sent to the global constant `DEFAULT_API_URL`, which is typically defined in the SDK as:

```python
DEFAULT_API_URL = "https://dev.polyteia.com"
```

If you're working with a different environment (e.g. production), you can override the default by passing a custom URL into any function call:

```python
API_URL = "https://prd.polyteia.com"
api.get_org_access_token(org_id="org_xyz", PAK=PAK, API_URL=API_URL)
```

> 💡 You only need to override `API_URL` when working outside the default development environment.

---

## 🧪 Toolkit Usage Examples

### Example 1: Authenticate & Get Access Token

Before using the API, authenticate using your organization ID and Personal Access Key (PAK):

```python
from gos_api_sdk import api_utils as api

# Replace with your organization ID and PAK
org_id = "org_xyz"
PAK = "your_personal_access_key"

# Get an access token for API operations
access_token = api.get_org_access_token(org_id=org_id, PAK=PAK)
```

### Example 2: Create a Dataset

Use the access token to create a dataset under a specific solution:

```python
# Define dataset details
solution_id = "sol_123"
dataset_name = "dataset_123"
dataset_description = "Demo Description"
dataset_source = "demo_source"
dataset_slug = "unique_slug"

# Create the dataset
ds_id = api.create_dataset(
    solution_id=solution_id,
    name=dataset_name,
    description=dataset_description,
    source=dataset_source,
    slug=dataset_slug,
    access_token=access_token
)
```

### Example 3: Upload a DataFrame to the Dataset

Once the dataset is created, upload a Polars DataFrame using a generated upload token:

```python
import polars as pl

# Prepare your DataFrame
df = pl.DataFrame({
    "jahr": [2021, 2022],
    "betrag": [12345.67, 23456.78]
})

# Generate upload token
upload_token = api.generate_upload_token(
    ds_id=ds_id,
    content_type="application/vnd.apache.parquet",
    access_token=access_token
)

# Upload the file to the dataset
api.upload_file(upload_token, df, access_token=access_token)
```

---

## 📤 Supported Input and Output Types

### Input types for Uploading Data

The SDK’s `upload_file()` function accepts a variety of input types, giving you flexibility across different data processing workflows. Internally, all inputs are converted to a `pyarrow.Table` for efficient Parquet-based upload.

| Format Type       | Accepted Object         | Notes                                       |
| ----------------- | ----------------------- | ------------------------------------------- |
| Polars DataFrame  | `polars.DataFrame`      | Recommended internal format                 |
| Pandas DataFrame  | `pandas.DataFrame`      | Automatically converted to Arrow            |
| PyArrow Table     | `pyarrow.Table`         | Used as-is                                  |
| PySpark DataFrame | `pyspark.sql.DataFrame` | Automatically converted via `.toPandas()`   |
| List of dicts     | `list[dict]`            | Each dict = one row                         |
| Single dictionary | `dict`                  | Treated as a single-row table               |
| List of lists     | `list[list]`            | Requires `columns` argument to infer schema |
| NumPy array       | `numpy.ndarray` (2D)    | Requires `columns` argument                 |

> 💡 When using list-of-lists or NumPy arrays, the `columns` parameter **must be provided** to infer column names.

### Output Types for Downloading Data

The `download_file()` function supports multiple return formats based on the `output_format` argument. By default, it returns a `polars.DataFrame`.

| Output Format    | Argument Value | Returned Object    |
| ---------------- | -------------- | ------------------ |
| Polars DataFrame | `"polars"`     | `polars.DataFrame` |
| Pandas DataFrame | `"pandas"`     | `pandas.DataFrame` |
| PyArrow Table    | `"arrow"`      | `pyarrow.Table`    |

---

## ⚠️ Error Handling

All SDK functions use a shared utility — `handle_api_response()` — to consistently manage API responses.

This function ensures:

* Safe JSON parsing (with clear errors on invalid responses)
* HTTP status code validation (supports expected codes like 200/201)
* Nested key checks (e.g. ensure `"data"` or `"data.token"` exists)
* Context-specific, descriptive exception messages for debugging

### 🔍 How It Works

`handle_api_response()` inspects every API response to ensure:

* The response is valid JSON
* The status code is expected (200, 201 by default)
* Any specified keys (like `"data"` or `"data.id"`) exist
* If any of these checks fail, it raises a detailed, contextual exception

### 💥 Example Errors

#### Identified Errors (Invalid Input or Auth)

```bash
# Wrong org_id (not found)
Exception: Get org access token for org_cvrpb2l5460gkf91b failed (HTTP 404):
{'code': 404, 'message': 'not found'}

# Wrong PAK (unauthorized)
Exception: Get org access token for org_cvrpb2l5460gkf91borg failed (HTTP 401):
{'error': 'Unauthorized'}
```

#### Unidentified Error (Unexpected Backend Response)

```bash
# Dataset creation with existing slug, response missing 'data'
Exception: Create dataset failed: Missing key 'data' in response:
{'error': {'code': 500, 'message': 'internal error'}}
```

By handling all errors through one centralized method, the SDK ensures consistent, debuggable behavior across every function — whether it returns data or not.

---

## 🤝 Contributing

### 🧩 Adding Functions to the Toolkit

When adding a new function to the SDK, follow these conventions:

#### Use `handle_api_response()` for API calls

Always wrap the response with `handle_api_response()` to ensure:

* Consistent error handling
* Clear debugging
* Safe key access even when the function returns `None`
* Define the `context` in the function call as needed for the respective description

**Example:**

```python
def delete_dataset(ds_id: str, access_token: str, API_URL: str = DEFAULT_API_URL) -> None:
    headers = {"Authorization": f"Bearer {access_token}"}
    payload = {"command": "delete_dataset", "params": {"id": ds_id}}

    response = requests.post(f"{API_URL}/api", headers=headers, json=payload)
    handle_api_response(response, context="Delete dataset")
```

#### Add the function to `__init__.py`

Make the function importable at the package level by adding it to `gos_api_sdk/__init__.py`.

**Example:**

```python
# gos_api_sdk/__init__.py

from .api_utils import (
    hello_world,
    new_added_function
)

__all__ = [
    "hello_world",
    "new_added_function"
]
```

Adhering to these standards ensures the SDK remains stable, testable, and easy to extend.

### 🔁 Push & Merge Workflow

Pull requests are welcome!

Please **do not push directly to `main`**. Always:

1. Create a **feature branch** for your changes.
2. Open a **merge request** targeting `main`.
3. **Mention the TOOLKIT maintainers/admins as reviewers** in your merge request for review and approval.

Following this process ensures code quality, visibility, and safe collaboration.

---

## 🔁 CI/CD & Release Management

### 📌 Version Bumping

Versioning for this SDK is handled automatically using [`bump-my-version`](https://github.com/callowayproject/bump-my-version) as part of the release process.

* When a new feature branch is merged into `main`, or when changes are pushed directly to `main`, the version is bumped based on the nature of the update (patch, minor, or major).
* This ensures every production-ready build from `main` has a unique, traceable version identifier.

The merge/push automatically updates the version in all relevant files:

* `pyproject.toml`
* `setup.py`

> ℹ️ Contributors do not need to manually run version bumping commands. Versioning is managed by the SDK maintainers as part of the release process.

---

## ✅ Testing

Tests can be placed in `gos_api_sdk/testing/`:

```bash
pytest gos_api_sdk/testing/
```

> Note: this folder is excluded via `.gitignore`.

---

## 🛠 Future Development Roadmap

* Additional API communication functions will be added as needed, based on evolving project requirements.
* CI/CD improvements are planned, including:

  * Linting and formatting checks
  * Automated testing
  * GitHub Actions for release pipelines and quality gates

---

## 📝 License

Polyteia License
