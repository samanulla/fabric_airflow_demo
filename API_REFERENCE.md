# API Reference

Complete API reference for the Fabric Airflow API Client library.

## Table of Contents

- [Configuration](#configuration)
- [Files API](#files-api)
- [Control Plane API](#control-plane-api)
- [Airflow Native API](#airflow-native-api)
- [CRUD API](#crud-api)
- [Exception Classes](#exception-classes)
- [Model Classes](#model-classes)

---

## Configuration

### Config Class

**Module**: `fabric.airflow.client.base.config`

Central configuration class for managing API clients using the Singleton pattern.

#### Class Methods

##### `setup(config_file: Optional[Union[str, Path]] = None) -> Config`

Initialize the singleton Config instance from a configuration file or environment variables.

**Parameters:**
- `config_file` (Optional[Union[str, Path]]): Path to INI configuration file. If None, loads from environment variables.

**Returns:**
- `Config`: Initialized Config instance

**Raises:**
- `FileNotFoundError`: If config file is specified but doesn't exist
- `ConfigurationError`: If file cannot be read or parsed

**Example:**
```python
# From file
Config.setup('config.ini')

# From environment
Config.setup()
```

##### `get_instance() -> Config`

Get or create the singleton Config instance.

**Returns:**
- `Config`: Singleton Config instance

##### `set_instance(config: Config)`

Set the singleton Config instance.

**Parameters:**
- `config` (Config): Config instance to set as singleton

##### `files_client() -> AirflowFilesApiClient`

Get Files API client from singleton instance.

**Returns:**
- `AirflowFilesApiClient`: Pre-configured Files API client

##### `control_plane_client() -> FabricControlPlaneApiClient`

Get Control Plane API client from singleton instance.

**Returns:**
- `FabricControlPlaneApiClient`: Pre-configured Control Plane API client

##### `airflow_native_client() -> AirflowApiClient`

Get Airflow Native API client from singleton instance.

**Returns:**
- `AirflowApiClient`: Pre-configured Airflow Native API client

##### `crud_client() -> AirflowCrudApiClient`

Get CRUD API client from singleton instance.

**Returns:**
- `AirflowCrudApiClient`: Pre-configured CRUD API client

#### Instance Methods

##### `get_files_client() -> AirflowFilesApiClient`

Get or create Files API client instance.

##### `get_control_plane_client() -> FabricControlPlaneApiClient`

Get or create Control Plane API client instance.

##### `get_airflow_native_client() -> AirflowApiClient`

Get or create Airflow Native API client instance.

##### `get_crud_client() -> AirflowCrudApiClient`

Get or create CRUD API client instance.

#### Properties

##### `tenant_id: str`

Azure AD tenant ID (read-only).

##### `workspace_id: str`

Workspace ID (read-only).

##### `airflow_job_id: str`

Airflow job ID (read-only).

##### `fabric_base_url: str`

Fabric API base URL (read-only).

##### `airflow_webserver_url: str`

Airflow webserver URL (read-only).

---

## Files API

### AirflowFilesApiClient

**Module**: `fabric.airflow.client.fabric_files_api_client`

Client for managing files in Airflow (DAGs, plugins, requirements).

#### Methods

##### `create_or_update_file(file_path: str, content: Union[str, bytes]) -> ApiResponse`

Create or update a file in Airflow.

**Parameters:**
- `file_path` (str): Relative path to the file (e.g., `"dags/my_dag.py"`)
- `content` (Union[str, bytes]): File content (text or binary)

**Returns:**
- `ApiResponse`: Response object with status and body

**Raises:**
- `ValidationError`: Invalid file path or content (400)
- `AuthenticationError`: Authentication failed (401)
- `ForbiddenError`: Insufficient permissions (403)
- `APIError`: Other API errors

**Example:**
```python
# Text file
with open('my_dag.py', 'r') as f:
    content = f.read()
response = files_client.create_or_update_file('dags/my_dag.py', content)

# Binary file
with open('plugin.so', 'rb') as f:
    content = f.read()
response = files_client.create_or_update_file('plugins/plugin.so', content)
```

##### `get_file(file_path: str) -> ApiResponse`

Download a file from Airflow.

**Parameters:**
- `file_path` (str): Relative path to the file

**Returns:**
- `ApiResponse`: Response with file content in body (bytes)

**Raises:**
- `NotFoundError`: File not found (404)
- `AuthenticationError`: Authentication failed (401)
- `ForbiddenError`: Insufficient permissions (403)

**Example:**
```python
response = files_client.get_file('dags/my_dag.py')
content = response.body.decode('utf-8')  # For text files
```

##### `delete_file(file_path: str) -> ApiResponse`

Delete a file from Airflow.

**Parameters:**
- `file_path` (str): Relative path to the file

**Returns:**
- `ApiResponse`: Response object

**Raises:**
- `NotFoundError`: File not found (404)
- `AuthenticationError`: Authentication failed (401)
- `ForbiddenError`: Insufficient permissions (403)

**Example:**
```python
response = files_client.delete_file('dags/my_dag.py')
```

##### `list_files(root_path: str) -> ApiResponse`

List files in a directory.

**Parameters:**
- `root_path` (str): Directory path (e.g., `"dags"`, `"plugins"`, `"/"`)

**Returns:**
- `ApiResponse`: Response with list of files in body

**Response Body Structure:**
```json
{
  "files": [
    {
      "filePath": "dags/my_dag.py",
      "sizeInBytes": 1024
    }
  ]
}
```

**Example:**
```python
response = files_client.list_files(root_path='dags')
for file in response.body.get('files', []):
    print(f"{file['filePath']}: {file['sizeInBytes']} bytes")
```

##### `list_items_in_directory(directory_path: str) -> ApiResponse`

List items in a specific directory.

**Parameters:**
- `directory_path` (str): Directory path

**Returns:**
- `ApiResponse`: Response with directory items

---

## Control Plane API

### FabricControlPlaneApiClient

**Module**: `fabric.airflow.client.fabric_control_plane_api_client`

Client for managing Airflow workspace settings and pool templates.

#### Methods

##### `get_workspace_settings() -> AirflowWorkspaceSettings`

Get workspace settings for the configured workspace and Airflow job.

**Returns:**
- `AirflowWorkspaceSettings`: Workspace settings object

**Example:**
```python
settings = cp_client.get_workspace_settings()
print(f"Settings: {settings}")
```

##### `create_pool_template(pool: AirflowPoolTemplate) -> str`

Create a new pool template.

**Parameters:**
- `pool` (AirflowPoolTemplate): Pool template configuration

**Returns:**
- `str`: Pool template ID

**Raises:**
- `ValidationError`: Invalid pool configuration (400)
- `AuthenticationError`: Authentication failed (401)
- `ForbiddenError`: Insufficient permissions (403)

**Example:**
```python
from fabric.airflow.client.fabric_control_plane_model import (
    AirflowPoolTemplate, WorkerScalability
)

pool = AirflowPoolTemplate(
    poolTemplateName="MyPool",
    nodeSize="Small",
    workerScalability=WorkerScalability(minNodeCount=1, maxNodeCount=3),
    apacheAirflowJobVersion="1.0.0"
)
pool_id = cp_client.create_pool_template(pool)
```

##### `get_pool_template(pool_id: str) -> AirflowPoolTemplate`

Get a specific pool template by ID.

**Parameters:**
- `pool_id` (str): Pool template ID

**Returns:**
- `AirflowPoolTemplate`: Pool template object

**Raises:**
- `NotFoundError`: Pool not found (404)

##### `list_pool_templates() -> AirflowPoolsTemplate`

List all pool templates.

**Returns:**
- `AirflowPoolsTemplate`: Object containing list of pool templates

**Example:**
```python
pools = cp_client.list_pool_templates()
for pool in pools.value:
    print(f"Pool: {pool.poolTemplateName}")
```

##### `update_pool_template(pool_id: str, pool: AirflowPoolTemplate) -> str`

Update an existing pool template.

**Parameters:**
- `pool_id` (str): Pool template ID
- `pool` (AirflowPoolTemplate): Updated pool configuration

**Returns:**
- `str`: Pool template ID

##### `delete_pool_template(pool_id: str) -> ApiResponse`

Delete a pool template.

**Parameters:**
- `pool_id` (str): Pool template ID

**Returns:**
- `ApiResponse`: Response object

---

## Airflow Native API

### AirflowApiClient

**Module**: `fabric.airflow.client.airflow_api_client`

Client for interacting with Airflow REST API endpoints.

#### Methods

##### `list_dags(limit: int = 100, offset: int = 0) -> ApiResponse`

List DAGs from Airflow.

**Parameters:**
- `limit` (int): Maximum number of DAGs to return (default: 100)
- `offset` (int): Number of DAGs to skip (default: 0)

**Returns:**
- `ApiResponse`: Response with DAG list

**Response Body Structure:**
```json
{
  "dags": [
    {
      "dag_id": "my_dag",
      "is_active": true,
      "is_paused": false
    }
  ],
  "total_entries": 10
}
```

**Example:**
```python
response = native_client.list_dags(limit=10)
for dag in response.body['dags']:
    print(f"DAG: {dag['dag_id']}, Active: {dag['is_active']}")
```

##### `trigger_dag_run(dag_id: str, conf: Optional[Dict] = None) -> ApiResponse`

Trigger a DAG run.

**Parameters:**
- `dag_id` (str): DAG identifier
- `conf` (Optional[Dict]): Configuration for the DAG run

**Returns:**
- `ApiResponse`: Response with DAG run details

**Example:**
```python
response = native_client.trigger_dag_run(
    'my_dag',
    conf={'param1': 'value1'}
)
```

##### `get_dag_runs(dag_id: str, limit: int = 25) -> ApiResponse`

Get DAG runs for a specific DAG.

**Parameters:**
- `dag_id` (str): DAG identifier
- `limit` (int): Maximum number of runs to return

**Returns:**
- `ApiResponse`: Response with DAG runs

---

## CRUD API

### AirflowCrudApiClient

**Module**: `fabric.airflow.client.fabric_crud_api_client`

Client for creating and managing Airflow jobs.

#### Methods

##### `create_airflow_job(workspace_id: str, request: AirflowItemRequest) -> AirflowItem`

Create a simple Airflow job.

**Parameters:**
- `workspace_id` (str): Workspace ID
- `request` (AirflowItemRequest): Job creation request

**Returns:**
- `AirflowItem`: Created Airflow job object

**Example:**
```python
from fabric.airflow.client.fabric_crud_model import AirflowItemRequest

request = AirflowItemRequest(
    displayName="MyAirflowJob",
    description="Created via API"
)
airflow_item = crud_client.create_airflow_job(workspace_id, request)
print(f"Created job: {airflow_item.id}")
```

##### `create_airflow_job_with_definition(workspace_id: str, definition: FabricItemDefinition) -> AirflowItem`

Create an Airflow job with full definition (DAGs, environment variables, etc.).

**Parameters:**
- `workspace_id` (str): Workspace ID
- `definition` (FabricItemDefinition): Complete job definition

**Returns:**
- `AirflowItem`: Created Airflow job object

**Example:**
```python
from fabric.airflow.client.fabric_crud_model import (
    FabricItemDefinition, AirflowDefinition
)

definition = FabricItemDefinition(
    displayName="MyAirflowJobWithDef",
    description="With environment and DAGs"
)

airflow_def = AirflowDefinition()
airflow_def.airflowProperties.add_environment_variable("MY_VAR", "value")
definition.add_airflow_definition(airflow_def)
definition.add_dag_from_file('/dags/my_dag.py', 'local/path/to/dag.py')

airflow_item = crud_client.create_airflow_job_with_definition(workspace_id, definition)
```

##### `update_airflow_job_definition(workspace_id: str, airflow_id: str, definition: FabricItemDefinition, update_metadata: bool = False) -> ApiResponse`

Update an existing Airflow job's definition.

**Parameters:**
- `workspace_id` (str): Workspace ID
- `airflow_id` (str): Airflow job ID
- `definition` (FabricItemDefinition): Updated definition
- `update_metadata` (bool): Whether to update metadata (default: False)

**Returns:**
- `ApiResponse`: Response object

##### `list_airflow_jobs(workspace_id: str) -> ApiResponse`

List all Airflow jobs in a workspace.

**Parameters:**
- `workspace_id` (str): Workspace ID

**Returns:**
- `ApiResponse`: Response with list of Airflow jobs

##### `delete_airflow_job(workspace_id: str, airflow_id: str) -> ApiResponse`

Delete an Airflow job.

**Parameters:**
- `workspace_id` (str): Workspace ID
- `airflow_id` (str): Airflow job ID

**Returns:**
- `ApiResponse`: Response object

---

## Exception Classes

**Module**: `fabric.airflow.client.base.api_exceptions`

### APIError

Base exception for all API errors.

**Attributes:**
- `message` (str): Error message
- `status` (int): HTTP status code
- `request_id` (Optional[str]): Request ID for troubleshooting
- `body` (Optional[Any]): Full response body

### ValidationError

Raised for HTTP 400 Bad Request errors.

### AuthenticationError

Raised for HTTP 401 Unauthorized errors.

### ForbiddenError

Raised for HTTP 403 Forbidden errors.

### NotFoundError

Raised for HTTP 404 Not Found errors.

### ClientError

Base exception for 4xx client errors.

### ServerError

Base exception for 5xx server errors.

**Example:**
```python
from fabric.airflow.client.base.api_exceptions import (
    NotFoundError, AuthenticationError, APIError
)

try:
    response = files_client.get_file('dags/my_dag.py')
except NotFoundError as e:
    print(f"File not found: {e.message}")
    print(f"Request ID: {e.request_id}")
except AuthenticationError as e:
    print(f"Auth failed: {e.message}")
except APIError as e:
    print(f"API error [{e.status}]: {e.message}")
```

---

## Model Classes

### ApiResponse

**Module**: `fabric.airflow.client.base.api_helper`

Response object returned by API calls.

**Attributes:**
- `status` (int): HTTP status code
- `body` (Any): Response body (parsed JSON or bytes)
- `headers` (Dict): Response headers

### AirflowPoolTemplate

**Module**: `fabric.airflow.client.fabric_control_plane_model`

Pool template configuration.

**Attributes:**
- `poolTemplateName` (str): Pool name
- `nodeSize` (str): Node size ("Small", "Medium", "Large")
- `workerScalability` (WorkerScalability): Scalability configuration
- `apacheAirflowJobVersion` (str): Airflow version

### WorkerScalability

**Module**: `fabric.airflow.client.fabric_control_plane_model`

Worker scalability configuration.

**Attributes:**
- `minNodeCount` (int): Minimum number of nodes
- `maxNodeCount` (int): Maximum number of nodes

### AirflowItemRequest

**Module**: `fabric.airflow.client.fabric_crud_model`

Simple Airflow job creation request.

**Attributes:**
- `displayName` (str): Job display name
- `description` (Optional[str]): Job description

### FabricItemDefinition

**Module**: `fabric.airflow.client.fabric_crud_model`

Complete Airflow job definition.

**Attributes:**
- `displayName` (str): Job display name
- `description` (Optional[str]): Job description

**Methods:**
- `add_airflow_definition(definition: AirflowDefinition)`: Add Airflow configuration
- `add_dag_from_file(dag_path: str, local_file_path: str)`: Add DAG from local file

### AirflowDefinition

**Module**: `fabric.airflow.client.fabric_crud_model`

Airflow configuration definition.

**Attributes:**
- `airflowProperties` (AirflowProperties): Airflow properties

### AirflowProperties

**Module**: `fabric.airflow.client.fabric_crud_model`

Airflow properties (environment variables, requirements, etc.).

**Methods:**
- `add_environment_variable(key: str, value: str)`: Add environment variable
- `add_requirement(package: str)`: Add Python package requirement

---

## Type Hints

All methods include comprehensive type hints for better IDE support:

```python
from typing import Optional, Union, Dict, Any
from pathlib import Path

def create_or_update_file(
    self,
    file_path: str,
    content: Union[str, bytes]
) -> ApiResponse:
    ...
```

---

## Best Practices

1. **Always handle exceptions**: Use specific exception types for better error handling
2. **Reuse clients**: Use the Config singleton pattern to reuse client instances
3. **Enable debug logging**: Set `debug=True` during development
4. **Use type hints**: Leverage IDE autocomplete with provided type hints
5. **Check response status**: Always check `response.status` before processing
6. **Use request IDs**: Include `request_id` when reporting errors

---

For more examples, see:
- [README.md](README.md) - Quick start and overview
- [CONFIG_GUIDE.md](CONFIG_GUIDE.md) - Detailed configuration guide
- [src/sample/example_usage.py](src/sample/example_usage.py) - Complete usage examples
