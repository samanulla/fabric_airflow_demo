# Fabric Airflow API Client

A Python client library for Microsoft Fabric Airflow APIs, providing a comprehensive and easy-to-use interface for managing Apache Airflow resources in Microsoft Fabric.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [API Clients](#api-clients)
- [Usage Examples](#usage-examples)
- [Error Handling](#error-handling)
- [Best Practices](#best-practices)
- [API Reference](#api-reference)
- [Contributing](#contributing)
- [License](#license)

## Features

✨ **Comprehensive API Coverage**
- Files API - Upload, download, and manage DAG files, plugins, and requirements
- Control Plane API - Manage workspace settings and pool templates
- Airflow Native API - Interact with Airflow REST API endpoints
- CRUD API - Create and manage Airflow jobs and definitions

🔒 **Security First**
- Service Principal (SPN) authentication with Azure AD
- Automatic token refresh with caching
- Environment variable support for sensitive credentials
- No hardcoded secrets in code

⚙️ **Easy Configuration**
- Singleton pattern for consistent configuration
- Support for configuration files (INI format)
- Environment variable fallback
- Pre-configured client factories

🛡️ **Robust Error Handling**
- Specific exception types for different HTTP status codes
- Request ID tracking for troubleshooting
- Detailed error messages with context

📝 **Developer Friendly**
- Type hints throughout
- Comprehensive documentation
- Example code and sample DAGs
- Extensive test coverage

## Installation

### From Source

```bash
git clone <repository-url>
cd ApiTest
pip install -e .
```

### Requirements

- Python >= 3.8
- MSAL >= 1.27
- Azure Identity >= 1.17
- Requests >= 2.31

## Quick Start

### 1. Set up Configuration

Create a `config.ini` file:

```ini
[DEFAULT]
# Azure AD authentication
tenant_id = your-tenant-id
client_id = your-client-id
client_secret = your-client-secret

# Workspace configuration
workspace_id = your-workspace-id
airflow_job_id = your-airflow-job-id

# API endpoints
fabric_base_url = https://api.fabric.microsoft.com
airflow_webserver_url = https://your-airflow.com

# Authentication scopes
airflow_api_scope = scope/.default
fabric_api_scope = https://api.fabric.microsoft.com/.default

# Options
debug = true
is_preview_enabled = true
```

### 2. Initialize and Use

```python
from fabric.airflow.client.base.config import Config

# Initialize from config file
Config.setup('config.ini')

# Get pre-configured clients
files_client = Config.files_client()
control_plane_client = Config.control_plane_client()

# Upload a DAG file
with open('my_dag.py', 'r') as f:
    dag_content = f.read()
    
response = files_client.create_or_update_file('dags/my_dag.py', dag_content)
print(f"DAG uploaded: {response.status}")
```

## Configuration

### Configuration Methods

#### Method 1: Configuration File (Recommended for Development)

```python
from fabric.airflow.client.base.config import Config

# Load from INI file
Config.setup('config.ini')

# Use clients
files_client = Config.files_client()
```

#### Method 2: Environment Variables (Recommended for Production)

```bash
export FABRIC_TENANT_ID="your-tenant-id"
export FABRIC_CLIENT_ID="your-client-id"
export FABRIC_CLIENT_SECRET="your-client-secret"
export FABRIC_WORKSPACE_ID="your-workspace-id"
export FABRIC_AIRFLOW_JOB_ID="your-airflow-job-id"
```

```python
from fabric.airflow.client.base.config import Config

# Initialize from environment variables
Config.setup()

# Use clients
files_client = Config.files_client()
```

#### Method 3: Direct Configuration (For Testing)

```python
from fabric.airflow.client.base.config import Config

config = Config(
    tenant_id="your-tenant-id",
    client_id="your-client-id",
    client_secret="your-client-secret",
    workspace_id="your-workspace-id",
    airflow_job_id="your-airflow-job-id",
    debug=True
)

Config.set_instance(config)
files_client = Config.files_client()
```

### Environment Variables

| Variable | Description | Required | Default |
|----------|-------------|----------|---------|
| `FABRIC_TENANT_ID` | Azure AD tenant ID | Yes | - |
| `FABRIC_CLIENT_ID` | Application client ID | Yes | - |
| `FABRIC_CLIENT_SECRET` | Application client secret | Yes | - |
| `FABRIC_WORKSPACE_ID` | Workspace ID | Yes* | - |
| `FABRIC_AIRFLOW_JOB_ID` | Airflow job ID | Yes* | - |
| `FABRIC_BASE_URL` | Fabric API base URL | No | `https://api.fabric.microsoft.com` |
| `AIRFLOW_WEBSERVER_URL` | Airflow webserver URL | Yes** | - |
| `AIRFLOW_API_SCOPE` | Airflow API scope | Yes** | - |
| `FABRIC_API_SCOPE` | Fabric API scope | No | `https://api.fabric.microsoft.com/.default` |
| `DEBUG` | Enable debug logging | No | `false` |

\* Required for workspace-scoped operations  
\** Required for Airflow Native API operations

## API Clients

### Files API Client

Manage files in Airflow (DAGs, plugins, requirements).

```python
files_client = Config.files_client()

# Upload a file
files_client.create_or_update_file('dags/my_dag.py', content)

# List files
response = files_client.list_files(root_path='dags')

# Download a file
response = files_client.get_file('dags/my_dag.py')

# Delete a file
files_client.delete_file('dags/my_dag.py')
```

### Control Plane API Client

Manage workspace settings and pool templates.

```python
from fabric.airflow.client.fabric_control_plane_model import (
    AirflowPoolTemplate, WorkerScalability
)

cp_client = Config.control_plane_client()

# Get workspace settings
settings = cp_client.get_workspace_settings()

# Create pool template
pool = AirflowPoolTemplate(
    poolTemplateName="MyPool",
    nodeSize="Small",
    workerScalability=WorkerScalability(minNodeCount=1, maxNodeCount=3),
    apacheAirflowJobVersion="1.0.0"
)
pool_id = cp_client.create_pool_template(pool)

# List pool templates
pools = cp_client.list_pool_templates()

# Delete pool template
cp_client.delete_pool_template(pool_id)
```

### Airflow Native API Client

Interact with Airflow REST API.

```python
native_client = Config.airflow_native_client()

# List DAGs
response = native_client.list_dags(limit=10)

# Trigger DAG run
response = native_client.trigger_dag_run('my_dag_id')

# Get DAG runs
response = native_client.get_dag_runs('my_dag_id')
```

### CRUD API Client

Create and manage Airflow jobs.

```python
from fabric.airflow.client.fabric_crud_model import (
    AirflowItemRequest, FabricItemDefinition, AirflowDefinition
)

crud_client = Config.crud_client()
workspace_id = Config.get_instance().workspace_id

# Create simple Airflow job
request = AirflowItemRequest(
    displayName="MyAirflowJob",
    description="Created via API"
)
airflow_item = crud_client.create_airflow_job(workspace_id, request)

# Create with definition
definition = FabricItemDefinition(
    displayName="MyAirflowJobWithDef",
    description="Created with definition"
)
airflow_def = AirflowDefinition()
airflow_def.airflowProperties.add_environment_variable("MY_VAR", "value")
definition.add_airflow_definition(airflow_def)
definition.add_dag_from_file('/dags/my_dag.py', 'path/to/local/my_dag.py')

airflow_item = crud_client.create_airflow_job_with_definition(workspace_id, definition)

# List Airflow jobs
response = crud_client.list_airflow_jobs(workspace_id)

# Update Airflow job definition
crud_client.update_airflow_job_definition(workspace_id, airflow_job_id, definition)

# Delete Airflow job
crud_client.delete_airflow_job(workspace_id, airflow_job_id)
```

## Usage Examples

Complete examples are available in the `src/sample` directory:

- **`example_usage.py`** - Comprehensive examples for all API clients
- **`sample_dag.py`** - Example Airflow DAG demonstrating common patterns
- **`config.ini`** - Sample configuration file

Run the examples:

```bash
# Ensure config.ini is set up with your credentials
python src/sample/example_usage.py
```

## Error Handling

The library provides specific exception types for different error scenarios:

```python
from fabric.airflow.client.base.api_exceptions import (
    ValidationError,      # 400 Bad Request
    AuthenticationError,  # 401 Unauthorized
    ForbiddenError,       # 403 Forbidden
    NotFoundError,        # 404 Not Found
    ClientError,          # 4xx errors
    ServerError,          # 5xx errors
    APIError              # Base exception
)

try:
    files_client.get_file('dags/my_dag.py')
except NotFoundError as e:
    print(f"File not found: {e.message}")
    print(f"Request ID: {e.request_id}")
except AuthenticationError as e:
    print(f"Authentication failed: {e.message}")
except APIError as e:
    print(f"API error: {e.message}")
```

All exceptions include:
- `message`: Human-readable error message
- `status`: HTTP status code
- `request_id`: Unique request identifier for troubleshooting
- `body`: Full response body (if available)

## Best Practices

### 1. Configuration Management

✅ **DO**: Use environment variables in production
```python
Config.setup()  # Loads from environment
```

❌ **DON'T**: Hardcode credentials
```python
# Bad practice
config = Config(client_secret="hardcoded-secret")
```

### 2. Resource Management

✅ **DO**: Reuse clients
```python
Config.setup('config.ini')
files_client = Config.files_client()  # Cached and reused
```

❌ **DON'T**: Create multiple Config instances
```python
# Bad practice
config1 = Config(...)
config2 = Config(...)  # Creates confusion
```

### 3. Error Handling

✅ **DO**: Handle specific exceptions
```python
try:
    client.get_file('dags/my_dag.py')
except NotFoundError:
    print("File doesn't exist")
except AuthenticationError:
    print("Auth failed")
```

❌ **DON'T**: Use broad exception catching
```python
# Bad practice
try:
    client.get_file('dags/my_dag.py')
except Exception:
    pass  # Swallows important errors
```

### 4. Logging

✅ **DO**: Enable debug logging during development
```python
import logging
logging.basicConfig(level=logging.DEBUG)

config = Config(debug=True)
```

### 5. Security

✅ **DO**: Add config files to `.gitignore`
```
config.ini
*.cfg
.env
```

✅ **DO**: Use Azure Key Vault in production
```python
from azure.keyvault.secrets import SecretClient

secret_client = SecretClient(...)
config = Config(
    tenant_id=secret_client.get_secret("tenant-id").value,
    client_secret=secret_client.get_secret("client-secret").value
)
```

## API Reference

### Config Class

**Methods:**
- `setup(config_file: Optional[str] = None) -> Config` - Initialize singleton from file or environment
- `get_instance() -> Config` - Get singleton instance
- `set_instance(config: Config)` - Set singleton instance
- `files_client() -> AirflowFilesApiClient` - Get Files API client
- `control_plane_client() -> FabricControlPlaneApiClient` - Get Control Plane API client
- `airflow_native_client() -> AirflowApiClient` - Get Airflow Native API client
- `crud_client() -> AirflowCrudApiClient` - Get CRUD API client

### Files API Methods

- `create_or_update_file(file_path: str, content: Union[str, bytes]) -> ApiResponse`
- `get_file(file_path: str) -> ApiResponse`
- `delete_file(file_path: str) -> ApiResponse`
- `list_files(root_path: str) -> ApiResponse`
- `list_items_in_directory(directory_path: str) -> ApiResponse`

### Control Plane API Methods

- `get_workspace_settings() -> AirflowWorkspaceSettings`
- `create_pool_template(pool: AirflowPoolTemplate) -> str`
- `get_pool_template(pool_id: str) -> AirflowPoolTemplate`
- `list_pool_templates() -> AirflowPoolsTemplate`
- `update_pool_template(pool_id: str, pool: AirflowPoolTemplate) -> str`
- `delete_pool_template(pool_id: str) -> ApiResponse`

### CRUD API Methods

- `create_airflow_job(workspace_id: str, request: AirflowItemRequest) -> AirflowItem`
- `create_airflow_job_with_definition(workspace_id: str, definition: FabricItemDefinition) -> AirflowItem`
- `update_airflow_job_definition(workspace_id: str, airflow_id: str, definition: FabricItemDefinition, update_metadata: bool = False) -> ApiResponse`
- `list_airflow_jobs(workspace_id: str) -> ApiResponse`
- `delete_airflow_job(workspace_id: str, airflow_id: str) -> ApiResponse`

## Testing

Run the test suite:

```bash
# Run all tests
python -m unittest discover tests

# Run specific test file
python -m unittest tests.test_files_api_client

# Run specific test
python -m unittest tests.test_files_api_client.TestFilesApiClientIntegration.test_files_client_type
```

### Test Configuration

Tests use configuration from `tests/config.ini`. Create this file with your test credentials:

```ini
[DEFAULT]
tenant_id = your-test-tenant-id
client_id = your-test-client-id
client_secret = your-test-client-secret
workspace_id = your-test-workspace-id
airflow_job_id = your-test-airflow-job-id
```

**Important**: Never commit `tests/config.ini` to version control.

## Project Structure

```
ApiTest/
├── src/
│   ├── fabric/airflow/client/
│   │   ├── base/
│   │   │   ├── api_helper.py              # Base API client
│   │   │   ├── airflow_api_helper.py      # Airflow-specific base
│   │   │   ├── authentication_provider.py # Auth handling
│   │   │   ├── api_exceptions.py          # Exception classes
│   │   │   └── config.py                  # Configuration management
│   │   ├── fabric_files_api_client.py     # Files API
│   │   ├── fabric_control_plane_api_client.py  # Control Plane API
│   │   ├── airflow_api_client.py          # Airflow Native API
│   │   ├── fabric_crud_api_client.py      # CRUD API
│   │   └── *.py                           # Model classes
│   └── sample/
│       ├── example_usage.py               # Usage examples
│       ├── sample_dag.py                  # Example DAG
│       └── config.ini                     # Sample config
├── tests/
│   ├── test_files_api_client.py
│   ├── test_airflow_api_client.py
│   ├── test_airflow_control_plane_api_pool_mgmt.py
│   └── config.ini                         # Test configuration
├── pyproject.toml                         # Project metadata
├── README.md                              # This file
├── CONFIG_GUIDE.md                        # Detailed configuration guide
└── .gitignore                             # Git ignore rules
```

## Contributing

Contributions are welcome! Please follow these guidelines:

1. **Code Style**: Follow PEP 8 conventions
2. **Type Hints**: Add type hints to all functions
3. **Documentation**: Update docstrings and README
4. **Tests**: Add tests for new features
5. **Security**: Never commit credentials or secrets

## Troubleshooting

### Common Issues

**Authentication Errors**
```
AuthenticationError: Failed to authenticate
```
- Verify `tenant_id`, `client_id`, and `client_secret` are correct
- Ensure the Service Principal has appropriate permissions
- Check if the token scope is correct

**Configuration Errors**
```
ConfigurationError: workspace_id is required
```
- Ensure all required configuration values are provided
- Check environment variables are set correctly
- Verify config.ini file exists and is readable

**Not Found Errors**
```
NotFoundError: File not found
```
- Verify the file path is correct (e.g., `dags/my_dag.py`)
- Check if the file exists in the Airflow environment
- Ensure you have access to the workspace

### Enable Debug Logging

```python
import logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

config = Config(debug=True)
```

### Get Request ID for Support

All API errors include a `request_id` for troubleshooting:

```python
try:
    client.get_file('dags/my_dag.py')
except APIError as e:
    print(f"Request ID: {e.request_id}")
    print(f"Error: {e.message}")
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues and questions:
- Open an issue on GitHub
- Contact the Fabric Airflow team
- Check the [Configuration Guide](CONFIG_GUIDE.md) for detailed setup instructions

## Changelog

### Version 0.0.1 (Initial Release)

- Files API client for DAG/plugin management
- Control Plane API client for workspace and pool operations
- Airflow Native API client for Airflow REST API
- CRUD API client for Airflow job management
- Configuration management with INI files and environment variables
- Comprehensive error handling with specific exception types
- Service Principal authentication with token caching
- Example code and sample DAGs
- Full test coverage
