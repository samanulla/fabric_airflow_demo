# Configuration Guide for Fabric Airflow API Clients

This guide explains how to configure and use the Fabric Airflow API clients with the centralized `Config` module.

## Overview

The `Config` module provides centralized configuration management with support for:
- ✅ Multi-environment configuration (DEV, TEST, PROD) from single INI file
- ✅ Environment variables for sensitive credentials (recommended for production)
- ✅ Direct configuration for testing/development
- ✅ Singleton pattern for consistent configuration
- ✅ Factory methods for creating pre-configured API clients
- ✅ Safe credential handling without exposing secrets in code

## Configuration Methods

### Method 1: Configuration File with Environment Variable (Recommended)

Use the `CONFIG_FILE_PATH` environment variable to point to your configuration file, and specify the environment section to use.

**Step 1:** Set the environment variable:

```powershell
# PowerShell (Windows)
$env:CONFIG_FILE_PATH = "c:\src\ApiTest\config.ini"

# For persistent setting, add to your PowerShell profile or use:
[System.Environment]::SetEnvironmentVariable('CONFIG_FILE_PATH', 'c:\src\ApiTest\config.ini', 'User')
```

```bash
# Bash (Linux/Mac)
export CONFIG_FILE_PATH="/path/to/config.ini"

# For persistent setting, add to ~/.bashrc or ~/.bash_profile
echo 'export CONFIG_FILE_PATH="/path/to/config.ini"' >> ~/.bashrc
```

**Step 2:** Load configuration in your code:

```python
import os
from fabric.airflow.client.base.config import Config

# Initialize Config from environment variable
# Use 'DEV', 'TEST', or 'PROD' environment, or omit for DEFAULT
Config.setup(os.getenv('CONFIG_FILE_PATH'), 'DEV')

# Now use pre-configured clients anywhere in your application
files_client = Config.files_client()
control_plane_client = Config.control_plane_client()
airflow_native_client = Config.airflow_native_client()
crud_client = Config.crud_client()

# Use the clients
workspace_settings = control_plane_client.get_workspace_settings()
print(f"Workspace settings: {workspace_settings}")
```

### Method 2: Configuration File with Direct Path

You can also specify the config file path directly:

```python
from fabric.airflow.client.base.config import Config

# Initialize Config from file with specific environment
Config.setup('config.ini', 'PROD')

# Or use DEFAULT section
Config.setup('config.ini')
```

**Multi-Environment INI File Format** (`config.ini`):
```ini
# Configuration file with multi-environment support
# Set CONFIG_FILE_PATH environment variable to point to this file

[DEFAULT]
# Shared configuration (inherited by all environments)
# Azure AD authentication
tenant_id = your-tenant-id
client_id = your-client-id
client_secret = your-client-secret

# Authentication scopes
airflow_api_scope = 5d13f7d7-0567-429c-9880-320e9555e5fc/.default
fabric_api_scope = https://api.fabric.microsoft.com/.default

# Default options
debug = false

[DEV]
# Development environment
workspace_id = your-dev-workspace-id
airflow_job_id = your-dev-airflow-job-id
fabric_base_url = https://api.fabric.microsoft.com
airflow_webserver_url = https://your-dev-webserver.airflow.svc.datafactory.azure.com/

[TEST]
# Test environment
workspace_id = your-test-workspace-id
airflow_job_id = your-test-airflow-job-id
fabric_base_url = https://dailyapi.fabric.microsoft.com
airflow_webserver_url = https://your-test-webserver.airflow.svc.datafactory.azure.com/
debug = true
is_preview_enabled = true

[PROD]
# Production environment
workspace_id = your-prod-workspace-id
airflow_job_id = your-prod-airflow-job-id
fabric_base_url = https://api.fabric.microsoft.com
airflow_webserver_url = https://your-prod-webserver.airflow.svc.datafactory.azure.com/
```

**Environment Section Inheritance:**
- Values in `DEFAULT` section are inherited by all environment sections (DEV, TEST, PROD)
- Environment-specific sections override DEFAULT values
- This allows sharing SPN credentials across environments while customizing workspace/job IDs
- If no environment is specified in `Config.setup()`, DEFAULT section is used

**Note:** Only INI format (`.ini` or `.cfg` extensions) is supported for configuration files.

### Method 3: Environment Variables (Alternative for Production)

Instead of using a config file, you can set environment variables directly. Call `Config.setup()` without parameters to use environment variables:

```bash
# Required for authentication
export FABRIC_TENANT_ID="your-tenant-id"
export FABRIC_CLIENT_ID="your-client-id"
export FABRIC_CLIENT_SECRET="your-client-secret"

# Required for workspace operations
export FABRIC_WORKSPACE_ID="your-workspace-id"
export FABRIC_AIRFLOW_JOB_ID="your-airflow-job-id"

# Optional - defaults provided
export FABRIC_BASE_URL="https://api.fabric.microsoft.com"
export FABRIC_API_SCOPE="https://api.fabric.microsoft.com/.default"
export AIRFLOW_WEBSERVER_URL="https://your-airflow-url.com"
export AIRFLOW_API_SCOPE="your-airflow-scope/.default"
export DEBUG="true"
```

Then use the singleton pattern in your code:

```python
from fabric.airflow.client.base.config import Config

# Initialize from environment variables (call setup with no arguments)
Config.setup()

# Get pre-configured clients (config loaded from environment)
files_client = Config.files_client()
control_plane_client = Config.control_plane_client()
airflow_native_client = Config.airflow_native_client()
crud_client = Config.crud_client()

# Use the clients
workspace_settings = control_plane_client.get_workspace_settings()
print(f"Workspace settings: {workspace_settings}")
```

**Note:** Calling `Config.setup()` without arguments automatically falls back to environment variables.

### Method 3: Direct Configuration (For Testing/Development)

Create a config instance with credentials:

```python
from fabric.airflow.client.base.config import Config

# Create config with explicit values
config = Config(
    tenant_id="your-tenant-id",
    client_id="your-client-id",
    client_secret="your-client-secret",
    workspace_id="your-workspace-id",
    airflow_job_id="your-airflow-job-id",
    fabric_base_url="https://api.fabric.microsoft.com",
    debug=True
)

# Get clients from the config instance
files_client = config.get_files_client()
control_plane_client = config.get_control_plane_client()
crud_client = config.get_crud_client()

# Or set as singleton for global access
Config.set_instance(config)

# Now can use class methods anywhere
files_client = Config.files_client()
```

### Method 4: Hybrid Approach

The `setup()` method with file argument takes precedence, but values not in the file will fall back to environment variables:

```python
from fabric.airflow.client.base.config import Config
import os

# Set some values in environment (these act as defaults/fallbacks)
os.environ['DEBUG'] = 'true'
os.environ['FABRIC_BASE_URL'] = 'https://dailyapi.fabric.microsoft.com'

# Load from file (file values override environment)
# But if a value is missing from the file, it will use environment variable
Config.setup('config.ini')

# Get clients
files_client = Config.files_client()
```

Alternatively, you can switch between file and environment-based config:

```python
from fabric.airflow.client.base.config import Config

# For development: use config file
if os.getenv('ENVIRONMENT') == 'development':
    Config.setup('config.ini')
else:
    # For production: use environment variables
    Config.setup()

# Same code works for both
files_client = Config.files_client()
```

## Available Clients

### 1. Files API Client

For file operations (upload/download DAGs, plugins, requirements):

```python
from fabric.airflow.client.base.config import Config

files_client = Config.files_client()

# Upload a DAG file
with open("my_dag.py", "rb") as f:
    files_client.upload_file("/dags/my_dag.py", f.read())

# List files
response = files_client.list_items_in_directory("/dags")
```

### 2. Control Plane API Client

For environment and workspace operations:

```python
from fabric.airflow.client.base.config import Config

cp_client = Config.control_plane_client()

# Get workspace settings
settings = cp_client.get_workspace_settings()

# Create pool template
from fabric.airflow.client.airflow_control_plane_model import AirflowPoolTemplate
pool = AirflowPoolTemplate(
    poolTemplateName="MyPool",
    nodeSize="Small",
    # ...
)
pool_id = cp_client.create_pool_template(pool)
```

### 3. Airflow Native API Client

For Airflow REST API operations:

```python
from fabric.airflow.client.base.config import Config

native_client = Config.airflow_native_client()

# List DAGs
response = native_client.list_dags(limit=10)

# Trigger DAG run
response = native_client.trigger_dag_run("my_dag_id")
```

### 4. CRUD API Client

For creating/managing Airflow jobs:

```python
from fabric.airflow.client.base.config import Config
from fabric_model import AirflowItemRequest

crud_client = Config.crud_client()

# Create Airflow job
request = AirflowItemRequest(
    displayName="MyAirflowJob",
    description="Created via API"
)
workspace_id = Config.get_instance().workspace_id
airflow_item = crud_client.create_airflow_job(workspace_id, request)
```

## Configuration Properties

Access configuration values:

```python
from fabric.airflow.client.base.config import Config

config = Config.get_instance()

# Access properties
print(f"Workspace ID: {config.workspace_id}")
print(f"Airflow Job ID: {config.airflow_job_id}")
print(f"Fabric Base URL: {config.fabric_base_url}")
print(f"Tenant ID: {config.tenant_id}")
```

## Error Handling

The config module will raise `ConfigurationError` if required values are missing:

```python
from fabric.airflow.client.base.config import Config, ConfigurationError

try:
    config = Config()  # No tenant_id provided
    client = config.get_files_client()
except ConfigurationError as e:
    print(f"Configuration error: {e}")
    # Handle missing configuration
```

## Best Practices

1. **Production**: Use environment variables (`Config.setup()` with no arguments) to avoid hardcoding credentials
2. **Development**: Use INI config file (`Config.setup('config.ini')`) for convenience
3. **Testing**: Use the `tests/config.py` pattern with test credentials
4. **Version Control**: Never commit `config.ini` or credentials to version control (add to `.gitignore`)
5. **Secrets Management**: Use Azure Key Vault or similar for production secrets
6. **File Format**: Only INI format is supported - simple, built-in, no external dependencies

## Example: Complete Application

```python
import os
from fabric.airflow.client.base.config import Config

# Initialize configuration
# Option 1: From file (development)
Config.setup('config.ini')

# Option 2: From environment (production)
# Config.setup()

def main():
    # Get clients
    files_client = Config.files_client()
    cp_client = Config.control_plane_client()
    
    # Get workspace settings
    settings = cp_client.get_workspace_settings()
    print(f"Workspace: {settings}")
    
    # Upload a DAG
    with open("my_dag.py", "rb") as f:
        files_client.upload_file("/dags/my_dag.py", f.read())
    
    # List DAG files
    response = files_client.list_items_in_directory("/dags")
    print(f"DAG files: {response.body}")

if __name__ == "__main__":
    main()
```
