# Configuration client for test runs
# Using MSiT Fabric workspace: 

from fabric.airflow.client.share.api_helper import (AuthenticationProvider)
from fabric.airflow.client.airflow_native_api import AirflowNativeApiClient 
from fabric.airflow.client.airflow_files_api import AirflowFilesApiClient
from fabric.airflow.client.airflow_control_plane_api import AirflowControlPlaneApiClient

class _ConfigClient:
    """Internal configuration client for API tests"""
    
    def __init__(self):
        # Workspace and service configuration
        self._fabric_base_url = "https://dailyapi.fabric.microsoft.com"
        self._workspace_id = "fdb6f419-ed52-468b-ae0b-277c67834468"
        self._airflow_job_id = "0b141ecd-ef2f-427d-b03e-947b8ec43b29"
        self._airflow_base_url = "https://fcca0257191759.centraluseuap.airflow.svc.datafactory.azure.com/"

        # Private authentication configuration
        self._tenant_id = "5ad710af-d4e9-4e36-9d03-548e8c6e64e4"
        self._client_id = "97c709cf-d3a0-4cbc-830d-6cdb72dfeea4"
        self._client_secret = "l0A8Q~9YjzzJfHjfiVV1jxajQWCKndgqU0F-vbPW"
        
        # scopes
        self._airflow_api_scope = "5d13f7d7-0567-429c-9880-320e9555e5fc/.default"
        self._fabric_api_scope = "https://api.fabric.microsoft.com/.default"

        self._debug = True
        self.is_preview_enabled = True

        # Initialize API clients
        self._initialize_clients()
    
    def _initialize_clients(self):
        """Initialize API clients"""
        auth_provider = AuthenticationProvider(
            tenant_id=self._tenant_id,
            client_id=self._client_id,
            client_secret=self._client_secret,
            scope=self._airflow_api_scope
        )
        self._airflow_native_client = AirflowNativeApiClient(
            base_url=self._airflow_base_url,
            auth_provider=auth_provider,
            debug=self._debug)
        
        files_auth_provider = AuthenticationProvider(
            tenant_id=self._tenant_id,
            client_id=self._client_id,
            client_secret=self._client_secret,
            scope=self._fabric_api_scope
        )
        self._files_client = AirflowFilesApiClient(
            workspace_id=self._workspace_id,
            airflow_job_id=self._airflow_job_id,
            base_url=self._fabric_base_url,
            auth_provider=files_auth_provider,
            debug=self._debug,
            is_preview_enabled=self.is_preview_enabled
        )

        self._control_plane_client = AirflowControlPlaneApiClient(
            workspace_id=self._workspace_id,
            airflow_job_id=self._airflow_job_id,
            base_url=self._fabric_base_url,
            auth_provider=files_auth_provider,
            debug=self._debug,
            is_preview_enabled=self.is_preview_enabled
        )

    @property
    def airflow_webserver_url(self) -> str:
        return self._airflow_base_url
    
    @property
    def workspace_id(self) -> str:
        return self._workspace_id

    @property
    def airflow_job_id(self) -> str:
        return self._airflow_job_id

    @property
    def airflow_api_scope(self) -> str:
        return self._airflow_api_scope

    @property
    def fabric_api_scope(self) -> str:
        return self._fabric_api_scope

    def get_airflow_native_client(self) -> AirflowNativeApiClient:
        """Get configured Airflow native API client"""
        return self._airflow_native_client

    def get_files_client(self) -> AirflowFilesApiClient:
        """Get configured Files API client"""
        return self._files_client

    def get_control_plane_client(self) -> AirflowControlPlaneApiClient:
        """Get configured Control Plane API client"""
        return self._control_plane_client

    @classmethod
    def airflow_native_client(cls) -> AirflowNativeApiClient:
        """Get configured Airflow native API client"""
        return _instance.get_airflow_native_client()

    @classmethod
    def files_client(cls) -> AirflowFilesApiClient:
        """Get configured Files API client"""
        return _instance.get_files_client()

    @classmethod
    def control_plane_client(cls) -> AirflowControlPlaneApiClient:
        """Get configured Control Plane API client"""
        return _instance.get_control_plane_client()

    @classmethod
    def get_airflow_webserver_url(cls) -> str:
        """Get Airflow webserver URL"""
        return _instance.airflow_webserver_url

    @classmethod
    def get_workspace_id(cls) -> str:
        """Get workspace ID"""
        return _instance.workspace_id

    @classmethod
    def get_airflow_job_id(cls) -> str:
        """Get Airflow job ID"""
        return _instance.airflow_job_id


# Create the singleton instance
_instance = _ConfigClient()

# Expose the class publicly
ConfigClient = _ConfigClient