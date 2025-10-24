
from fabric.airflow.client.share.airflow_api_helper import AirflowBaseApiClient
from fabric.airflow.client.share.api_exceptions import APIError
from fabric.airflow.client.share.api_helper import AuthenticationProvider, ApiResponse
from fabric.airflow.client.airflow_control_plane_model import (
    AirflowWorkspaceSettings,
    AirflowEnvironmentSettingsPayload,
    AirflowEnvironmentComputeRequest,
    AirflowEnvironmentStorageRequest,
    AirflowEnvironmentVersionRequest,
    AirflowPoolsTemplate,
    AirflowPoolTemplate
)
import typing as t
import logging


# ---------- Setup logging for debug mode ----------
logger = logging.getLogger(__name__)

class AirflowControlPlaneApiClient(AirflowBaseApiClient):
    """
    Python client for Airflow Control Plane API endpoints.

    Inherits from AirflowBaseApiClient and provides specialized methods for Airflow environment 
    and workspace operations. All methods automatically use the workspace_id and airflow_job_id 
    provided during initialization.
    
    For file operations, use AirflowFilesApiClient instead.
    """

    def __init__(
        self,
        auth_provider: AuthenticationProvider,
        workspace_id: str,
        airflow_job_id: str,
        base_url: str = "https://api.fabric.microsoft.com",
        **kwargs
    ):
        """
        Initialize the AirflowControlPlaneApiClient.
        
        Args:
            auth_provider: AuthenticationProvider instance for token management
            workspace_id: Workspace ID for all operations
            airflow_job_id: Airflow job ID for all operations
            base_url: Base URL for the API
            **kwargs: Additional arguments passed to AirflowBaseApiClient
        """
        super().__init__(auth_provider, workspace_id, airflow_job_id, base_url=base_url, **kwargs)

    # ----- Workspace Settings (public) -----

    def get_workspace_settings(self) -> AirflowWorkspaceSettings:
        """
        Get workspace settings for Airflow jobs.
        
        Returns:
            AirflowWorkspaceSettings: Parsed workspace settings object
            
        Raises:
            APIError: If the API call fails (raised by base class)
        """
        path = f"{self._jobs_root()}/settings"
        response = self._request("GET", path)  # Base class handles errors
        return AirflowWorkspaceSettings.from_dict(response.body)

    def patch_workspace_settings(
        self,
        request: AirflowWorkspaceSettings,
    ) -> ApiResponse:
        """Update workspace settings for Airflow jobs."""
        path = f"{self._jobs_root()}/settings"
        return self._request("PATCH", path, json_body=request.to_dict())

    # ----- Workspace Settings Pool Templates -----

    def _extract_pool_id_from_location(self, response: ApiResponse) -> str:
        """Extract pool ID from Location header (last GUID after /)"""
        location = response.headers.get('Location', '')
        if location:
            # Extract the last GUID from the location URL
            parts = location.split('/')
            if parts:
                return parts[-1]  # Return the last part (should be the GUID)
        return ""

    def create_pool_template(
        self,
        request: AirflowPoolTemplate,
    ) -> str:
        """
        Create a new pool template and return the pool ID.
        
        Returns:
            str: The pool ID extracted from the Location header
            
        Raises:
            APIError: If creation fails or pool ID cannot be extracted
        """
        path = f"{self._jobs_root()}/settings/pools"
        response = self.post(path, json_body=request.to_dict())  # Base class handles errors
        
        pool_id = self._extract_pool_id_from_location(response)
        if not pool_id:
            raise APIError(
                status=response.status,
                message=f"Pool created successfully but could not extract pool ID from Location header. "
                        f"Response status: {response.status}, Headers: {response.headers}"
            )
        
        return pool_id

    def list_pool_templates_parsed(self) -> AirflowPoolsTemplate:
        """
        List all pool templates in workspace and return parsed structure.
        
        Returns:
            AirflowPoolsTemplate: Parsed pool templates list with helper methods
        
        Raises:
            APIError: If the API call fails (raised by base class)
        """
        response = self.get(f"{self._jobs_root()}/settings/pools")  # Base class handles errors
        return AirflowPoolsTemplate.from_dict(response.body)

    def get_pool_template(self, pool_template_id: str) -> AirflowPoolTemplate:
        """
        Get specific pool template by ID and return parsed structure.
        
        Args:
            pool_template_id: Pool template ID or name
            
        Returns:
            AirflowPoolTemplate: Parsed pool template data
            
        Raises:
            NotFoundError: If pool template not found (404)
            APIError: If other API errors occur (raised by base class)
        """
        response = self.get(f"{self._jobs_root()}/settings/pools/{pool_template_id}")  # Base class handles errors
        return AirflowPoolTemplate.from_dict(response.body)

    def delete_pool_template(self, pool_template_id: str) -> ApiResponse:
        """Delete pool template by ID."""
        path = f"{self._jobs_root()}/settings/pools/{pool_template_id}"
        return self._request("DELETE", path)

    # ----- Environment Start/Stop/Status -----

    def start_environment(self) -> ApiResponse:
        """Start Airflow environment."""
        path = f"{self._job_instance()}/environment/start"
        return self._request("POST", path)

    def stop_environment(self) -> ApiResponse:
        """Stop Airflow environment."""
        path = f"{self._job_instance()}/environment/stop"
        return self._request("POST", path)

    def get_environment_status(self) -> ApiResponse:
        """Get Airflow environment status."""
        path = f"{self._job_instance()}/environment"
        return self._request("GET", path)

    # ----- Environment Logs -----

    def get_environment_logs(
        self,
        log_filter: t.Optional[str] = None,
    ) -> ApiResponse:
        """Get Airflow environment logs."""
        path = f"{self._job_instance()}/environment/logs"
        params = {}
        if log_filter:
            params["$filter"] = log_filter
        return self._request("GET", path, params=params, stream=True)

    # ----- Environment Libraries -----

    def get_environment_libraries(self) -> ApiResponse:
        """Get installed libraries in Airflow environment."""
        path = f"{self._job_instance()}/environment/libraries"
        return self._request("GET", path)

    # ----- Environment Requirements (deploy) -----

    def update_environment_requirements(
        self,
        file_path: t.Optional[str] = None,
        requirements_content: t.Union[str, bytes, None] = None,
    ) -> ApiResponse:
        """
        Update environment requirements.
        
        Args:
            file_path: Path to requirements file (sent as query parameter)
            requirements_content: Requirements content as string or bytes (sent as body)
        """
        path = f"{self._job_instance()}/environment/deployRequirements"
        params = {}
        headers = {}
        data = None

        if file_path:
            params["filePath"] = file_path
        else:
            if requirements_content is None:
                raise ValueError("requirements_content must be provided when file_path is not specified.")
            if isinstance(requirements_content, str):
                data = requirements_content.encode("utf-8")
                headers["Content-Type"] = "text/plain"
            else:
                data = requirements_content
                headers["Content-Type"] = "application/octet-stream"

        return self._request("POST", path, params=params, data=data, headers=headers)

    # ----- Environment Settings -----

    def get_environment_settings(self) -> ApiResponse:
        """Get Airflow environment settings."""
        path = f"{self._job_instance()}/environment/settings"
        return self._request("GET", path)

    def update_environment_settings(
        self,
        payload: AirflowEnvironmentSettingsPayload,
    ) -> ApiResponse:
        """Update Airflow environment settings."""
        path = f"{self._job_instance()}/environment/updateSettings"
        return self._request("POST", path, json_body=payload.to_dict())

    # ----- Environment Compute -----

    def get_environment_compute(self) -> ApiResponse:
        """Get Airflow environment compute configuration."""
        path = f"{self._job_instance()}/environment/compute"
        return self._request("GET", path)

    def update_environment_compute(
        self,
        request: AirflowEnvironmentComputeRequest,
    ) -> ApiResponse:
        """Update Airflow environment compute configuration."""
        path = f"{self._job_instance()}/environment/updateCompute"
        return self._request("POST", path, json_body=request.to_dict())

    # ----- Environment Version -----

    def update_environment_version(
        self,
        request: AirflowEnvironmentVersionRequest,
    ) -> ApiResponse:
        """Update Airflow environment version."""
        path = f"{self._job_instance()}/environment/updateVersion"
        return self._request("POST", path, json_body=request.to_dict())

    # ----- Environment Storage -----

    def get_environment_storage(self) -> ApiResponse:
        """Get Airflow environment storage configuration."""
        path = f"{self._job_instance()}/environment/storage"
        return self._request("GET", path)

    def update_environment_storage(
        self,
        request: AirflowEnvironmentStorageRequest,
    ) -> ApiResponse:
        """Update Airflow environment storage configuration."""
        path = f"{self._job_instance()}/environment/updateStorage"
        return self._request("POST", path, json_body=request.to_dict())
