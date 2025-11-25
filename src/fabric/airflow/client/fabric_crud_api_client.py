from fabric.airflow.client.base_api_client import BaseApiClient, AuthenticationProvider, ApiResponse
from fabric.airflow.client.fabric_crud_model import AirflowItem, FabricItemDefinition, FabricItem
import typing as t
import logging


# ---------- Setup logging for debug mode ----------
logger = logging.getLogger(__name__)


class AirflowCrudApiClient(BaseApiClient):
    """
    Python client for Fabric Airflow CRUD operations.
    
    Handles creation, reading, updating, and deletion of Apache Airflow jobs in Microsoft Fabric.
    Create methods return FabricItem objects on success or raise exceptions on failure.
    
    Inherits from BaseApiClient for consistent authentication, error handling, and HTTP operations.
    """

    def __init__(
        self,
        auth_provider: AuthenticationProvider,
        base_url: str = "https://api.fabric.microsoft.com",
        **kwargs
    ):
        """
        Initialize the AirflowCrudApiClient.
        
        Args:
            auth_provider: AuthenticationProvider instance for token management
            base_url: Base URL for the API
            **kwargs: Additional arguments passed to BaseApiClient
        """
        super().__init__(auth_provider, base_url=base_url, **kwargs)

    # ----- Route construction helpers -----

    def _path_workspace_items(self, workspace_id: str) -> str:
        """Get workspace items root path."""
        return f"v1/workspaces/{workspace_id}/items"

    def _path_airflow_jobs(self, workspace_id: str) -> str:
        """Get Airflow jobs root path."""
        return f"v1/workspaces/{workspace_id}/apacheAirflowJobs"

    def _path_airflow_job_instance(self, workspace_id: str, airflow_job_id: str) -> str:
        """Get specific Airflow job instance path."""
        return f"{self._path_airflow_jobs(workspace_id)}/{airflow_job_id}"

    # ----- Airflow Job Creation -----

    def create_airflow_job(
        self,
        workspace_id: str,
        request: AirflowItem,
    ) -> FabricItem:
        """
        Create a new Airflow job with blank payload.
        
        Args:
            workspace_id: Workspace ID where to create the job
            request: AirflowItemRequest with job details
            
        Returns:
            FabricItem: Created Fabric item
            
        Raises:
            ValidationError: For 400 validation errors
            AuthenticationError: For 401 authentication errors
            ForbiddenError: For 403 permission errors
            NotFoundError: For 404 not found errors
            ClientError: For other 4xx client errors
            ServerError: For 5xx server errors
        """
        response = self.post(
            path=self._path_airflow_jobs(workspace_id), 
            json_body=request.to_dict())
        return self._handle_create_response(response)

    def create_airflow_job_with_definition(
        self,
        workspace_id: str,
        request: FabricItemDefinition,
    ) -> FabricItem:
        """
        Create a new Airflow job with definition (files, configuration, etc.).
        
        Args:
            workspace_id: Workspace ID where to create the job
            request: AirflowDefinitionRequest with job details and definition
            
        Returns:
            FabricItem: Created Fabric item
            
        Raises:
            ValidationError: For 400 validation errors
            AuthenticationError: For 401 authentication errors
            ForbiddenError: For 403 permission errors
            NotFoundError: For 404 not found errors
            ClientError: For other 4xx client errors
            ServerError: For 5xx server errors
        """
        response = self.post(
            path=self._path_workspace_items(workspace_id),
            json_body=request.to_dict())
        return self._handle_create_response(response)

    def _handle_create_response(self, response: ApiResponse) -> FabricItem:
        """
        Handle create operation response - convert successful response to FabricItem.
        
        Args:
            response: ApiResponse from base client (only success responses reach here)
            
        Returns:
            FabricItem: Created Fabric item
            
        Raises:
            ValueError: For successful response with no body
        """
        if not response.body:
            raise ValueError("Successful response but no body received")
        
        return FabricItem.from_dict(response.body)

    # ----- Airflow Job Reading -----

    def get_airflow_job(
        self,
        workspace_id: str,
        airflow_job_id: str,
    ) -> ApiResponse:
        """
        Get Airflow job metadata.
        
        Args:
            workspace_id: Workspace ID
            airflow_job_id: Airflow job ID
            
        Returns:
            ApiResponse: Response containing job metadata
        """
        return self.get(
            path=self._path_airflow_job_instance(workspace_id, airflow_job_id))

    def get_airflow_job_definition(
        self,
        workspace_id: str,
        airflow_job_id: str,
        response_format: str = "json",
    ) -> FabricItemDefinition:
        """
        Get Airflow job definition (files and configuration).
        
        Args:
            workspace_id: Workspace ID
            airflow_job_id: Airflow job ID
            response_format: Response format ("json" recommended, "zip" also available)
            
        Returns:
            FabricItemDefinition: Job definition with all parts (DAGs, configuration, etc.)
            
        Raises:
            ValidationError: For 400 validation errors
            AuthenticationError: For 401 authentication errors
            ForbiddenError: For 403 permission errors
            NotFoundError: For 404 not found errors
            ClientError: For other 4xx client errors
            ServerError: For 5xx server errors
        """
        response = self.post(
            path=f"{self._path_airflow_job_instance(workspace_id, airflow_job_id)}/getDefinition",
            params={"format": response_format} if response_format != "json" else None)
        
        # Parse response into FabricItemDefinition
        if not response.body or 'definition' not in response.body:
            raise ValueError("Invalid API response: missing definition")
        
        return FabricItemDefinition.from_api_response(
            display_name=response.body.get('displayName', 'Unknown'),
            definition_parts=response.body['definition'].get('parts', []),
            description=response.body.get('description')
        )

    def list_airflow_jobs(
        self,
        workspace_id: str,
        continuation_token: t.Optional[str] = None,
    ) -> ApiResponse:
        """
        List all Airflow jobs in workspace.
        
        Args:
            workspace_id: Workspace ID
            continuation_token: Token for pagination
            
        Returns:
            ApiResponse: Response containing list of jobs
        """
        return self.get(
            path = self._path_airflow_jobs(workspace_id),
            params={"continuationToken": continuation_token} if continuation_token else None)

    # ----- Airflow Job Updating -----

    def update_airflow_job_definition(
        self,
        workspace_id: str,
        airflow_job_id: str,
        definition: FabricItemDefinition,
        update_metadata: bool = True,
    ) -> ApiResponse:
        """
        Update Airflow job definition (files and configuration).
        
        Args:
            workspace_id: Workspace ID
            airflow_job_id: Airflow job ID
            request: AirflowDefinitionRequest with new definition
            update_metadata: Whether to update metadata as well
            
        Returns:
            ApiResponse: Response from update operation
        """
        return self.post(
            path = f"{self._path_airflow_job_instance(workspace_id, airflow_job_id)}/updateDefinition", 
            json_body=definition.to_dict(), 
            params={"updateMetadata": "true" if update_metadata else "false"})

    # ----- Airflow Job Deletion -----

    def delete_airflow_job(
        self,
        workspace_id: str,
        airflow_job_id: str,
    ) -> ApiResponse:
        """
        Delete Airflow job.
        
        Args:
            workspace_id: Workspace ID
            airflow_job_id: Airflow job ID
            
        Returns:
            ApiResponse: Response from delete operation
        """
        return self.delete(
            path=self._path_airflow_job_instance(workspace_id, airflow_job_id)
        )

    # ----- Workspace Operations -----

    def get_workspace_info(
        self,
        workspace_id: str,
    ) -> ApiResponse:
        """
        Get workspace information.
        
        Args:
            workspace_id: Workspace ID
            
        Returns:
            ApiResponse: Response containing workspace details
        """
        path = f"v1/workspaces/{workspace_id}"
        return self.get(path)

    def list_workspace_items(
        self,
        workspace_id: str,
        type_filter: t.Optional[str] = None,
        continuation_token: t.Optional[str] = None,
    ) -> ApiResponse:
        """
        List all items in workspace.
        
        Args:
            workspace_id: Workspace ID
            type_filter: Filter by item type (e.g., "ApacheAirflowJob")
            continuation_token: Token for pagination
            
        Returns:
            ApiResponse: Response containing list of items
        """
        params = {}
        if type_filter:
            params["type"] = type_filter
        if continuation_token:
            params["continuationToken"] = continuation_token
        
        return self.get(
            path=self._path_workspace_items(workspace_id),
            params=params if params else None
        )

# For usage examples, see: src/sample/example_usage.py