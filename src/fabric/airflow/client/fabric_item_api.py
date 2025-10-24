import os
from fabric.airflow.client.share.api_helper import BaseApiClient, AuthenticationProvider, ApiResponse
from fabric.airflow.client.share.api_exceptions import (
    APIError,
    ClientError,
    ServerError,
    AuthenticationError,
    ForbiddenError,
    NotFoundError,
    ValidationError
)
from fabric_model import AirflowItemRequest, AirflowDefinitionRequest, AirflowDefinition, AirflowProperties, ComputeProperties, FabricItem
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

    def _workspace_items_root(self, workspace_id: str) -> str:
        """Get workspace items root path."""
        return f"v1/workspaces/{workspace_id}/items"

    def _airflow_jobs_root(self, workspace_id: str) -> str:
        """Get Airflow jobs root path."""
        return f"v1/workspaces/{workspace_id}/apacheAirflowJobs"

    def _airflow_job_instance(self, workspace_id: str, airflow_job_id: str) -> str:
        """Get specific Airflow job instance path."""
        return f"{self._airflow_jobs_root(workspace_id)}/{airflow_job_id}"

    # ----- Airflow Job Creation -----

    def create_airflow_job(
        self,
        workspace_id: str,
        request: AirflowItemRequest,
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
        path = self._workspace_items_root(workspace_id)
        response = self.post(path, json_body=request.to_dict())
        return self._handle_create_response(response)

    def create_airflow_job_with_definition(
        self,
        workspace_id: str,
        request: AirflowDefinitionRequest,
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
        path = self._workspace_items_root(workspace_id)
        response = self.post(path, json_body=request.to_dict())
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
        path = self._airflow_job_instance(workspace_id, airflow_job_id)
        return self.get(path)

    def get_airflow_job_definition(
        self,
        workspace_id: str,
        airflow_job_id: str,
        format: str = "zip",
    ) -> ApiResponse:
        """
        Get Airflow job definition (files and configuration).
        
        Args:
            workspace_id: Workspace ID
            airflow_job_id: Airflow job ID
            format: Response format (zip, json)
            
        Returns:
            ApiResponse: Response containing job definition
        """
        path = f"{self._airflow_job_instance(workspace_id, airflow_job_id)}/getDefinition"
        params = {"format": format} if format != "zip" else None
        return self.post(path, params=params)

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
        path = self._airflow_jobs_root(workspace_id)
        params = {"continuationToken": continuation_token} if continuation_token else None
        return self.get(path, params=params)

    # ----- Airflow Job Updating -----

    def update_airflow_job_definition(
        self,
        workspace_id: str,
        airflow_job_id: str,
        request: AirflowDefinitionRequest,
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
        path = f"{self._airflow_job_instance(workspace_id, airflow_job_id)}/updateDefinition"
        params = {"updateMetadata": "true" if update_metadata else "false"}
        return self.post(path, json_body=request.to_dict(), params=params)

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
        path = self._airflow_job_instance(workspace_id, airflow_job_id)
        return self.delete(path)

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
        path = self._workspace_items_root(workspace_id)
        params = {}
        if type_filter:
            params["type"] = type_filter
        if continuation_token:
            params["continuationToken"] = continuation_token
        
        return self.get(path, params=params if params else None)

# ---------- Example usage ----------

if __name__ == "__main__":
    import logging
    from fabric.airflow.client.share.api_helper import AuthenticationProvider
    
    # Setup logging for debug mode
    logging.basicConfig(level=logging.INFO)
    
    # GET credential - use your actual credentials
    tenant_id = "5ad710af-d4e9-4e36-9d03-548e8c6e64e4"
    client_id = "97c709cf-d3a0-4cbc-830d-6cdb72dfeea4"
    client_secret = "K1i8Q~YF~NedGBGi~XYhp1Hnjz_KhtvUp65hZaoX"  # Replace with actual secret
    
    # Create auth provider with SPN authentication
    auth_provider = AuthenticationProvider(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
        scope="https://api.fabric.microsoft.com/.default"
    )
    
    # Create client with debug mode enabled
    client = AirflowCrudApiClient(
        auth_provider=auth_provider,
        base_url="https://dailyapi.fabric.microsoft.com",
        debug=True  # Enable debug mode
    )

    # https://daily.fabric.microsoft.com/groups/fdb6f419-ed52-468b-ae0b-277c67834468/list?experience=fabric-developer
    workspace_id = "fdb6f419-ed52-468b-ae0b-277c67834468"
    airflow_display_name = "MyCustomAirflowJob3"

    try:
        # Example 1: Create simple Airflow job (blank payload)
        simple_request = AirflowItemRequest(
            displayName=airflow_display_name,
            description="Created via API with blank payload"
        )        
        airflow_item = client.create_airflow_job(workspace_id, simple_request)
        print("Airflow job created!")
        print(f"ID: {airflow_item.id}, Name: {airflow_item.displayName}")
        
        # Example 2: Create Airflow job with definition
        # airflow_definition = AirflowDefinition()
        # definition_request = AirflowDefinitionRequest(
        #     displayName=airflow_display_name,
        #     description="Custom Airflow created via Fabric API with definition",
        # )
        # definition_request.add_airflow_definition(airflow_definition)
        # definition_request.add_dag_from_file("/dags/MyTestDag.py", os.path.join(os.getcwd(), "tests", "resources", "MyTestDag.py"))
        # airflow_item = client.create_airflow_job_with_definition(workspace_id, definition_request)
        # print("Airflow job created!")
        # print(f"ID: {airflow_item.id}, Name: {airflow_item.displayName}")

        # Update Airflow Definition
        # add requirements, variables and reset exisitng dags
        # switch compute to custom pool with large size
        # airflow_definition = AirflowDefinition()

        # airflow_definition.airflowProperties \
        #     .add_environment_variable("VAR1", "MyVarValue") \
        #     .add_airflow_config_override("AIRFLOW__celery_worker_autoscale", "[5,1]") \
        #     .add_requirement("pandas")

        # airflow_definition.computeProperties \
        #     .set_compute_pool("CustomPool") \
        #     .set_compute_size("Large") \
        #     .set_autoscale_nodes(5,6) # should not be specified

        # definition_request = AirflowDefinitionRequest(
        #     displayName="MyCustomAirflowJob1",
        #     description="Custom Airflow created via Fabric API with definition",
        # )
        # definition_request.add_airflow_definition(airflow_definition)
        # definition_request.add_dag_from_file("/dags/MyTestDag2.py", os.path.join(os.getcwd(), "test", "MyTestDag.py"))

        # client.update_airflow_job_definition(
        #     workspace_id, 
        #     airflow_item.id, 
        #     definition_request, 
        #     update_metadata=True
        # )


        # # Example 3: List all Airflow jobs
        # response = client.list_airflow_jobs(workspace_id)
        # print("="*10)
        # print("Airflow jobs listed!")
        # print(f"Status: {response.status}")
        # print(f"Body: {response.body}")
        
    except ValidationError as ex:
        print(f"Validation Error (400): {ex}")
        print(f"Request ID: {ex.request_id}")
        print(f"Response body: {ex.body}")
    except AuthenticationError as ex:
        print(f"Authentication Error (401): {ex}")
        print(f"Request ID: {ex.request_id}")
        print(f"Response body: {ex.body}")
    except ForbiddenError as ex:
        print(f"Forbidden Error (403): {ex}")
        print(f"Request ID: {ex.request_id}")
        print(f"Response body: {ex.body}")
    except NotFoundError as ex:
        print(f"Not Found Error (404): {ex}")
        print(f"Request ID: {ex.request_id}")
        print(f"Response body: {ex.body}")
    except ClientError as ex:
        print(f"Client Error [{ex.status}]: {ex}")
        print(f"Request ID: {ex.request_id}")
        print(f"Response body: {ex.body}")
    except ServerError as ex:
        print(f"Server Error [{ex.status}]: {ex}")
        print(f"Request ID: {ex.request_id}")
        print(f"Response body: {ex.body}")
    except Exception as ex:
        print(f"Unexpected error: {ex}")