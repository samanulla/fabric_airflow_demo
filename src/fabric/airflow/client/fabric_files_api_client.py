from .base_api_client_airflow import AirflowBaseApiClient
from .base_api_client import AuthenticationProvider, ApiResponse
import typing as t
import logging

# ---------- Setup logging for debug mode ----------
logger = logging.getLogger(__name__)


class AirflowFilesApiClient(AirflowBaseApiClient):
    """
    Python client for Airflow Files API endpoints.

    Inherits from AirflowBaseApiClient and provides specialized methods for Airflow file operations.
    All methods automatically use the workspace_id and airflow_job_id provided during initialization.
    
    Common file paths:
    - DAG files: "dags/my_dag.py"
    - Plugin files: "plugins/my_plugin.py"  
    - Requirements: "requirements.txt"
    - Configuration: "airflow.cfg"
    - Custom modules: "include/my_module.py"
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
        Initialize the AirflowFilesApiClient.
        
        Args:
            auth_provider: AuthenticationProvider instance for token management
            workspace_id: Workspace ID for all operations
            airflow_job_id: Airflow job ID for all operations
            base_url: Base URL for the API
            **kwargs: Additional arguments passed to AirflowBaseApiClient
        """
        super().__init__(auth_provider, workspace_id, airflow_job_id, base_url=base_url, **kwargs)

    # ----- Files (create/update, get, list, delete) -----

    def create_or_update_file(
        self,
        file_path: str,
        content: t.Union[str, bytes],
    ) -> ApiResponse:
        """
        Create or update a file in the Airflow job.
        
        Args:
            file_path: Path of the file within the job (e.g., "dags/my_dag.py", "plugins/my_plugin.py")
            content: File content as string or bytes
            
        Returns:
            ApiResponse: Response from the create/update operation
            
        Examples:
            # Upload a DAG file
            client.create_or_update_file("dags/my_dag.py", dag_python_code)
            
            # Upload a plugin file
            client.create_or_update_file("plugins/my_plugin.py", plugin_code)
            
            # Upload requirements
            client.create_or_update_file("requirements.txt", "pandas>=1.0\\nnumpy>=1.20")
        """
        path = f"{self._job_instance()}/files/{file_path.lstrip('/')}"
        headers = {}
        if isinstance(content, str):
            data = content.encode("utf-8")
            headers["Content-Type"] = "text/plain"
        else:
            data = content
            headers["Content-Type"] = "application/octet-stream"
        return self.put(path, data=data, headers=headers)

    def get_file(
        self,
        file_path: str,
    ) -> ApiResponse:
        """
        Get file content from Airflow job.
        
        Args:
            file_path: Path of the file within the job (e.g., "dags/my_dag.py")
            
        Returns:
            ApiResponse: Response containing file content as bytes
            
        Examples:
            # Get a DAG file
            response = client.get_file("dags/my_dag.py")
            dag_content = response.body.decode("utf-8")
            
            # Get requirements file
            response = client.get_file("requirements.txt")
        """
        path = f"{self._job_instance()}/files/{file_path.lstrip('/')}"
        return self.get(path, stream=True)

    def list_files(
        self,
        root_path: t.Optional[str] = None,
        continuation_token: t.Optional[str] = None,
    ) -> ApiResponse:
        """
        List files in Airflow job.
        
        Args:
            root_path: Root path to list files from (e.g., "dags", "plugins")
            continuation_token: Token for pagination (optional)
            
        Returns:
            ApiResponse: Response containing list of files
            
        Examples:
            # List all files
            response = client.list_files()
            
            # List only DAG files
            response = client.list_files(root_path="dags")
            
            # List only plugin files
            response = client.list_files(root_path="plugins")
        """
        path = f"{self._job_instance()}/files"
        params = {}
        if root_path:
            params["rootPath"] = root_path
        if continuation_token:
            params["continuationToken"] = continuation_token
        return self.get(path, params=params)

    def delete_file(
        self,
        file_path: str,
    ) -> ApiResponse:
        """
        Delete file from Airflow job.
        
        Args:
            file_path: Path of the file within the job (e.g., "dags/old_dag.py")
            
        Returns:
            ApiResponse: Response from the delete operation
            
        Examples:
            # Delete a DAG file
            client.delete_file("dags/old_dag.py")
            
            # Delete a plugin file
            client.delete_file("plugins/unused_plugin.py")
        """
        path = f"{self._job_instance()}/files/{file_path.lstrip('/')}"
        return self.delete(path)


# For usage examples, see: src/sample/example_usage.py
