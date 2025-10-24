from .share.airflow_api_helper import AirflowBaseApiClient
from .share.api_helper import AuthenticationProvider, ApiResponse

import typing as t
import logging

import os

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
    
    # https://daily.fabric.microsoft.com/groups/fdb6f419-ed52-468b-ae0b-277c67834468/list?experience=fabric-developer
    workspace_id = "fdb6f419-ed52-468b-ae0b-277c67834468"  # Replace with actual workspace ID
    airflow_job_id = "0b141ecd-ef2f-427d-b03e-947b8ec43b29"  # Replace with actual job ID

    # Create auth provider with SPN authentication
    auth_provider = AuthenticationProvider(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
        scope="https://api.fabric.microsoft.com/.default"
    )
    
    # Create files client with workspace and job context
    files_client = AirflowFilesApiClient(
        auth_provider=auth_provider,
        workspace_id=workspace_id,
        airflow_job_id=airflow_job_id,
        base_url="https://dailyapi.fabric.microsoft.com",
        is_preview_enabled=True,
        debug=True  # Enable debug mode
    )

    my_test_dag_file = os.path.join(os.getcwd(), "test", "resources", "MyTestDag.py")
    aks_status_plugin_file = os.path.join(os.getcwd(), "test", "resources", "AKS_Status.py")
    aks_pod_log_plugin_file = os.path.join(os.getcwd(), "test", "resources", "AKS_pod_log.py")

    with open(my_test_dag_file, 'r', encoding='utf-8') as f:
        dag_content = f.read()

    with open(aks_status_plugin_file, 'r', encoding='utf-8') as f:
        aks_status_plugin_content = f.read()

    with open(aks_pod_log_plugin_file, 'r', encoding='utf-8') as f:
        aks_pod_log_plugin_content = f.read()
    
    # Upload DAG file - user specifies the full path
    response = files_client.create_or_update_file("dags/MyTestDag.py",  dag_content)
    response = files_client.create_or_update_file("plugins/aks_status.py", aks_status_plugin_content)
    response = files_client.create_or_update_file("plugins/aks_pod_log.py", aks_pod_log_plugin_content)       
    
    # List DAG files
    response = files_client.list_files(root_path="dags")
    print("DAG files:")
    print(f"Body: {response.body}")
    
    # List plugin files
    response = files_client.list_files(root_path="plugins")
    print("Plugin files:")
    print(f"Body: {response.body}")

    # List root folder files
    response = files_client.list_files(root_path="/")
    print("Root folder files:")
    print(f"Body: {response.body}")

    response = files_client.get_file("dags/MyTestDag.py")
    print(f"Downloaded DAG file content: {response.body.decode('utf-8')}")

    response = files_client.create_or_update_file("dags/MyTestDag.py", "#TEST PAYLOAD")

    response = files_client.get_file("dags/MyTestDag.py")
    print(f"Downloaded DAG file content: {response.body.decode('utf-8')}")


    response = files_client.delete_file("dags/MyTestDag.py")
    print("Deleted DAG file MyTestDag.py")


    # Upload binary a binary file for testing purposes
    dll_file_path = r"C:\Program Files (x86)\dotnet\host\fxr\9.0.9\hostfxr.dll"
    
    # Read the binary file
    with open(dll_file_path, 'rb') as f:  # Note: 'rb' for binary mode
        dll_content = f.read()
    
    print(f"Read {len(dll_content)} bytes from {dll_file_path}")
    
    # Upload the DLL file to the plugins directory (or wherever you want it)
    response = files_client.create_or_update_file("plugins/hostfxr.dll", dll_content)
    print(f"Uploaded hostfxr.dll - Status: {response.status}")
    
    # Verify the upload by listing plugin files
    response = files_client.list_files(root_path="plugins")
    print("Plugin files after upload:")
    print(f"Body: {response.body}")
    
    # Optionally download it back to verify
    response = files_client.get_file("plugins/hostfxr.dll")
    downloaded_size = len(response.body)
    print(f"Downloaded hostfxr.dll - Size: {downloaded_size} bytes")
    
    if downloaded_size == len(dll_content):
        print("✅ File upload/download verification successful!")
    else:
        print("❌ File size mismatch - upload may have failed")
