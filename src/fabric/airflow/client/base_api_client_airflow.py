from fabric.airflow.client.base_api_client import BaseApiClient, AuthenticationProvider
import typing as t
import requests
import logging

# ---------- Setup logging for debug mode ----------
logger = logging.getLogger(__name__)


class AirflowBaseApiClient(BaseApiClient):
    """
    Base API client for Airflow operations that stores workspace_id and airflow_job_id.
    
    This class eliminates the need to pass workspace_id and airflow_job_id to every method call.
    All derived Airflow API clients should inherit from this class instead of BaseApiClient directly.
    
    For Fabric Control Plane and Files APIs, the request ID is returned in the JSON response body
    under the 'requestId' property, not in headers.
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
        Initialize the AirflowBaseApiClient with workspace and job context.
        
        Args:
            auth_provider: AuthenticationProvider instance for token management
            workspace_id: Workspace ID for all operations
            airflow_job_id: Airflow job ID for all operations
            base_url: Base URL for the API
            **kwargs: Additional arguments passed to BaseApiClient
        """
        super().__init__(auth_provider, base_url=base_url, **kwargs)
        
        self.workspace_id = workspace_id
        self.airflow_job_id = airflow_job_id

    def _extract_request_id(self, response: requests.Response, body: t.Any = None) -> t.Optional[str]:
        """
        Extract request ID from Fabric API response.
        
        For Fabric Control Plane and Files APIs, the request ID is in the JSON response body
        under the 'requestId' property, not in headers.
        
        Args:
            response: The HTTP response object
            body: Parsed response body (JSON or text)
            
        Returns:
            Optional[str]: Request ID if found, None otherwise
        """
        # For Fabric APIs, try body first (requestId property)
        if isinstance(body, dict):
            request_id = body.get("requestId") or body.get("request_id")
            if request_id:
                return request_id
        
        # Fall back to checking headers (for other endpoints)
        return super()._extract_request_id(response, body)

    # ----- Helper methods for URL construction -----

    def _workspace_root(self) -> str:
        """Get workspace root path using stored workspace_id."""
        return f"v1/workspaces/{self.workspace_id}"

    def _jobs_root(self) -> str:
        """Get jobs root path using stored workspace_id."""
        return f"{self._workspace_root()}/apacheAirflowJobs"

    def _job_instance(self) -> str:
        """Get job instance path using stored workspace_id and airflow_job_id."""
        return f"{self._jobs_root()}/{self.airflow_job_id}"

    # ----- Helper methods for URL construction -----