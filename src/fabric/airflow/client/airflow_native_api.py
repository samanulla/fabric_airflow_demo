import datetime
from fabric.airflow.client.share.api_helper import AuthenticationProvider, ApiResponse, BaseApiClient

import typing as t
import logging

# ---------- Setup logging for debug mode ----------
logger = logging.getLogger(__name__)


class AirflowNativeApiClient(BaseApiClient):
    """
    Python client for Airflow Native API endpoints.
    
    Inherits directly from BaseApiClient and provides methods for native Airflow operations:
    - List deployed DAGs
    - Run DAG instances  
    - Check DAG run status
    - Health checks
    
    This client connects directly to Airflow's REST API, not through Fabric's wrapper.
    """

    def __init__(
        self,
        auth_provider: AuthenticationProvider,
        base_url: str,
        **kwargs
    ):
        """
        Initialize the AirflowNativeApiClient.
        
        Args:
            auth_provider: AuthenticationProvider instance for token management
            base_url: Base URL for the Airflow instance (e.g., "https://airflow.company.com")
            **kwargs: Additional arguments passed to BaseApiClient
        """
        super().__init__(auth_provider, base_url=base_url, is_preview_enabled=False, **kwargs)

    # ----- DAG Management -----

    def list_dags(
        self,
        limit: t.Optional[int] = None,
        offset: t.Optional[int] = None,
        order_by: t.Optional[str] = None,
        tags: t.Optional[t.List[str]] = None,
        only_active: t.Optional[bool] = None,
        paused: t.Optional[bool] = None,
    ) -> ApiResponse:
        """
        List all deployed DAGs using native Airflow API.
        
        Args:
            limit: The numbers of items to return (default: 100)
            offset: The number of items to skip before starting to collect the result set
            order_by: The name of the field to order the results by
            tags: List of tags to filter DAGs by
            only_active: Only show active DAGs
            paused: Show only paused/unpaused DAGs
            
        Returns:
            ApiResponse: Response containing list of DAGs
        """
        path = "api/v1/dags"  # Standard Airflow REST API path
        params = {}
        
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset
        if order_by:
            params["order_by"] = order_by
        if tags:
            params["tags"] = tags
        if only_active is not None:
            params["only_active"] = str(only_active).lower()
        if paused is not None:
            params["paused"] = str(paused).lower()
            
        return self.get(path, params=params if params else None)

    def trigger_dag(
        self,
        dag_id: str,
        dag_run_id: t.Optional[str] = None,
        execution_date: t.Optional[datetime.datetime] = None,
        conf: t.Optional[dict] = None,
        note: t.Optional[str] = None,
    ) -> ApiResponse:
        """
        Trigger a new DAG run using native Airflow API.
        
        Args:
            dag_id: The DAG ID
            dag_run_id: The DAG run ID (if not provided, will be auto-generated)
            execution_date: The execution date
            conf: JSON configuration to pass to the DAG
            note: Note for this DAG run
            
        Returns:
            ApiResponse: Response containing created DAG run details
        """
        path = f"api/v1/dags/{dag_id}/dagRuns"  # Standard Airflow path
        body = {}
        
        if dag_run_id:
            body["dag_run_id"] = dag_run_id
        if execution_date:
            body["execution_date"] = execution_date.isoformat()
        if conf:
            body["conf"] = conf
        if note:
            body["note"] = note
            
        return self.post(path, json_body=body)

    def get_dag_run(self, dag_id: str, dag_run_id: str) -> ApiResponse:
        """
        Get details of a specific DAG run.
        
        Args:
            dag_id: The DAG ID
            dag_run_id: The DAG run ID
            
        Returns:
            ApiResponse: Response containing DAG run details
        """
        path = f"api/v1/dags/{dag_id}/dagRuns/{dag_run_id}"
        return self.get(path)

    def health_check(self) -> ApiResponse:
        """
        Check the health of the Airflow instance using native API.
        
        Returns:
            ApiResponse: Response containing health status
        """
        path = "health"  # Standard Airflow health endpoint
        return self.get(path)

    def get_version(self) -> ApiResponse:
        """
        Get Airflow version information.
        
        Returns:
            ApiResponse: Response containing version details
        """
        path = "api/v1/version"
        return self.get(path)
