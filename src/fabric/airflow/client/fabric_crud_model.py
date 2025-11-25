import dataclasses
import json
import base64
import typing as t
import logging

logger = logging.getLogger(__name__)

# Public API - only export these classes
__all__ = [
    'FabricItemDefinition',
    'AirflowItem',
    'FabricItem'
]

@dataclasses.dataclass
class _FabricItemDefinitionPart:
    """
    Internal class representing a part of the Fabric item definition.
    
    This class is not intended for direct use by users. Use FabricItemDefinition methods instead.
    """
    path: str
    payload: t.Union[str, dict]  # Original payload (string or dict), not yet base64 encoded
    payloadType: str = "InlineBase64"
    
    @classmethod
    def _from_string(cls, path: str, content: str) -> '_FabricItemDefinitionPart':
        """Internal: Create a part from string content."""
        return cls(path=path, payload=content, payloadType="InlineBase64")
    
    @classmethod
    def _from_dict(cls, path: str, content: dict) -> '_FabricItemDefinitionPart':
        """Internal: Create a part from dictionary content."""
        return cls(path=path, payload=content, payloadType="InlineBase64")
    
    @classmethod
    def _from_encoded(cls, path: str, encoded_payload: str, payload_type: str = "InlineBase64") -> '_FabricItemDefinitionPart':
        """Internal: Create a part from already base64-encoded payload (from API response)."""
        # Decode the payload if it's InlineBase64
        if payload_type == "InlineBase64":
            decoded_bytes = base64.b64decode(encoded_payload)          
            payload =  decoded_bytes.decode('utf-8')

        else:
            # For other payload types, keep as-is
            payload = encoded_payload
        
        return cls(path=path, payload=payload, payloadType=payload_type)
    
    def as_json(self) -> t.Optional[dict]:
        """
        Get the payload as a JSON dictionary.
        
        Returns:
            Dictionary if payload is valid JSON, None otherwise
            
        Example:
            >>> part = definition.get_part("apacheairflowjob-content.json")
            >>> config = part.as_json()
            >>> config['requirements'].append('flask-bcrypt')
        """
        if isinstance(self.payload, dict):
            return self.payload
        elif isinstance(self.payload, str):
            try:
                parsed = json.loads(self.payload)
                # Update the payload to be a dict for easier modification
                self.payload = parsed
                return parsed
            except (json.JSONDecodeError, TypeError):
                return None
        return None
    
    def to_dict(self) -> dict:
        """Convert to dictionary for API request, encoding payload to base64."""
        # Encode to base64
        if isinstance(self.payload, dict):
            payload_bytes = json.dumps(self.payload).encode('utf-8')
        else:
            payload_bytes = self.payload.encode('utf-8')
        encoded_payload = base64.b64encode(payload_bytes).decode('utf-8')
        
        return {
            "path": self.path,
            "payload": encoded_payload,
            "payloadType": self.payloadType
        }


@dataclasses.dataclass
class FabricItemDefinition:
    """
    Request model for creating an Airflow job with definition.
    
    This class simplifies creating Airflow jobs by allowing you to:
    - Specify the Airflow configuration from a JSON file
    - Add DAG files from disk or string content
    
    Example:
        >>> # Create Airflow job with definition and DAGs
        >>> definition = FabricItemDefinition(
        ...     displayName="MyAirflowJob",
        ...     description="My job description",
        ...     airflow_definition_file="path/to/airflow-config.json"
        ... )
        >>> definition.add_dag_file("dags/my_dag.py", "/path/to/my_dag.py")
        >>> definition.add_dag("dags/another_dag.py", "from airflow import DAG...")
    """
    displayName: str
    description: t.Optional[str] = None
    parts: t.List[_FabricItemDefinitionPart] = dataclasses.field(default_factory=list, init=False)
    
    def __init__(self, displayName: str, airflow_definition_file: str, description: t.Optional[str] = None):
        """
        Initialize FabricItemDefinition with Airflow configuration.
        
        Args:
            displayName: The display name for the Airflow job
            airflow_definition_file: Path to the Airflow definition JSON file (apacheairflowjob-content.json)
            description: Optional description for the Airflow job
        """
        self.displayName = displayName
        self.description = description
        self.parts = []
        
        # Add required .platform file
        platform_payload = {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
            "metadata": {
                "type": "ApacheAirflowJob",
                "displayName": self.displayName
            },
            "config": {
                "version": "2.0",
                "logicalId": "00000000-0000-0000-0000-000000000000"
            }
        }
        self.parts.append(_FabricItemDefinitionPart._from_dict(".platform", platform_payload))
        
        # Add Airflow definition from file
        with open(airflow_definition_file, 'r', encoding='utf-8') as f:
            airflow_definition = json.load(f)
        self.parts.append(_FabricItemDefinitionPart._from_dict("apacheairflowjob-content.json", airflow_definition))
    
    def add_dag_file(self, dag_path: str, file_path: str) -> 'FabricItemDefinition':
        """
        Add a DAG file from disk. Returns self for method chaining.
        
        Args:
            dag_path: Destination path in the Airflow job (e.g., "dags/my_dag.py")
            file_path: Source file path on disk to read from
            
        Example:
            >>> definition.add_dag_file("dags/my_dag.py", "/local/path/to/my_dag.py")
        """
        # Check if part already exists
        existing_part = self.get_part(dag_path)
        if existing_part:
            logger.warning(f"Overriding existing part: {dag_path}")
            # Remove the existing part
            self.parts = [p for p in self.parts if p.path != dag_path]
        
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        self.parts.append(_FabricItemDefinitionPart._from_string(dag_path, content))
        return self
    
    def add_dag(self, dag_path: str, content: str) -> 'FabricItemDefinition':
        """
        Add a DAG from string content. Returns self for method chaining.
        
        Args:
            dag_path: Destination path in the Airflow job (e.g., "dags/my_dag.py")
            content: DAG file content as string
            
        Example:
            >>> dag_code = '''
            ... from airflow import DAG
            ... dag = DAG('my_dag', ...)
            ... '''
            >>> definition.add_dag("dags/my_dag.py", dag_code)
        """
        # Check if part already exists
        existing_part = self.get_part(dag_path)
        if existing_part:
            logger.warning(f"Overriding existing part: {dag_path}")
            # Remove the existing part
            self.parts = [p for p in self.parts if p.path != dag_path]
        
        self.parts.append(_FabricItemDefinitionPart._from_string(dag_path, content))
        return self
    
    @classmethod
    def from_api_response(cls, display_name: str, definition_parts: t.List[dict], description: t.Optional[str] = None) -> 'FabricItemDefinition':
        """
        Create FabricItemDefinition from API response.
        
        This is used internally when retrieving job definitions from the API.
        
        Args:
            display_name: Display name for the job
            definition_parts: List of definition parts from API response
            description: Optional description
            
        Returns:
            FabricItemDefinition: Reconstructed definition object
        """
        # Create a minimal instance without calling __init__ (to avoid file loading)
        instance = cls.__new__(cls)
        instance.displayName = display_name
        instance.description = description
        instance.parts = []
        
        # Reconstruct parts from API response (payload is already base64 encoded)
        for part_dict in definition_parts:
            part = _FabricItemDefinitionPart._from_encoded(
                path=part_dict['path'],
                encoded_payload=part_dict['payload'],
                payload_type=part_dict.get('payloadType', 'InlineBase64')
            )
            instance.parts.append(part)
        
        return instance
    
    def get_part(self, path: str) -> t.Optional[_FabricItemDefinitionPart]:
        """
        Get a part by its path.
        
        Args:
            path: The path of the part to retrieve (e.g., "apacheairflowjob-content.json", "dags/my_dag.py")
            
        Returns:
            The part if found, None otherwise
            
        Example:
            >>> definition = crud_client.get_airflow_job_definition(workspace_id, job_id)
            >>> airflow_config = definition.get_part("apacheairflowjob-content.json")
            >>> print(airflow_config.payload)  # dict with Airflow configuration
        """
        for part in self.parts:
            if part.path == path:
                return part
        return None
    
    def get_airflow_definition(self) -> t.Optional[_FabricItemDefinitionPart]:
        """
        Get the Airflow job definition part (apacheairflowjob-content.json).
        
        Returns:
            The definition part, or None if not found
            
        Example:
            >>> definition = crud_client.get_airflow_job_definition(workspace_id, job_id)
            >>> airflow_part = definition.get_airflow_definition()
            >>> config = airflow_part.as_json()
            >>> config['requirements'].append('flask-bcrypt')
            >>> config['configurationOverrides']['my_setting'] = 'value'
        """
        return self.get_part("apacheairflowjob-content.json")
    
    def to_dict(self) -> dict:
        d = {
            "displayName": self.displayName,
            "definition": {
                "parts": [part.to_dict() for part in self.parts]
            }
        }
        if self.description:
            d["description"] = self.description
        return d


@dataclasses.dataclass
class AirflowItem:
    """Request model for creating Airflow item with basic properties."""
    displayName: str
    description: t.Optional[str] = None
    
    def to_dict(self) -> dict:
        d = {
            "displayName": self.displayName,
            "type": "ApacheAirflowJob"
        }
        if self.description:
            d["description"] = self.description
        return d


@dataclasses.dataclass
class FabricItem:
    """Represents a Fabric item (e.g., Apache Airflow Job, Notebook, etc.)."""
    id: str
    type: str
    displayName: str
    workspaceId: str
    description: t.Optional[str] = None
    
    @classmethod
    def from_dict(cls, data: dict) -> 'FabricItem':
        """Create a FabricItem from a dictionary response."""
        return cls(
            id=data['id'],
            type=data['type'],
            displayName=data['displayName'],
            workspaceId=data['workspaceId'],
            description=data.get('description')
        )
    
    def to_dict(self) -> dict:
        """Convert to dictionary."""
        d = {
            "id": self.id,
            "type": self.type,
            "displayName": self.displayName,
            "workspaceId": self.workspaceId
        }
        if self.description:
            d["description"] = self.description
        return d