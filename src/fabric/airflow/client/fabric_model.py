import dataclasses
import json
import base64
import typing as t

@dataclasses.dataclass
class ComputeProperties:
    """Represents compute configuration properties."""
    computePool: str = "StarterPool" # or CustomPool
    computeSize: t.Optional[str] = None # Small or Large

    enableAutoscale: bool = True
    autoScaleMinNodes: int = 5
    autoScaleMaxNodes: int = 6
    extraNodes: int = 0
    enableAvailabilityZones: t.Optional[bool] = None  # should not be specified

    # Compute Properties Management Methods
    def set_compute_pool(self, pool: str) -> 'ComputeProperties':
        """Set the compute pool. Returns self for method chaining."""
        self.computePool = pool
        return self

    def set_compute_size(self, size: str) -> 'ComputeProperties':
        """Set the compute size. Returns self for method chaining."""
        self.computeSize = size
        return self

    def enable_autoscale(self, enable: bool = True) -> 'ComputeProperties':
        """Enable/disable autoscale. Returns self for method chaining."""
        self.enableAutoscale = enable
        return self

    def set_autoscale_nodes(self, min_nodes: int, max_nodes: int) -> 'ComputeProperties':
        """Set autoscale node range. Returns self for method chaining."""
        self.autoScaleMinNodes = min_nodes
        self.autoScaleMaxNodes = max_nodes
        return self

    # def enable_availability_zones(self, enable: bool = True) -> 'ComputeProperties':
    #     """Enable/disable availability zones. Returns self for method chaining."""
    #     self.enableAvailabilityZones = enable
    #     return self

    def set_extra_nodes(self, nodes: int) -> 'ComputeProperties':
        """Set the number of extra nodes. Returns self for method chaining."""
        self.extraNodes = nodes
        return self

    def to_dict(self) -> dict:
        return {
            "computePool": self.computePool,
            "computeSize": self.computeSize,
            "enableAutoscale": self.enableAutoscale,
            "autoScaleMinNodes": self.autoScaleMinNodes,
            "autoScaleMaxNodes": self.autoScaleMaxNodes,
            "extraNodes": self.extraNodes
        }

@dataclasses.dataclass
class AirflowProperties:
    """Represents Airflow-specific properties configuration."""
    airflowEnvironment: str = "FabricAirflowJob-1.0.0"
    airflowVersion: str = "2.10.5"
    pythonVersion: str = "3.12"
    enableAADIntegration: bool = True
    enableTriggerers: bool = True
    packageProviderPath: str = "plugins"
    
    # Use internal dictionaries and lists that will be managed by methods
    _airflowConfigurationOverrides: dict = dataclasses.field(default_factory=dict)
    _environmentVariables: dict = dataclasses.field(default_factory=dict)
    _airflowRequirements: t.List[str] = dataclasses.field(default_factory=list)
    _secrets: t.List[dict] = dataclasses.field(default_factory=list)

    def add_airflow_config_override(self, key: str, value: str) -> 'AirflowProperties':
        """Add an Airflow configuration override. Returns self for method chaining."""
        self._airflowConfigurationOverrides[key] = value
        return self

    def remove_airflow_config_override(self, key: str) -> 'AirflowProperties':
        """Remove an Airflow configuration override. Returns self for method chaining."""
        self._airflowConfigurationOverrides.pop(key, None)
        return self

    def get_airflow_config_overrides(self) -> dict:
        """Get all Airflow configuration overrides."""
        return self._airflowConfigurationOverrides.copy()

    def add_environment_variable(self, key: str, value: str) -> 'AirflowProperties':
        """Add an environment variable. Returns self for method chaining."""
        self._environmentVariables[key] = value
        return self

    def remove_environment_variable(self, key: str) -> 'AirflowProperties':
        """Remove an environment variable. Returns self for method chaining."""
        self._environmentVariables.pop(key, None)
        return self

    def get_environment_variables(self) -> dict:
        """Get all environment variables."""
        return self._environmentVariables.copy()

    def add_requirement(self, requirement: str) -> 'AirflowProperties':
        """Add a Python requirement. Returns self for method chaining."""
        if requirement not in self._airflowRequirements:
            self._airflowRequirements.append(requirement)
        return self

    def remove_requirement(self, requirement: str) -> 'AirflowProperties':
        """Remove a Python requirement. Returns self for method chaining."""
        if requirement in self._airflowRequirements:
            self._airflowRequirements.remove(requirement)
        return self

    def get_requirements(self) -> t.List[str]:
        """Get all Python requirements."""
        return self._airflowRequirements.copy()

    def to_dict(self) -> dict:
        return {
            "airflowConfigurationOverrides": self._airflowConfigurationOverrides,
            "airflowEnvironment": self.airflowEnvironment,
            "airflowRequirements": self._airflowRequirements,
            "airflowVersion": self.airflowVersion,
            "enableAADIntegration": self.enableAADIntegration,
            "enableTriggerers": self.enableTriggerers,
            "environmentVariables": self._environmentVariables,
            "packageProviderPath": self.packageProviderPath,
            "pythonVersion": self.pythonVersion,
            "secrets": self._secrets
        }


@dataclasses.dataclass
class AirflowDefinition:
    """Represents the complete Airflow job definition payload."""
    type: str = "Airflow"
    airflowProperties = AirflowProperties()
    computeProperties = ComputeProperties()

    # Getter methods for complete properties
    def get_airflow_properties(self) -> AirflowProperties:
        """Get the complete AirflowProperties object."""
        return self.airflowProperties

    def get_compute_properties(self) -> ComputeProperties:
        """Get the complete ComputeProperties object."""
        return self.computeProperties


    def to_dict(self) -> dict:
        return {
            "properties": {
                "type": self.type,
                "typeProperties": {
                    "airflowProperties": self.airflowProperties.to_dict(),
                    "computeProperties": self.computeProperties.to_dict()
                }
            }
        }

@dataclasses.dataclass
class FabricDefinitionPart:
    """Represents a part of the Fabric item definition."""
    path: str
    payload: t.Union[str, dict, bytes, AirflowDefinition]
    payloadType: str = "InlineBase64"
    
    def to_dict(self) -> dict:
        """Convert to dictionary with base64 encoded payload."""
        if isinstance(self.payload, AirflowDefinition):
            payload_bytes = json.dumps(self.payload.to_dict()).encode('utf-8')
        elif isinstance(self.payload, dict):
            payload_bytes = json.dumps(self.payload).encode('utf-8')
        elif isinstance(self.payload, str):
            payload_bytes = self.payload.encode('utf-8')
        elif isinstance(self.payload, bytes):
            payload_bytes = self.payload
        else:
            raise ValueError(f"Unsupported payload type: {type(self.payload)}")
        
        # Base64 encode the payload
        encoded_payload = base64.b64encode(payload_bytes).decode('utf-8')
        
        return {
            "path": self.path,
            "payload": encoded_payload,
            "payloadType": self.payloadType
        }
    

@dataclasses.dataclass
class AirflowDefinitionRequest:
    """Request model for creating Airflow item with definition."""
    displayName: str
    description: t.Optional[str] = None
    parts: t.List[FabricDefinitionPart] = dataclasses.field(default_factory=list)
    
    def __init__(self, displayName: str, description: t.Optional[str] = None):
        """
        Initialize AirflowDefinitionRequest with basic properties.
        
        Args:
            displayName: The display name for the Airflow item
            description: Optional description for the Airflow item
        """
        self.displayName = displayName
        self.description = description
        self.parts = []
    
    def add_part(self, part: FabricDefinitionPart) -> 'AirflowDefinitionRequest':
        """Add a definition part. Returns self for method chaining."""
        self.parts.append(part)
        return self
        
    def add_dag_from_file(self, destinationFilePath: str, file_path: str) -> 'AirflowDefinitionRequest':
        """Add a DAG from file. Returns self for method chaining."""
        
        with open(file_path, 'r', encoding='utf-8') as f:
            dag_content = f.read()

        dag_part = FabricDefinitionPart(
            path=destinationFilePath,
            payload=dag_content,
            payloadType="InlineBase64"
        )

        self.parts.append(dag_part)
        return self
    
    def add_airflow_definition(self, airflow_definition: AirflowDefinition) -> 'AirflowDefinitionRequest':
        """User is creating an Airflow Artifact, add .project file as well"""
        payload = json.dumps(
        {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
            "metadata": {
                "type": "ApacheAirflowJob",
                "displayName": f"{self.displayName}"
            },
            "config": {
                "version": "2.0",
                "logicalId": "00000000-0000-0000-0000-000000000000"
            }
        })
        self.add_part(FabricDefinitionPart(".platform", payloadType="InlineBase64", payload=payload))

        """Add an Airflow definition. Returns self for method chaining."""
        definition_part = FabricDefinitionPart(
            path="apacheairflowjob-content.json",
            payload=airflow_definition,
            payloadType="InlineBase64"
        )
        self.parts.append(definition_part)
        return self
    
    def get_parts(self) -> t.List[FabricDefinitionPart]:
        """Get all definition parts."""
        return self.parts.copy()
    
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
class AirflowItemRequest:
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