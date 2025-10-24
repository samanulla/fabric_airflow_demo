import dataclasses
import typing as t


@dataclasses.dataclass
@dataclasses.dataclass
class AirflowWorkspaceSettings:
    defaultPoolTemplateId: str
    
    def to_dict(self) -> dict:
        return {"defaultPoolTemplateId": self.defaultPoolTemplateId}
    
    @classmethod
    def from_dict(cls, data: dict) -> 'AirflowWorkspaceSettings':
        return cls(
            defaultPoolTemplateId=data.get('defaultPoolTemplateId', '')
        )


@dataclasses.dataclass
class AirflowEnvironmentSettingsPayload:
    environmentVariables: t.Optional[t.List[dict]] = None  # list[{"name": "...", "value": "..."}]
    airflowConfigurationOverrides: t.Optional[t.List[dict]] = None
    triggerers: t.Optional[str] = None  # 'Enabled' | 'Disabled'

    def to_dict(self) -> dict:
        d: dict = {}
        if self.environmentVariables is not None:
            d["environmentVariables"] = self.environmentVariables
        if self.airflowConfigurationOverrides is not None:
            d["airflowConfigurationOverrides"] = self.airflowConfigurationOverrides
        if self.triggerers is not None:
            d["triggerers"] = self.triggerers
        return d


@dataclasses.dataclass
class AirflowEnvironmentComputeRequest:
    poolTemplateId: str
    def to_dict(self) -> dict:
        return {"poolTemplateId": self.poolTemplateId}


@dataclasses.dataclass
class AirflowEnvironmentStorageRequest:
    storage: dict  # shape per spec (e.g., {"type": "...", "properties": {...}})
    def to_dict(self) -> dict:
        return {"storage": self.storage}


@dataclasses.dataclass
class AirflowEnvironmentVersionRequest:
    apacheAirflowJobVersion: str
    def to_dict(self) -> dict:
        return {"apacheAirflowJobVersion": self.apacheAirflowJobVersion}


# ---------- Response Data Models ----------

@dataclasses.dataclass
class AirflowJobVersionDetails:
    """Details about the Apache Airflow job version"""
    apacheAirflowVersion: str
    pythonVersion: str

    @classmethod
    def from_dict(cls, data: dict) -> "AirflowJobVersionDetails":
        return cls(
            apacheAirflowVersion=data.get("apacheAirflowVersion", ""),
            pythonVersion=data.get("pythonVersion", "")
        )


@dataclasses.dataclass
class WorkerScalability:
    """Worker scalability configuration"""
    minNodeCount: int
    maxNodeCount: int

    @classmethod
    def from_dict(cls, data: dict) -> "WorkerScalability":
        return cls(
            minNodeCount=data.get("minNodeCount", 1),
            maxNodeCount=data.get("maxNodeCount", 1)
        )

    def to_dict(self) -> dict:
        return {
            "minNodeCount": self.minNodeCount,
            "maxNodeCount": self.maxNodeCount
        }

@dataclasses.dataclass
class AirflowPoolTemplate:

    """Individual pool template in the workspace"""
    poolTemplateName: str
    nodeSize: str  # 'Small' | 'Large'
    workerScalability: t.Optional[WorkerScalability] = None  # optional - can be set by client
    apacheAirflowJobVersion: t.Optional[str] = None  # optional - can be set by client
    poolTemplateId: t.Optional[str] = None  # readonly - set by server
    apacheAirflowJobVersionDetails: t.Optional[AirflowJobVersionDetails] = None  # readonly - set by server
    availabilityZones: t.Optional[bool] = None  # readonly - set by server
    shutdownPolicy: t.Optional[str] = None  # 'OneHourInactivity' | 'AlwaysOn' - optional - can be set by client

    @classmethod
    def from_dict(cls, data: dict) -> "AirflowPoolTemplate":
        # Parse nested objects if present
        worker_scalability = None
        if "workerScalability" in data and data["workerScalability"]:
            worker_scalability = WorkerScalability.from_dict(data["workerScalability"])
        
        job_version_details = None
        if "apacheAirflowJobVersionDetails" in data and data["apacheAirflowJobVersionDetails"]:
            job_version_details = AirflowJobVersionDetails.from_dict(data["apacheAirflowJobVersionDetails"])
        
        return cls(
            poolTemplateName=data.get("poolTemplateName", ""),
            nodeSize=data.get("nodeSize", "Small"),
            workerScalability=worker_scalability,
            apacheAirflowJobVersion=data.get("apacheAirflowJobVersion"),
            poolTemplateId=data.get("poolTemplateId"),
            apacheAirflowJobVersionDetails=job_version_details,
            availabilityZones=data.get("availabilityZones"),
            shutdownPolicy=data.get("shutdownPolicy")
        )

    def to_dict(self) -> dict:
        """
        Convert to dictionary for API requests.
        Required fields are always included.
        Optional fields are only included if they have been provided (not None).
        Read-only fields (poolTemplateId, apacheAirflowJobVersionDetails, availabilityZones) are never included.
        """
        result: t.Dict[str, t.Any] = {
            "poolTemplateName": self.poolTemplateName,
            "nodeSize": self.nodeSize
        }
        
        # Include optional fields only if they are provided
        if self.workerScalability is not None:
            result["workerScalability"] = self.workerScalability.to_dict()
        
        if self.apacheAirflowJobVersion is not None:
            result["apacheAirflowJobVersion"] = self.apacheAirflowJobVersion
        
        if self.shutdownPolicy is not None:
            result["shutdownPolicy"] = self.shutdownPolicy
        
        return result


@dataclasses.dataclass
class AirflowPoolsTemplate:
    """Response structure for listing pool templates in workspace"""
    poolTemplates: t.List[AirflowPoolTemplate]

    @classmethod
    def from_dict(cls, data: dict) -> "AirflowPoolsTemplate":
        pool_templates_data = data.get("poolTemplates", [])
        pool_templates = [
            AirflowPoolTemplate.from_dict(template_data) 
            for template_data in pool_templates_data
        ]
        return cls(poolTemplates=pool_templates)

    def get_pool_by_id(self, pool_id: str) -> t.Optional[AirflowPoolTemplate]:
        """Find a pool template by ID"""
        for pool in self.poolTemplates:
            if pool.poolTemplateId == pool_id:
                return pool
        return None