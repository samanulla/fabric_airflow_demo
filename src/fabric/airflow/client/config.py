"""
Configuration module for Fabric Airflow API clients.

This module provides centralized configuration management with support for:
- Environment variables for sensitive credentials
- Singleton pattern for consistent configuration across the application
- Factory methods for creating pre-configured API clients
- Safe credential handling without exposing secrets in code

Environment Variables:
    FABRIC_TENANT_ID: Azure AD tenant ID
    FABRIC_CLIENT_ID: Application client ID for SPN authentication
    FABRIC_CLIENT_SECRET: Application client secret for SPN authentication
    FABRIC_WORKSPACE_ID: Default workspace ID
    FABRIC_AIRFLOW_JOB_ID: Default Airflow job ID
    FABRIC_BASE_URL: Fabric API base URL (default: https://api.fabric.microsoft.com)
    AIRFLOW_WEBSERVER_URL: Airflow webserver base URL
    AIRFLOW_API_SCOPE: Airflow API scope for authentication (default: 5d13f7d7-0567-429c-9880-320e9555e5fc/.default)
    FABRIC_API_SCOPE: Fabric API scope (default: https://api.fabric.microsoft.com/.default)
    DEBUG: Enable debug logging (true/false)
"""

import os
import typing as t
from pathlib import Path
from fabric.airflow.client.authentication_provider import AuthenticationProvider


class ConfigurationError(Exception):
    """Raised when required configuration is missing"""
    pass


class Config:
    """
    Configuration manager for Fabric Airflow API clients.
    
    Loads configuration from INI files or environment variables and provides factory methods
    for creating pre-configured API clients.
    
    Example from INI file:
        config = Config.from_file('config.ini', environment='DEV')
        files_client = config.files_client()
        control_plane_client = config.control_plane_client()
        
    Example from environment variables:
        # Set environment variables first
        os.environ['FABRIC_TENANT_ID'] = 'your-tenant-id'
        # ... set other variables
        
        config = Config.from_env()
        files_client = config.files_client()
    """
    
    def __init__(
        self,
        tenant_id: t.Optional[str] = None,
        client_id: t.Optional[str] = None,
        client_secret: t.Optional[str] = None,
        workspace_id: t.Optional[str] = None,
        airflow_job_id: t.Optional[str] = None,
        fabric_base_url: t.Optional[str] = None,
        airflow_webserver_url: t.Optional[str] = None,
        airflow_api_scope: t.Optional[str] = None,
        fabric_api_scope: t.Optional[str] = None,
        debug: t.Optional[bool] = None,
        is_preview_enabled: bool = True
    ):
        """
        Initialize configuration with validation.
        
        Args:
            tenant_id: Azure AD tenant ID
            client_id: Application client ID
            client_secret: Application client secret
            workspace_id: Workspace ID
            airflow_job_id: Airflow job ID
            fabric_base_url: Fabric API base URL
            airflow_webserver_url: Airflow webserver URL
            airflow_api_scope: Airflow API scope
            fabric_api_scope: Fabric API scope
            debug: Enable debug mode
            is_preview_enabled: Enable preview features
        """
        # Store configuration values with fallback to hardcoded defaults
        self._tenant_id = tenant_id
        self._client_id = client_id
        self._client_secret = client_secret
        self._workspace_id = workspace_id
        self._airflow_job_id = airflow_job_id
        self._fabric_base_url = fabric_base_url or 'https://api.fabric.microsoft.com'
        self._airflow_webserver_url = airflow_webserver_url
        self._airflow_api_scope = airflow_api_scope or '5d13f7d7-0567-429c-9880-320e9555e5fc/.default'
        self._fabric_api_scope = fabric_api_scope or 'https://api.fabric.microsoft.com/.default'
        self._debug = debug if debug is not None else False
        self._is_preview_enabled = is_preview_enabled
        
        # Validate configuration
        self._validate_config(
            tenant_id=self._tenant_id,
            client_id=self._client_id,
            client_secret=self._client_secret
        )
        
        # Cached clients
        self._files_client = None
        self._control_plane_client = None
        self._native_client = None
        self._crud_client = None
    
    def _validate_config(self, **required_fields):
        """Validate that required configuration fields are available"""
        for field_name, field_value in required_fields.items():
            if not field_value:
                env_var = f"FABRIC_{field_name.upper()}"
                raise ConfigurationError(
                    f"{field_name} is required. Set {env_var} environment variable or pass to Config()"
                )
    
    # Property accessors
    
    @property
    def tenant_id(self) -> str:
        """Get tenant ID"""
        return self._tenant_id  # type: ignore
    
    @property
    def client_id(self) -> str:
        """Get client ID"""
        return self._client_id  # type: ignore
    
    @property
    def client_secret(self) -> str:
        """Get client secret"""
        return self._client_secret  # type: ignore
    
    @property
    def workspace_id(self) -> str:
        """Get workspace ID"""
        if not self._workspace_id:
            raise ConfigurationError("workspace_id not configured. Set FABRIC_WORKSPACE_ID environment variable or pass to Config()")
        return self._workspace_id
    
    @property
    def airflow_job_id(self) -> str:
        """Get Airflow job ID"""
        if not self._airflow_job_id:
            raise ConfigurationError("airflow_job_id not configured. Set FABRIC_AIRFLOW_JOB_ID environment variable or pass to Config()")
        return self._airflow_job_id
    
    @property
    def fabric_base_url(self) -> str:
        """Get Fabric base URL"""
        return self._fabric_base_url
    
    @property
    def airflow_webserver_url(self) -> str:
        """Get Airflow webserver URL"""
        if not self._airflow_webserver_url:
            raise ConfigurationError("airflow_webserver_url not configured. Set AIRFLOW_WEBSERVER_URL environment variable or pass to Config()")
        return self._airflow_webserver_url
    
    # Factory methods for auth providers
    
    def create_fabric_auth_provider(self) -> AuthenticationProvider:
        """Create authentication provider for Fabric API"""
        return AuthenticationProvider(
            tenant_id=self._tenant_id,
            client_id=self._client_id,
            client_secret=self._client_secret,
            scope=self._fabric_api_scope
        )
    
    def create_airflow_auth_provider(self) -> AuthenticationProvider:
        """Create authentication provider for Airflow Native API"""
        return AuthenticationProvider(
            tenant_id=self._tenant_id,
            client_id=self._client_id,
            client_secret=self._client_secret,
            scope=self._airflow_api_scope
        )
    
    # Factory class methods for creating Config instances
    
    @classmethod
    def from_file(cls, config_file: t.Union[str, Path], environment: str = 'DEFAULT') -> 'Config':
        """
        Create Config instance from an INI configuration file.
        
        Example INI format with multiple environments:
        [DEFAULT]
        tenant_id = your-tenant-id
        client_id = your-client-id
        client_secret = your-client-secret
        
        [DEV]
        workspace_id = dev-workspace-id
        airflow_job_id = dev-airflow-job-id
        airflow_webserver_url = https://dev-airflow.com
        
        [PROD]
        workspace_id = prod-workspace-id
        airflow_job_id = prod-airflow-job-id
        airflow_webserver_url = https://prod-airflow.com
        
        Example usage:
            config = Config.from_file('config.ini', environment='DEV')
            files_client = config.files_client()
        
        Args:
            config_file: Path to configuration file (.ini or .cfg)
            environment: Section name to load from INI file (default: 'DEFAULT')
            
        Returns:
            Config: Initialized Config instance
            
        Raises:
            ConfigurationError: If file cannot be read or parsed
            FileNotFoundError: If config file doesn't exist
        """
        config_path = Path(config_file)
        
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        # Determine file format by extension
        ext = config_path.suffix.lower()
        
        try:
            if ext in ('.ini', '.cfg'):
                config_data = cls._load_ini_config(config_path, environment)
            else:
                raise ConfigurationError(
                    f"Unsupported config file format: {ext}. "
                    f"Supported formats: .ini, .cfg"
                )
            
            # Create Config instance from loaded data
            return cls(**config_data)
            
        except Exception as e:
            if isinstance(e, (ConfigurationError, FileNotFoundError)):
                raise
            raise ConfigurationError(f"Failed to load configuration from {config_path}: {e}")
    
    @classmethod
    def from_env(cls) -> 'Config':
        """
        Create Config instance from environment variables.
        
        Example usage:
            import os
            os.environ['FABRIC_TENANT_ID'] = 'your-tenant-id'
            # ... set other environment variables
            
            config = Config.from_env()
            files_client = config.files_client()
        
        Returns:
            Config: Initialized Config instance
        """
        return cls(
            tenant_id=os.getenv('FABRIC_TENANT_ID'),
            client_id=os.getenv('FABRIC_CLIENT_ID'),
            client_secret=os.getenv('FABRIC_CLIENT_SECRET'),
            workspace_id=os.getenv('FABRIC_WORKSPACE_ID'),
            airflow_job_id=os.getenv('FABRIC_AIRFLOW_JOB_ID'),
            fabric_base_url=os.getenv('FABRIC_BASE_URL'),
            airflow_webserver_url=os.getenv('AIRFLOW_WEBSERVER_URL'),
            airflow_api_scope=os.getenv('AIRFLOW_API_SCOPE'),
            fabric_api_scope=os.getenv('FABRIC_API_SCOPE'),
            debug=os.getenv('DEBUG', '').lower() in ('true', '1', 'yes', 'on'),
            is_preview_enabled=True
        )
    
    @staticmethod
    def _load_ini_config(config_path: Path, environment: str = 'DEFAULT') -> dict:
        """
        Load configuration from INI file for specified environment.
        
        Supports multiple environments using INI sections. Values from the specified
        environment section override values from DEFAULT section.
        
        Args:
            config_path: Path to the INI configuration file
            environment: Section name to load (default: 'DEFAULT')
            
        Returns:
            dict: Configuration data merged from DEFAULT and environment section
        """
        import configparser
        
        parser = configparser.ConfigParser()
        parser.read(config_path, encoding='utf-8')
        
        config_data = {}
        
        # First, load from DEFAULT section if it exists
        if parser.has_section('DEFAULT') or parser.defaults():
            for key, value in parser.items('DEFAULT'):
                config_data[key] = Config._parse_config_value(value)
        
        # Then, load from specified environment section (overrides DEFAULT)
        if environment != 'DEFAULT' and parser.has_section(environment):
            for key, value in parser.items(environment):
                # Skip if this key is from DEFAULT section (already processed)
                if key not in parser.defaults():
                    config_data[key] = Config._parse_config_value(value)
        
        return config_data
    
    @staticmethod
    def _parse_config_value(value: str) -> t.Union[bool, str]:
        """Parse configuration value from string"""
        if value.lower() in ('true', 'yes', '1', 'on'):
            return True
        elif value.lower() in ('false', 'no', '0', 'off'):
            return False
        return value
    
    # Instance methods for creating API clients
    
    def files_client(self):
        """Get or create AirflowFilesApiClient instance"""
        from fabric.airflow.client.fabric_files_api_client import AirflowFilesApiClient
        
        if self._files_client is None:
            auth_provider = self.create_fabric_auth_provider()
            self._files_client = AirflowFilesApiClient(
                workspace_id=self.workspace_id,
                airflow_job_id=self.airflow_job_id,
                base_url=self._fabric_base_url,
                auth_provider=auth_provider,
                debug=self._debug,
                is_preview_enabled=self._is_preview_enabled
            )
        return self._files_client
    
    def control_plane_client(self):
        """Get or create FabricControlPlaneApiClient instance"""
        from fabric.airflow.client.fabric_control_plane_api_client import FabricControlPlaneApiClient
        
        if self._control_plane_client is None:
            auth_provider = self.create_fabric_auth_provider()
            self._control_plane_client = FabricControlPlaneApiClient(
                workspace_id=self.workspace_id,
                airflow_job_id=self.airflow_job_id,
                base_url=self._fabric_base_url,
                auth_provider=auth_provider,
                debug=self._debug,
                is_preview_enabled=self._is_preview_enabled
            )
        return self._control_plane_client
    
    def airflow_native_client(self):
        """Get or create AirflowApiClient instance"""
        from fabric.airflow.client.airflow_api_client import AirflowApiClient
        
        if self._native_client is None:
            auth_provider = self.create_airflow_auth_provider()
            self._native_client = AirflowApiClient(
                base_url=self.airflow_webserver_url,
                auth_provider=auth_provider,
                debug=self._debug
            )
        return self._native_client
    
    def crud_client(self):
        """Get or create AirflowCrudApiClient instance"""
        from fabric.airflow.client.fabric_crud_api_client import AirflowCrudApiClient
        
        if self._crud_client is None:
            auth_provider = self.create_fabric_auth_provider()
            self._crud_client = AirflowCrudApiClient(
                auth_provider=auth_provider,
                base_url=self._fabric_base_url,
                debug=self._debug,
                is_preview_enabled=self._is_preview_enabled
            )
        return self._crud_client
