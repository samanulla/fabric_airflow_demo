"""
Example script demonstrating Fabric Airflow API client usage.

This script shows how to use all API clients with Config.setup() from a configuration file.
All examples use config.ini in the sample folder.
"""

import logging
import os
import typing as t
import json
import base64
from pathlib import Path
from fabric.airflow.client.config import Config, ConfigurationError
from fabric.airflow.client.api_exceptions import (
    ValidationError, AuthenticationError, ForbiddenError, 
    NotFoundError, ClientError, ServerError
)
import random

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get the config file path relative to this script
SAMPLE_DIR = Path(__file__).parent
CONFIG_FILE = os.getenv('CONFIG_FILE_PATH') 

if not CONFIG_FILE:
    print("❌ ERROR: CONFIG_FILE_PATH environment variable is not set!")
    print("   Please set it to point to your config.ini file:")
    print("   PowerShell: $env:CONFIG_FILE_PATH = 'c:\\src\\ApiTest\\config.ini'")
    print("   Or add it to .env file in the workspace root")
    exit(1)

# Global config instance
config: Config

def initialize_config():
    """Initialize Config from config.ini file"""
    global config
    try:
        assert CONFIG_FILE is not None, "CONFIG_FILE should be set (checked above)"
        # Load config from ApiTest environment by default
        config = Config.from_file(CONFIG_FILE, 'ApiTest')
        logger.info(f"✅ Configuration loaded from {CONFIG_FILE} [ApiTest environment]")
        return True
    except FileNotFoundError:
        logger.error(f"❌ {CONFIG_FILE} not found. Please create it with your credentials.")
        return False
    except ConfigurationError as e:
        logger.error(f"❌ Configuration error: {e}")
        return False


def example_files_api():
    """Example: Files API - Upload, list, download, and delete DAG files"""
    print("\n" + "="*60)
    print("FILES API EXAMPLES")
    print("="*60)
    
    try:
        files_client = config.files_client()
        
        # Use sample DAG from the same folder
        dag_file = SAMPLE_DIR / "sample_dag.py"
        
        if dag_file.exists():
            # Upload DAG file
            with open(dag_file, 'r', encoding='utf-8') as f:
                dag_content = f.read()
            
            logger.info("Uploading sample DAG file...")
            response = files_client.create_or_update_file("dags/sample_dag.py", dag_content)
            logger.info(f"✅ DAG uploaded - Status: {response.status}")
            
            # List DAG files
            logger.info("Listing DAG files...")
            response = files_client.list_files(root_path="dags")
            logger.info(f"✅ Found {len(response.body.get('files', []))} DAG files")
            for file in response.body.get('files', [])[:5]:  # Show first 5
                logger.info(f"   - {file.get('filePath')} ({file.get('sizeInBytes', 0)} bytes)")
            
            # Download DAG file
            logger.info("Downloading DAG file...")
            response = files_client.get_file("dags/sample_dag.py")
            logger.info(f"✅ Downloaded {len(response.body)} bytes")
            
            # Update DAG file (add a comment)
            logger.info("Updating DAG file...")
            updated_content = "# This file was updated via API\n" + dag_content
            response = files_client.create_or_update_file("dags/sample_dag.py", updated_content)
            logger.info(f"✅ DAG updated - Status: {response.status}")
            
            # List files in root directory
            logger.info("Listing root directory...")
            response = files_client.list_files(root_path="/")
            logger.info(f"✅ Root directories:")
            for item in response.body.get('files', []):
                logger.info(f"   - {item.get('filePath')}")
            
            # Delete DAG file (commented out to keep the file)
            # logger.info("Deleting DAG file...")
            # response = files_client.delete_file("dags/sample_dag.py")
            # logger.info(f"✅ DAG deleted - Status: {response.status}")
        else:
            logger.warning(f"Sample DAG file not found: {dag_file}")
            logger.warning("Run this script from the project root or ensure sample_dag.py exists")
            
    except Exception as e:
        logger.error(f"❌ Files API error: {e}")


def example_control_plane_api():
    """Example: Control Plane API - Workspace settings and pool management"""
    print("\n" + "="*60)
    print("CONTROL PLANE API EXAMPLES")
    print("="*60)
    
    try:
        from fabric.airflow.client.fabric_control_plane_model import (
            AirflowPoolTemplate, 
            WorkerScalability
        )
        
        cp_client = config.control_plane_client()
        
        # Get workspace settings
        logger.info("Getting workspace settings...")
        settings = cp_client.get_workspace_settings()
        logger.info(f"✅ Workspace settings retrieved")
        logger.info(f"   Settings: {settings}")
        
        # Create pool template (commented out to avoid creating resources)
        # logger.info("Creating pool template...")
        # pool = AirflowPoolTemplate(
        #     poolTemplateName="ExamplePool",
        #     nodeSize="Small",
        #     workerScalability=WorkerScalability(minNodeCount=1, maxNodeCount=3),
        #     apacheAirflowJobVersion="1.0.0"
        # )
        # pool_id = cp_client.create_pool_template(pool)
        # logger.info(f"✅ Pool created with ID: {pool_id}")
        
    except Exception as e:
        logger.error(f"❌ Control Plane API error: {e}")


def example_airflow_native_api():
    """Example: Airflow Native API - DAG operations via Airflow REST API"""
    print("\n" + "="*60)
    print("AIRFLOW NATIVE API EXAMPLES")
    print("="*60)
    
    try:
        native_client = config.airflow_native_client()
        
        # List DAGs
        logger.info("Listing DAGs...")
        response = native_client.list_dags(limit=10)
        
        if response.status == 200:
            dags = response.body.get('dags', [])
            logger.info(f"✅ Found {len(dags)} DAGs")
            for dag in dags[:5]:  # Show first 5
                logger.info(f"   - {dag.get('dag_id')}: {dag.get('is_active')}")
        
        # Note: Other operations like trigger_dag_run would require an existing DAGclea
        # response = native_client.trigger_dag_run("my_dag_id")
        
    except Exception as e:
        logger.error(f"❌ Airflow Native API error: {e}")


def fetch_and_update_definition():
    """
    Fetch an Airflow job definition, modify it, and update it back.
    
    Args:
        workspace_id: The workspace ID
        airflow_job_id: The Airflow job ID
        
    Example:
        >>> def add_my_changes(definition):
        ...     definition.add_dag("dags/my_dag.py", "from airflow import DAG...")
        ...     airflow_part = definition.get_airflow_definition()
        ...     if airflow_part:
        ...         config = airflow_part.as_json()
        ...         # Modify config...
        ...         airflow_part.payload = config
        >>> 
        >>> fetch_and_update_definition(workspace_id, job_id, add_my_changes)
    """
    crud_client = config.crud_client()
    workspace_id = config.workspace_id
    airflow_job_id = config.airflow_job_id
    
    try:
        # Fetch the definition
        logger.info(f"Fetching Airflow job definition for {airflow_job_id}...")
        definition = crud_client.get_airflow_job_definition(
            workspace_id=workspace_id, 
            airflow_job_id=airflow_job_id
        )
        logger.info(f"✅ Fetched definition with {len(definition.parts)} parts")
        
        # Apply modifications (may return a new FabricItemDefinition)
        logger.info("Applying modifications...")
        new_definition = _modify_definition(definition)
        if new_definition:
            definition = new_definition
        
        # Update the definition
        logger.info("Updating Airflow job definition...")
        try:
            # Log the outgoing request body parts (decode InlineBase64 payloads for inspection)
            outgoing = definition.to_dict()
            parts = outgoing.get('definition', {}).get('parts', [])
            for p in parts:
                path = p.get('path')
                payload = p.get('payload')
                ptype = p.get('payloadType')
                if ptype == 'InlineBase64' and isinstance(payload, str):
                    try:
                        decoded = base64.b64decode(payload).decode('utf-8')
                        logger.info(f"Outgoing part '{path}': {decoded}")
                    except Exception:
                        logger.info(f"Outgoing part '{path}': <unable to decode payload>")
                else:
                    logger.info(f"Outgoing part '{path}': {payload}")
        except Exception:
            logger.info("Could not serialize outgoing payload for debug")

        crud_client.update_airflow_job_definition(
            workspace_id=workspace_id,
            airflow_job_id=airflow_job_id, 
            definition=definition,
            update_metadata=False
        )
        logger.info(f"✅ Successfully updated Airflow job definition")
        
    except Exception as e:
        logger.error(f"❌ Failed to update definition: {e}")
        raise


def _modify_definition(definition):
    """Modify the Airflow job definition"""
    # Get current Airflow configuration part
    airflow_part = definition.get_airflow_definition()
    if airflow_part:
        # Get the configuration as JSON
        airflow_config = airflow_part.as_json()
        
        if airflow_config:
            # Navigate to airflowProperties (properties.typeProperties.airflowProperties)
            type_props = airflow_config.get('properties', {}).get('typeProperties', {})
            airflow_props = type_props.get('airflowProperties', {})
            # Load requested updates from sample file (if present)
            update_file = SAMPLE_DIR / "update_airflow_definition.json"
            requested = {}
            if update_file.exists():
                try:
                    with open(update_file, 'r', encoding='utf-8') as f:
                        upd = json.load(f)
                        requested = upd.get('properties', {}).get('typeProperties', {}).get('airflowProperties', {}) or {}
                except Exception as e:
                    logger.error(f"Failed to load update file: {e}")

            # If the update file accidentally nests 'airflowConfigurationOverrides' under itself, unwrap one level
            # BUT only unwrap when the outer dict contains only that nested key. This avoids discarding sibling keys
            # such as environmentVariables which should remain available.
            if isinstance(requested, dict):
                ao = requested.get('airflowConfigurationOverrides')
                if isinstance(ao, dict) and 'airflowConfigurationOverrides' in ao and len(ao) == 1:
                    requested['airflowConfigurationOverrides'] = ao.get('airflowConfigurationOverrides') or {}

                # If the user placed environmentVariables or airflowRequirements inside airflowConfigurationOverrides,
                # lift them to top-level requested so they will be applied to airflow_props.
                if isinstance(ao, dict):
                    if 'environmentVariables' in ao and 'environmentVariables' not in requested:
                        requested['environmentVariables'] = ao.get('environmentVariables')
                    if 'airflowRequirements' in ao and 'airflowRequirements' not in requested:
                        requested['airflowRequirements'] = ao.get('airflowRequirements')

            # Apply values from update file for allowed keys
            # airflowConfigurationOverrides (dict), environmentVariables (dict), airflowRequirements (list)
            if 'airflowConfigurationOverrides' in requested:
                if 'airflowConfigurationOverrides' not in airflow_props or not isinstance(airflow_props.get('airflowConfigurationOverrides'), dict):
                    airflow_props['airflowConfigurationOverrides'] = {}
                # shallow merge from requested
                for k, v in (requested.get('airflowConfigurationOverrides') or {}).items():
                    airflow_props['airflowConfigurationOverrides'][k] = v

            if 'environmentVariables' in requested:
                if 'environmentVariables' not in airflow_props or not isinstance(airflow_props.get('environmentVariables'), dict):
                    airflow_props['environmentVariables'] = {}
                for k, v in (requested.get('environmentVariables') or {}).items():
                    airflow_props['environmentVariables'][k] = v

            if 'airflowRequirements' in requested:
                airflow_props['airflowRequirements'] = list(requested.get('airflowRequirements') or [])

            # If user provided secrets in the update file, copy them into airflowProperties
            # This mirrors behavior when creating a job with secrets present.
            if 'secrets' in requested:
                # ensure secrets is a list
                try:
                    secrets_list = list(requested.get('secrets') or [])
                except Exception:
                    secrets_list = []
                if secrets_list:
                    airflow_props['secrets'] = secrets_list

            # Update the part's payload with modified configuration
            airflow_part.payload = airflow_config
            logger.info(f"✅ Applied updates from update_airflow_definition.json to airflowProperties")


def _modify_definition_orig(definition):
    """Modify the Airflow job definition"""
    # Get current Airflow configuration part
    airflow_part = definition.get_airflow_definition()
    if airflow_part:
        # Get the configuration as JSON
        airflow_config = airflow_part.as_json()
        
        if airflow_config:
            # Navigate to airflowProperties (properties.typeProperties.airflowProperties)
            type_props = airflow_config.get('properties', {}).get('typeProperties', {})
            airflow_props = type_props.get('airflowProperties', {})
            
            # Add configuration override to airflowConfigurationOverrides
            if 'airflowConfigurationOverrides' not in airflow_props:
                airflow_props['airflowConfigurationOverrides'] = {}
            airflow_props['airflowConfigurationOverrides']['my_custom_setting'] = 'my_custom_settings-aman'
            
            # Add environment variable to environmentVariables
            if 'environmentVariables' not in airflow_props:
                airflow_props['environmentVariables'] = {}
            airflow_props['environmentVariables']['MY_ENV_VAR'] = 'aman-ENV'
            
            # Add requirement to airflowRequirements
            if 'airflowRequirements' not in airflow_props:
                airflow_props['airflowRequirements'] = []
            if 'flask-bcrypt' not in airflow_props['airflowRequirements']:
                airflow_props['airflowRequirements'].append('flask-bcrypt')
            
            airflow_props['airflowRequirements'].append('pandas==1.3.3')  # Example of specific version
            
            # Update the part's payload with modified configuration
            airflow_part.payload = airflow_config
            
            logger.info(f"✅ Added configuration override, environment variable, and flask-bcrypt requirement")



def example_crud_api():
    """Example: CRUD API - Create and manage Airflow jobs"""
    print("\n" + "="*60)
    print("CRUD API EXAMPLES")
    print("="*60)
    
    try:
        from fabric.airflow.client.fabric_crud_model import (
            AirflowItem, FabricItemDefinition
        )
        
        crud_client = config.crud_client()
        workspace_id = config.workspace_id
        
        # List existing Airflow jobs
        logger.info("Listing Airflow jobs...")
        response = crud_client.list_airflow_jobs(workspace_id)
        logger.info(f"✅ List jobs - Status: {response.status}")
              
        # # Example: Create blank Airflow job (commented out to avoid creating resources)
        # logger.info("Creating blank Airflow job...")
        # request = AirflowItem(
        #     displayName="ExampleAirflowJob",
        #     description="Created via example_usage.py"
        # )
        # airflow_item = crud_client.create_airflow_job(workspace_id, request)
        # logger.info(f"✅ Airflow job created: {airflow_item.id}")
        
        # Example: Create Airflow job with definition and DAGs
        logger.info("Creating Airflow job with definition...")
        suffix = random.randint(1000, 9999)
        definition = FabricItemDefinition(
            displayName=f"ExampleWithDefinition-{suffix}",
            airflow_definition_file="C:\\src\\ApiTest\\src\\sample\\airflow_definition.json",
            description="Created with sample DAG definition"
        )
        crud_client.create_airflow_job_with_definition(workspace_id, definition)
        
    except ValidationError as ex:
        logger.error(f"❌ Validation Error (400): {ex.message}")
        logger.error(f"   Request ID: {ex.request_id}")
    except AuthenticationError as ex:
        logger.error(f"❌ Authentication Error (401): {ex.message}")
    except ForbiddenError as ex:
        logger.error(f"❌ Forbidden Error (403): {ex.message}")
    except NotFoundError as ex:
        logger.error(f"❌ Not Found Error (404): {ex.message}")
    except Exception as e:
        logger.error(f"❌ CRUD API error: {e}")


def example_error_handling():
    """Example: Proper error handling with specific exception types"""
    print("\n" + "="*60)
    print("ERROR HANDLING EXAMPLE")
    print("="*60)
    
    try:
        files_client = config.files_client()
        
        # This will likely cause a NotFoundError if the file doesn't exist
        logger.info("Attempting to get a non-existent file to demonstrate error handling...")
        response = files_client.get_file("dags/this_file_does_not_exist.py")
        
    except ValidationError as ex:
        logger.info(f"✅ Caught ValidationError (400): {ex.message}")
        logger.info(f"   Request ID: {ex.request_id}")
    except AuthenticationError as ex:
        logger.info(f"✅ Caught AuthenticationError (401): {ex.message}")
    except ForbiddenError as ex:
        logger.info(f"✅ Caught ForbiddenError (403): {ex.message}")
    except NotFoundError as ex:
        logger.info(f"✅ Caught NotFoundError (404): {ex.message}")
        logger.info(f"   Request ID: {ex.request_id}")
        logger.info("   This is expected - demonstrating proper error handling!")
    except ClientError as ex:
        logger.info(f"✅ Caught ClientError [{ex.status}]: {ex.message}")
    except ServerError as ex:
        logger.info(f"✅ Caught ServerError [{ex.status}]: {ex.message}")
    except Exception as ex:
        logger.error(f"❌ Unexpected error: {ex}")


if __name__ == "__main__":
    print("="*60)
    print("FABRIC AIRFLOW API CLIENT EXAMPLES")
    print("="*60)
    print(f"Using config file: {CONFIG_FILE}")
    print()
    
    # Initialize configuration
    if not initialize_config():
        print("\n❌ Failed to initialize configuration. Exiting.")
        exit(1)
    
    # Run examples    
    #example_crud_api() 
    fetch_and_update_definition()
    # example_files_api() #files
    #example_control_plane_api() 
    #example_airflow_native_api()
    #example_error_handling()
    
    print("\n" + "="*60)
    print("EXAMPLES COMPLETED")
    print("="*60)
    print(f"Note: Some operations are commented out to avoid creating resources.")
    print(f"      Edit {CONFIG_FILE} with your credentials to run these examples.")
    print("="*60)
