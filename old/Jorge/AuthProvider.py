from FabricClient import FabricClient
from azure.identity import ClientSecretCredential
from azure.identity import InteractiveBrowserCredential

class AuthenticationProvider:

    @classmethod
    def get_user_credential(cls, tenant_id:str="", authority: str="https://login.microsoftonline.com", scope:str="https://api.fabric.microsoft.com/.default"):
        
        # Create an interactive browser credential
        interactiveCredential = InteractiveBrowserCredential()

        # Get a token for the Fabric API
        token = interactiveCredential.get_token("https://api.fabric.microsoft.com/.default").token
        return token

    @classmethod
    def get_spn_token(cls, tenant_id:str, client_id: str, client_secret: str, scope: str="https://api.fabric.microsoft.com/.default"):

        credential = ClientSecretCredential(tenant_id, client_id, client_secret)
        access_token = credential.get_token("https://api.fabric.microsoft.com/.default").token
        return access_token

    
if __name__ == "__main__":

    workspace_id = "fdb6f419-ed52-468b-ae0b-277c67834468"
    artifact_id = "0b141ecd-ef2f-427d-b03e-947b8ec43b29" 

    # GET SPN credential
    tenant_id = "5ad710af-d4e9-4e36-9d03-548e8c6e64e4"
    client_id = "97c709cf-d3a0-4cbc-830d-6cdb72dfeea4"
    client_secret = "l0A8Q~9YjzzJfHjfiVV1jxajQWCKndgqU0F-vbPW"
    token = AuthenticationProvider.get_spn_token(tenant_id, client_id, client_secret);

    # GET user token credential
    #token = AuthenticationProvider.get_user_credential()
    client = FabricClient(access_token= token, base_url = "https://dailyapi.fabric.microsoft.com/v1")

    # Settings body example
    settingsEx = {
        "environmentVariables": [
            {
                "name": "hello",
                "value": "world"
            }
        ],
        "airflowConfigurationOverrides": [
            {
                "name": "AIRFLOW__SECRETS__BACKEND_KWARGS",
                "value": "test config"
            }
        ],
        "triggerers" : "Disabled"
    }

    # Airflow Environment Settings APIs
    #client.airflow_update_environment_settings(workspace_id, artifact_id, settingsEx)
    #client.airflow_get_environment_settings(workspace_id, artifact_id)

    #Airflow Environment Compute APIs
    client.airflow_get_workspace_settings(workspace_id=workspace_id)

    client.airflow_get_environment_compute(workspace_id, artifact_id)

    # File APIs
    file_path = "dags/requirements.txt"
    payload_str = "This is a test file"
    #client.airflow_list_files(workspace_id, artifact_id, file_path)
    #client.airflow_createOrUpdate_file(workspace_id, artifact_id, file_path, payload_str)
    #client.airflow_get_file(workspace_id, artifact_id, file_path)

    # Airflow Workspace Settings APIs
    poolTemplateEx = { 
        "poolTemplateName": "testPoolTemplate",
        "nodeSize": "Small",
        "workerScalability": {  
            "minNodeCount": 5,
            "maxNodeCount": 10,
        },
        "apacheAirflowJobVersion": "1.0.0",
    }  

    client.airflow_get_workspace_settings(workspace_id)
    #client.airflow_patch_workspace_settings(workspace_id, defaultPoolTemplateId="<enter guid>")
    #client.airflow_create_pool_template(workspace_id, artifact_id, poolTemplateEx)
    #client.airflow_get_pool_template(workspace_id, poolTemplateId="<enter guid>")
    #client.airflow_list_pool_templates(workspace_id)
    #client.airflow_delete_pool_template(workspace_id, poolTemplateId="<enter guid>")