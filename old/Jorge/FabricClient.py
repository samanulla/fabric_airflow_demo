import requests  
from DataStructure import AirflowDefinition 
import urllib.parse

class FabricClient:

    def __init__(self, access_token: str, base_url: str = "https://api.fabric.microsoft.com/v1"):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update(
        {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Referer": "PythonClient"
        })

    def printRequest(self, response: requests.Response):
        request = response.request
        print('---------------------------------------------------------------------')
        print(f'{request.method} {request.url} HTTP/1.1')
        print('')
        print('HEADER')
        for k, v in request.headers.items():
            print(f'{k}: {v}')
        print('')
        print('BODY')
        print(request.body)
        print('')
        print('-------------')
        print('HEADER')
        for k, v in response.headers.items():
            print(f'{k}: {v}')
        print('BODY')
        print(response.content)
        print('---------------------------------------------------------------------')

    def GetApacheAirflowJobDefinition(self, workspace_id: str, artifact_id: str):
        url = f"{self.base_url}/workspaces/{workspace_id}/ApacheAirflowJobs/{artifact_id}/getDefinition"
        response = self.session.post(url)
        if response.status_code == 200:
            return AirflowDefinition.parse(response.content)
        else:
            raise Exception(f"Failed to fetch workspace: {response.status_code} {response.text}")
        
    def UpdateApacheAirflowJobDefinition(self, workspace_id: str, artifact_id: str, payload: str):
        url = f"{self.base_url}/workspaces/{workspace_id}/ApacheAirflowJobs/{artifact_id}/updateDefinition?updateMetadata=True"
        response = self.session.post(url, data=payload)
        if response.status_code == 200:
            return 
        else:
            raise Exception(f"Failed to fetch workspace: {response.status_code} {response.text}")

    def UpdateApacheAirflowJob(self, workspace_id: str, artifact_id: str): #todo
        url = f"{self.base_url}/workspaces/{workspace_id}/ApacheAirflowJobs/{artifact_id}"
        response = self.session.patch(url)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch workspace: {response.status_code} {response.text}")  

    def get_ApacheAirflowJobs(self, workspace_id: str):
        url = f"{self.base_url}/workspaces/{workspace_id}/ApacheAirflowJobs"
        response = self.session.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch workspace: {response.status_code} {response.text}")
                
    def get_ApacheAirflowJob(self, workspace_id: str, artifact_id: str):
        url = f"{self.base_url}/workspaces/{workspace_id}/ApacheAirflowJobs/{artifact_id}"
        response = self.session.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch workspace: {response.status_code} {response.text}")

    def get_DeleteApacheAirflowJob(self, workspace_id: str, artifact_id: str):
        url = f"{self.base_url}/workspaces/{workspace_id}/ApacheAirflowJobs/{artifact_id}"
        response = self.session.delete(url)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch workspace: {response.status_code} {response.text}")  

    def get_workspace_info(self, workspace_id: str):
        url = f"{self.base_url}/workspaces/{workspace_id}"
        response = self.session.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch workspace: {response.status_code} {response.text}")

    def get_workspace_items(self, workspace_id: str):
        url = f"{self.base_url}/workspaces/{workspace_id}/items"
        response = self.session.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch workspace: {response.status_code} {response.text}")
        
    # Airflow Workspace Settings API Operations
    def airflow_get_workspace_settings(self, workspace_id: str):
        url = f"{self.base_url}/workspaces/{workspace_id}/apacheAirflowJobs/settings?preview=true"
        response = self.session.get(url)
        self.printRequest(response)
        if response.status_code == 200:
            return response
        else:
            raise Exception(f"Failed to fetch Airflow workspace settings: {response.status_code} {response.text}")

    def airflow_patch_workspace_settings(self, workspace_id: str, defaultPoolTemplateId: str):
        url = f"{self.base_url}/workspaces/{workspace_id}/apacheAirflowJobs/settings?preview=true"
        response = self.session.patch(url, json={"defaultPoolTemplateId": defaultPoolTemplateId})
        self.printRequest(response)
        if response.status_code == 200:
            return response
        else:
            raise Exception(f"Failed to set Airflow workspace settings: {response.status_code} {response.text}")
        
    def airflow_create_pool_template(self, workspace_id: str, poolTemplate: dict):
        url = f"{self.base_url}/workspaces/{workspace_id}/apacheAirflowJobs/settings/pools?preview=true"
        response = self.session.post(url, json=poolTemplate)
        self.printRequest(response)
        if response.status_code == 200:
            return response
        else:
            raise Exception(f"Failed to create Airflow pool template: {response.status_code} {response.text}")
        
    def airflow_get_pool_template(self, workspace_id: str, poolTemplateId: str):
        url = f"{self.base_url}/workspaces/{workspace_id}/apacheAirflowJobs/settings/pools/{poolTemplateId}?preview=true"
        response = self.session.get(url)
        self.printRequest(response)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to get Airflow pool template: {response.status_code} {response.text}")
        
    def airflow_list_pool_templates(self, workspace_id: str):
        url = f"{self.base_url}/workspaces/{workspace_id}/apacheAirflowJobs/settings/pools?preview=true"
        response = self.session.get(url)
        self.printRequest(response)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to create Airflow pool template: {response.status_code} {response.text}")
        
    def airflow_delete_pool_template(self, workspace_id: str, poolTemplateId: str):
        url = f"{self.base_url}/workspaces/{workspace_id}/apacheAirflowJobs/settings/pools/{poolTemplateId}?preview=true"
        response = self.session.delete(url)
        self.printRequest(response)
        if response.status_code == 200:
            print("Airflow pool template deleted successfully.")
        else:
            raise Exception(f"Failed to delete Airflow pool template: {response.status_code} {response.text}")

    # Environment API Operations
    def airflow_start_environment(self, workspace_id: str, artifact_id: str):
        url = f"{self.base_url}/workspaces/{workspace_id}/apacheAirflowJobs/{artifact_id}/environment/start?preview=true"
        response = self.session.post(url)
        self.printRequest(response)
        if response.status_code == 202:
            print("Your Airflow Job will start soon...")
            return response.json()
        else:
            raise Exception(f"Failed to start Airflow: {response.status_code} {response.text}")       

    def airflow_stop_environment(self, workspace_id: str, artifact_id: str):
        url = f"{self.base_url}/workspaces/{workspace_id}/apacheAirflowJobs/{artifact_id}/environment/stop?preview=true"
        print(url)
        response = self.session.post(url)
        print(response.headers)
        if response.status_code == 202:
            return response.json()
        else:
            raise Exception(f"Failed to stop Airflow: {response.status_code} {response.text}") 

    def airflow_get_environment(self, workspace_id: str, artifact_id: str):
        url = f"{self.base_url}/workspaces/{workspace_id}/apacheAirflowJobs/{artifact_id}/environment?preview=true"
        response = self.session.get(url)
        self.printRequest(response)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch Airflow environment: {response.status_code} {response.text}")

    def airflow_get_environment_settings(self, workspace_id: str, artifact_id: str):
        url = f"{self.base_url}/workspaces/{workspace_id}/apacheAirflowJobs/{artifact_id}/environment/settings?preview=true"
        response = self.session.get(url)
        self.printRequest(response)
        if response.status_code == 200:
            return response
        else:
            raise Exception(f"Failed to fetch Airflow environment settings: {response.status_code} {response.text}")

    def airflow_update_environment_settings(self, workspace_id: str, artifact_id: str, settings: dict):
        url = f"{self.base_url}/workspaces/{workspace_id}/apacheAirflowJobs/{artifact_id}/environment/updateSettings?preview=true"
        response = self.session.post(url, json=settings)
        self.printRequest(response)
        if response.status_code == 200:
            return response
        else:
            raise Exception(f"Failed to update Airflow environment settings: {response.status_code} {response.text}") 

    def airflow_get_environment_compute(self, workspace_id: str, artifact_id: str):
        url = f"{self.base_url}/workspaces/{workspace_id}/apacheAirflowJobs/{artifact_id}/environment/compute?preview=true"
        response = self.session.get(url)
        self.printRequest(response)
        if response.status_code == 200:
            return response
        else:
            raise Exception(f"Failed to fetch Airflow environment compute: {response.status_code} {response.text}") 
        
    def airflow_update_environment_compute(self, workspace_id: str, artifact_id: str, poolTemplateId: str):
        url = f"{self.base_url}/workspaces/{workspace_id}/apacheAirflowJobs/{artifact_id}/environment/updateCompute?preview=true"
        response = self.session.post(url, json={"poolTemplateId": poolTemplateId})
        self.printRequest(response)
        if response.status_code == 200:
            return response
        else:
            raise Exception(f"Failed to update Airflow environment compute: {response.status_code} {response.text}")
        
    def airflow_update_environment_version(self, workspace_id: str, artifact_id: str, version: str):
        url = f"{self.base_url}/workspaces/{workspace_id}/apacheAirflowJobs/{artifact_id}/environment/updateVersion?preview=true"
        response = self.session.post(url, json={"apacheAirflowJobVersion": version})
        self.printRequest(response)
        if response.status_code == 200:
            return response
        else:
            raise Exception(f"Failed to update Airflow environment version: {response.status_code} {response.text}")

    # File API Operations
    def airflow_createOrUpdate_file(self, workspace_id: str, artifact_id: str, file_path: str, payload_str: str):
        payload_bytes = payload_str.encode("utf-8")  
        url = f"{self.base_url}/workspaces/{workspace_id}/apacheAirflowJobs/{artifact_id}/files/{file_path}?preview=true"
        print(url)
        headers = self.session.headers.copy()
        headers["Content-Type"] = "application/octet-stream"
        response = self.session.put(url, data=payload_bytes, headers=headers)
        self.printRequest(response)
        if response.status_code == 200:
            print("Done")
        else:
            raise Exception(f"Failed to fetch file: {response.status_code} {response.text}")
        
    def airflow_get_file(self, workspace_id: str, artifact_id: str, file_path: str):
        url = f"{self.base_url}/workspaces/{workspace_id}/apacheAirflowJobs/{artifact_id}/files/{file_path}?preview=true"
        print(url)
        response = self.session.get(url)
        self.printRequest(response)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch file: {response.status_code} {response.text}")

    def airflow_list_files(self, workspace_id: str, artifact_id: str, rootPath: str = ""):
        url = f"{self.base_url}/workspaces/{workspace_id}/apacheAirflowJobs/{artifact_id}/files?preview=true"
        if rootPath:
            url = f"{url}&rootPath={urllib.parse.quote(rootPath)}"
        print(url)
        response = self.session.get(url)
        self.printRequest(response)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch file: {response.status_code} {response.text}")
        
    def airflow_delete_file(self, workspace_id: str, artifact_id: str, file_path: str):
        url = f"{self.base_url}/workspaces/{workspace_id}/apacheAirflowJobs/{artifact_id}/files/{file_path}?preview=true"
        print(url)
        response = self.session.delete(url)
        self.printRequest(response)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch file: {response.status_code} {response.text}")

