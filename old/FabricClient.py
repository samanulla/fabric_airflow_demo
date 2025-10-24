import requests  
from ApiClient.DataStructure import AirflowDefinition 

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


    def GetItemDefinition(self, workspace_id: str, artifact_id: str):
        url = f"{self.base_url}/workspaces/{workspace_id}/items/{artifact_id}/getDefinition"
        response = self.session.get(url)
        if response.status_code == 200:
            return response.content
        else:
            raise Exception(f"Failed to fetch workspace: {response.status_code} {response.text}")

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

    def airflow_status(self, workspace_id: str, artifact_id: str):
        url = f"{self.base_url}/workspaces/{workspace_id}/apacheairflowjobs/{artifact_id}/environmentstatus"
        response = self.session.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch Airflow status: {response.status_code} {response.text}")

    def airflow_start(self, workspace_id: str, artifact_id: str):
        url = f"{self.base_url}/workspaces/{workspace_id}/apacheairflowjobs/{artifact_id}/startenvironment"
        response = self.session.post(url)
        if response.status_code == 202:
            print("Your Airflow Job will start soon...")
            return response.json()
        else:
            raise Exception(f"Failed to start Airflow: {response.status_code} {response.text}")       

    def airflow_stop(self, workspace_id: str, artifact_id: str):
        url = f"{self.base_url}/workspaces/{workspace_id}/apacheairflowjobs/{artifact_id}/stopenvironment"
        print(url)
        response = self.session.post(url)
        print(response.headers)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to stop Airflow: {response.status_code} {response.text}")    

