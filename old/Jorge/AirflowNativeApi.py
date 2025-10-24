import requests, json

class AirflowNativeClient:
    def __init__(self, access_token: str, base_url: str):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update(
        {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Referer": "PythonClient"
        })

    @classmethod
    def printRequest(cls, response: requests.Response):
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

    @classmethod
    def GenerateMwcToken(
            cls, 
            url:str = f"https://df-msit-scus-redirect.analysis.windows.net/metadata/v201606/generatemwctoken",
            workspace_id:str = 'f6fefa7e-3ef9-477c-846d-f8801ca482f6', 
            aadToken:str = ''): 
        
        headers = {
            "Authorization": f"Bearer {aadToken}",
            #"Content-Length": "208",
            "Content-Type": "application/json;charset=UTF-8",
            "Origin": "https://msit.fabric.microsoft.com",
            "Priority": "u=1, i",
            "Referer": "https://msit.fabric.microsoft.com/",
            "RequestId": "228b9550-a95f-447c-ce03-c40ba8dec5cc",
            "Sec-CH-UA": '"Chromium";v="136", "Microsoft Edge";v="136", "Not.A/Brand";v="99"',
            "Sec-CH-UA-Mobile": "?0",
            "Sec-CH-UA-Platform": '"Windows"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "cross-site",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0",
            "X-PowerBI-HostEnv": "Power BI Web App"
        }    
        data = f'{{"type":"[Start] GetMWCToken","workspaceObjectId":"{workspace_id}","workloadType":"DI","asyncId":"1dc32b5e-440b-4599-be6f-34a34d67195b","iframeId":"6f4db58d-a05a-487c-bd79-bd9281fea52b"}}'
        response = requests.Session().post(url=url, data=data, headers=headers)
    
        if response.status_code == 200:
            #cls.printRequest(response)
            return json.loads(response.content).get("Token", {})
        else:
            raise Exception(f"Failed to generate token: {response.status_code} {response.text}")

    @classmethod
    def GenerateMwcTokenV2(
            cls, 
            url:str = f"https://df-msit-scus-redirect.analysis.windows.net/metadata/v201606/generatemwctokenv2",
            workspace_id:str = 'f6fefa7e-3ef9-477c-846d-f8801ca482f6', 
            aadToken:str = ''): 
        
        headers = {
            "Authorization": f"Bearer {aadToken}",
            #"Content-Length": "208",
            "Content-Type": "application/json;charset=UTF-8",
        }    
        data = f'{{"type":"[Start] GetMWCTokenV2","workspaceObjectId":"{workspace_id}","workloadType":"DI"}}'
        response = requests.Session().post(url=url, data=data, headers=headers)
        cls.printRequest(response)
    
        if response.status_code == 200:
            return json.loads(response.content).get("Token", {})
        else:
            raise Exception(f"Failed to generate token: {response.status_code} {response.text}")

    def ListDags(self):
        url = f"{self.base_url}/api/v1/dags"
        response = self.session.get(url, allow_redirects=False)
        self.printRequest(response)
        if response.status_code == 200:
            return response
        else:
            raise Exception(f"Failed to run DAG: {response.status_code} {response.text}")


    def RunDag(self, dag_id: str):
        url = f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns"
        response = self.session.post(url)
        if response.status_code == 200:
            return response
        else:
            raise Exception(f"Failed to run DAG: {response.status_code} {response.text}")
        

    