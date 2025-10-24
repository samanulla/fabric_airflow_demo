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
        print(f'{request.method} {response.status_code} {request.url} HTTP/1.1')
        print('')
        print('HEADER')
        for k, v in request.headers.items():
            print(f'{k}: {v}')
        print('')
        print('BODY')
        print(request.body)
        print('')
        print('===================')
        print('HEADER')
        for k, v in response.headers.items():
            print(f'{k}: {v}')
        print('BODY')
        print(response.content[:500])
        print('---------------------------------------------------------------------')

    @classmethod
    def GenerateMwcToken(
            cls, 
            url:str = f"https://df-msit-scus-redirect.analysis.windows.net/metadata/v201606/generatemwctoken",
            workspace_id:str = 'f6fefa7e-3ef9-477c-846d-f8801ca482f6', 
            aadToken:str = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IkNOdjBPSTNSd3FsSEZFVm5hb01Bc2hDSDJYRSIsImtpZCI6IkNOdjBPSTNSd3FsSEZFVm5hb01Bc2hDSDJYRSJ9.eyJhdWQiOiJodHRwczovL2FuYWx5c2lzLndpbmRvd3MubmV0L3Bvd2VyYmkvYXBpIiwiaXNzIjoiaHR0cHM6Ly9zdHMud2luZG93cy5uZXQvNzJmOTg4YmYtODZmMS00MWFmLTkxYWItMmQ3Y2QwMTFkYjQ3LyIsImlhdCI6MTc0ODAzMzY5MCwibmJmIjoxNzQ4MDMzNjkwLCJleHAiOjE3NDgwMzgwOTYsImFjY3QiOjAsImFjciI6IjEiLCJhaW8iOiJBY1FBTy84WkFBQUFQVXJMVWk0Nk1PazF1cEgrd0pBREgzWURrY0Q5TW5kcnI1RVZnMGt6MEdtMjN5TzVkM2YyLyt0R3A0OTI4RXF1cHJiejBndUJ0RWIwOGVTZ3ZmTmFLT0hUSzBGM1VkbVhyR0pOTVpyVkhZTWVrU2FyTDNEWG96a3V6T3NGNEd3NzB3MDhwbkhQQ3QxUmtucG01bllaUjV5NkV3bHVlYnV6c1hLUG9UZzBrbk44ZGJlYlFqZzhqNG1rUy9TcldqZDFlRFhLQlQrRlNZNHZ6ZWRwVWM5cnJxVXIrelIxczVycDJYaGRIaEVtM2hkay96K2x5SXRvQ1B3SGwvTUc0dkxQIiwiYW1yIjpbInJzYSIsIm1mYSJdLCJhcHBpZCI6Ijg3MWMwMTBmLTVlNjEtNGZiMS04M2FjLTk4NjEwYTdlOTExMCIsImFwcGlkYWNyIjoiMCIsImNvbnRyb2xzIjpbImFwcF9yZXMiXSwiY29udHJvbHNfYXVkcyI6WyIwMDAwMDAwOS0wMDAwLTAwMDAtYzAwMC0wMDAwMDAwMDAwMDAiLCIwMDAwMDAwMy0wMDAwLTBmZjEtY2UwMC0wMDAwMDAwMDAwMDAiXSwiZGV2aWNlaWQiOiI4MzY1MWM0My0xZjZlLTQ1MTEtODdhMS0yM2RkYzIzNDk5MzIiLCJmYW1pbHlfbmFtZSI6IkZvbnRlcyIsImdpdmVuX25hbWUiOiJWaW5pY2l1cyIsImlkdHlwIjoidXNlciIsImlwYWRkciI6IjczLjQyLjE3Ni4yMDQiLCJuYW1lIjoiVmluaWNpdXMgRm9udGVzIiwib2lkIjoiNGFjNTQ4MDAtZWMzMi00MjQ1LTlkOTAtNzVmN2ZhNTU0ODhjIiwib25wcmVtX3NpZCI6IlMtMS01LTIxLTE0NjEzMDUtODE4NzcxNDAxLTE0OTE0MjExMDUtMTEwMTAzIiwicHVpZCI6IjEwMDMwMDAwODAwNkMyMDUiLCJyaCI6IjEuQVJvQXY0ajVjdkdHcjBHUnF5MTgwQkhiUndrQUFBQUFBQUFBd0FBQUFBQUFBQUFhQU5nYUFBLiIsInNjcCI6InVzZXJfaW1wZXJzb25hdGlvbiIsInNpZCI6IjAwNGY4Nzc5LTBkY2QtMTljOC1iOTk2LWU2ZThiMTJjMDk5ZSIsInNpZ25pbl9zdGF0ZSI6WyJkdmNfbW5nZCIsImR2Y19jbXAiLCJrbXNpIl0sInN1YiI6Ijl4LXZkY1VjYS1SMXR4TGxrQUhJdmVwdDhsQW9tV0pzYS12ajd1RkRMYnMiLCJ0aWQiOiI3MmY5ODhiZi04NmYxLTQxYWYtOTFhYi0yZDdjZDAxMWRiNDciLCJ1bmlxdWVfbmFtZSI6InZmb250ZXNAbWljcm9zb2Z0LmNvbSIsInVwbiI6InZmb250ZXNAbWljcm9zb2Z0LmNvbSIsInV0aSI6IlItNHVNdjY1NDBTUVJ2YVhGdzQ1QUEiLCJ2ZXIiOiIxLjAiLCJ3aWRzIjpbImI3OWZiZjRkLTNlZjktNDY4OS04MTQzLTc2YjE5NGU4NTUwOSJdLCJ4bXNfY2MiOlsiQ1AxIl0sInhtc19mdGQiOiJmUldmbUQyTjRReW9NY0JjV3l3aG94LTRVd1pBcFNRLVNBNzF5UmJvQlk4QmRYTnViM0owYUMxa2MyMXoiLCJ4bXNfaWRyZWwiOiIyMiAxIn0.LKGML0duu270qe0t1N9Q6DKrjchfI3WDuhzI5VTfJIqhTYbHgZv_gm2MitUjcP-giNA6kM_4D4yyE6RRz-VHu6Ebs9JUADDqly0XyBf6BhSgpZTNHO5cWMOYIswDoDRoZAyoPwdHwTf4S5w80cDvkfPu01FD8O5ivLMWfBTqXN4_iQWkWgicnjPLeVgzvqUBpwFIswqQwegGU2TWjTq17nHaKA6IM1lBBEn-29yLHN2P6700-04ipFR8YuR1f82og_xQj9LrtQxpolf9i8oZ4L_gXT2rUU3wok6KhotiNb3VGZL-o1-glpKrHxGG4CpdTX2HcokZrYr4L3Gz1SeyAg'): 
        
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
            aadToken:str = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IkNOdjBPSTNSd3FsSEZFVm5hb01Bc2hDSDJYRSIsImtpZCI6IkNOdjBPSTNSd3FsSEZFVm5hb01Bc2hDSDJYRSJ9.eyJhdWQiOiJodHRwczovL2FuYWx5c2lzLndpbmRvd3MubmV0L3Bvd2VyYmkvYXBpIiwiaXNzIjoiaHR0cHM6Ly9zdHMud2luZG93cy5uZXQvNzJmOTg4YmYtODZmMS00MWFmLTkxYWItMmQ3Y2QwMTFkYjQ3LyIsImlhdCI6MTc0ODAzMzY5MCwibmJmIjoxNzQ4MDMzNjkwLCJleHAiOjE3NDgwMzgwOTYsImFjY3QiOjAsImFjciI6IjEiLCJhaW8iOiJBY1FBTy84WkFBQUFQVXJMVWk0Nk1PazF1cEgrd0pBREgzWURrY0Q5TW5kcnI1RVZnMGt6MEdtMjN5TzVkM2YyLyt0R3A0OTI4RXF1cHJiejBndUJ0RWIwOGVTZ3ZmTmFLT0hUSzBGM1VkbVhyR0pOTVpyVkhZTWVrU2FyTDNEWG96a3V6T3NGNEd3NzB3MDhwbkhQQ3QxUmtucG01bllaUjV5NkV3bHVlYnV6c1hLUG9UZzBrbk44ZGJlYlFqZzhqNG1rUy9TcldqZDFlRFhLQlQrRlNZNHZ6ZWRwVWM5cnJxVXIrelIxczVycDJYaGRIaEVtM2hkay96K2x5SXRvQ1B3SGwvTUc0dkxQIiwiYW1yIjpbInJzYSIsIm1mYSJdLCJhcHBpZCI6Ijg3MWMwMTBmLTVlNjEtNGZiMS04M2FjLTk4NjEwYTdlOTExMCIsImFwcGlkYWNyIjoiMCIsImNvbnRyb2xzIjpbImFwcF9yZXMiXSwiY29udHJvbHNfYXVkcyI6WyIwMDAwMDAwOS0wMDAwLTAwMDAtYzAwMC0wMDAwMDAwMDAwMDAiLCIwMDAwMDAwMy0wMDAwLTBmZjEtY2UwMC0wMDAwMDAwMDAwMDAiXSwiZGV2aWNlaWQiOiI4MzY1MWM0My0xZjZlLTQ1MTEtODdhMS0yM2RkYzIzNDk5MzIiLCJmYW1pbHlfbmFtZSI6IkZvbnRlcyIsImdpdmVuX25hbWUiOiJWaW5pY2l1cyIsImlkdHlwIjoidXNlciIsImlwYWRkciI6IjczLjQyLjE3Ni4yMDQiLCJuYW1lIjoiVmluaWNpdXMgRm9udGVzIiwib2lkIjoiNGFjNTQ4MDAtZWMzMi00MjQ1LTlkOTAtNzVmN2ZhNTU0ODhjIiwib25wcmVtX3NpZCI6IlMtMS01LTIxLTE0NjEzMDUtODE4NzcxNDAxLTE0OTE0MjExMDUtMTEwMTAzIiwicHVpZCI6IjEwMDMwMDAwODAwNkMyMDUiLCJyaCI6IjEuQVJvQXY0ajVjdkdHcjBHUnF5MTgwQkhiUndrQUFBQUFBQUFBd0FBQUFBQUFBQUFhQU5nYUFBLiIsInNjcCI6InVzZXJfaW1wZXJzb25hdGlvbiIsInNpZCI6IjAwNGY4Nzc5LTBkY2QtMTljOC1iOTk2LWU2ZThiMTJjMDk5ZSIsInNpZ25pbl9zdGF0ZSI6WyJkdmNfbW5nZCIsImR2Y19jbXAiLCJrbXNpIl0sInN1YiI6Ijl4LXZkY1VjYS1SMXR4TGxrQUhJdmVwdDhsQW9tV0pzYS12ajd1RkRMYnMiLCJ0aWQiOiI3MmY5ODhiZi04NmYxLTQxYWYtOTFhYi0yZDdjZDAxMWRiNDciLCJ1bmlxdWVfbmFtZSI6InZmb250ZXNAbWljcm9zb2Z0LmNvbSIsInVwbiI6InZmb250ZXNAbWljcm9zb2Z0LmNvbSIsInV0aSI6IlItNHVNdjY1NDBTUVJ2YVhGdzQ1QUEiLCJ2ZXIiOiIxLjAiLCJ3aWRzIjpbImI3OWZiZjRkLTNlZjktNDY4OS04MTQzLTc2YjE5NGU4NTUwOSJdLCJ4bXNfY2MiOlsiQ1AxIl0sInhtc19mdGQiOiJmUldmbUQyTjRReW9NY0JjV3l3aG94LTRVd1pBcFNRLVNBNzF5UmJvQlk4QmRYTnViM0owYUMxa2MyMXoiLCJ4bXNfaWRyZWwiOiIyMiAxIn0.LKGML0duu270qe0t1N9Q6DKrjchfI3WDuhzI5VTfJIqhTYbHgZv_gm2MitUjcP-giNA6kM_4D4yyE6RRz-VHu6Ebs9JUADDqly0XyBf6BhSgpZTNHO5cWMOYIswDoDRoZAyoPwdHwTf4S5w80cDvkfPu01FD8O5ivLMWfBTqXN4_iQWkWgicnjPLeVgzvqUBpwFIswqQwegGU2TWjTq17nHaKA6IM1lBBEn-29yLHN2P6700-04ipFR8YuR1f82og_xQj9LrtQxpolf9i8oZ4L_gXT2rUU3wok6KhotiNb3VGZL-o1-glpKrHxGG4CpdTX2HcokZrYr4L3Gz1SeyAg'): 
        
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
        response = self.session.get(url, allow_redirects=True)
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
        

    