import requests  
import time

class FabricItemClient:

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
        
    def run_fabric_item(self, workspace_id: str, item_id: str, job_type: str, job_params: dict | None) -> str:
            """
            Run a Fabric item.

            :param workspace_id: The workspace Id in which the item is located.
            :param item_id: The item Id. To check available items, Refer to: https://learn.microsoft.com/rest/api/fabric/admin/items/list-items?tabs=HTTP#itemtype.
            :param job_type: The type of job to run. For running a notebook, this should be "RunNotebook".
            :param job_params: An optional dictionary of parameters to pass to the job.

            :return: The run Id of item.
            """
            url = f"{self.base_url}/workspaces/{workspace_id}/items/{item_id}/jobs/instances?jobType={job_type}"
            data = {"executionData": {"parameters": job_params}} if job_params else {}

            response = self.session.post(url, json=data)
            self.printRequest(response)

            print("------------------------------------------------------------")
            print(response.status_code)

            
            location_header = response.headers.get("Location")
            # execution_id = respose.json().get('executionId')
            if location_header is None:
                raise Exception("Location header not found in run on demand item response.")
            # if execution_id is None:
            #     raise AirflowException("executionId not found in run on demand item response.")
            
            return self.fabric_item_run_status(location_header)
            

    def fabric_item_run_status(self, location: str):

        # Busy wait loop for execution completion
        while True:
            status_response = self.session.get(location)
            statusCode = status_response.status_code

            print(f"Execution status: {statusCode}")

            if statusCode not in [202]:
                print(f"Notebook execution completed with status: {statusCode}")
                break

            print("Waiting")    
            time.sleep(10)  # Wait 10 seconds before checking again
    