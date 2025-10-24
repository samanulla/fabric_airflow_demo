



# from airflow_api import AirflowNativeClient
# if __name__ == "__main__":

#     # # Artifact definition
#     workspace_id = "cb9c7d63-3263-4996-9014-482eb8788007"
#     artifact_id = "2b757224-08f7-4566-bb1e-2a857b074110" 

#     # # GET credential
#     tenant_id = "249bbd2f-29a0-4a32-a667-ce59e2aee561"
#     client_id = "9abcc0ae-393f-42ae-b46e-8f4dff217fd9"
#     client_secret = "IIS8Q~wqsWYxAUnK6zf~5JTNV2SdFirYvh3ygdyP"

#     # Try with different scope first
#     print("Testing with Microsoft Graph scope...")
#     auth_provider_graph = AuthenticationProvider(
#         tenant_id, 
#         client_id, 
#         client_secret, 
#         scope = "https://management.azure.com/.default")
#         #scope= "5d13f7d7-0567-429c-9880-320e9555e5fc/.default") #"https://api.fabric.microsoft.com/.default")
#     access_token = auth_provider_graph.get_token()
#     print("✓ Graph API token acquired successfully!")
#     print("Token info:", auth_provider_graph.get_token_info())
#     print("Token:", access_token)

#     if (access_token):
#         print("Using Graph token for Fabric API calls.")
#         api = AirflowNativeClient(
#             access_token=access_token, 
#             base_url="https://22cf8020991754.northcentralus.airflow.svc.datafactory.azure.com")
#         api.ListDags()
    