import os

from fabric.airflow.client.airflow_files_api import AirflowFilesApiClient

from config import ConfigClient



if __name__ == "__main__":
    import logging
    
    # Setup logging for debug mode
    logging.basicConfig(level=logging.INFO)
    
    # Use ConfigClient instead of hardcoded credentials
    files_client = ConfigClient.files_client()

    my_test_dag_file = os.path.join(os.getcwd(), "tests", "resources", "MyTestDag.py")
    aks_status_plugin_file = os.path.join(os.getcwd(), "tests", "resources", "AKS_Status.py")
    aks_pod_log_plugin_file = os.path.join(os.getcwd(), "tests", "resources", "AKS_pod_log.py")

    with open(my_test_dag_file, 'r', encoding='utf-8') as f:
        dag_content = f.read()

    with open(aks_status_plugin_file, 'r', encoding='utf-8') as f:
        aks_status_plugin_content = f.read()

    with open(aks_pod_log_plugin_file, 'r', encoding='utf-8') as f:
        aks_pod_log_plugin_content = f.read()
    
    # Upload DAG file - user specifies the full path
    response = files_client.create_or_update_file("dags/MyTestDag.py",  dag_content)
    response = files_client.create_or_update_file("plugins/aks_status.py", aks_status_plugin_content)
    response = files_client.create_or_update_file("plugins/aks_pod_log.py", aks_pod_log_plugin_content)       
    
    # List DAG files
    response = files_client.list_files(root_path="dags")
    print("DAG files:")
    print(f"Body: {response.body}")
    
    # List plugin files
    response = files_client.list_files(root_path="plugins")
    print("Plugin files:")
    print(f"Body: {response.body}")

    # List root folder files
    response = files_client.list_files(root_path="/")
    print("Root folder files:")
    print(f"Body: {response.body}")

    response = files_client.get_file("dags/MyTestDag.py")
    print(f"Downloaded DAG file content: {response.body.decode('utf-8')}")

    response = files_client.create_or_update_file("dags/MyTestDag.py", "#TEST PAYLOAD")

    response = files_client.get_file("dags/MyTestDag.py")
    print(f"Downloaded DAG file content: {response.body.decode('utf-8')}")


    response = files_client.delete_file("dags/MyTestDag.py")
    print("Deleted DAG file MyTestDag.py")


    # Upload binary a binary file for testing purposes
    dll_file_path = r"C:\Program Files (x86)\dotnet\host\fxr\9.0.9\hostfxr.dll"
    
    # Read the binary file
    with open(dll_file_path, 'rb') as f:  # Note: 'rb' for binary mode
        dll_content = f.read()
    
    print(f"Read {len(dll_content)} bytes from {dll_file_path}")
    
    # Upload the DLL file to the plugins directory (or wherever you want it)
    response = files_client.create_or_update_file("plugins/hostfxr.dll", dll_content)
    print(f"Uploaded hostfxr.dll - Status: {response.status}")
    
    # Verify the upload by listing plugin files
    response = files_client.list_files(root_path="plugins")
    print("Plugin files after upload:")
    print(f"Body: {response.body}")
    
    # Optionally download it back to verify
    response = files_client.get_file("plugins/hostfxr.dll")
    downloaded_size = len(response.body)
    print(f"Downloaded hostfxr.dll - Size: {downloaded_size} bytes")
    
    if downloaded_size == len(dll_content):
        print("✅ File upload/download verification successful!")
    else:
        print("❌ File size mismatch - upload may have failed")

