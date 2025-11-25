import os
import unittest
import time

from fabric.airflow.client.fabric_files_api_client import AirflowFilesApiClient
from fabric.airflow.client.api_exceptions import APIError
from fabric.airflow.client.config import Config

# Initialize Config from environment variable CONFIG_FILE_PATH
# Set environment variable: $env:CONFIG_FILE_PATH = "c:\src\ApiTest\config.ini"
config_file = os.getenv('CONFIG_FILE_PATH')
assert config_file is not None, "CONFIG_FILE_PATH environment variable must be set"
config = Config.from_file(config_file, 'TEST')

class TestFilesApiClientIntegration(unittest.TestCase):
    """Integration test cases for Files API client operations exposed by ConfigClient"""

    def setUp(self):
        """Set up test fixtures"""
        self.files_client = config.files_client()
        self.test_text_content = "# Test DAG file\nfrom airflow import DAG\nprint('Hello World')"
        self.test_binary_content = b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01'
        
        # Use timestamp to ensure unique file names for each test run
        self.timestamp = str(int(time.time()))
        self.test_dag_path = f"dags/test_dag_{self.timestamp}.py"
        self.test_plugin_path = f"plugins/test_plugin_{self.timestamp}.py"
        self.test_binary_path = f"plugins/test_binary_{self.timestamp}.dll"

    def tearDown(self):
        """Clean up test files"""
        test_files = [
            self.test_dag_path,
            self.test_plugin_path, 
            self.test_binary_path
        ]
        
        for file_path in test_files:
            try:
                self.files_client.delete_file(file_path)
            except (APIError):
                # Ignore errors during cleanup
                pass

    def test_files_client_type(self):
        """Test that ConfigClient returns correct files client type"""
        self.assertIsInstance(self.files_client, AirflowFilesApiClient)

    def test_create_dag_file(self):
        """Test creating a DAG file (text content)"""
        response = self.files_client.create_or_update_file(self.test_dag_path, self.test_text_content)

        # Verify response
        self.assertIn(response.status, [200, 201])  # Either created or updated
        if hasattr(response, 'headers') and response.headers:
            self.assertIn('Content-Type', response.headers)

    def test_create_plugin_file(self):
        """Test creating a plugin file (text content)"""
        plugin_content = "# Test plugin\ndef my_plugin_function():\n    return 'Hello from plugin'"
        response = self.files_client.create_or_update_file(self.test_plugin_path, plugin_content)

        self.assertIn(response.status, [200, 201])

    def test_create_binary_file(self):
        """Test creating a binary file"""
        response = self.files_client.create_or_update_file(self.test_binary_path, self.test_binary_content)

        self.assertIn(response.status, [200, 201])
        # Note: Content type verification depends on server implementation

    def test_update_existing_file(self):
        """Test updating an existing file"""
        # First create the file
        self.files_client.create_or_update_file(self.test_dag_path, self.test_text_content)
        
        # Then update it
        updated_content = "# Updated DAG file\nprint('Updated content')"
        response = self.files_client.create_or_update_file(self.test_dag_path, updated_content)

        self.assertIn(response.status, [200, 201])

    def test_delete_file(self):
        """Test deleting a file"""
        # First create a file to delete
        self.files_client.create_or_update_file(self.test_dag_path, self.test_text_content)
        
        # Then delete it
        response = self.files_client.delete_file(self.test_dag_path)

        self.assertIn(response.status, [200, 204])  # Success or No Content

    def test_get_text_file(self):
        """Test getting a text file"""
        # First create the file
        self.files_client.create_or_update_file(self.test_dag_path, self.test_text_content)
        
        # Then retrieve it
        response = self.files_client.get_file(self.test_dag_path)

        self.assertEqual(response.status, 200)
        retrieved_content = response.body.decode('utf-8') if isinstance(response.body, bytes) else response.body
        self.assertEqual(retrieved_content, self.test_text_content)

    def test_get_binary_file(self):
        """Test getting a binary file"""
        # First create the binary file
        self.files_client.create_or_update_file(self.test_binary_path, self.test_binary_content)
        
        # Then retrieve it
        response = self.files_client.get_file(self.test_binary_path)

        self.assertEqual(response.status, 200)
        self.assertEqual(response.body, self.test_binary_content)

    def test_list_files_dags_path(self):
        """Test listing files in dags directory and validate JSON structure"""
        # Create a test file first
        self.files_client.create_or_update_file(self.test_dag_path, self.test_text_content)
        
        response = self.files_client.list_files(root_path="dags")

        self.assertEqual(response.status, 200)
        
        # Body already represented as a dict
        response_data = response.body 

        # Validate JSON structure
        self.assertIn('files', response_data)
        self.assertIsInstance(response_data['files'], list)
        
        # Find our specific test file that we just uploaded

    def test_list_files_plugins_path(self):
        """Test listing files in plugins directory and validate JSON structure"""
        # Create test files first
        plugin_content = "# test plugin with some content"
        self.files_client.create_or_update_file(self.test_plugin_path, plugin_content)
        self.files_client.create_or_update_file(self.test_binary_path, self.test_binary_content)
        
        response = self.files_client.list_files(root_path="plugins")

        self.assertEqual(response.status, 200)
        
        # Body already represented as a dict
        response_data = response.body 

        # Validate JSON structure
        self.assertIn('files', response_data)
        self.assertIsInstance(response_data['files'], list)
        
        # Track our specific uploaded files
        plugin_file_found = False
        binary_file_found = False
        expected_plugin_name = f'test_plugin_{self.timestamp}.py'
        expected_binary_name = f'test_binary_{self.timestamp}.dll'
        
        for file_entry in response_data['files']:
            self.assertIn('filePath', file_entry)
            self.assertIn('sizeInBytes', file_entry)
            self.assertIsInstance(file_entry['sizeInBytes'], int)
            
            if expected_plugin_name in file_entry['filePath']:
                plugin_file_found = True
                #self.assertGreater(file_entry['sizeInBytes'], 0)  # Should have our content
                self.assertEqual(file_entry['filePath'], f'plugins/{expected_plugin_name}')
                # Verify size matches approximately what we uploaded
                # expected_size = len(plugin_content.encode('utf-8'))
                # self.assertEqual(file_entry['sizeInBytes'], expected_size)
            elif expected_binary_name in file_entry['filePath']:
                binary_file_found = True
                self.assertEqual(file_entry['filePath'], f'plugins/{expected_binary_name}')
                # Verify binary size matches what we uploaded
                #self.assertGreater(file_entry['sizeInBytes'], 0)  # Should have our binary content                
                #self.assertEqual(file_entry['sizeInBytes'], len(self.test_binary_content))
                
        self.assertTrue(plugin_file_found, f"Our uploaded plugin file {expected_plugin_name} not found")
        self.assertTrue(binary_file_found, f"Our uploaded binary file {expected_binary_name} not found")

    def test_list_files_root_path(self):
        """Test listing files in root directory and validate JSON structure"""
        response = self.files_client.list_files(root_path="/")

        self.assertEqual(response.status, 200)
        
        # Body already represented as a dict
        response_data = response.body 
        
        # Validate JSON structure
        self.assertIn('files', response_data)
        self.assertIsInstance(response_data['files'], list)
        
        # Root directory should contain directories like 'dags/' and 'plugins/'
        directory_paths = [file_entry['filePath'] for file_entry in response_data['files']]
        
        # Check for expected directories (these should always exist in Airflow)
        dags_dir_found = any(path == 'dags/' for path in directory_paths)
        plugins_dir_found = any(path == 'plugins/' for path in directory_paths)
        
        self.assertTrue(dags_dir_found, f"dags/ directory not found in root listing. Found: {directory_paths}")
        self.assertTrue(plugins_dir_found, f"plugins/ directory not found in root listing. Found: {directory_paths}")
        
        # Validate each entry has required fields
        for file_entry in response_data['files']:
            self.assertIn('filePath', file_entry)
            # Root level directories might not have sizeInBytes field
