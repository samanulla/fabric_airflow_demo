import os
import unittest
import json

from fabric.airflow.client.api_exceptions import APIError
from fabric.airflow.client.airflow_api_client import AirflowApiClient
from fabric.airflow.client.config import Config

# Initialize Config from environment variable CONFIG_FILE_PATH
# Set environment variable: $env:CONFIG_FILE_PATH = "c:\src\ApiTest\config.ini"
config_file = os.getenv('CONFIG_FILE_PATH')
assert config_file is not None, "CONFIG_FILE_PATH environment variable must be set"
config = Config.from_file(config_file, 'TEST')

class TestAirflowNativeApiIntegration(unittest.TestCase):
    """Integration test cases for Airflow Native API client operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.native_client = config.airflow_native_client()

    def test_native_client_type(self):
        """Test that ConfigClient returns correct native client type"""
        self.assertIsInstance(self.native_client, AirflowApiClient)

    def test_list_dags(self):
        """Test SPN authentication by listing DAGs - this is the primary test"""
        try:
            response = self.native_client.list_dags(limit=10)
            
            # If we get here, authentication worked
            self.assertIsNotNone(response)
            self.assertIn(response.status, [200])  # 200=success, 404=not found, 403=forbidden but auth worked
            
            # Parse response body if successful
            if hasattr(response, 'body') and response.body:
                if isinstance(response.body, str):
                    response_data = json.loads(response.body)
                else:
                    response_data = response.body
                
                # Validate response structure (typical Airflow DAGs response)
                if isinstance(response_data, dict):
                    self.assertIn('dags', response_data)
                    self.assertIsInstance(response_data['dags'], list)
                    print(f"Authentication successful! Found {len(response_data['dags'])} DAGs")
                else:
                    print("Authentication successful! Response received")
            else:
                print("Authentication successful! Empty response")

        except APIError as e:
            self.fail(e.message)

    def test_list_dags_with_parameters(self):
        """Test listing DAGs with various parameters to verify API functionality"""
        try:
            # Test with different parameters
            response = self.native_client.list_dags(
                limit=5,
                only_active=True,
                paused=False
            )
            
            self.assertIsNotNone(response)
            self.assertIn(response.status, [200])
            print(f"Parameterized DAG listing test passed (status: {response.status})")
            
        except APIError as e:
            self.fail(e.message)
