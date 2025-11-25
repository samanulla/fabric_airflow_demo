import os
import unittest
import uuid

from fabric.airflow.client.config import Config
from fabric.airflow.client.fabric_control_plane_api_client import FabricControlPlaneApiClient
from fabric.airflow.client.fabric_control_plane_model import (
    AirflowPoolTemplate,
    AirflowPoolsTemplate,
    AirflowWorkspaceSettings,
    WorkerScalability,
)

# Initialize Config from environment variable CONFIG_FILE_PATH
# Set environment variable: $env:CONFIG_FILE_PATH = "c:\src\ApiTest\config.ini"
config_file = os.getenv('CONFIG_FILE_PATH')
assert config_file is not None, "CONFIG_FILE_PATH environment variable must be set"
config = Config.from_file(config_file, 'TEST')

class TestAirflowControlPlaneApiIntegration(unittest.TestCase):
    """Integration tests for Airflow Control Plane API"""

    @classmethod
    def setUpClass(cls):
        """Set up test class with API client"""
        cls.client: FabricControlPlaneApiClient = config.control_plane_client()
        cls.workspace_id = config.workspace_id

    def _create_pool_template(self, pool_name: str, min_nodes: int, max_nodes: int) -> AirflowPoolTemplate:
        """Create a pool template with specified configuration"""
        return AirflowPoolTemplate(
            poolTemplateName=pool_name,
            nodeSize="Small",
            workerScalability=WorkerScalability(minNodeCount=min_nodes, maxNodeCount=max_nodes),
            apacheAirflowJobVersion="1.0.0"
            # Optional read-only fields are omitted - will be set by server
        )

    def _test_pool_lifecycle(self, pool_template: AirflowPoolTemplate, pool_type: str) -> None:
        """
        Test complete lifecycle for a pool template:
        1. Create pool template
        2. Fetch and validate pool configuration
        3. List pools and validate presence
        4. Delete pool template
        
        Args:
            pool_template: AirflowPoolTemplate object with pool configuration
            pool_type: Description of pool type for logging
        """
        # Step 1: Create pool template
        print(f"Step 1: Creating {pool_type}...")
        
        pool_id = self.client.create_pool_template(pool_template)
        self.assertIsNotNone(pool_id, "Pool ID should be returned from API client")
        self.assertTrue(len(pool_id) > 0, "Pool ID should not be empty")
        print(f"Created {pool_type}: {pool_template.poolTemplateName} (ID: {pool_id})")
        
        # Step 2: Fetch and validate pool configuration
        print(f"Step 2: Fetching and validating {pool_type} configuration...")
        fetched_pool : AirflowPoolTemplate = self.client.get_pool_template(pool_id)
        self.assertIsNotNone(fetched_pool, "Fetched pool should not be None")
        
        # Validate configuration matches what we sent
        self.assertEqual(fetched_pool.poolTemplateName, pool_template.poolTemplateName)
        self.assertEqual(fetched_pool.nodeSize, pool_template.nodeSize)
        
        # Validate workerScalability if provided in template
        self.assertIsNotNone(fetched_pool.workerScalability, "Worker scalability should always be reported, even if not specified at creation time.")
        if fetched_pool.workerScalability and pool_template.workerScalability:
            self.assertEqual(fetched_pool.workerScalability.minNodeCount, pool_template.workerScalability.minNodeCount)
            self.assertEqual(fetched_pool.workerScalability.maxNodeCount, pool_template.workerScalability.maxNodeCount)
        
        # Validate apacheAirflowJobVersion if provided        
        self.assertIsNotNone(fetched_pool.apacheAirflowJobVersion, "apacheAirflowJobVersion should always be reported, even if not specified at creation time.")
        if pool_template.apacheAirflowJobVersion is not None:
            self.assertEqual(fetched_pool.apacheAirflowJobVersion, pool_template.apacheAirflowJobVersion)
        
        # Validate read-only fields populated by server
        self.assertIsNotNone(fetched_pool.poolTemplateId)
        if fetched_pool.poolTemplateId:
            self.assertTrue(len(fetched_pool.poolTemplateId) > 0)
        print(f"Validated {pool_type} configuration matches expected values")
        
        # Step 3: List pools and validate presence
        print(f"Step 3: Listing pools and validating {pool_type} presence...")
        pools_list_obj = self.client.list_pool_templates_parsed()
        self.assertIsNotNone(pools_list_obj)
        self.assertIsInstance(pools_list_obj, AirflowPoolsTemplate)
        
        pool_names = [pool.poolTemplateName for pool in pools_list_obj.poolTemplates]
        self.assertIn(pool_template.poolTemplateName, pool_names, 
                     f"{pool_type} {pool_template.poolTemplateName} not found in pool list")
        
        # Use helper method to find specific pool
        found_pool = pools_list_obj.get_pool_by_id(pool_id)
        self.assertIsNotNone(found_pool, f"Could not find {pool_template.poolTemplateName} using helper method")

        # Validate workerScalability if provided in template
        if found_pool is not None:
            self.assertIsNotNone(found_pool.workerScalability, "Worker scalability should always be reported, even if not specified at creation time.")
            if found_pool.workerScalability and pool_template.workerScalability:
                self.assertEqual(found_pool.workerScalability.minNodeCount, pool_template.workerScalability.minNodeCount)
                self.assertEqual(found_pool.workerScalability.maxNodeCount, pool_template.workerScalability.maxNodeCount)

        print(f"{pool_type} found in list with correct configuration")
        
        # Step 4: Delete pool template
        print(f"Step 4: Deleting {pool_type}...")
        delete_response = self.client.delete_pool_template(pool_id)
        self.assertIsNotNone(delete_response)
        self.assertIn(delete_response.status, [200, 204])  # Success or No Content
        print(f"Successfully deleted {pool_type}: {pool_id}")
        
        # Verify pool is removed from list
        pools_list_after_delete = self.client.list_pool_templates_parsed()
        pool_names_after = [pool.poolTemplateName for pool in pools_list_after_delete.poolTemplates]
        self.assertNotIn(pool_template.poolTemplateName, pool_names_after,
                       f"{pool_type} {pool_template.poolTemplateName} should be removed from list")
        print(f"Confirmed {pool_type} removed from list after deletion\n")

    def test_00_pool_template_minimal_config(self):
        """
        Test creating a pool with minimal configuration (required fields only)
        Server-side optional fields like shutdownPolicy, availabilityZones are omitted
        """
        print("=" * 80)
        print("Testing pool template with minimal configuration")
        print("=" * 80)
        
        # Create pool with only required fields (poolTemplateName, nodeSize, workerScalability, apacheAirflowJobVersion)
        # Omit server-side optional fields (shutdownPolicy, availabilityZones)
        minimal_template = AirflowPoolTemplate(
            poolTemplateName=f"test-minimal-{str(uuid.uuid4())[:8]}",
            nodeSize="Small",
            workerScalability=WorkerScalability(minNodeCount=2, maxNodeCount=2),
            apacheAirflowJobVersion="1.0.0"
            # shutdownPolicy and availabilityZones omitted - should use server defaults
        )
        self._test_pool_lifecycle(
            pool_template=minimal_template,
            pool_type="minimal config pool [2,2]"
        )
                  


    def test_01_pool_templates_lifecycle(self):
        """
        Comprehensive test for pool template lifecycle across different scaling configurations:
        - Fixed scale pool [6,6] - no autoscaling
        - Auto scale pool [6,7] - autoscaling enabled
        - No scale pool [1,1] - single node
        
        For each pool type, tests:
        1. Create pool template
        2. Fetch and validate pool configuration
        3. List pools and validate presence
        4. Delete pool template
        """
        try:
            # Generate unique pool names for this test run
            test_run_id = str(uuid.uuid4())[:8]
            
            print("=" * 80)
            print("Testing Fixed Scale Pool [6,6]")
            print("=" * 80)
            fixed_scale_template = self._create_pool_template(
                pool_name=f"test-pool-fixed-scale-{test_run_id}",
                min_nodes=6,
                max_nodes=6
            )
            self._test_pool_lifecycle(
                pool_template=fixed_scale_template,
                pool_type="fixed scale pool [6,6]"
            )
            
            print("=" * 80)
            print("Testing Auto Scale Pool [6,7]")
            print("=" * 80)
            auto_scale_template = self._create_pool_template(
                pool_name=f"test-pool-auto-scale-{test_run_id}",
                min_nodes=6,
                max_nodes=7
            )
            self._test_pool_lifecycle(
                pool_template=auto_scale_template,
                pool_type="auto scale pool [6,7]"
            )
                      
        except Exception as e:
            if "404" in str(e):
                print(f"[WARNING] Control Plane API not available: {e}")
                self.skipTest("Control Plane API endpoints not available in this environment")
            else:
                raise

    def test_02_default_pool_management_workflow(self):
        """
        Comprehensive test for default pool management workflow:
        1. Check default is starter pool (GUID 0000...)
        2. Create a temporary pool for testing
        3. Make the pool the default
        4. Check default pool changed
        5. Delete the pool
        6. Check it reverted to 0000...
        """
        try:
            # Step 1: Check current default pool (should be starter pool with GUID 0000...)
            print("Step 1: Checking initial default pool...")
            settings = self.client.get_workspace_settings()
            self.assertIsNotNone(settings)
            self.assertIsInstance(settings, AirflowWorkspaceSettings)
            
            # Store the original default pool for restoration later
            original_default_pool = settings.defaultPoolTemplateId
            print(f"Initial default pool: {original_default_pool}")
            
            # Verify it's the starter pool (should start with '0000' or be empty/None for starter)
            if original_default_pool:
                self.assertTrue(
                    original_default_pool.startswith('0000'),
                    f"Expected starter pool (0000...), but got: {original_default_pool}"
                )
            print("Confirmed initial default is starter pool")
            
            # Step 2: Create a temporary pool for testing
            print("Step 2: Creating temporary pool for default testing...")
            temp_pool_name = f"test-default-pool-{str(uuid.uuid4())[:8]}"
            temp_pool_template = self._create_pool_template(
                pool_name=temp_pool_name,
                min_nodes=2,
                max_nodes=2
            )
            
            pool_id = self.client.create_pool_template(temp_pool_template)
            self.assertIsNotNone(pool_id, "Pool ID should be returned from API client")
            print(f"Created temporary pool: {temp_pool_name} (ID: {pool_id})")
            
            try:
                # Step 3: Make the pool the default
                print("Step 3: Setting new default pool...")
                response = self.client.patch_workspace_settings(
                    AirflowWorkspaceSettings(defaultPoolTemplateId=pool_id))
                self.assertIsNotNone(response)
                self.assertIn(response.status, [200, 204])  # Success or No Content
                print(f"Set {temp_pool_name} (ID: {pool_id}) as default pool")
                
                # Step 4: Check default pool changed
                print("Step 4: Verifying default pool changed...")
                settings = self.client.get_workspace_settings()                
                self.assertIsNotNone(settings)
                self.assertIsInstance(settings, AirflowWorkspaceSettings)
                current_default = settings.defaultPoolTemplateId                
                self.assertEqual(
                    current_default, pool_id, 
                    f"Default pool should be {pool_id}, but got {current_default}")
                print(f"Verified new default pool ID: {current_default}")
                
                # Step 5: Delete the pool that's currently the default
                print("Step 5: Deleting the default pool...")
                delete_response = self.client.delete_pool_template(pool_id)                
                self.assertIsNotNone(delete_response)
                self.assertIn(delete_response.status, [200, 204])  # Success or No Content
                print(f"Deleted pool: {pool_id}")
                
                # Step 6: Check it reverted to 0000...
                print("Step 6: Verifying reversion to starter pool...")
                settings = self.client.get_workspace_settings()            
                self.assertIsNotNone(settings)
                self.assertIsInstance(settings, AirflowWorkspaceSettings)
                self.assertIsNotNone(settings.defaultPoolTemplateId)
                
                # Should have reverted to starter pool (0000... or empty/None)
                self.assertTrue(
                    settings.defaultPoolTemplateId.startswith('0000'),
                    f"Expected reversion to starter pool (0000...), but got: {settings.defaultPoolTemplateId}"
                )

                print(f"Confirmed reversion to starter pool: {settings.defaultPoolTemplateId}")
                print("Default pool management workflow completed successfully!")
                
            except Exception as cleanup_error:
                # If something fails after pool creation, make sure we clean up the pool
                try:
                    self.client.delete_pool_template(pool_id)
                    print(f"Cleaned up temporary pool after error: {pool_id}")
                except:
                    pass
                raise cleanup_error
            
        except Exception as e:
            if "404" in str(e):
                print(f"[WARNING] Control Plane API not available: {e}")
                self.skipTest("Control Plane API endpoints not available in this environment")
            else:
                raise


if __name__ == '__main__':
    unittest.main()
