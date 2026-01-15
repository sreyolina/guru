from azure.search.documents.indexes import SearchIndexerClient
from azure.search.documents.indexes.models import SearchIndexerDataSourceConnection
from azure.identity import DefaultAzureCredential

class AzureSearchDataSourceManager:
    def __init__(self):
        # Configuration from your environment
        self.search_endpoint = "https://gurusearchai.search.windows.net"
        self.storage_account_name = "gurustorageacct"
        self.subscription_id = "e15576d7-67e8-4ed2-acab-42c5885ea1fd"
        self.resource_group = "testpoc"
        self.user_assigned_client_id = "6833750b-2598-4229-92a3-6d6d0df26e0f"
        self.container_name = "guru-medicaid-ia-sit"
        
        # Build resource IDs
        self.storage_resource_id = f"/subscriptions/{self.subscription_id}/resourceGroups/{self.resource_group}/providers/Microsoft.Storage/storageAccounts/{self.storage_account_name}"
        self.managed_identity_resource_id = f"/subscriptions/{self.subscription_id}/resourcegroups/{self.resource_group}/providers/Microsoft.ManagedIdentity/userAssignedIdentities/guruMA"
        
        # Initialize the client with DefaultAzureCredential
        credential = DefaultAzureCredential()
        self.indexer_client = SearchIndexerClient(
            endpoint=self.search_endpoint,
            credential=credential
        )
    
    def create_data_source(self, datasource_name="guru-medicaid-datasource"):
        """
        Creates an Azure AI Search data source with user-assigned managed identity authentication
        
        Args:
            datasource_name: Name for the data source (default: "guru-medicaid-datasource")
        
        Returns:
            SearchIndexerDataSourceConnection: The created data source
        """
        try:
            # Build connection string with managed identity
            # Format: ResourceId={storage_resource_id};Identity=[system|{managed_identity_resource_id}]
            connection_string = f"ResourceId={self.storage_resource_id};Identity={self.managed_identity_resource_id};"
            
            # Create the data source connection without identity parameter
            # The identity is specified in the connection string itself
            data_source = SearchIndexerDataSourceConnection(
                name=datasource_name,
                type="azureblob",
                connection_string=connection_string,
                container={"name": self.container_name}
            )
            
            # Create or update the data source
            result = self.indexer_client.create_or_update_data_source_connection(data_source)
            
            print(f"✅ Data source '{datasource_name}' created successfully!")
            print(f"   Storage Account: {self.storage_account_name}")
            print(f"   Container: {self.container_name}")
            print(f"   Authentication: User-Assigned Managed Identity")
            print(f"   Identity Resource ID: {self.managed_identity_resource_id}")
            
            return result
            
        except Exception as e:
            print(f"❌ Error creating data source: {str(e)}")
            import traceback
            traceback.print_exc()
            raise
    
    def verify_data_source(self, datasource_name="guru-medicaid-datasource"):
        """
        Verifies that the data source exists and displays its configuration
        
        Args:
            datasource_name: Name of the data source to verify
        """
        try:
            data_source = self.indexer_client.get_data_source_connection(datasource_name)
            
            print(f"\n📋 Data Source Configuration:")
            print(f"   Name: {data_source.name}")
            print(f"   Type: {data_source.type}")
            print(f"   Container: {data_source.container.name}")
            # Connection string is not returned for security reasons
            if data_source.connection_string:
                print(f"   Connection String: {data_source.connection_string[:80]}...")
            else:
                print(f"   Connection String: [Hidden for security]")
            
            return data_source
            
        except Exception as e:
            print(f"❌ Error verifying data source: {str(e)}")
            raise
    
    def list_data_sources(self):
        """
        Lists all data sources in the search service
        """
        try:
            data_sources = self.indexer_client.get_data_source_connections()
            print("\n📋 Available Data Sources:")
            for ds in data_sources:
                print(f"   - {ds.name} (Type: {ds.type})")
            return list(data_sources)
        except Exception as e:
            print(f"❌ Error listing data sources: {str(e)}")
            raise
    
    def delete_data_source(self, datasource_name="guru-medicaid-datasource"):
        """
        Deletes the specified data source
        
        Args:
            datasource_name: Name of the data source to delete
        """
        try:
            self.indexer_client.delete_data_source_connection(datasource_name)
            print(f"🗑️  Data source '{datasource_name}' deleted successfully!")
        except Exception as e:
            print(f"❌ Error deleting data source: {str(e)}")
            raise


def main():
    """
    Main execution function
    """
    # Initialize the manager
    manager = AzureSearchDataSourceManager()
    
    # Create the data source
    print("Creating Azure AI Search data source with managed identity...\n")
    data_source = manager.create_data_source()
    
    # Verify the data source was created
    print("\nVerifying data source...")
    manager.verify_data_source()
    
    print("\n✨ Setup complete!")
    print("\n📝 Important Notes:")
    print("   1. Ensure the managed identity 'guruMA' has 'Storage Blob Data Reader' role on the storage account")
    print("   2. Ensure the managed identity is assigned to your Azure AI Search service")
    print("   3. The connection string embeds the identity information directly")
    print("\n💡 Connection String Format:")
    print("   ResourceId={storage_resource_id};Identity={managed_identity_resource_id};")


if __name__ == "__main__":
    main()