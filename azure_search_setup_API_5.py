"""
Azure Cognitive Search - Automated Setup Script with JSON Config and API
This script creates search indexes, data sources, and indexers from JSON configuration via API
Supports dual indexers per state: state-specific and common container
Updated to use User-Assigned Managed Identity for data source connections
"""

import os
import json
import requests
from flask import Flask, jsonify, request
from azure.core.credentials import AzureKeyCredential
from azure.identity import DefaultAzureCredential
from azure.search.documents.indexes import SearchIndexClient, SearchIndexerClient
from azure.search.documents.indexes.models import (
    SearchIndex,
    SimpleField,
    SearchableField,
    SearchFieldDataType,
    SearchIndexer,
    SearchIndexerDataContainer,
    SearchIndexerDataSourceConnection,
    SearchIndexerDataIdentity,
    SearchIndexerDataUserAssignedIdentity,
    FieldMapping,
    IndexingSchedule
)
from datetime import timedelta
import logging

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)


class AzureSearchSetup:
    """Handles the complete setup of Azure Cognitive Search resources"""
    
    def __init__(self, config_path='search_config.json'):
        """Initialize with configuration from JSON file and environment variables"""
        # Load JSON configuration
        self.config = self._load_config(config_path)
        
        # Azure Search Configuration
        self.search_endpoint = os.getenv('AZURE_SEARCH_ENDPOINT', self.config.get('search_endpoint', 'https://gurusearchai.search.windows.net'))
        self.search_key = os.getenv('AZURE_SEARCH_KEY')
        self.use_user_assigned_identity = os.getenv('USE_USER_ASSIGNED_IDENTITY', 'false').lower() == 'true'
        self.user_assigned_client_id = os.getenv('USER_ASSIGNED_CLIENT_ID', self.config.get('user_assigned_client_id', '6833750b-2598-4229-92a3-6d6d0df26e0f'))
        
        # Azure Storage Configuration
        self.storage_account_name = os.getenv('AZURE_STORAGE_ACCOUNT_NAME', self.config.get('storage_account_name', 'gurustorageacct'))
        self.storage_connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        self.subscription_id = os.getenv('AZURE_SUBSCRIPTION_ID', self.config.get('subscription_id', 'e15576d7-67e8-4ed2-acab-42c5885ea1fd'))
        self.resource_group = os.getenv('AZURE_RESOURCE_GROUP', self.config.get('resource_group', 'testpoc'))
        
        # User-Assigned Managed Identity Resource ID for data source
        self.managed_identity_resource_id = os.getenv(
            'MANAGED_IDENTITY_RESOURCE_ID',
            f"/subscriptions/{self.subscription_id}/resourcegroups/{self.resource_group}/providers/Microsoft.ManagedIdentity/userAssignedIdentities/guruMA"
        )
        
        # Search Configuration
        self.top_k = int(os.getenv('AZURE_SEARCH_INDEX_TOP_K', self.config.get('top_k', 5)))
        
        # State configurations from JSON
        self.states = self.config.get('states', {})
        
        # Common container configuration
        self.common_container = self.config.get('common_container', 'guru-medicaid-common-sit')
        
        # Debug logging for environment variables
        logger.info("=" * 60)
        logger.info("Environment Configuration Loaded:")
        logger.info(f"  AZURE_SEARCH_ENDPOINT: {self.search_endpoint}")
        logger.info(f"  AZURE_STORAGE_ACCOUNT_NAME: {self.storage_account_name}")
        logger.info(f"  USE_STORAGE_MANAGED_IDENTITY: {os.getenv('USE_STORAGE_MANAGED_IDENTITY')}")
        logger.info(f"  USE_USER_ASSIGNED_IDENTITY: {self.use_user_assigned_identity}")
        logger.info(f"  USER_ASSIGNED_CLIENT_ID: {self.user_assigned_client_id}")
        logger.info(f"  MANAGED_IDENTITY_RESOURCE_ID: {self.managed_identity_resource_id}")
        logger.info(f"  AZURE_SUBSCRIPTION_ID: {self.subscription_id}")
        logger.info(f"  AZURE_RESOURCE_GROUP: {self.resource_group}")
        logger.info(f"  AZURE_SEARCH_KEY present: {'Yes' if self.search_key else 'No'}")
        logger.info("=" * 60)
        
        # Initialize clients
        self._initialize_clients()
    
    def _load_config(self, config_path):
        """Load configuration from JSON file"""
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
                logger.info(f"Configuration loaded from {config_path}")
                return config
        except FileNotFoundError:
            logger.warning(f"Config file {config_path} not found, using defaults")
            return {}
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in config file: {str(e)}")
            raise
    
    def _initialize_clients(self):
        """Initialize Azure Search clients with appropriate authentication"""
        try:
            if self.search_key:
                credential = AzureKeyCredential(self.search_key)
                logger.info("Using API Key authentication")
            else:
                from azure.identity import ManagedIdentityCredential, ChainedTokenCredential
                
                if self.use_user_assigned_identity:
                    logger.info(f"Using User-Assigned Managed Identity: {self.user_assigned_client_id}")
                    credential = ManagedIdentityCredential(client_id=self.user_assigned_client_id)
                else:
                    try:
                        system_credential = ManagedIdentityCredential()
                        logger.info("Testing System-Assigned Managed Identity authentication...")
                        token = system_credential.get_token("https://search.azure.com/.default")
                        logger.info(f"✓ Successfully acquired token using System-Assigned Managed Identity")
                        logger.info(f"✓ Token expires at: {token.expires_on}")
                        credential = system_credential
                    except Exception as auth_error:
                        logger.warning(f"System-assigned identity failed: {auth_error}")
                        logger.info("Trying user-assigned managed identity: guruMA")
                        user_credential = ManagedIdentityCredential(client_id=self.user_assigned_client_id)
                        try:
                            token = user_credential.get_token("https://search.azure.com/.default")
                            logger.info(f"✓ Successfully acquired token using User-Assigned Managed Identity (guruMA)")
                            credential = user_credential
                        except Exception as user_error:
                            logger.error(f"User-assigned identity also failed: {user_error}")
                            logger.error("Falling back to DefaultAzureCredential")
                            credential = DefaultAzureCredential()
                
                logger.info("=" * 60)
                logger.info("Using Managed Identity authentication")
                logger.info("=" * 60)
            
            self.index_client = SearchIndexClient(endpoint=self.search_endpoint, credential=credential)
            self.indexer_client = SearchIndexerClient(endpoint=self.search_endpoint, credential=credential)
            logger.info("Azure Search clients initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize clients: {str(e)}")
            raise
    
    def create_search_index(self, index_name: str, state_code: str) -> SearchIndex:
        """Create a search index with all required fields"""
        try:
            logger.info(f"Creating search index: {index_name}")
            
            fields = [
                SimpleField(name="metadata_storage_path", type=SearchFieldDataType.String, key=True, retrievable=True, filterable=False, sortable=False, facetable=False),
                SearchableField(name="content", type=SearchFieldDataType.String, searchable=True, retrievable=True, filterable=False, sortable=False, facetable=False, analyzer_name="standard.lucene"),
                SimpleField(name="state", type=SearchFieldDataType.String, retrievable=False, filterable=True, sortable=False, facetable=True),
                SimpleField(name="page_number", type=SearchFieldDataType.String, retrievable=True, filterable=True),
                SimpleField(name="total_pages", type=SearchFieldDataType.String, retrievable=False, filterable=False),
                SimpleField(name="parent_document", type=SearchFieldDataType.String, retrievable=True, filterable=True, facetable=True),
                SimpleField(name="document_type", type=SearchFieldDataType.String, retrievable=True, filterable=True, facetable=True),
                SimpleField(name="uploaded_by", type=SearchFieldDataType.String, retrievable=False, filterable=True),
                SimpleField(name="is_single_page", type=SearchFieldDataType.String, retrievable=False, filterable=True),
                SimpleField(name="metadata_storage_content_type", type=SearchFieldDataType.String, retrievable=False),
                SimpleField(name="metadata_storage_size", type=SearchFieldDataType.Int64, retrievable=False),
                SimpleField(name="metadata_storage_last_modified", type=SearchFieldDataType.DateTimeOffset, retrievable=False, filterable=True, sortable=True),
                SimpleField(name="metadata_storage_name", type=SearchFieldDataType.String, retrievable=False),
                SimpleField(name="metadata_storage_file_extension", type=SearchFieldDataType.String, retrievable=False, filterable=True, facetable=True),
                SimpleField(name="metadata_content_type", type=SearchFieldDataType.String, retrievable=False),
                SimpleField(name="metadata_language", type=SearchFieldDataType.String, retrievable=False),
                SimpleField(name="metadata_author", type=SearchFieldDataType.String, retrievable=False),
                SearchableField(name="metadata_title", type=SearchFieldDataType.String, retrievable=True, searchable=True, filterable=False),
                SimpleField(name="metadata_creation_date", type=SearchFieldDataType.DateTimeOffset, retrievable=True, sortable=True, filterable=True)
            ]
            
            index = SearchIndex(name=index_name, fields=fields)
            result = self.index_client.create_or_update_index(index)
            logger.info(f"Index '{index_name}' created successfully")
            return result
        except Exception as e:
            logger.error(f"Failed to create index '{index_name}': {str(e)}")
            raise
    
    def create_data_source_connection(self, datasource_name: str, container_name: str) -> SearchIndexerDataSourceConnection:
        """Create a data source connection to Azure Blob Storage using User-Assigned Managed Identity"""
        try:
            logger.info(f"Creating data source connection: {datasource_name}")
            
            use_managed_identity = os.getenv('USE_STORAGE_MANAGED_IDENTITY', 'false').lower() == 'true'
            
            if use_managed_identity:
                logger.info("=" * 60)
                logger.info("Using User-Assigned Managed Identity for storage authentication")
                logger.info(f"Managed Identity Resource ID: {self.managed_identity_resource_id}")
                
                # Build ResourceId connection string for the storage account
                storage_resource_id = f"/subscriptions/{self.subscription_id}/resourceGroups/{self.resource_group}/providers/Microsoft.Storage/storageAccounts/{self.storage_account_name}"
                connection_string = f"ResourceId={storage_resource_id};"
                
                logger.info(f"Storage Resource ID: {storage_resource_id}")
                logger.info(f"Container Name: {container_name}")
                logger.info("=" * 60)
                
                # Create the User-Assigned Identity object with the correct resource_id parameter
                user_assigned_identity = SearchIndexerDataIdentity(
                    resource_id=self.managed_identity_resource_id
                )
                
                logger.info(f"Created SearchIndexerDataUserAssignedIdentity object")
                logger.info(f"Identity resource_id: {user_assigned_identity.resource_id}")
                
                # Create container
                container = SearchIndexerDataContainer(name=container_name, query=None)
                
                # Create data source with user-assigned managed identity
                # IMPORTANT: Must explicitly set identity parameter to use user-assigned identity
                data_source = SearchIndexerDataSourceConnection(
                    name=datasource_name,
                    type="azureblob",
                    connection_string=connection_string,
                    container=container,
                    identity=user_assigned_identity  # This is critical - without this, it defaults to system-assigned
                )
                
                logger.info(f"Creating data source with identity: {data_source.identity}")
                
                result = self.indexer_client.create_or_update_data_source_connection(data_source)
                logger.info(f"✓ Data source '{datasource_name}' created successfully with User-Assigned Managed Identity")
                logger.info(f"✓ Verify in Azure Portal that identity type is 'UserAssigned'")
                return result
                    
            elif self.storage_connection_string:
                logger.info("Using connection string for storage authentication")
                connection_string = self.storage_connection_string
                container = SearchIndexerDataContainer(name=container_name, query=None)
                data_source = SearchIndexerDataSourceConnection(
                    name=datasource_name,
                    type="azureblob",
                    connection_string=connection_string,
                    container=container
                )
                result = self.indexer_client.create_or_update_data_source_connection(data_source)
                logger.info(f"Data source '{datasource_name}' created successfully")
                return result
            else:
                logger.info("Using storage account key for authentication")
                storage_key = os.getenv('AZURE_STORAGE_KEY')
                if not storage_key:
                    raise ValueError("Storage authentication not configured. Please set either AZURE_STORAGE_CONNECTION_STRING, AZURE_STORAGE_KEY, or USE_STORAGE_MANAGED_IDENTITY=true")
                connection_string = f"DefaultEndpointsProtocol=https;AccountName={self.storage_account_name};AccountKey={storage_key};EndpointSuffix=core.windows.net"
                container = SearchIndexerDataContainer(name=container_name, query=None)
                data_source = SearchIndexerDataSourceConnection(
                    name=datasource_name,
                    type="azureblob",
                    connection_string=connection_string,
                    container=container
                )
                result = self.indexer_client.create_or_update_data_source_connection(data_source)
                logger.info(f"Data source '{datasource_name}' created successfully")
                return result
                
        except Exception as e:
            logger.error(f"Failed to create data source '{datasource_name}': {str(e)}")
            raise
    
    def create_indexer(self, indexer_name: str, index_name: str, datasource_name: str) -> SearchIndexer:
        """Create an indexer to populate the search index from blob storage"""
        try:
            logger.info(f"Creating indexer: {indexer_name}")
            
            field_mappings = [FieldMapping(source_field_name="metadata_storage_path", target_field_name="metadata_storage_path", mapping_function={"name": "base64Encode"})]
            
            indexer_parameters = {
                "batchSize": 10,
                "maxFailedItems": 0,
                "maxFailedItemsPerBatch": 0,
                "configuration": {
                    "dataToExtract": "contentAndMetadata",
                    "parsingMode": "default",
                    "indexedFileNameExtensions": ".pdf,.docx,.doc,.txt,.html,.htm,.xml,.json",
                    "excludedFileNameExtensions": ".png,.jpg,.jpeg,.gif,.bmp,.tiff",
                    "failOnUnsupportedContentType": False,
                    "failOnUnprocessableDocument": False
                }
            }
            
            indexer = SearchIndexer(
                name=indexer_name,
                data_source_name=datasource_name,
                target_index_name=index_name,
                field_mappings=field_mappings,
                parameters=indexer_parameters,
                schedule=IndexingSchedule(interval=timedelta(minutes=5))
            )
            
            result = self.indexer_client.create_or_update_indexer(indexer)
            logger.info(f"Indexer '{indexer_name}' created successfully")
            return result
        except Exception as e:
            logger.error(f"Failed to create indexer '{indexer_name}': {str(e)}")
            raise
    
    def setup_all_from_config(self):
        """Run the complete setup process for all states defined in config"""
        results = {"success": [], "failed": []}
        
        try:
            # Create common data source once
            common_datasource_name = "datasource-common"
            logger.info(f"\n{'='*60}")
            logger.info(f"Creating common data source for container: {self.common_container}")
            logger.info(f"{'='*60}")
            
            try:
                self.create_data_source_connection(common_datasource_name, self.common_container)
                logger.info(f"✓ Common data source created successfully")
            except Exception as e:
                logger.error(f"✗ Failed to create common data source: {str(e)}")
                results["failed"].append({"resource": "common_datasource", "error": str(e)})
            
            # Setup resources for each state
            for code, state_info in self.states.items():
                try:
                    logger.info(f"\n{'='*60}")
                    logger.info(f"Setting up search resources for {state_info['name']} ({code.upper()})")
                    logger.info(f"{'='*60}")
                    
                    # Create index
                    self.create_search_index(state_info['index'], code)
                    
                    # Create state-specific data source
                    state_datasource_name = f"datasource-{code}"
                    self.create_data_source_connection(state_datasource_name, state_info['container'])
                    
                    # Create state-specific indexer
                    state_indexer_name = f"indexer-{code}"
                    self.create_indexer(state_indexer_name, state_info['index'], state_datasource_name)
                    
                    # Create common indexer for this state
                    common_indexer_name = f"indexer-{code}-common"
                    self.create_indexer(common_indexer_name, state_info['index'], common_datasource_name)
                    
                    logger.info(f"✓ Setup completed for {state_info['name']}")
                    results["success"].append({
                        "state": code, 
                        "name": state_info['name'],
                        "index": state_info['index'],
                        "datasources": [state_datasource_name, common_datasource_name],
                        "indexers": [state_indexer_name, common_indexer_name]
                    })
                except Exception as e:
                    logger.error(f"✗ Setup failed for {state_info['name']}: {str(e)}")
                    results["failed"].append({"state": code, "name": state_info['name'], "error": str(e)})
            
            logger.info(f"\n{'='*60}")
            logger.info("All setup operations completed!")
            logger.info(f"Total indexes created: {len([s for s in results['success'] if 'state' in s])}")
            logger.info(f"Total data sources created: {len([s for s in results['success'] if 'state' in s]) + 1}")
            logger.info(f"Total indexers created: {len([s for s in results['success'] if 'state' in s]) * 2}")
            logger.info(f"{'='*60}\n")
            return results
        except Exception as e:
            logger.error(f"Setup failed: {str(e)}")
            raise


# Flask API endpoints
@app.route('/api/setup', methods=['POST'])
def setup_resources():
    """API endpoint to trigger setup of all search resources from config"""
    try:
        # Ensure .env is loaded (in case working directory changed)
        load_dotenv(override=True)
        logger.info(f"API endpoint triggered - USE_USER_ASSIGNED_IDENTITY: {os.getenv('USE_USER_ASSIGNED_IDENTITY')}")
        logger.info(f"API endpoint triggered - USER_ASSIGNED_CLIENT_ID: {os.getenv('USER_ASSIGNED_CLIENT_ID')}")
        
        config_path = request.json.get('config_path', 'search_config.json') if request.is_json else 'search_config.json'
        
        setup = AzureSearchSetup(config_path=config_path)
        results = setup.setup_all_from_config()
        
        return jsonify({
            "status": "completed",
            "message": "Setup process completed",
            "results": results
        }), 200
    except Exception as e:
        logger.error(f"API setup failed: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "service": "Azure Search Setup API"}), 200


if __name__ == "__main__":
    port = int(os.getenv('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)