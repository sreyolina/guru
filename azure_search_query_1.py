"""
Azure Cognitive Search - Query API Script
This script provides search query functionality with Flask API endpoints
Supports querying across state-specific indexes with filtering and ranking
"""

import os
import json
from flask import Flask, jsonify, request
from azure.core.credentials import AzureKeyCredential
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
from azure.search.documents import SearchClient
from azure.search.documents.models import QueryType
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)


class AzureSearchQuery:
    """Handles search queries against Azure Cognitive Search indexes"""
    
    def __init__(self, config_path='search_config.json'):
        """Initialize with configuration from JSON file and environment variables"""
        # Load JSON configuration
        self.config = self._load_config(config_path)
        
        # Azure Search Configuration
        self.search_endpoint = os.getenv('AZURE_SEARCH_ENDPOINT', 
                                        self.config.get('search_endpoint', 
                                                       'https://gurusearchai.search.windows.net'))
        self.search_key = os.getenv('AZURE_SEARCH_KEY')
        self.use_user_assigned_identity = os.getenv('USE_USER_ASSIGNED_IDENTITY', 'false').lower() == 'true'
        self.user_assigned_client_id = os.getenv('USER_ASSIGNED_CLIENT_ID', 
                                                 self.config.get('user_assigned_client_id', 
                                                               '8fbeec2f-6a56-451d-b12d-05c128588eef'))
        
        # Search Configuration
        self.top_k = int(os.getenv('AZURE_SEARCH_INDEX_TOP_K', self.config.get('top_k', 5)))
        
        # State configurations from JSON
        self.states = self.config.get('states', {})
        
        # Initialize credential
        self.credential = self._get_credential()
        
        logger.info("Azure Search Query client initialized successfully")
    
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
    
    def _get_credential(self):
        """Get appropriate credential for authentication"""
        if self.search_key:
            logger.info("Using API Key authentication")
            return AzureKeyCredential(self.search_key)
        else:
            if self.use_user_assigned_identity:
                logger.info(f"Using User-Assigned Managed Identity: {self.user_assigned_client_id}")
                return ManagedIdentityCredential(client_id=self.user_assigned_client_id)
            else:
                logger.info("Using DefaultAzureCredential")
                return DefaultAzureCredential()
    
    def _get_search_client(self, state_code: str) -> SearchClient:
        """Get search client for a specific state index"""
        if state_code not in self.states:
            raise ValueError(f"Invalid state code: {state_code}. Valid codes: {', '.join(self.states.keys())}")
        
        index_name = self.states[state_code]['index']
        return SearchClient(
            endpoint=self.search_endpoint,
            index_name=index_name,
            credential=self.credential
        )
    
    def search(self, query: str, state_code: str, top: int = None, 
               filters: dict = None, search_mode: str = 'any',
               query_type: str = 'simple', include_total_count: bool = True,
               highlight_fields: list = None, highlight_pre_tag: str = '<em>',
               highlight_post_tag: str = '</em>'):
        """
        Execute a search query against a specific state index
        
        Args:
            query: Search query string
            state_code: State code (ia, in, oh, tx, va, wa)
            top: Number of results to return (default: from config)
            filters: Dictionary of filters to apply
            search_mode: 'any' or 'all' - how to combine search terms
            query_type: 'simple', 'full', or 'semantic'
            include_total_count: Include total count of matching documents
            highlight_fields: List of fields to highlight (default: ['content', 'metadata_title'])
            highlight_pre_tag: HTML tag before highlighted text (default: '<em>')
            highlight_post_tag: HTML tag after highlighted text (default: '</em>')
        
        Returns:
            Dictionary with search results including highlights
        """
        try:
            search_client = self._get_search_client(state_code)
            
            # Build filter string from filters dict
            filter_str = self._build_filter_string(filters) if filters else None
            
            # Set top_k
            top_k = top if top else self.top_k
            
            # Map query type
            query_type_enum = self._get_query_type(query_type)
            
            # Default highlight fields
            if highlight_fields is None:
                highlight_fields = ['content', 'metadata_title']
            
            logger.info(f"Executing search query on {state_code.upper()}: '{query}'")
            if filter_str:
                logger.info(f"With filters: {filter_str}")
            
            # Execute search with highlighting
            results = search_client.search(
                search_text=query,
                filter=filter_str,
                top=top_k,
                search_mode=search_mode,
                query_type=query_type_enum,
                include_total_count=include_total_count,
                select=['content', 'metadata_storage_path', 'page_number', 
                       'parent_document', 'document_type', 'metadata_title',
                       'metadata_creation_date'],
                highlight_fields=','.join(highlight_fields),
                highlight_pre_tag=highlight_pre_tag,
                highlight_post_tag=highlight_post_tag
            )
            
            # Process results
            search_results = []
            for result in results:
                result_data = {
                    'content': result.get('content', ''),
                    'score': result.get('@search.score', 0),
                    'page_number': result.get('page_number', ''),
                    'parent_document': result.get('parent_document', ''),
                    'document_type': result.get('document_type', ''),
                    'metadata_title': result.get('metadata_title', ''),
                    'metadata_creation_date': str(result.get('metadata_creation_date', '')),
                    'storage_path': result.get('metadata_storage_path', '')
                }
                
                # Add highlights if available
                highlights = result.get('@search.highlights', {})
                if highlights:
                    result_data['highlights'] = highlights
                    
                    # Add convenient highlighted_content field
                    if 'content' in highlights:
                        result_data['highlighted_content'] = ' ... '.join(highlights['content'])
                    
                    # Add convenient highlighted_title field
                    if 'metadata_title' in highlights:
                        result_data['highlighted_title'] = highlights['metadata_title'][0] if highlights['metadata_title'] else None
                
                search_results.append(result_data)
            
            response = {
                'query': query,
                'state': state_code,
                'state_name': self.states[state_code]['name'],
                'total_count': results.get_count() if include_total_count else None,
                'results_count': len(search_results),
                'results': search_results
            }
            
            logger.info(f"Search completed. Found {len(search_results)} results")
            return response
            
        except Exception as e:
            logger.error(f"Search failed: {str(e)}")
            raise
    
    def _get_query_type(self, query_type_str: str):
        """Convert query type string to QueryType enum"""
        query_type_map = {
            'simple': QueryType.SIMPLE,
            'full': QueryType.FULL,
            'semantic': QueryType.SEMANTIC
        }
        return query_type_map.get(query_type_str.lower(), QueryType.SIMPLE)
    
    def _build_filter_string(self, filters: dict) -> str:
        """
        Build OData filter string from filters dictionary
        
        Example filters:
        {
            'document_type': 'policy',
            'page_number': '5',
            'metadata_creation_date': {'gte': '2024-01-01T00:00:00Z'}
        }
        """
        filter_parts = []
        
        for field, value in filters.items():
            if isinstance(value, dict):
                # Handle range queries
                if 'gte' in value:
                    filter_parts.append(f"{field} ge {value['gte']}")
                if 'lte' in value:
                    filter_parts.append(f"{field} le {value['lte']}")
                if 'gt' in value:
                    filter_parts.append(f"{field} gt {value['gt']}")
                if 'lt' in value:
                    filter_parts.append(f"{field} lt {value['lt']}")
            else:
                # Handle equality
                filter_parts.append(f"{field} eq '{value}'")
        
        return ' and '.join(filter_parts)
    
    def search_multiple_states(self, query: str, state_codes: list = None, 
                              top: int = None, filters: dict = None):
        """
        Search across multiple state indexes
        
        Args:
            query: Search query string
            state_codes: List of state codes to search (default: all states)
            top: Number of results per state
            filters: Filters to apply
        
        Returns:
            Dictionary with combined results from all states
        """
        try:
            # Default to all states if not specified
            if not state_codes:
                state_codes = list(self.states.keys())
            
            logger.info(f"Executing multi-state search across: {', '.join([s.upper() for s in state_codes])}")
            
            all_results = []
            state_summaries = []
            
            for state_code in state_codes:
                try:
                    result = self.search(query, state_code, top, filters)
                    all_results.extend(result['results'])
                    state_summaries.append({
                        'state': state_code,
                        'state_name': result['state_name'],
                        'results_count': result['results_count'],
                        'total_count': result['total_count']
                    })
                except Exception as e:
                    logger.warning(f"Search failed for state {state_code}: {str(e)}")
                    state_summaries.append({
                        'state': state_code,
                        'error': str(e)
                    })
            
            # Sort all results by score
            all_results.sort(key=lambda x: x['score'], reverse=True)
            
            # Return top results across all states
            top_k = top if top else self.top_k
            
            return {
                'query': query,
                'states_searched': state_codes,
                'state_summaries': state_summaries,
                'total_results': len(all_results),
                'top_results': all_results[:top_k],
                'all_results': all_results
            }
            
        except Exception as e:
            logger.error(f"Multi-state search failed: {str(e)}")
            raise


# Flask API endpoints
@app.route('/api/search', methods=['POST'])
def search_api():
    """
    API endpoint for single-state search
    
    Request body:
    {
        "query": "medicaid eligibility",
        "state": "ia",
        "top": 5,
        "filters": {
            "document_type": "policy"
        },
        "search_mode": "any",
        "query_type": "simple",
        "highlight_fields": ["content", "metadata_title"],
        "highlight_pre_tag": "<mark>",
        "highlight_post_tag": "</mark>"
    }
    """
    try:
        if not request.is_json:
            return jsonify({"error": "Request must be JSON"}), 400
        
        data = request.json
        query = data.get('query')
        state_code = data.get('state')
        
        if not query or not state_code:
            return jsonify({"error": "Both 'query' and 'state' are required"}), 400
        
        # Optional parameters
        top = data.get('top')
        filters = data.get('filters')
        search_mode = data.get('search_mode', 'any')
        query_type = data.get('query_type', 'simple')
        highlight_fields = data.get('highlight_fields')
        highlight_pre_tag = data.get('highlight_pre_tag', '<em>')
        highlight_post_tag = data.get('highlight_post_tag', '</em>')
        
        # Initialize search
        config_path = data.get('config_path', 'search_config.json')
        searcher = AzureSearchQuery(config_path=config_path)
        
        # Execute search
        results = searcher.search(
            query=query,
            state_code=state_code,
            top=top,
            filters=filters,
            search_mode=search_mode,
            query_type=query_type,
            highlight_fields=highlight_fields,
            highlight_pre_tag=highlight_pre_tag,
            highlight_post_tag=highlight_post_tag
        )
        
        return jsonify(results), 200
        
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.error(f"Search API failed: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/search/multi-state', methods=['POST'])
def multi_state_search_api():
    """
    API endpoint for multi-state search
    
    Request body:
    {
        "query": "medicaid eligibility",
        "states": ["ia", "in", "oh"],  // optional, defaults to all states
        "top": 10,
        "filters": {
            "document_type": "policy"
        }
    }
    """
    try:
        if not request.is_json:
            return jsonify({"error": "Request must be JSON"}), 400
        
        data = request.json
        query = data.get('query')
        
        if not query:
            return jsonify({"error": "'query' is required"}), 400
        
        # Optional parameters
        state_codes = data.get('states')
        top = data.get('top')
        filters = data.get('filters')
        
        # Initialize search
        config_path = data.get('config_path', 'search_config.json')
        searcher = AzureSearchQuery(config_path=config_path)
        
        # Execute multi-state search
        results = searcher.search_multiple_states(
            query=query,
            state_codes=state_codes,
            top=top,
            filters=filters
        )
        
        return jsonify(results), 200
        
    except Exception as e:
        logger.error(f"Multi-state search API failed: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/search/states', methods=['GET'])
def list_states():
    """API endpoint to list available states"""
    try:
        config_path = request.args.get('config_path', 'search_config.json')
        searcher = AzureSearchQuery(config_path=config_path)
        
        states_info = []
        for code, info in searcher.states.items():
            states_info.append({
                'code': code,
                'name': info['name'],
                'index': info['index'],
                'container': info['container']
            })
        
        return jsonify({
            "total_states": len(states_info),
            "states": states_info
        }), 200
        
    except Exception as e:
        logger.error(f"List states failed: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "service": "Azure Search Query API"
    }), 200


def main():
    """Main function for command-line usage"""
    import sys
    
    if len(sys.argv) < 3:
        print("Usage: python azure_search_query.py <query> <state_code> [top]")
        print("Example: python azure_search_query.py 'medicaid eligibility' ia 5")
        print("\nAvailable state codes: ia, in, oh, tx, va, wa")
        sys.exit(1)
    
    query = sys.argv[1]
    state_code = sys.argv[2]
    top = int(sys.argv[3]) if len(sys.argv) > 3 else None
    
    try:
        searcher = AzureSearchQuery()
        results = searcher.search(query, state_code, top)
        
        print(json.dumps(results, indent=2))
        
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    # Check if running as API or CLI
    if len(os.sys.argv) > 1 and os.sys.argv[1] != 'api':
        main()
    else:
        port = int(os.getenv('PORT', 5001))
        print(f"\n{'='*60}")
        print("Azure Search Query API Server")
        print(f"Starting on http://0.0.0.0:{port}")
        print(f"{'='*60}\n")
        app.run(host='0.0.0.0', port=port, debug=False)