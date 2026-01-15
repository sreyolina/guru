"""
Azure Cognitive Search - Indexer Runner Script
This script runs indexers to ingest data from blob storage into search indexes
Supports dual indexers per state: state-specific and common container
"""

import os
from azure.core.credentials import AzureKeyCredential
from azure.identity import DefaultAzureCredential
from azure.search.documents.indexes import SearchIndexerClient
import logging
import time
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class AzureIndexerRunner:
    """Handles running and monitoring Azure Search indexers"""
    
    def __init__(self):
        """Initialize with configuration from environment variables"""
        self.search_endpoint = os.getenv('AZURE_SEARCH_ENDPOINT', 'https://gurusearchai.search.windows.net')
        self.search_key = os.getenv('AZURE_SEARCH_KEY')  # Leave None for Entra auth
        
        # State configurations with dual indexers
        self.states = {
            'ia': {
                'name': 'Iowa',
                'indexers': {
                    'state': 'indexer-ia',
                    'common': 'indexer-ia-common'
                }
            },
            'in': {
                'name': 'Indiana',
                'indexers': {
                    'state': 'indexer-in',
                    'common': 'indexer-in-common'
                }
            },
            'oh': {
                'name': 'Ohio',
                'indexers': {
                    'state': 'indexer-oh',
                    'common': 'indexer-oh-common'
                }
            },
            'tx': {
                'name': 'Texas',
                'indexers': {
                    'state': 'indexer-tx',
                    'common': 'indexer-tx-common'
                }
            },
            'va': {
                'name': 'Virginia',
                'indexers': {
                    'state': 'indexer-va',
                    'common': 'indexer-va-common'
                }
            },
            'wa': {
                'name': 'Washington',
                'indexers': {
                    'state': 'indexer-wa',
                    'common': 'indexer-wa-common'
                }
            }
        }
        
        # Initialize client
        self._initialize_client()
    
    def _initialize_client(self):
        """Initialize Azure Search indexer client with appropriate authentication"""
        try:
            # Use Microsoft Entra (Azure AD) authentication if no key provided
            if self.search_key:
                credential = AzureKeyCredential(self.search_key)
            else:
                credential = DefaultAzureCredential()
            
            self.indexer_client = SearchIndexerClient(
                endpoint=self.search_endpoint,
                credential=credential
            )
            
            logger.info("Azure Search indexer client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize client: {str(e)}")
            raise
    
    def run_indexer(self, state_code: str, indexer_type: str = 'both'):
        """
        Run indexer(s) for a specific state
        
        Args:
            state_code: State code (ia, in, oh, tx, va, wa)
            indexer_type: Type of indexer to run ('state', 'common', or 'both')
        """
        try:
            if state_code not in self.states:
                logger.error(f"Invalid state code: {state_code}")
                return False
            
            state_name = self.states[state_code]['name']
            indexers = self.states[state_code]['indexers']
            
            results = {}
            
            if indexer_type in ['state', 'both']:
                indexer_name = indexers['state']
                logger.info(f"Running state indexer for {state_name} ({state_code.upper()}): {indexer_name}")
                try:
                    self.indexer_client.run_indexer(indexer_name)
                    logger.info(f"✓ Indexer '{indexer_name}' started successfully")
                    results['state'] = True
                except Exception as e:
                    logger.error(f"✗ Failed to run state indexer: {str(e)}")
                    results['state'] = False
            
            if indexer_type in ['common', 'both']:
                if indexer_type == 'both':
                    time.sleep(1)  # Small delay between indexers
                
                indexer_name = indexers['common']
                logger.info(f"Running common indexer for {state_name} ({state_code.upper()}): {indexer_name}")
                try:
                    self.indexer_client.run_indexer(indexer_name)
                    logger.info(f"✓ Indexer '{indexer_name}' started successfully")
                    results['common'] = True
                except Exception as e:
                    logger.error(f"✗ Failed to run common indexer: {str(e)}")
                    results['common'] = False
            
            return all(results.values()) if results else False
            
        except Exception as e:
            logger.error(f"Failed to run indexer for state '{state_code}': {str(e)}")
            return False
    
    def get_indexer_status(self, state_code: str, indexer_type: str = 'both'):
        """
        Get the status of indexer(s)
        
        Args:
            state_code: State code (ia, in, oh, tx, va, wa)
            indexer_type: Type of indexer to check ('state', 'common', or 'both')
        """
        try:
            if state_code not in self.states:
                logger.error(f"Invalid state code: {state_code}")
                return None
            
            indexers = self.states[state_code]['indexers']
            statuses = {}
            
            if indexer_type in ['state', 'both']:
                indexer_name = indexers['state']
                status = self.indexer_client.get_indexer_status(indexer_name)
                statuses['state'] = status
                self._print_indexer_status(indexer_name, status, "State-Specific")
            
            if indexer_type in ['common', 'both']:
                indexer_name = indexers['common']
                status = self.indexer_client.get_indexer_status(indexer_name)
                statuses['common'] = status
                self._print_indexer_status(indexer_name, status, "Common")
            
            return statuses
            
        except Exception as e:
            logger.error(f"Failed to get indexer status for state '{state_code}': {str(e)}")
            return None
    
    def _print_indexer_status(self, indexer_name: str, status, indexer_label: str):
        """Helper method to print indexer status"""
        print(f"\n{'='*60}")
        print(f"Indexer Status: {indexer_name} ({indexer_label})")
        print(f"{'='*60}")
        print(f"Status: {status.status}")
        print(f"Last Result: {status.last_result.status if status.last_result else 'N/A'}")
        
        if status.last_result:
            print(f"Items Processed: {status.last_result.items_processed}")
            print(f"Items Failed: {status.last_result.items_failed}")
            print(f"Start Time: {status.last_result.start_time}")
            print(f"End Time: {status.last_result.end_time}")
            
            if status.last_result.errors:
                print(f"\nErrors ({len(status.last_result.errors)}):")
                for error in status.last_result.errors[:5]:  # Show first 5 errors
                    print(f"  - {error.error_message}")
            
            if status.last_result.warnings:
                print(f"\nWarnings ({len(status.last_result.warnings)}):")
                for warning in status.last_result.warnings[:5]:  # Show first 5 warnings
                    print(f"  - {warning.message}")
        
        print("="*60 + "\n")
    
    def reset_indexer(self, state_code: str, indexer_type: str = 'both'):
        """
        Reset indexer(s) to clear their state
        
        Args:
            state_code: State code (ia, in, oh, tx, va, wa)
            indexer_type: Type of indexer to reset ('state', 'common', or 'both')
        """
        try:
            if state_code not in self.states:
                logger.error(f"Invalid state code: {state_code}")
                return False
            
            indexers = self.states[state_code]['indexers']
            results = {}
            
            if indexer_type in ['state', 'both']:
                indexer_name = indexers['state']
                logger.info(f"Resetting state indexer: {indexer_name}")
                try:
                    self.indexer_client.reset_indexer(indexer_name)
                    logger.info(f"✓ Indexer '{indexer_name}' reset successfully")
                    results['state'] = True
                except Exception as e:
                    logger.error(f"✗ Failed to reset state indexer: {str(e)}")
                    results['state'] = False
            
            if indexer_type in ['common', 'both']:
                indexer_name = indexers['common']
                logger.info(f"Resetting common indexer: {indexer_name}")
                try:
                    self.indexer_client.reset_indexer(indexer_name)
                    logger.info(f"✓ Indexer '{indexer_name}' reset successfully")
                    results['common'] = True
                except Exception as e:
                    logger.error(f"✗ Failed to reset common indexer: {str(e)}")
                    results['common'] = False
            
            return all(results.values()) if results else False
            
        except Exception as e:
            logger.error(f"Failed to reset indexer for state '{state_code}': {str(e)}")
            return False
    
    def run_all_indexers(self, indexer_type: str = 'both'):
        """
        Run all indexers for all states
        
        Args:
            indexer_type: Type of indexer to run ('state', 'common', or 'both')
        """
        logger.info("\n" + "="*60)
        logger.info(f"Running all indexers (type: {indexer_type})")
        logger.info("="*60 + "\n")
        
        results = {}
        for state_code in self.states.keys():
            success = self.run_indexer(state_code, indexer_type)
            results[state_code] = success
            time.sleep(2)  # Small delay between starting indexers
        
        # Summary
        logger.info("\n" + "="*60)
        logger.info("Indexer Run Summary")
        logger.info("="*60)
        
        for state_code, success in results.items():
            state_name = self.states[state_code]['name']
            status = "✓ Success" if success else "✗ Failed"
            logger.info(f"{state_name} ({state_code.upper()}): {status}")
        
        logger.info("="*60 + "\n")
    
    def monitor_indexer(self, state_code: str, indexer_type: str = 'both', 
                       check_interval: int = 10, max_checks: int = 30):
        """
        Monitor indexer(s) progress
        
        Args:
            state_code: State code to monitor
            indexer_type: Type of indexer to monitor ('state', 'common', or 'both')
            check_interval: Seconds between status checks
            max_checks: Maximum number of status checks
        """
        try:
            if state_code not in self.states:
                logger.error(f"Invalid state code: {state_code}")
                return
            
            state_name = self.states[state_code]['name']
            indexers = self.states[state_code]['indexers']
            
            logger.info(f"Monitoring indexer(s) for {state_name} ({state_code.upper()})")
            logger.info(f"Checking every {check_interval} seconds (max {max_checks} checks)\n")
            
            indexers_to_monitor = []
            if indexer_type in ['state', 'both']:
                indexers_to_monitor.append(('State', indexers['state']))
            if indexer_type in ['common', 'both']:
                indexers_to_monitor.append(('Common', indexers['common']))
            
            for i in range(max_checks):
                timestamp = datetime.now().strftime("%H:%M:%S")
                print(f"\n[{timestamp}] Check #{i+1}/{max_checks}")
                print("-" * 60)
                
                all_completed = True
                
                for label, indexer_name in indexers_to_monitor:
                    status = self.indexer_client.get_indexer_status(indexer_name)
                    current_status = status.status
                    last_result = status.last_result
                    
                    if last_result:
                        print(f"{label:12} | Status: {current_status:10} | "
                              f"Last: {last_result.status:15} | "
                              f"Processed: {last_result.items_processed:4} | "
                              f"Failed: {last_result.items_failed:4}")
                        
                        if last_result.status not in ['success', 'transientFailure', 'persistentFailure']:
                            all_completed = False
                    else:
                        print(f"{label:12} | Status: {current_status:10} | No execution history yet")
                        all_completed = False
                
                if all_completed:
                    logger.info(f"\nAll monitored indexers completed")
                    break
                
                if i < max_checks - 1:
                    time.sleep(check_interval)
            
            # Final status
            print("\n" + "="*60)
            print("Final Status Report")
            print("="*60)
            self.get_indexer_status(state_code, indexer_type)
            
        except Exception as e:
            logger.error(f"Failed to monitor indexer: {str(e)}")


def main():
    """Main execution function - Runs all indexers for all states"""
    print("\n" + "="*60)
    print("Azure Cognitive Search - Indexer Runner")
    print("Running ALL indexers for ALL states")
    print("="*60 + "\n")
    
    try:
        runner = AzureIndexerRunner()
        
        print("🚀 Starting all indexers (both state-specific and common)...")
        print("This will start 12 indexers across 6 states (IA, IN, OH, TX, VA, WA)\n")
        
        # Run all indexers
        runner.run_all_indexers('both')
        
        print("\n✓ All indexers have been started successfully!")
        print("Check Azure Portal to monitor their progress.")
        
    except Exception as e:
        print(f"\n✗ Operation failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()