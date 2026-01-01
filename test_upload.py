from dotenv import load_dotenv
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from io import BytesIO
import os

class BlobTestDataUploader:
    def __init__(self):
        # Load environment variables
        load_dotenv()
        
        # Use Managed Identity
        self.credential = DefaultAzureCredential()
        self.storage_account_name = os.environ["AZURE_STORAGE_ACCOUNT_NAME"]
        self.account_url = f"https://{self.storage_account_name}.blob.core.windows.net"
        
        # Initialize Blob Service Client with Managed Identity
        self.blob_service_client = BlobServiceClient(
            account_url=self.account_url,
            credential=self.credential
        )
        
        # Container names
        self.containers = [
            os.environ.get("AZURE_BLOB_CONTAINER_STATE1", "state1-container"),
            os.environ.get("AZURE_BLOB_CONTAINER_STATE2", "state2-container"),
            os.environ.get("AZURE_BLOB_CONTAINER_STATE3", "state3-container"),
            os.environ.get("AZURE_BLOB_CONTAINER_STATE4", "state4-container"),
            os.environ.get("AZURE_BLOB_CONTAINER_STATE5", "state5-container"),
            os.environ.get("AZURE_BLOB_CONTAINER_STATE6", "state6-container")
        ]
        
        # State names
        self.state_names = [
            os.environ.get("STATE1_NAME", "State1"),
            os.environ.get("STATE2_NAME", "State2"),
            os.environ.get("STATE3_NAME", "State3"),
            os.environ.get("STATE4_NAME", "State4"),
            os.environ.get("STATE5_NAME", "State5"),
            os.environ.get("STATE6_NAME", "State6")
        ]
    
    def create_dummy_pdf(self, title: str, num_pages: int = 3, content_prefix: str = "") -> BytesIO:
        """Create a dummy PDF with specified number of pages"""
        buffer = BytesIO()
        pdf = canvas.Canvas(buffer, pagesize=letter)
        
        for page_num in range(1, num_pages + 1):
            # Add title
            pdf.setFont("Helvetica-Bold", 16)
            pdf.drawString(100, 750, title)
            
            # Add page number
            pdf.setFont("Helvetica", 12)
            pdf.drawString(100, 720, f"Page {page_num} of {num_pages}")
            
            # Add some content
            pdf.setFont("Helvetica", 10)
            y_position = 680
            
            content_lines = [
                f"{content_prefix} - This is page {page_num}",
                f"Document: {title}",
                f"",
                f"Sample content for testing Azure Cognitive Search indexing.",
                f"This document contains {num_pages} pages in total.",
                f"",
                f"Keywords: test, document, page{page_num}, {content_prefix.lower()}",
                f"",
                f"Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
                f"Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
                f"Ut enim ad minim veniam, quis nostrud exercitation ullamco.",
                f"",
                f"Page {page_num} specific information:",
                f"- Section: {chr(64 + page_num)}",
                f"- Content ID: {title.replace(' ', '_')}_p{page_num}",
                f"- Status: Active"
            ]
            
            for line in content_lines:
                pdf.drawString(100, y_position, line)
                y_position -= 15
            
            pdf.showPage()
        
        pdf.save()
        buffer.seek(0)
        return buffer
    
    def ensure_container_exists(self, container_name: str):
        """Create container if it doesn't exist"""
        try:
            container_client = self.blob_service_client.get_container_client(container_name)
            if not container_client.exists():
                container_client.create_container()
                print(f"✓ Created container: {container_name}")
            else:
                print(f"✓ Container already exists: {container_name}")
        except Exception as e:
            print(f"✗ Error with container {container_name}: {str(e)}")
            raise
    
    def upload_pdf_to_blob(self, container_name: str, blob_name: str, pdf_buffer: BytesIO):
        """Upload PDF to blob storage"""
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=container_name, 
                blob=blob_name
            )
            
            # Set metadata including page number info
            metadata = {
                "content_type": "application/pdf",
                "uploaded_by": "test_script"
            }
            
            blob_client.upload_blob(
                pdf_buffer, 
                overwrite=True,
                metadata=metadata
            )
            print(f"  ✓ Uploaded: {blob_name}")
            return True
        except Exception as e:
            print(f"  ✗ Failed to upload {blob_name}: {str(e)}")
            return False
    
    def create_test_pdfs_for_state(self, state_name: str, container_name: str, num_docs: int = 3):
        """Create and upload test PDFs for a specific state"""
        print(f"\n{'='*60}")
        print(f"Creating test PDFs for {state_name}")
        print(f"Container: {container_name}")
        print(f"{'='*60}")
        
        # Ensure container exists
        self.ensure_container_exists(container_name)
        
        # Create and upload multiple test documents
        for doc_num in range(1, num_docs + 1):
            # Create PDF with 3-5 pages
            num_pages = 3 + (doc_num % 3)
            title = f"{state_name} Document {doc_num}"
            content_prefix = f"{state_name} Regulation"
            
            print(f"\nCreating: {title} ({num_pages} pages)")
            pdf_buffer = self.create_dummy_pdf(title, num_pages, content_prefix)
            
            # Upload to blob
            blob_name = f"{state_name.lower()}_doc_{doc_num}.pdf"
            self.upload_pdf_to_blob(container_name, blob_name, pdf_buffer)
    
    def create_test_pdfs_for_all_states(self, num_docs_per_state: int = 3):
        """Create and upload test PDFs for all states"""
        print(f"\n{'='*70}")
        print(f"STARTING TEST DATA UPLOAD")
        print(f"Creating {num_docs_per_state} documents per state")
        print(f"{'='*70}")
        
        for state_name, container_name in zip(self.state_names, self.containers):
            try:
                self.create_test_pdfs_for_state(state_name, container_name, num_docs_per_state)
            except Exception as e:
                print(f"\n✗ Error processing {state_name}: {str(e)}")
                continue
        
        print(f"\n{'='*70}")
        print("TEST DATA UPLOAD COMPLETED")
        print(f"{'='*70}\n")
    
    def list_blobs_in_container(self, container_name: str):
        """List all blobs in a container"""
        try:
            container_client = self.blob_service_client.get_container_client(container_name)
            blob_list = container_client.list_blobs()
            
            print(f"\nBlobs in {container_name}:")
            count = 0
            for blob in blob_list:
                print(f"  - {blob.name} ({blob.size} bytes)")
                count += 1
            
            if count == 0:
                print(f"  (No blobs found)")
            else:
                print(f"  Total: {count} blobs")
            
            return count
        except Exception as e:
            print(f"Error listing blobs in {container_name}: {str(e)}")
            return 0
    
    def list_all_blobs(self):
        """List blobs in all containers"""
        print(f"\n{'='*70}")
        print("LISTING ALL BLOBS")
        print(f"{'='*70}")
        
        total_blobs = 0
        for state_name, container_name in zip(self.state_names, self.containers):
            try:
                count = self.list_blobs_in_container(container_name)
                total_blobs += count
            except Exception as e:
                print(f"Error with {state_name}: {str(e)}")
                continue
        
        print(f"\n{'='*70}")
        print(f"TOTAL BLOBS ACROSS ALL CONTAINERS: {total_blobs}")
        print(f"{'='*70}\n")
    
    def clean_container(self, container_name: str):
        """Delete all blobs in a container"""
        try:
            container_client = self.blob_service_client.get_container_client(container_name)
            blob_list = container_client.list_blobs()
            
            count = 0
            for blob in blob_list:
                blob_client = container_client.get_blob_client(blob.name)
                blob_client.delete_blob()
                count += 1
            
            print(f"✓ Deleted {count} blobs from {container_name}")
            return count
        except Exception as e:
            print(f"✗ Error cleaning {container_name}: {str(e)}")
            return 0
    
    def clean_all_containers(self):
        """Delete all test blobs from all containers"""
        print(f"\n{'='*70}")
        print("CLEANING ALL CONTAINERS")
        print(f"{'='*70}")
        
        confirm = input("\n⚠️  This will delete ALL blobs. Continue? (yes/no): ")
        if confirm.lower() != 'yes':
            print("Cancelled.")
            return
        
        total_deleted = 0
        for state_name, container_name in zip(self.state_names, self.containers):
            try:
                count = self.clean_container(container_name)
                total_deleted += count
            except Exception as e:
                print(f"Error with {state_name}: {str(e)}")
                continue
        
        print(f"\n{'='*70}")
        print(f"TOTAL BLOBS DELETED: {total_deleted}")
        print(f"{'='*70}\n")


# Example usage
if __name__ == "__main__":
    uploader = BlobTestDataUploader()
    
    # Create test PDFs for all states (3 documents per state, each with 3-5 pages)
    uploader.create_test_pdfs_for_all_states(num_docs_per_state=3)
    
    # List all uploaded blobs
    uploader.list_all_blobs()
    
    # If you want to create test data for a single state:
    # uploader.create_test_pdfs_for_state("State1", "state1-container", num_docs=5)
    
    # To clean up test data (use with caution!):
    # uploader.clean_all_containers()