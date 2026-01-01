from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

# -------------------------
# Configuration
# -------------------------
STORAGE_ACCOUNT_NAME = "gurustorageacct"
CONTAINER_NAME = "guru1"
BLOB_NAME = "connect.txt"

# Blob service URL
account_url = f"https://gurustorageacct.blob.core.windows.net"

# -------------------------
# Authenticate using Managed Identity / Azure CLI
# -------------------------
credential = DefaultAzureCredential()

blob_service_client = BlobServiceClient(
    account_url=account_url,
    credential=credential
)

# -------------------------
# Read the text file
# -------------------------
blob_client = blob_service_client.get_blob_client(
    container=CONTAINER_NAME,
    blob=BLOB_NAME
)

blob_data = blob_client.download_blob().readall()

# Convert bytes to string
text_content = blob_data.decode("utf-8")

print("File contents:")
print(text_content)
