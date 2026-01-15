# guru


 Grant Proper RBAC Permissions (Recommended for Production)
Your managed identity needs these roles:

Go to Azure Portal
Navigate to: Azure AI Search service → Access control (IAM)
Add role assignment:

Role: Search Service Contributor (to create indexes/indexers)
Assign access to: Managed Identity or User
Select your identity/user


Also for Blob Storage:

Navigate to: Storage Account → Access control (IAM)
Add role: Storage Blob Data Contributor