Based on this article:

- https://learn.microsoft.com/en-us/azure/storage/common/storage-sas-overview

## SAS Token

### Explanation of SAS Token

A Shared Access Signature (SAS) is a token that provides secure delegated access to resources in an Azure storage account. It allows you to have granular control over how a client can access your data. Here's a detailed explanation:

- **What It Controls**: With a SAS, you can specify what resources the client may access, what permissions they have to those resources, and how long the SAS is valid.

- **Types of SAS**: Azure Storage supports three types of shared access signatures:

  1. **User Delegation SAS**: Secured with Azure AD credentials and applies to Blob storage only.
  2. **Service SAS**: Secured with the storage account key and delegates access to a specific Azure Storage service.
  3. **Account SAS**: Secured with the storage account key and delegates access to resources in one or more of the storage services.

- **How It Works**: A SAS is a token appended to the URI for an Azure Storage resource. It contains query parameters that indicate how the resources may be accessed. One of the parameters, the signature, is constructed from the SAS parameters and signed with the key used to create the SAS. Azure Storage uses this signature to authorize access.

- **When to Use**: SAS is useful when you want to give secure access to resources in your storage account to clients who do not otherwise have permissions. It can be used to allow clients to read and write their own data to your storage account, either through a front-end proxy service or directly using SAS.

### Summary of the Article

The article provides a comprehensive overview of Shared Access Signatures (SAS) in Azure Storage. It explains what SAS is, the types of SAS (User Delegation SAS, Service SAS, Account SAS), and how it works.

The article also includes information about signing a SAS token with a user delegation key or an account key and recommends using Azure AD credentials for superior security. It illustrates how to use a SAS to give secure access to resources in your storage account, including examples of service URIs with SAS tokens and diagrams showing different scenarios where SAS can be used.
