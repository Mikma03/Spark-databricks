Article in Azure

- https://learn.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes

The article titled "Secret scopes - Azure Databricks" from Microsoft Learn provides an in-depth guide on managing secrets in Azure Databricks. Here's a summary of the key points:

1. **Secret Scopes**: A secret scope is a collection of secrets identified by a name. A workspace can have a maximum of 100 secret scopes.
2. **Types of Secret Scopes**:

   - **Azure Key Vault-backed Scopes**: These scopes reference secrets stored in Azure Key Vault, providing a read-only interface to the Key Vault. Operations like `PutSecret` and `DeleteSecret` are not allowed.
   - **Databricks-backed Scopes**: These are stored in an encrypted database managed by Azure Databricks. The names must be unique within a workspace and follow specific character rules.

3. **Scope Permissions**: Controlled by ACLs, scopes are created with `MANAGE` permission for the creator. Granular permissions can be assigned if the account has the Premium plan.
4. **Best Practices**: Recommendations include creating different scopes for different services like Azure Synapse Analytics and Azure Blob storage, and considering how to achieve this using different scope types.
5. **Creating an Azure Key Vault-backed Secret Scope**: The article provides detailed instructions on creating this type of scope using the UI or the Databricks CLI. Requirements include specific roles and permissions in Azure Active Directory and the Azure key vault instance.
6. **Databricks CLI Usage**: The article also includes information on using the Databricks CLI to create and manage secret scopes, including examples and links to further resources.

The article is rich with examples, links to related documentation, and images to guide the reader through the process of managing secret scopes in Azure Databricks.
