Article in Azure
- https://learn.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes

## What is Secret Scope?

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

## Overview

There are two types of secret scope: Azure Key Vault-backed and Databricks-backed.
### Azure Key Vault-backed scopes

To reference secrets stored in an [Azure Key Vault](https://learn.microsoft.com/en-us/azure/key-vault/key-vault-overview), you can create a secret scope backed by Azure Key Vault. You can then leverage all of the secrets in the corresponding Key Vault instance from that secret scope. Because the Azure Key Vault-backed secret scope is a read-only interface to the Key Vault, the `PutSecret` and `DeleteSecret` the [Secrets API](https://docs.databricks.com/api/azure/workspace/secrets) operations are not allowed. To manage secrets in Azure Key Vault, you must use the Azure [Set Secret](https://learn.microsoft.com/en-us/rest/api/keyvault/secrets/set-secret/set-secret?tabs=HTTP) REST API or Azure portal UI.

Note

Creating an Azure Key Vault-backed secret scope role grants the **Get** and **List** permissions to the resource ID for the Azure Databricks service using key vault access policies, even if the key vault is using the Azure RBAC permissions model.

### Databricks-backed scopes

A Databricks-backed secret scope is stored in (backed by) an encrypted database owned and managed by Azure Databricks. The secret scope name:

- Must be unique within a workspace.
- Must consist of alphanumeric characters, dashes, underscores, `@`, and periods, and may not exceed 128 characters.

The names are considered non-sensitive and are readable by all users in the workspace.

You create a Databricks-backed secret scope using the [Databricks CLI](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/cli/databricks-cli) (version 0.7.1 and above). Alternatively, you can use the [Secrets API](https://docs.databricks.com/api/azure/workspace/secrets).


### Best practices

As a team lead, you might want to create different scopes for Azure Synapse Analytics and Azure Blob storage credentials and then provide different subgroups in your team access to those scopes. You should consider how to achieve this using the different scope types:

- If you use a Databricks-backed scope and add the secrets in those two scopes, they will be different secrets (Azure Synapse Analytics in scope 1, and Azure Blob storage in scope 2).
- If you use an Azure Key Vault-backed scope with each scope referencing a **different Azure Key Vault** and add your secrets to those two Azure Key Vaults, they will be different sets of secrets (Azure Synapse Analytics ones in scope 1, and Azure Blob storage in scope 2). These will work like Databricks-backed scopes.
- If you use two Azure Key Vault-backed scopes with both scopes referencing the **same Azure Key Vault** and add your secrets to that Azure Key Vault, all Azure Synapse Analytics and Azure Blob storage secrets will be available. Since ACLs are at the scope level, all members across the two subgroups will see all secrets. This arrangement does not satisfy your use case of restricting access to a set of secrets to each group.