
## YouTube resources

Setting up Databricks Unity Catalog Environments
- https://www.youtube.com/watch?v=B0Ox7jdoPNQ&list=PLHN2ijxAWBaMY4_1xMhOoeRWiZ6_d8F9s&index=120&t=461s&ab_channel=AdvancingAnalytics

Databricks Unity Catalog Demo
- https://www.youtube.com/watch?v=U1Ez5LNzl48&ab_channel=Databricks

Unity Catalog Introduction
- https://www.youtube.com/watch?v=yc5BHW149hs&list=PLY-V_O-O7h4fwcHcXgkR_zTLvddvE_GfC&ab_channel=NextGenLearning


## Azure Databricks resources

https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/

Unity Catalog is a unified governance solution for data and AI assets on the Lakehouse. It is a part of Azure Databricks and provides centralized access control, auditing, lineage, and data discovery capabilities across Azure Databricks workspaces. Here's an overview of its key features and components:

### Key Features:

1. **Define once, secure everywhere:** Unity Catalog offers a single place to administer data access policies that apply across all workspaces and personas.
2. **Standards-compliant security model:** Its security model is based on standard ANSI SQL, allowing administrators to grant permissions using familiar syntax.
3. **Built-in auditing and lineage:** It automatically captures user-level audit logs and lineage data that tracks how data assets are created and used.
4. **Data discovery:** Unity Catalog allows tagging and documenting data assets and provides a search interface to help data consumers find data.
5. **System tables (Public Preview):** It enables easy access and query to operational data, including audit logs, billable usage, and lineage.

### Object Model:

- **Metastore:** The top-level container for metadata, organizing data in a three-level namespace.
- **Catalog:** The first layer of the object hierarchy, used to organize data assets.
- **Schema:** Also known as databases, schemas are the second layer of the object hierarchy and contain tables and views.
- **Volume:** Volumes sit alongside tables and views at the lowest level of the object hierarchy and provide governance for non-tabular data.
- **Table:** Tables and views are at the lowest level in the object hierarchy.

### Additional Components:

- **Managed Storage:** When creating a metastore, an Azure Data Lake Storage Gen2 container is associated for managed storage.
- **Storage Credentials and External Locations:** Unity Catalog manages access to underlying cloud storage through storage credentials and external locations.
- **Catalogs and Schemas:** Catalogs and schemas are used to organize data assets, and permissions are managed through data permissions like `USE CATALOG`, `USE SCHEMA`, etc.
- **Volumes (Public Preview):** Volumes reside in the third layer of Unity Catalog's namespace and contain directories and files for data stored in any format, providing non-tabular access to data.

Unity Catalog's design aims to provide a comprehensive solution for managing and governing data across various workspaces, ensuring security, compliance, and ease of access for users and administrators. It integrates with existing Azure services and offers a robust set of features for modern data management needs.
