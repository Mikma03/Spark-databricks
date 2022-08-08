

<!-- TOC ignore:true -->
## Table of contents

<!-- TOC -->

- [Microsoft - Data engineering with Azure Databricks](#microsoft---data-engineering-with-azure-databricks)
  - [Link to GitHub repo:](#link-to-github-repo)
- [Describe Azure Databrick](#describe-azure-databrick)
- [Spark architecture fundamentals](#spark-architecture-fundamentals)
  - [The cluster: Drivers, executors, slots & tasks](#the-cluster-drivers-executors-slots--tasks)
  - [Jobs & stages](#jobs--stages)
  - [Cluster management](#cluster-management)
- [Work with DataFrames in Azure Databricks](#work-with-dataframes-in-azure-databricks)

<!-- /TOC -->

# Microsoft - Data engineering with Azure Databricks

Learn how to harness the power of Apache Spark and powerful clusters running on the Azure Databricks platform to run large data engineering workloads in the cloud.

This learning path helps prepare you for Exam DP-203: Data Engineering on Microsoft Azure.

- https://docs.microsoft.com/en-us/certifications/exams/dp-203

## Link to GitHub repo:

- https://github.com/MicrosoftLearning/microsoft-learning-paths-databricks-notebooks

# Describe Azure Databrick

Azure Databricks is a fully-managed version of the open-source Apache Spark analytics and data processing engine. Azure Databricks is an enterprise-grade and secure cloud-based big data and machine learning platform.

Databricks provides a notebook-oriented Apache Spark as-a-service workspace environment, making it easy to manage clusters and explore data interactively.

![azdb1](img/azure-databricks.png)

- SQL - Closer to ANSI SQL 2003 compliance

  - Now running all 99 TPC-DS queries
  - New standards-compliant parser (with good error messages!)
  - Subqueries (correlated & uncorrelated)
  - Approximate aggregate stats


# Spark architecture fundamentals

**High-level overview**

From a high level, the Azure Databricks service launches and manages Apache Spark clusters within your Azure subscription. Apache Spark clusters are groups of computers that are treated as a single computer and handle the execution of commands issued from notebooks. Using a master-worker type architecture, clusters allow processing of data to be parallelized across many computers to improve scale and performance. They consist of a Spark Driver (master) and worker nodes. The `driver` node sends work to the `worker` nodes and instructs them to pull data from a specified data source.

![img2](img/apache-spark-physical-cluster.png)

Internally, Azure Kubernetes Service (AKS) is used to run the Azure Databricks control-plane and data-planes via containers running on the latest generation of Azure hardware (Dv3 VMs), with NvMe SSDs capable of blazing 100us latency on IO. These make Databricks I/O performance even better. In addition, accelerated networking provides the fastest virtualized network infrastructure in the cloud. Azure Databricks utilizes these features to further improve Spark performance. Once the services within this managed resource group are ready, you will be able to manage the Databricks cluster through the Azure Databricks UI and through features such as auto-scaling and auto-termination.

![img3](img/azure-databricks-architecture.png)

## The cluster: Drivers, executors, slots & tasks

![img4](img/spark-cluster-slots.png)

- The Driver is the JVM in which our application runs.
- The secret to Spark's awesome performance is parallelism.
    - Scaling vertically is limited to a finite amount of RAM, Threads and CPU speeds.
    - Scaling horizontally means we can simply add new "nodes" to the cluster almost endlessly.
- We parallelize at two levels:
    - The first level of parallelization is the Executor - a Java virtual machine running on a node, typically, one instance per node.
    - The second level of parallelization is the Slot - the number of which is determined by the number of cores and CPUs of each node.
- Each Executor has a number of Slots to which parallelized Tasks can be assigned to it by the Driver.


## Jobs & stages

- Each parallelized action is referred to as a Job.
- The results of each Job (parallelized/distributed action) is returned to the Driver.
- Depending on the work required, multiple Jobs will be required.
- Each Job is broken down into Stages.
- This would be analogous to building a house (the job)
    - The first stage would be to lay the foundation.
    - The second stage would be to erect the walls.
    - The third stage would be to add the room.
    - Attempting to do any of these steps out of order just won't make sense, if not just impossible.

## Cluster management

- At a much lower level, Spark Core employs a Cluster Manager that is responsible for provisioning nodes in our cluster.
  - Databricks provides a robust, high-performing Cluster Manager as part of its overall offerings.
- In each of these scenarios, the Driver is [presumably] running on one node, with each Executors running on N different nodes.
- For the sake of this learning path, we don't need to concern ourselves with cluster management, thanks to Azure Databricks.
- From a developer's and learner's perspective my primary focus is on...
    - The number of Partitions my data is divided into.
    - The number of Slots I have for parallel execution.
    - How many Jobs am I triggering?
    - And lastly the Stages those jobs are divided into.

```
How do you list files in DBFS within a notebook?

%fs ls /my-file-path
```


# Work with DataFrames in Azure Databricks

Use the `count()` method to count rows in a DataFrame
Use the `display()` function to display a DataFrame in the Notebook
Cache a DataFrame for quicker operations if the data is needed a second time
Use the `limit` function to display a small set of rows from a larger DataFrame
Use `select()` to select a subset of columns from a DataFrame
Use `distinct()` and dropDuplicates to remove duplicate data
Use `drop()` to remove columns from a DataFrame

