<!-- TOC -->

- [Resourcesc](#resourcesc)
- [Hadoop - intro](#hadoop---intro)
  - [MapReduce](#mapreduce)
  - [How MapReduce Works](#how-mapreduce-works)
  - [Main Features of MapReduce](#main-features-of-mapreduce)
  - [Conclusion](#conclusion)
- [Clouds](#clouds)
- [Hadoops API](#hadoops-api)
- [Resource manager](#resource-manager)
- [What is Spark?](#what-is-spark)
- [Driver and Executor i Spark](#driver-and-executor-i-spark)
- [What is a Directed Acyclic Graph (DAG)?](#what-is-a-directed-acyclic-graph-dag)
  - [DAG in Apache Spark](#dag-in-apache-spark)
  - [Example](#example)
  - [Conclusion](#conclusion-1)

<!-- /TOC -->

## Resourcesc

Links to resources. The best book about Spark:

- https://learning.oreilly.com/library/view/learning-spark-2nd/9781492050032/

Resources of AWS compared to Azure:

- https://learning.oreilly.com/library/view/learning-spark-2nd/9781492050032/

## Hadoop - intro

Hadoop was first, Spark is evolution of Hadoop. There was a need for distributed storage as well for distributed processing (computing). Both are open-source software projects. Need to speed: big data happened. Sark started in 2014. Hadoop first open-source project for distributed storage.

Hadoop 2.0 - 2010 till 2020

- layer 03: computing - MapReduce - paralel processing = Java + Python
  - This is algorithm and mask for working with resources.
- layer 02: resource manager - YARN
- layer 01: storage - HDFS

There is Hadoop 3.0, but most companies using 2.0. In 3.0 architecture is the same as in 2.0.

Why Hadoop is not so popular anymore? **Spark is replacement for MapReduce**. Spark is in memory processing - RAM; intermediate data is stored in RAM.

But MapReduce keep data in HDFS and that could be HDD drives. That is why Spark will be always faster - as all computing will happen in memory.

Spark can run standalone, that means Spark can run without Hadoop. Using Spark run standalone we can run Spark in Kubernetes / Docker cluster.

What happen when Spark is ot of memory?

- Temporary memory will be used.

### MapReduce

New paradigm of programming based on functional programming. This framework was dedicated for distributed data.

### How MapReduce Works

MapReduce consists of two main phases: the Map phase and the Reduce phase.

1. **Map Phase**:

   - **Input**: The input data is divided into chunks, and each chunk is processed by a separate Map task.
   - **Processing**: The Map function takes a set of key/value pairs and processes each pair to generate zero or more intermediate key/value pairs.
   - **Output**: The intermediate key/value pairs are then grouped by key and sent to the Reduce phase.

2. **Reduce Phase**:

   - **Input**: The intermediate key/value pairs from the Map phase are sorted and grouped by key.
   - **Processing**: The Reduce function takes the intermediate key/value pairs for a particular key and combines them in some way to produce a smaller set of values.
   - **Output**: The final result is a set of key/value pairs that represent the processed data.

### Main Features of MapReduce

1. **Distributed Processing**: MapReduce can process data across a large number of machines in parallel, making it suitable for handling vast amounts of data.
2. **Fault Tolerance**: If a machine fails during processing, the task is automatically reassigned to another machine. This ensures that the process continues even in the face of hardware failures.
3. **Scalability**: You can add more machines to the cluster to handle larger data sets. MapReduce scales horizontally, meaning that it can handle more data by simply adding more machines.
4. **Data Locality**: MapReduce tries to run the Map tasks on the nodes where the data resides. This reduces the amount of data that must be transferred across the network and speeds up processing.
5. **Simplicity**: Developers only need to write the Map and Reduce functions. The underlying system takes care of distributing the data, managing the tasks, and handling failures.
6. **Flexibility**: MapReduce can process structured and unstructured data, and it can be used with various data formats and programming languages.
7. **Optimization**: Many implementations of MapReduce, such as Hadoop, provide ways to optimize the processing, such as combiners that reduce the amount of data transferred between the Map and Reduce phases.

### Conclusion

MapReduce is a powerful tool for processing large data sets across distributed clusters. Its main features, such as distributed processing, fault tolerance, and scalability, make it a popular choice for big data processing. By dividing the processing into Map and Reduce phases, it provides a flexible and efficient way to handle vast amounts of data.

## Clouds

Why we are using clouds as Azure or AWS? Because we are not worrying about data loss and we can destroy our resources if we do not need them.

More... we want separate storage from compute. From this idea data lakes comes from and storage for raw data: blob. In AWS we have S3 (simple storage service).

Still, good servers will be expensive. Good means SLA will be acceptable.

Next important fact, storage is far more cheaper than computing this is why we want that to be separated.

## Hadoops API

If files (data) are available in HDFS then Spark need to use Hadoop API. Nevertheless, if files are available in S3 or Spark running in standalone mode then still is no need for Hadoop API. As resource manager we can use also Mesos.

## Resource manager

Link for more information about RM's:

- https://spark.apache.org/docs/latest/cluster-overview.html#cluster-manager-types

Apache Spark supports several types of cluster managers, and the choice of the best one may depend on specific requirements and preferences. Here's an overview of the cluster managers supported by Spark:

1. **Standalone**: A simple cluster manager included with Spark, making it easy to set up a cluster.
2. **Apache Mesos**: A general cluster manager that can also run Hadoop MapReduce and service applications. (Note: Mesos is marked as deprecated in Spark's documentation.)
3. **Hadoop YARN**: The resource manager in Hadoop 2 and 3.
4. **Kubernetes**: An open-source system for automating deployment, scaling, and management of containerised applications.

## What is Spark?

Unified engine for data processing / computing for processing on clusters (paralel processing). We can install Spark on local PC - one mode cluster.

Spark 1.0 was released in 2014. Created as PhD project in Berkley. 2020 year Spark 3.0 was released. At the beginning Spark was available from RDD. Next in higher version of Spark Dataframe and Dataset were available.

## Driver and Executor i Spark

Architecture of Spark App

- https://spark.apache.org/docs/latest/cluster-overview.html#components

Driver is responsible for program flow. Executor is responsible for executing task. For example when we want load data from S3 location then Executor will load data and Driver instruct that operation.

So driver telling what to do and when; executor will execute tasks.

## What is a Directed Acyclic Graph (DAG)?

A Directed Acyclic Graph (DAG) is a finite directed graph with no directed cycles. This means that it consists of vertices and edges, with each edge directed from one vertex to another, such that there is no way to start at any vertex and follow a consistently directed sequence of edges that eventually loops back to that same vertex.

### DAG in Apache Spark

In Apache Spark, a DAG represents a sequence of computations performed on data. Here's how it works:

1. **Stages**: Spark breaks down the job into stages, which are sets of tasks that can be executed in parallel. Stages are created based on transformations and actions applied to the data. Transformations that have a narrow dependency (like `map`) allow tasks to be executed in parallel, while transformations with a wide dependency (like `reduceByKey`) may cause a new stage to be created.
    
2. **Tasks**: Each stage consists of multiple tasks that can be executed in parallel. A task is the smallest unit of work in Spark, representing a computation on a partition of the data.
    
3. **Vertices and Edges**: In the DAG, vertices represent the RDDs (Resilient Distributed Datasets), and the edges represent the transformations applied to the RDDs.
    
4. **Execution**: When an action is called on an RDD, Spark constructs a DAG representing the transformations that need to be executed. Then, the DAG is divided into stages, and the stages are submitted to the cluster manager for execution.
    
5. **Fault Tolerance**: One of the key benefits of the DAG model in Spark is fault tolerance. If a task fails, Spark can recompute the lost data by looking at the DAG and only re-executing the necessary stages. This is more efficient than Hadoop's MapReduce, where the entire job may need to be rerun.
    

### Example

Consider a simple Spark job that reads data, applies a `map` transformation, and then a `reduceByKey` transformation. The DAG for this job would consist of two stages:

- **Stage 1**: Contains tasks for the `map` transformation. These tasks can be executed in parallel.
- **Stage 2**: Contains tasks for the `reduceByKey` transformation. This stage depends on the completion of Stage 1.

### Conclusion

The DAG model in Apache Spark provides a clear and efficient way to represent and execute complex data processing tasks. By breaking down the job into stages and tasks, and representing them in a graph structure, Spark can execute tasks in parallel, provide fault tolerance, and optimize the execution of the job. It's a key reason why Spark is known for its performance and flexibility in handling large-scale data processing.