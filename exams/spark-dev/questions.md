
<!-- TOC -->

- [Q1 - executors in Spark](#q1---executors-in-spark)
- [Q2 - task in the Spark](#q2---task-in-the-spark)
- [Q3 - cluster manager](#q3---cluster-manager)
- [Q4 - dynamic partition pruning](#q4---dynamic-partition-pruning)
- [Q5 - Spark over Hadoop](#q5---spark-over-hadoop)
- [Q6 - garbage collection](#q6---garbage-collection)
- [Q7 - Dataset API](#q7---dataset-api)
- [Q8 - cluster execution modes](#q8---cluster-execution-modes)
- [Q9 - executors](#q9---executors)
- [Q10 - RDDs](#q10---rdds)
- [Q11 - Spark UI](#q11---spark-ui)
- [Q12 - broadcast variables](#q12---broadcast-variables)
- [Q13 - Spark's performance](#q13---sparks-performance)
- [Q14 - shuffle](#q14---shuffle)
- [Q15 - Adaptive Query Execution](#q15---adaptive-query-execution)
- [Q16 - coalesce and repartition](#q16---coalesce-and-repartition)

<!-- /TOC -->
# Q1 - executors in Spark

- Which of the following options describes the responsibility of the executors in Spark?
  - The executors accept tasks from the driver, execute those tasks, and return results to the driver.

More info: 

Running Spark: an overview of Spark’s runtime architecture - Manning
- https://freecontent.manning.com/running-spark-an-overview-of-sparks-runtime-architecture/


# Q2 - task in the Spark

- Which of the following describes the role of tasks in the Spark execution hierarchy?

  - A task is a unit of work that is sent to the executor. Each stage has some tasks, one task per partition. The same task is done over different partitions of the RDD.

More info: 

Spark Certification Study Guide - Part 1 (Core) | Raki Rahman
- https://www.rakirahman.me/spark-certification-study-guide-part-1/#tasks


# Q3 - cluster manager

- Which of the following describes the role of the cluster manager?
  - The cluster manager allocates resources to Spark applications and maintains the executor processes in client mode. In client mode and cluster mode, the cluster manager is located on a node other than the client machine. From there it starts and ends executor processes on the cluster nodes as required by the Spark application running on the Spark driver.

  - Spark applications run as independent sets of processes on a cluster, coordinated by the SparkContext object in your main program (called the driver program).

    Specifically, to run on a cluster, the SparkContext can connect to several types of cluster managers (either Spark’s own standalone cluster manager, Mesos, YARN or Kubernetes), which allocate resources across applications. Once connected, Spark acquires executors on nodes in the cluster, which are processes that run computations and store data for your application. Next, it sends your application code (defined by JAR or Python files passed to SparkContext) to the executors. Finally, SparkContext sends tasks to the executors to run.

More info: 

  - Cluster Mode Overview - Spark 3.1.1 Documentation
    - https://spark.apache.org/docs/latest/cluster-overview.html#components


# Q4 - dynamic partition pruning

- Which of the following is the idea behind dynamic partition pruning in Spark?
  - Dynamic partition pruning is intended to skip over the data you do not need in the results of a query.

    Dynamic partition pruning provides an efficient way to selectively read data from files by skipping data that is irrelevant for the query. For example, if a query asks to consider only rows which have numbers >12 in column purchases via a filter, Spark would only read the rows that match this criteria from the underlying files. This method works in an optimal way if the purchases data is in a nonpartitioned table and the data to be filtered is partitioned.

    In standard database pruning means that the optimizer will avoid reading files that cannot contain the data that you are looking for. 

    Partition pruning in Spark is a performance optimization that limits the number of files and partitions that Spark reads when querying. After partitioning the data, queries that match certain partition filter criteria improve performance by allowing Spark to only read a subset of the directories and files. When partition filters are present, the catalyst optimizer pushes down the partition filters. The scan reads only the directories that match the partition filters, thus reducing disk I/O.

More info:

- Dynamic Partition Pruning in Spark 3.0 - DZone Big Data
  - https://dzone.com/articles/dynamic-partition-pruning-in-spark-30


# Q5 - Spark over Hadoop

- Which of the following is one of the big performance advantages that Spark has over Hadoop?

  - Spark can keep data in memory between queries, while Hadoop must write data to disk after each query.

More info: 

- Hadoop vs. Spark: A Head-To-Head Comparison | Logz.io
  - https://logz.io/blog/hadoop-vs-spark/#post-16858:~:text=Spark%20handles%20work%20in%20a%20similar,the%20user%20actively%20persists%20them.%20Initially,


# Q6 - garbage collection

- Which of the following statements about garbage collection in Spark is correct?

  - Serialized caching is a strategy to increase the performance of garbage collection.

    This statement is correct. The more Java objects Spark needs to collect during garbage collection, the longer it takes. Storing a collection of many Java objects, such as a DataFrame with a complex schema, through serialization as a single byte array thus increases performance. This means that garbage collection takes less time on a serialized DataFrame than an unserialized DataFrame.

  - Optimizing garbage collection performance in Spark may limit caching ability.

    This statement is correct. A full garbage collection run slows down a Spark application. When taking about "tuning" garbage collection, we mean reducing the amount or duration of these slowdowns.

    A full garbage collection run is triggered when the Old generation of the Java heap space is almost full. (If you are unfamiliar with this concept, check out the link to the Garbage Collection Tuning docs below.) Thus, one measure to avoid triggering a garbage collection run is to prevent the Old generation share of the heap space to be almost full.

    To achieve this, one may decrease its size. Objects with sizes greater than the Old generation space will then be discarded instead of cached (stored) in the space and helping it to be "almost full" . This will decrease the number of full garbage collection runs, increasing overall performance.

    Inevitably, however, objects will need to be recomputed when they are needed. So, this mechanism only works when a Spark application needs to reuse cached data as little as possible.

  - Garbage collection information can be accessed in the Spark UI's stage detail view.

    This statement is correct. The task table in the Spark UI's stage detail view has a "GC Time" column, indicating the garbage collection time needed per task.

  - In Spark, using the G1 garbage collector is an alternative to using the default Parallel garbage collector.

    This statement is correct. The G1 garbage collector, also known as garbage first garbage collector, is an alternative to the default Parallel garbage collector.

    While the default Parallel garbage collector divides the heap into a few static regions, the G1 garbage collector divides the heap into many small regions that are created dynamically. The G1 garbage collector has certain advantages over the Parallel garbage collector which improve performance particularly for Spark workloads that require high throughput and low latency.

    The G1 garbage collector is not enabled by default, and you need to explicitly pass an argument to Spark to enable it. For more information about the two garbage collectors, check out the Databricks article linked below.


More info:

- Would Spark unpersist the RDD itself when it realizes it won't be used anymore? - Stack Overflow
  - https://stackoverflow.com/questions/32636822/would-spark-unpersist-the-rdd-itself-when-it-realizes-it-wont-be-used-anymore/32640353#32640353
- Tuning Java Garbage Collection for Apache Spark Applications - The Databricks Blog
  - https://www.databricks.com/blog/2015/05/28/tuning-java-garbage-collection-for-spark-applications.html
- Tuning - Spark 3.0.0 Documentation
  - https://spark.apache.org/docs/3.0.0/tuning.html#garbage-collection-tuning
- Dive into Spark memory - Blog | luminousmen
  - https://luminousmen.com/post/dive-into-spark-memory


# Q7 - Dataset API

- Which of the following describes characteristics of the Dataset API?
  - The Dataset API is available in Scala, but it is not available in Python.

    The Dataset API uses fixed typing and is typically used for object-oriented programming. It is available when Spark is used with the Scala programming language, but not for Python. In Python, you use the DataFrame API, which is based on the Dataset API.

More info: 

- Learning Spark, 2nd Edition, Chapter 3, Datasets - Getting Started with Apache Spark on Databricks
  - https://www.databricks.com/spark/getting-started-with-apache-spark/datasets

# Q8 - cluster execution modes

- Which of the following describes the difference between client and cluster execution modes?
  - In client mode, the driver runs on the same machine as the Spark application, while in cluster mode, the driver runs on a different machine.

# Q9 - executors

- Which of the following statements about executors is correct, assuming that one can consider each of the JVMs working as executors as a pool of task execution slots?
  - Tasks run in parallel via slots.

    Given the assumption, an executor then has one or more "slots", defined by the equation `spark.executor.cores / spark.task.cpus`. With the executor's resources divided into slots, each task takes up a slot and multiple tasks can be executed in parallel.


More info: 

- Spark Architecture | Distributed Systems Architecture
  - https://0x0fff.com/spark-memory-management/


# Q10 - RDDs

- Which of the following statements about RDDs is incorrect?
  - An RDD consists of a single partition.

    Quite the opposite: Spark partitions RDDs and distributes the partitions across multiple nodes.

More info:

  One of the most important capabilities in Spark is persisting (or caching) a dataset in memory across operations. When you persist an RDD, each node stores any partitions of it that it computes in memory and reuses them in other actions on that dataset (or datasets derived from it). This allows future actions to be much faster (often by more than 10x). Caching is a key tool for iterative algorithms and fast interactive use.

  You can mark an RDD to be persisted using the `persist()` or `cache()` methods on it. The first time it is computed in an action, it will be kept in memory on the nodes. Spark’s cache is fault-tolerant – if any partition of an RDD is lost, it will automatically be recomputed using the transformations that originally created it.

  In addition, each persisted RDD can be stored using a different storage level, allowing you, for example, to persist the dataset on disk, persist it in memory but as serialized Java objects (to save space), replicate it across nodes. These levels are set by passing a StorageLevel object (Scala, Java, Python) to `persist()`. The `cache()` method is a shorthand for using the default storage level, which is S`torageLevel.MEMORY_ONLY` (store deserialized objects in memory). 

- https://spark.apache.org/docs/3.5.0/rdd-programming-guide.html#rdd-persistence


# Q11 - Spark UI

- Which of the following describes characteristics of the Spark UI?
  - There is a place in the Spark UI that shows the property spark.executor.memory.

    Correct, you can see Spark properties such as spark.executor.memory in the Environment tab.

# Q12 - broadcast variables

- Which of the following statements about broadcast variables is correct?
  - broadcast variables are immutable

    Correct, broadcast variables are immutable. They are read-only variables that are cached on each machine, rather than being shipped with tasks. They are used to save the cost of shipping a copy of a large read-only variable to all the tasks in a cluster. For example, if your application includes a large dataset that does not change, you can broadcast the dataset to the nodes in the cluster, instead of shipping a copy of it with tasks. This is especially useful when the dataset is too large to fit in the memory of each machine.

    Broadcast variables are created from a variable v by calling SparkContext.broadcast(v). The broadcast variable is a wrapper around v, and its value can be accessed by calling the value method. The code below shows how to create and use a broadcast variable.

    ```python
    broadcastVar = sc.broadcast([1, 2, 3])
    broadcastVar.value
    # [1, 2, 3]
    ```

    Broadcast variables are sent to executors only once, and maintained as immutable, read-only variables. Updates to the broadcast variable on the driver are not propagated to other executors. Hence, they can be used to give every node a copy of a large input dataset in an efficient manner. Spark also attempts to distribute broadcast variables using efficient broadcast algorithms to reduce communication cost.

    Broadcast variables are not used for sharing variables between executors. If you wish to do this, you can use a shared variable. See the section on shared variables below for more details.

More info:

Broadcast Variables
Broadcast variables allow the programmer to keep a read-only variable cached on each machine rather than shipping a copy of it with tasks. They can be used, for example, to give every node a copy of a large input dataset in an efficient manner. Spark also attempts to distribute broadcast variables using efficient broadcast algorithms to reduce communication cost.

Spark actions are executed through a set of stages, separated by distributed “shuffle” operations. Spark automatically broadcasts the common data needed by tasks within each stage. The data broadcasted this way is cached in serialized form and deserialized before running each task. This means that explicitly creating broadcast variables is only useful when tasks across multiple stages need the same data or when caching the data in deserialized form is important.

Broadcast variables are created from a variable v by calling SparkContext.broadcast(v). The broadcast variable is a wrapper around v, and its value can be accessed by calling the value method.

- https://spark.apache.org/docs/3.5.0/rdd-programming-guide.html#broadcast-variables

# Q13 - Spark's performance

- Which of the following is a viable way to improve Spark's performance when dealing with large amounts of data, given that there is only a single application running on the cluster?
  - Increase values for the properties `spark.sql.parallelism` and `spark.sql.shuffle.partitions`


More info:
- Basics of Apache Spark Configuration Settings
  - https://towardsdatascience.com/basics-of-apache-spark-configuration-settings-ca4faff40d45


# Q14 - shuffle

- Which of the following describes a shuffle?
  - A shuffle is a process that compares data between partitions.

    During a shuffle, data is compared between partitions because shuffling includes the process of sorting. For sorting, data need to be compared. Since per definition, more than one partition is involved in a shuffle, it can be said that data is compared across partitions. You can read more about the technical details of sorting in the blog post linked below.

    What is the shuffle in general? Imagine that you have a list of phone call detail records in a table and you want to calculate amount of calls happened each day. This way you would set the “day” as your key, and for each record (i.e. for each call) you would emit “1” as a value. After this you would sum up values for each key, which would be an answer to your question – total amount of records for each day. But when you store the data across the cluster, how can you sum up the values for the same key stored on different machines? The only way to do so is to make all the values for the same key be on the same machine, after this you would be able to sum them up.


    There are many different tasks that require shuffling of the data across the cluster, for instance table join – to join two tables on the field “id”, you must be sure that all the data for the same values of “id” for both of the tables are stored in the same chunks. Imagine the tables with integer keys ranging from 1 to 1’000’000. By storing the data in same chunks I mean that for instance for both tables values of the key 1-100 are stored in a single partition/chunk, this way instead of going through the whole second table for each partition of the first one, we can join partition with partition directly, because we know that the key values 1-100 are stored only in these two partitions. To achieve this both tables should have the same number of partitions, this way their join would require much less computations. So now you can understand how important shuffling is.

More infor:

- SPARK REPARTITION & COALESCE - EXPLAINED
  - https://datanoon.com/blog/spark_repartition_coalesce/

- Spark Architecture: Shuffle
  - https://0x0fff.com/spark-architecture-shuffle/

# Q15 - Adaptive Query Execution

- Which of the following describes Spark's Adaptive Query Execution?
  - AQF features are dynamically switching join strategies and dynamically optimizing skew join.


    Adaptive Query Execution (AQE) is an optimization technique in Spark SQL that makes use of the runtime statistics to choose the most efficient query execution plan, which is enabled by default since Apache Spark 3.2.0. Spark SQL can turn on and off AQE by spark.sql.adaptive.enabled as an umbrella configuration. As of Spark 3.0, there are three major features in AQE: including coalescing post-shuffle partitions, converting sort-merge join to broadcast join, and skew join optimization.

Additional info:

- Coalescing Post Shuffle Partitions

  This feature coalesces the post shuffle partitions based on the map output statistics when both `spark.sql.adaptive.enabled` and `spark.sql.adaptive.coalescePartitions.enabled` configurations are true. This feature simplifies the tuning of shuffle partition number when running queries. You do not need to set a proper shuffle partition number to fit your dataset. Spark can pick the proper shuffle partition number at runtime once you set a large enough initial number of shuffle partitions via `spark.sql.adaptive.coalescePartitions.initialPartitionNum` configuration.

  - https://spark.apache.org/docs/latest/sql-performance-tuning.html#coalescing-post-shuffle-partitions

More infor:

- Adaptive Query Execution: Speeding Up Spark SQL at Runtime
  - https://www.databricks.com/blog/2020/05/29/adaptive-query-execution-speeding-up-spark-sql-at-runtime.html
  - https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution

# Q16 - coalesce and repartition

- What is the difference beetwen REPARTITION & COALESCE?
  - Spark is a framework which provides parallel and distributed computing on big data. To perform it’s parallel processing, spark splits the data into smaller chunks(i.e. partitions) and distributes the same to each node in the cluster to provide a parallel execution of the data. This partitioning of data is performed by spark’s internals and the same can also be controlled by the user.

  - Repartition is a method in spark which can be used to either increase or decrease the RDD/DataFrame partitions. This is an expensive operation as it involves shuffling of data across the partitions. The number of partitions to be created can be passed as an argument to the repartition method. If no argument is passed to the repartition method, then the data is shuffled across the default partitions which is 200.


More info:
- https://datanoon.com/blog/spark_repartition_coalesce/