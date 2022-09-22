<!-- TOC -->

- [Notebooks for practise exams:](#notebooks-for-practise-exams)
- [Apache Spark official docs for exam](#apache-spark-official-docs-for-exam)
- [Practise tests - exams](#practise-tests---exams)
- [Q1: Option describes the responsibility of the executors in Spark?](#q1-option-describes-the-responsibility-of-the-executors-in-spark)
- [Q2: The role of tasks in the Spark execution hierarchy?](#q2-the-role-of-tasks-in-the-spark-execution-hierarchy)
- [Q3: Describes the role of the cluster manager?](#q3-describes-the-role-of-the-cluster-manager)
- [Q4: The idea behind dynamic partition pruning in Spark?](#q4-the-idea-behind-dynamic-partition-pruning-in-spark)
- [Q5: One of the big performance advantages that Spark has over Hadoop?](#q5-one-of-the-big-performance-advantages-that-spark-has-over-hadoop)
- [Q6: Deepest level in Spark's execution hierarchy?](#q6-deepest-level-in-sparks-execution-hierarchy)
- [Q7: Which of the following statements about garbage collection in Spark is incorrect?](#q7-which-of-the-following-statements-about-garbage-collection-in-spark-is-incorrect)
- [Q8: Which of the following describes characteristics of the Dataset API?](#q8-which-of-the-following-describes-characteristics-of-the-dataset-api)
- [Q9:  Which of the following describes the difference between client and cluster execution modes?](#q9--which-of-the-following-describes-the-difference-between-client-and-cluster-execution-modes)
- [Q10: Which of the following statements about executors is correct, assuming that one can consider each of the JVMs working as executors as a pool of task execution slots?](#q10-which-of-the-following-statements-about-executors-is-correct-assuming-that-one-can-consider-each-of-the-jvms-working-as-executors-as-a-pool-of-task-execution-slots)
- [Q11: Which of the following statements about RDDs is incorrect?](#q11-which-of-the-following-statements-about-rdds-is-incorrect)
- [Q12:](#q12)

<!-- /TOC -->

# Notebooks for practise exams:

https://flrs.github.io/spark_practice_tests_code/#1/20.html

# Apache Spark official docs for exam

https://www.webassessor.com/zz/DATABRICKS/Python_v2.html

# Practise tests - exams

# Q1: Option describes the responsibility of the executors in Spark?

A: The executors accept tasks from the driver, execute those tasks, and return results to the driver.

**More reading:**

https://freecontent.manning.com/running-spark-an-overview-of-sparks-runtime-architecture/#post-2923:~:text=Responsibilities%20of%20the%20executors,return%20the%20results%20to%20the%20driver.

Each executor has several task slots (or CPU cores) for running tasks in parallel. The executors in the figures have six tasks slots each.  Those slots in white boxes are vacant. You can set the number of task slots to a value two or three times the number of CPU cores. Although these task slots are often referred to as CPU cores in Spark, they’re implemented as threads and don’t need to correspond to the number of physical CPU cores on the machine.

# Q2: The role of tasks in the Spark execution hierarchy?

A: Tasks are the smallest element in the execution hierarchy.

**More reading:**

https://www.rakirahman.me/spark-certification-study-guide-part-1/#tasks

A task is a unit of work that is sent to the executor. Each stage has some tasks, one task per partition. The same task is done over different partitions of the RDD.

# Q3: Describes the role of the cluster manager?

A: The cluster manager allocates resources to Spark applications and maintains the executor processes in client mode.

**More reading:**

In cluster mode, the cluster manager is located on a node other than the client machine. From there it starts and ends executor processes on the cluster nodes as required by the Spark application running on the Spark driver.

- Spark – The Definitive Guide, Chapter 15: 

https://learning.oreilly.com/library/view/spark-the-definitive/9781491912201/ch15.html

Spark applications run as independent sets of processes on a cluster, coordinated by the SparkContext object in your main program (called the driver program).

Specifically, to run on a cluster, the SparkContext can connect to several types of cluster managers (either Spark’s own standalone cluster manager, Mesos, YARN or Kubernetes), which allocate resources across applications. Once connected, Spark acquires executors on nodes in the cluster, which are processes that run computations and store data for your application. Next, it sends your application code (defined by JAR or Python files passed to SparkContext) to the executors. Finally, SparkContext sends tasks to the executors to run.

# Q4: The idea behind dynamic partition pruning in Spark?

A: Dynamic partition pruning is intended to skip over the data you do not need in the results of a query.

**More reading:**

Dynamic partition pruning provides an efficient way to selectively read data from files by skipping data that is irrelevant for the query. For example, if a query asks to consider only rows which have numbers `>12` in column `purchases` via a `filter`, Spark would only read the rows that match this criteria from the underlying files. This method works in an optimal way if the `purchases` data is in a nonpartitioned table and the data to be filtered is partitioned. Data types do not play a role for the reoptimization.

https://dzone.com/articles/dynamic-partition-pruning-in-spark-30

https://learning.oreilly.com/library/view/learning-spark-2nd/9781492050032/ch12.html

https://www.databricks.com/blog/2020/05/13/now-on-databricks-a-technical-preview-of-databricks-runtime-7-including-a-preview-of-apache-spark-3-0.html


In Apache sparks 3.0, a new optimization called dynamic partition pruning is implemented that works both at:

- Logical planning level to find the dimensional filter and propagated across the join to the other side of the scan.

- Physical level to wire it together in a way that this filter executes only once on the dimension side.
- Then the results of the filter gets into reusing directly in the scan of the table. And with this two fold approach we can achieve significant speed ups in many queries in Spark.

# Q5: One of the big performance advantages that Spark has over Hadoop?

A: Spark achieves great performance by storing data and performing computation in memory, whereas large jobs in Hadoop require a large amount of relatively slow disk I/O operations.

**More reading:**

https://data-flair.training/blogs/dag-in-apache-spark/

https://learning.oreilly.com/library/view/learning-spark-2nd/9781492050032/ch01.html

https://logz.io/blog/hadoop-vs-spark/#post-16858:~:text=Spark%20handles%20work%20in%20a%20similar,the%20user%20actively%20persists%20them.%20Initially,

Hadoop is used mainly for disk-heavy operations with the MapReduce paradigm, and Spark is a more flexible, but more costly in-memory processing architecture. Both are Apache top-level projects, are often used together, and have similarities, but it’s important to understand the features of each when deciding to implement them

# Q6: Deepest level in Spark's execution hierarchy?

A: Task

**More reading:**

The hierarchy is, from top to bottom: Job, Stage, Task.

Executors and slots facilitate the execution of tasks, but they are not directly part of the hierarchy. Executors are launched by the driver on worker nodes for the purpose of running a specific Spark application. Slots help Spark parallelize work. An executor can have multiple slots which enable it to process multiple tasks in parallel.

# Q7: Which of the following statements about garbage collection in Spark is incorrect?

A: Manually persisting RDDs in Spark prevents them from being garbage collected.

**More reading: garbage collection**

- Garbage collection information can be accessed in the Spark UI's stage detail view.

- Optimizing garbage collection performance in Spark may limit caching ability.

- In Spark, using the G1 garbage collector is an alternative to using the default Parallel garbage collector.

- Serialized caching is a strategy to increase the performance of garbage collection.

https://stackoverflow.com/questions/32636822/would-spark-unpersist-the-rdd-itself-when-it-realizes-it-wont-be-used-anymore/32640353#32640353

https://www.databricks.com/blog/2015/05/28/tuning-java-garbage-collection-for-spark-applications.html

JVM garbage collection can be a problem when you have large “churn” in terms of the RDDs stored by your program. (It is usually not a problem in programs that just read an RDD once and then run many operations on it.) When Java needs to evict old objects to make room for new ones, it will need to trace through all your Java objects and find the unused ones. The main point to remember here is that the cost of garbage collection is proportional to the number of Java objects, so using data structures with fewer objects (e.g. an array of Ints instead of a LinkedList) greatly lowers this cost. An even better method is to persist objects in serialized form, as described above: now there will be only one object (a byte array) per RDD partition. Before trying other techniques, the first thing to try if GC is a problem is to use serialized caching.

GC can also be a problem due to interference between your tasks’ working memory (the amount of space needed to run the task) and the RDDs cached on your nodes. We will discuss how to control the space allocated to the RDD cache to mitigate this.

https://spark.apache.org/docs/3.0.0/tuning.html#garbage-collection-tuning

https://luminousmen.com/post/dive-into-spark-memory

# Q8: Which of the following describes characteristics of the Dataset API?

A: The Dataset API is available in Scala, but it is not available in Python.

**More reading:**

The Dataset API uses fixed typing and is typically used for object-oriented programming. It is available when Spark is used with the Scala programming language, but not for Python. In Python, you use the DataFrame API, which is based on the Dataset API.

https://learning.oreilly.com/library/view/learning-spark-2nd/9781492050032/ch03.html

https://www.databricks.com/spark/getting-started-with-apache-spark/datasets

https://docs.databricks.com/getting-started/spark/datasets.html?_ga=2.151242297.1803555863.1661803958-770412762.1659445387#dataset-notebook

# Q9:  Which of the following describes the difference between client and cluster execution modes?

A:  In cluster mode, the driver runs on the worker nodes, while the client mode runs the driver on the client machine.

**More reading:**

https://learning.oreilly.com/library/view/learning-spark-2nd/9781492050032/ch02.html

https://learning.oreilly.com/library/view/spark-the-definitive/9781491912201/ch15.html

# Q10: Which of the following statements about executors is correct, assuming that one can consider each of the JVMs working as executors as a pool of task execution slots?

A: Tasks run in parallel via slots.

Given the assumption, an executor then has one or more "slots", defined by the equation `spark.executor.cores` / `spark.task.cpus`. With the executor's resources divided into slots, each task takes up a slot and multiple tasks can be executed in parallel.

**More reading:**

https://0x0fff.com/spark-architecture/#:~:text=you%20can%20consider%20each%20of%20the%20jvms%20working%20as%20executors%20as%20a%20pool%20of%20task%20execution%20slots,%20each%20executor%20would%20give%20you%20spark.executor.cores%20/%20spark.task.cpus%20execution%20slots%20for%20your%20tasks,%20with%20a%20total%20of%20spark.executor.instances%20executors.

# Q11: Which of the following statements about RDDs is incorrect?

A: An RDD consists of a single partition.

**More reading: Info about RDDs**

The high-level DataFrame API is built on top of the low-level RDD API.

RDDs are immutable.

RDD stands for Resilient Distributed Dataset.

RDDs are great for precisely instructing Spark on how to do a query.

# Q12: 
