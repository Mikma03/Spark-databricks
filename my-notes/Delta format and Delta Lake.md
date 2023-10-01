Delta good explanation: Udemy
- https://www.udemy.com/course/databricks-certified-data-engineer-associate/learn/lecture/34664674#overview

Delta offcial Microsoft docs
- https://learn.microsoft.com/en-us/azure/databricks/delta/

Specification for the Delta Transaction Protocol
- https://github.com/delta-io/delta/blob/master/PROTOCOL.md

## Delta Log

Delta Log is a core component of Delta Lake, an open-source storage layer that brings reliability to Data Lakes. It works over your existing data and is fully compatible with API's like Spark, Hive, Presto, etc. Delta Lake provides ACID transactions, scalable metadata handling, and unifies streaming and batch data processing.

Here's how Delta Log works:

1. **Transaction Log**: Delta Log stores a transaction log in the metadata of the storage system (like HDFS or S3). This log keeps track of all changes to the data, including appends, modifications, and deletions.
    
2. **ACID Transactions**: By maintaining a detailed transaction log, Delta Lake can provide ACID (Atomicity, Consistency, Isolation, Durability) transactions. This ensures that all operations on the data are atomic and consistent, even in the face of failures.
    
3. **Versioning**: Delta Log maintains different versions of the dataset. Each transaction writes out a new version. This allows for time travel capabilities where you can query previous versions of the data.
    
4. **Concurrency Control**: Delta Log enables concurrent reads and writes by using optimistic concurrency control. Multiple writers can simultaneously modify the dataset, and Delta Lake will check for conflicts to ensure that the writes are compatible.
    
5. **Schema Enforcement and Evolution**: Delta Log helps in enforcing the schema of the data. If the data being written doesn't match the schema, the write is rejected. It also supports schema evolution where the schema can be modified, and those changes are tracked in the Delta Log.
    
6. **Efficient Storage Management**: Delta Log also helps in managing the storage more efficiently by providing mechanisms for compacting small files and removing files that are no longer needed.
    
7. **Integration with Spark**: Delta Lake and Delta Log are often used with Apache Spark to provide enhanced reliability and performance for Spark data pipelines.
    

In summary, Delta Log is a critical component of Delta Lake that enables versioning, transaction support, concurrency control, and other features that make working with large-scale data more reliable and efficient. It's a foundational technology for those looking to bring more robust data management practices to their data lakes.


## How Delta works?

Delta Lake is a storage layer that brings reliability to Data Lakes. It works over your existing data and is fully compatible with APIs like Spark, Hive, Presto, etc. The Delta format is a key part of how Delta Lake achieves its functionality. Here's how it works:

### 1. Storage Format:

Delta Lake stores data in Parquet format, an efficient columnar storage format. This allows Delta Lake to leverage the existing ecosystem of tools that work with Parquet.

### 2. Delta Log:

The Delta Log is a transaction log that records all changes to the data. Every transaction in Delta Lake (like a write, update, or delete) is recorded in the Delta Log. This log is stored as a set of JSON files in a `_delta_log` directory within the data directory.

### 3. ACID Transactions:

Delta Lake provides ACID transactions by using the Delta Log. Before a write is committed, Delta Lake checks the log to ensure that there are no conflicting writes. This ensures that all operations are atomic and consistent.

### 4. Versioning:

Each transaction in Delta Lake creates a new version of the data. The Delta Log contains a complete history of these versions. This allows for "time travel" where you can query previous versions of the data.

### 5. Schema Enforcement and Evolution:

Delta Lake enforces that the data written to a table matches the table's schema. If the data doesn't match, the write is rejected. Delta Lake also supports schema evolution, where the schema can be modified, and those changes are tracked in the Delta Log.

### 6. Compaction and Bin Packing:

Over time, the number of small files and the number of log files can grow. Delta Lake periodically compacts these files to optimize reads. It also provides mechanisms for manually managing the files.

### 7. Concurrency Control:

Delta Lake supports concurrent reads and writes. Multiple writers can simultaneously modify the data, and Delta Lake uses optimistic concurrency control to ensure that the writes are compatible.

### 8. Integration with Big Data Tools:

Delta Lake is designed to work with big data processing tools like Apache Spark. It provides a DataFrame-based API that integrates seamlessly with Spark's APIs.

### 9. Unification of Batch and Stream Processing:

Delta Lake allows you to use the same table for both batch and stream processing. This simplifies the architecture and allows for more robust data pipelines.

### Conclusion:

The Delta format in Delta Lake is a combination of Parquet files for storing the data and a Delta Log for tracking changes. This combination provides ACID transactions, versioning, schema enforcement, and other features that make working with large-scale data more reliable and efficient. It's a powerful tool for organizations looking to improve their data management practices.

## Intro to Delta Lake - Medium

Link to article:
- https://medium.com/@SaiParvathaneni/delta-lake-for-beginners-data-lake-data-warehouse-and-more-4017099b87a5

Delta Lake is an open-source storage layer built atop a data lake that confers reliability and ACID (Atomicity, Consistency, Isolation, and Durability) transactions. It enables a continuous and simplified data architecture for organizations. Delta lake stores data in Parquet formats and enables a lakehouse data architecture, which helps organizations achieve a single, continuous data system that combines the best features of both the data warehouse and data lake while supporting streaming and batch processing.

![[Pasted image 20230802071113.png]]

## Objects in Databricks

![[Pasted image 20230809181813.png]]

## Important commands related to Delta Lake in Databricks

Databricks.

1. **Creating a Delta Table**: You can create a Delta table using the following command:
    
    sqlCopy code
    
    `CREATE TABLE events USING DELTA LOCATION '/mnt/events'`
    

2. **Converting to Delta Table**: If you have an existing Parquet table, you can convert it to a Delta table with:
    
    sqlCopy code
    
    `` CONVERT TO DELTA parquet.`/path/to/table` ``
    

3. **Reading from a Delta Table**: You can read from a Delta table using standard Spark SQL or DataFrame API:
    
    pythonCopy code
    
    `df = spark.read.format("delta").load("/mnt/events")`
    

4. **Writing to a Delta Table**: You can write to a Delta table using:
    
    pythonCopy code
    
    `df.write.format("delta").save("/mnt/events")`
    

5. **Time Travel (Versioning)**: Delta Lake allows you to query an older snapshot of a table. Here's how you can do it:
    
    sqlCopy code
    
    `SELECT * FROM events TIMESTAMP AS OF '2019-01-01'`
    

6. **Optimize Command**: This command is used to optimize the layout of Delta Lake data:
    
    sqlCopy code
    
    `OPTIMIZE events`
    

7. **Vacuum Command**: This command is used to clean up the invalid files in the storage layer that are no longer in use:
    
    sqlCopy code
    
    `VACUUM events`
    

8. **Describe History Command**: This command provides the history of operations on a Delta table:
    
    sqlCopy code
    
    `DESCRIBE HISTORY events`
    

9. **Z-Ordering (Multi-dimensional clustering)**: This command is used to optimize queries by co-locating related information in the same set of files:
    
```SQL
OPTIMIZE events ZORDER BY (city, date)
```
    

10. **Merge Command**: This command is used to merge two tables based on given conditions:
    
    ```SQL
MERGE INTO events USING updates ON events.eventId = updates.eventId
```

These commands cover various aspects of working with Delta Lake in Databricks, including creating, reading, writing, optimizing, and managing Delta tables.

More examples and explanations of Databricks Delta Lake commands that I found from various sources:

### 1. Reading and Writing Data with Delta Lake

#### Reading Data

pythonCopy code

```python
from pyspark.sql import SparkSession  

spark = SparkSession.builder \     
					.appName("Read Data from Delta Lake") \     
					.getOrCreate()  
					
					
data = spark.read.format("delta").load("/path/to/delta-table") data.show()

```

#### Writing Data

pythonCopy code

`data.write.format("delta").mode("overwrite").save("/path/to/delta-table")`

[Source](https://databricks.com/blog/2019/02/04/introducing-delta-lake-open-source-reliable-data-lakes.html)

### 2. Converting Parquet to Delta

pythonCopy code

``from delta.tables import DeltaTable  parquet_table = "/path/to/parquet-table" delta_table = "/path/to/delta-table"  DeltaTable.convertToDelta(spark, f"parquet.`{parquet_table}`", "key long")   .write \   .format("delta") \   .mode("overwrite") \   .save(delta_table)``

[Source](https://docs.databricks.com/delta/delta-batch.html)

### 3. Update, Delete, and Merge Operations

#### Update

pythonCopy code

`from delta.tables import DeltaTable  deltaTable = DeltaTable.forPath(spark, "/path/to/delta-table") deltaTable.update(   condition="key = 1",   set={"value": "new-value"} )`

#### Delete

pythonCopy code

`deltaTable.delete("key = 1")`

#### Merge

pythonCopy code

`from pyspark.sql.functions import *  newData = spark.range(0, 100).withColumn("newCol", expr("id * 2"))  deltaTable.alias("oldData") \     .merge(         newData.alias("newData"),         "oldData.id = newData.id") \     .whenMatchedUpdate(set={"id": col("newData.id"), "newCol": col("newData.newCol")}) \     .whenNotMatchedInsert(values={"id": col("newData.id"), "newCol": col("newData.newCol")}) \     .execute()`

[Source](https://docs.databricks.com/delta/delta-update.html)

### 4. Time Travel (Versioning)

pythonCopy code

`versioned_data = spark.read.format("delta").option("versionAsOf", 5).load("/path/to/delta-table") versioned_data.show()`

[Source](https://docs.databricks.com/delta/delta-batch.html)

These examples cover various aspects of working with Delta Lake, including reading and writing data, converting data formats, performing CRUD operations, and utilizing versioning features.