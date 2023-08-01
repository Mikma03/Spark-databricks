<!-- TOC -->

- [Delta Log](#delta-log)
- [How Delta works?](#how-delta-works)
  - [1. Storage Format:](#1-storage-format)
  - [2. Delta Log:](#2-delta-log)
  - [3. ACID Transactions:](#3-acid-transactions)
  - [4. Versioning:](#4-versioning)
  - [5. Schema Enforcement and Evolution:](#5-schema-enforcement-and-evolution)
  - [6. Compaction and Bin Packing:](#6-compaction-and-bin-packing)
  - [7. Concurrency Control:](#7-concurrency-control)
  - [8. Integration with Big Data Tools:](#8-integration-with-big-data-tools)
  - [9. Unification of Batch and Stream Processing:](#9-unification-of-batch-and-stream-processing)
  - [Conclusion:](#conclusion)

<!-- /TOC -->

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
