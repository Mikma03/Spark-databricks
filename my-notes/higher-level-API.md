

All of this simplicity and expressivity that we developers cherish is possible because of the Spark SQL engine upon which the high-level Structured APIs are built. It is because of this engine, which underpins all the Spark components, that we get uniform APIs. 

Whether you express a query against a DataFrame in Structured Streaming or MLlib, you are always transforming and operating on DataFrames as structured data. We’ll take a closer look at the Spark SQL engine later in this chapter, but for now let’s explore those APIs and DSLs for common operations and how to use them for data analytics.

## Spark’s Basic Data Types

Matching its supported programming languages, Spark supports basic internal data types. These data types can be declared in your Spark application or defined in your schema. For example, in Scala, you can define or declare a particular column name to be of type `String`, `Byte`, `Long`, or Map, etc.

Link to book with more details regarding t data types

https://learning.oreilly.com/library/view/learning-spark-2nd/9781492050032/ch03.html#basic_scala_data_types_in_spark

Official Spark docs:

https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/column.html

## SparSession

   `# Create a SparkSession`
   `spark` `=` `(``SparkSession`
     `.``builder`
     `.``appName``(``"Example-3_6"``)`
     `.``getOrCreate``())`


## Columns and Expressions

As mentioned previously, named columns in DataFrames are conceptually similar to named columns in pandas or R DataFrames or in an RDBMS table: they describe a type of field. You can list all the columns by their names, and you can perform operations on their values using relational or computational expressions. In Spark’s supported languages, columns are objects with public methods (represented by the `Column` type).


Scala, Java, and Python all have [public methods associated with columns](https://oreil.ly/xVBIX). You’ll note that the Spark documentation refers to both `col` and `Column`. `Column` is the name of the object, while `col()` is a standard built-in function that returns a `Column`.

https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/column.html

## Rows

A row in Spark is a generic [`Row` object](https://oreil.ly/YLMnw), containing one or more columns. Each column may be of the same data type (e.g., integer or string), or they can have different types (integer, string, map, array, etc.). Because `Row` is an object in Spark and an ordered collection of fields, you can instantiate a `Row` in each of Spark’s supported languages and access its fields by an index starting at 0


## Common DataFrame Operations

To perform common data operations on DataFrames, you’ll first need to load a DataFrame from a data source that holds your structured data. Spark provides an interface, [`DataFrameReader`](https://oreil.ly/v3WLZ), that enables you to read data into a DataFrame from myriad data sources in formats such as JSON, CSV, Parquet, Text, Avro, ORC, etc. Likewise, to write a DataFrame back to a data source in a particular format, Spark uses [`DataFrameWriter`](https://oreil.ly/vzjau).


The `spark.read.csv()` function reads in the CSV file and returns a DataFrame of rows and named columns with the types dictated in the schema.

To write the DataFrame into an external data source in your format of choice, you can use the `DataFrameWriter` interface. Like `DataFrameReader`, it supports [multiple data sources](https://oreil.ly/4rYNZ). Parquet, a popular columnar format, is the default format; it uses snappy compression to compress the data. If the DataFrame is written as Parquet, the schema is preserved as part of the Parquet metadata. In this case, subsequent reads back into a DataFrame do not require you to manually supply a schema.