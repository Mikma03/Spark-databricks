
**RDD - Resilient Distributed Dataset**

RDD is not used usually. Dataframe and Dataset replaced that with other level of abstraction. Dataframe is something next in evolution of Spark. Dataframe and Dataset are collection of RDD. Dataframe and Dataset are higher level API for RDD.

RDD is more control for Dataframe and Dataset. It is immutable. RDD is for unstructured data. Once write then is immutable, that means we can not change content of RDD. 

 The [RDD](https://oreil.ly/KON5Y) is the most basic abstraction in Spark. There are three vital characteristics associated with an RDD:

- Dependencies
    
- Partitions (with some locality information)
    
- Compute function: Partition => `Iterator[T]`
    

All three are integral to the simple RDD programming API model upon which all higher-level functionality is constructed. First, a list of _dependencies_ that instructs Spark how an RDD is constructed with its inputs is required. When necessary to reproduce results, Spark can recreate an RDD from these dependencies and replicate operations on it. This characteristic gives RDDs resiliency.

Second, _partitions_ provide Spark the ability to split the work to parallelize computation on partitions across executors. In some cases—for example, reading from HDFS—Spark will use locality information to send work to executors close to the data. That way less data is transmitted over the network.

And finally, an RDD has a _compute function_ that produces an `Iterator[T]` for the data that will be stored in the RDD.

Simple and elegant! Yet there are a couple of problems with this original model. For one, the compute function (or computation) is opaque to Spark. That is, Spark does not know what you are doing in the compute function. Whether you are performing a join, filter, select, or aggregation, Spark only sees it as a lambda expression. Another problem is that the `Iterator[T]` data type is also opaque for Python RDDs; Spark only knows that it’s a generic object in Python.

Furthermore, because it’s unable to inspect the computation or expression in the function, Spark has no way to optimize the expression—it has no comprehension of its intention. And finally, Spark has no knowledge of the specific data type in `T`. To Spark it’s an opaque object; it has no idea if you are accessing a column of a certain type within an object. Therefore, all Spark can do is serialize the opaque object as a series of bytes, without using any data compression techniques.







