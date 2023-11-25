```python
.printSchema(
    colWidth: int = 30,
    toConsole: bool = True
) -> None
# Prints out the schema in the tree format.
```

```python
.repartition(
    numPartitions: int,
    *cols: str
) -> DataFrame
# Returns a new DataFrame partitioned by the given partitioning expressions.
```

```python
SparkContext(
    master: str = None,
    appName: str = None,
    sparkHome: str = None,
    pyFiles: str = None,
    environment: Dict[str, str] = None,
    batchSize: int = 0,
    serializer: str = PickleSerializer(),
    conf: SparkConf = None,
    gateway: Gateway = None,
    jsc: JavaObject = None,
    profiler_cls: str = 'BasicProfiler'
)
# The entry point to programming Spark with the Dataset and DataFrame API.
```

```python
.join(
    other: 'DataFrame',
    on: Union[str, List[str], Column, List[Column]],
    how: str = 'inner' | 'outer' | 'left_outer' | 'right_outer' | 'leftsemi'
) -> 'DataFrame'
# Joins with another DataFrame, using the given join expression.
```


```python
.dropna(
    how: str = 'any' | 'all',
    thresh: int = None,
    subset: Union[str, Tuple[str, ...]] = None
) -> 'DataFrame'
# Returns a new DataFrame omitting rows with null values.
```

```python
from pyspark import StorageLevel

df.persist(StorageLevel.MEMORY_ONLY)

# Note that the storage level MEMORY_ONLY means that all partitions that do not fit into memory will be recomputed when they are needed.

.persist(
    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK
) -> DataFrame
# persist() marks a DataFrame as eligible for caching (similar to cache()).
```

```python
.cache(
    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK
) -> DataFrame
# Persist this DataFrame with the default storage level (MEMORY_AND_DISK).
```


```python
.explode(
    column: Union[str, Column]
) -> DataFrame
# Creates a new row for each element in the given array or map column.
```

```python
.split(
    pattern: str,
    limit: int = -1
) -> List[str]
# Splits str around pattern (pattern is a regular expression).

# Example:
df.select(split(df['name'], ' ').alias('s')).collect()
```

```python
.coalesce(
    numPartitions: int
) -> DataFrame
# Returns a new DataFrame that has exactly numPartitions partitions.
```

```python
.select(
    *cols: str
) -> DataFrame
# Projects a set of expressions and returns a new DataFrame.
```

```python
.alias(
    alias: str
) -> DataFrame
# Returns a new DataFrame with an alias set.

# Example:
df.select(df.age.alias("age2")).collect()
```

```python
spark.read.parquet('path/to/file.parquet')
# Loads Parquet files, returning the result as a DataFrame.
```

```python
.col(
    colName: str
) -> Column
# Returns a Column based on the given column name.
```

```python
.take(
    num: int
) -> List[Row]
# Returns the first num rows as a list of Row.
```


```python
.filter(
    condition: Union[str, Column]
) -> DataFrame
# Filters rows using the given condition.

# Example:
df.filter(df.age > 3).collect()
```

```python
.collect(
) -> List[Row]
# Returns all the records as a list of Row.
```

```python
.withColumn(
    colName: str,
    col: Union[Column, str]
) -> DataFrame
# Returns a new DataFrame by adding a column or replacing the existing column that has the same name.
```

```python
.where(
    condition: Union[str, Column]
) -> DataFrame
# Filters rows using the given condition.
```

```python
.write.parquet('path/to/file.parquet')

# Saves the content of the DataFrame in Parquet format at the specified path.
```

```python
.groupBy(
    *cols: str
) -> GroupedData
# Groups the DataFrame using the specified columns, so we can run aggregation on them.
```

```python
.sort(
    *cols: str,
    **kwargs
) -> DataFrame

# Returns a new DataFrame sorted by the specified column(s).

# Example:
df.sort(df.age.desc()).collect()
```


```python
agg(
    *exprs: Union[Column, str]
) -> DataFrame
# Aggregate on the entire DataFrame without groups (shorthand for df.groupBy.agg()).
```

```python
avg(
    *cols: str
) -> DataFrame
# Compute the mean value for each numeric columns for each group.
```

```python
.withColumnRenamed(
    existing: str,
    new: str
) -> DataFrame
# Returns a new DataFrame by renaming an existing column.
```

```python
.orderBy(
    *cols: Union[Column, str],
    **kwargs
) -> DataFrame
# Returns a new DataFrame sorted by the specified column(s).
```

```python
.broadcast(
    df: DataFrame
) -> DataFrame
# Marks a DataFrame as small enough for use in broadcast joins.
```

```python
.partitionBy(
    *cols: Union[str, Column]
) -> DataFrame
# Partitions the output by the given columns on the file system.
```

```python
.drop(
    *cols: str
) -> DataFrame
# Returns a new DataFrame that drops the specified column.
# It is important to know that the drop operator expects column names to be passed as string arguments if multiple columns should be removed.
```

```python
.count(
) -> int
# Returns the number of rows in this DataFrame.
```

```python
.distinct(
) -> DataFrame
# Returns a new DataFrame containing the distinct rows in this DataFrame.
```

```python
dropDuplicates(
    subset: Union[str, List[str]] = None
) -> DataFrame
# Return a new DataFrame with duplicate rows removed, optionally only considering certain columns.
```

```python
lit(
    col: Union[str, int, float, bool]
) -> Column
# Creates a Column of literal value.
```

```python
```