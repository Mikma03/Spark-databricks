
## Function: print_schema_in_json

```python
import json
from pyspark.sql import DataFrame

def print_schema_in_json(df: DataFrame):
    """
    Print the schema of a Spark DataFrame in pretty JSON format.

    Args:
        df (DataFrame): The Spark DataFrame.

    """
    schema_json = df.schema.json()
    schema_dict = json.loads(schema_json)
    pretty_schema_json = json.dumps(schema_dict, indent=2)
    print(pretty_schema_json)

```

## Description

This function prints the schema of a given Spark DataFrame in a human-readable JSON format. It includes column names, data types, and the nullability of each column.

## Parameters

- `df` (pyspark.sql.DataFrame): The Spark DataFrame whose schema is to be printed.

## Returns

This function doesn't return anything. It prints the schema of the DataFrame to the standard output.

## Usage

First, import the necessary modules and initialize a Spark session:

```python

from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder.getOrCreate()
```


Next, load your DataFrame. For example, to load a CSV file:

```python
df = spark.read.format("csv").option("header", "true").load("your-file-path")

```

Finally, call the `print_schema_in_json` function to print the schema of the DataFrame:


```python
print_schema_in_json(df)

```

The output will be the schema of the DataFrame printed in a human-readable JSON format.

## Example

Given a DataFrame `df` with the following schema:

- `column1`: integer
- `column2`: string
- `column3`: string

The function will print:


```json
{
  "type" : "struct",
  "fields" : [
    {
      "name" : "column1",
      "type" : "integer",
      "nullable" : true,
      "metadata" : { }
    },
    {
      "name" : "column2",
      "type" : "string",
      "nullable" : true,
      "metadata" : { }
    },
    {
      "name" : "column3",
      "type" : "string",
      "nullable" : true,
      "metadata" : { }
    }
  ]
}

```
