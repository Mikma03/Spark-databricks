# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Coupon Sales Lab
# MAGIC Process and append streaming data on transactions using coupons.
# MAGIC 1. Read data stream
# MAGIC 2. Filter for transactions with coupons codes
# MAGIC 3. Write streaming query results to Delta
# MAGIC 4. Monitor streaming query
# MAGIC 5. Stop streaming query
# MAGIC 
# MAGIC ##### Classes
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.streaming.DataStreamReader.html#pyspark.sql.streaming.DataStreamReader" target="_blank">DataStreamReader</a>
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.streaming.DataStreamWriter.html#pyspark.sql.streaming.DataStreamWriter" target="_blank">DataStreamWriter</a>
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.streaming.StreamingQuery.html#pyspark.sql.streaming.StreamingQuery" target="_blank">StreamingQuery</a>

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md ### 1. Read data stream
# MAGIC - Set to process 1 file per trigger
# MAGIC - Read from Delta files in the source directory specified by **`sales_path`**
# MAGIC 
# MAGIC Assign the resulting DataFrame to **`df`**.

# COMMAND ----------

# ANSWER
df = (spark
      .readStream
      .option("maxFilesPerTrigger", 1)
      .format("delta")
      .load(sales_path)
     )

# COMMAND ----------

# MAGIC %md **1.1: CHECK YOUR WORK**

# COMMAND ----------

assert df.isStreaming
assert df.columns == ["order_id", "email", "transaction_timestamp", "total_item_quantity", "purchase_revenue_in_usd", "unique_items", "items"]
print("All test pass")

# COMMAND ----------

# MAGIC %md ### 2. Filter for transactions with coupon codes
# MAGIC - Explode the **`items`** field in **`df`** with the results replacing the existing **`items`** field
# MAGIC - Filter for records where **`items.coupon`** is not null
# MAGIC 
# MAGIC Assign the resulting DataFrame to **`coupon_sales_df`**.

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import col, explode

coupon_sales_df = (df
                   .withColumn("items", explode(col("items")))
                   .filter(col("items.coupon").isNotNull())
                  )

# COMMAND ----------

# MAGIC %md **2.1: CHECK YOUR WORK**

# COMMAND ----------

schema_str = str(coupon_sales_df.schema)
assert "StructField(items,StructType(List(StructField(coupon" in schema_str, "items column was not exploded"
print("All test pass")

# COMMAND ----------

# MAGIC %md ### 3. Write streaming query results to Delta
# MAGIC - Configure the streaming query to write Delta format files in "append" mode
# MAGIC - Set the query name to "coupon_sales"
# MAGIC - Set a trigger interval of 1 second
# MAGIC - Set the checkpoint location to **`coupons_checkpoint_path`**
# MAGIC - Set the output path to **`coupons_output_path`**
# MAGIC 
# MAGIC Start the streaming query and assign the resulting handle to **`coupon_sales_query`**.

# COMMAND ----------

# ANSWER

coupons_checkpoint_path = working_dir + "/coupon-sales/checkpoint"
coupons_output_path = working_dir + "/coupon-sales/output"

coupon_sales_query = (coupon_sales_df
                      .writeStream
                      .outputMode("append")
                      .format("delta")
                      .queryName("coupon_sales")
                      .trigger(processingTime="1 second")
                      .option("checkpointLocation", coupons_checkpoint_path)
                      .start(coupons_output_path)
                     )

# COMMAND ----------

# MAGIC %md **3.1: CHECK YOUR WORK**

# COMMAND ----------

until_stream_is_ready("coupon_sales")
assert coupon_sales_query.isActive
assert len(dbutils.fs.ls(coupons_output_path)) > 0
assert len(dbutils.fs.ls(coupons_checkpoint_path)) > 0
assert "coupon_sales" in coupon_sales_query.lastProgress["name"]
print("All test pass")

# COMMAND ----------

# MAGIC %md ### 4. Monitor streaming query
# MAGIC - Get the ID of streaming query and store it in **`queryID`**
# MAGIC - Get the status of streaming query and store it in **`queryStatus`**

# COMMAND ----------

# ANSWER
query_id = coupon_sales_query.id
print(query_id)

# COMMAND ----------

# ANSWER
query_status = coupon_sales_query.status
print(query_status)

# COMMAND ----------

# MAGIC %md **4.1: CHECK YOUR WORK**

# COMMAND ----------

assert type(query_id) == str
assert list(query_status.keys()) == ["message", "isDataAvailable", "isTriggerActive"]
print("All test pass")

# COMMAND ----------

# MAGIC %md ### 5. Stop streaming query
# MAGIC - Stop the streaming query

# COMMAND ----------

# ANSWER
coupon_sales_query.stop()

# COMMAND ----------

# MAGIC %md **5.1: CHECK YOUR WORK**

# COMMAND ----------

assert not coupon_sales_query.isActive
print("All test pass")

# COMMAND ----------

# MAGIC %md ### 6. Verify the records were written in Delta format

# COMMAND ----------

# ANSWER
display(spark.read.format("delta").load(coupons_output_path))

# COMMAND ----------

# MAGIC %md ### Classroom Cleanup
# MAGIC Run the cell below to clean up resources.

# COMMAND ----------

classroom_cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
