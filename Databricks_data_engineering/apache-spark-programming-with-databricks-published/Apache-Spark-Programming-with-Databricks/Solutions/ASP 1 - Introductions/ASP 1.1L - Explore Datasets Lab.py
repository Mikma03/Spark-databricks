# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Explore Datasets Lab
# MAGIC 
# MAGIC We will use tools introduced in this lesson to explore the datasets used in this course.
# MAGIC 
# MAGIC ### BedBricks Case Study
# MAGIC This course uses a case study that explores clickstream data for the online mattress retailer, BedBricks.  
# MAGIC You are an analyst at BedBricks working with the following datasets: **`events`**, **`sales`**, **`users`**, and **`products`**.
# MAGIC 
# MAGIC ##### Tasks
# MAGIC 1. View data files in DBFS using magic commands
# MAGIC 1. View data files in DBFS using dbutils
# MAGIC 1. Create tables from files in DBFS
# MAGIC 1. Execute SQL to answer questions on BedBricks datasets

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md ### 1. List data files in DBFS using magic commands
# MAGIC Use a magic command to display files located in the DBFS directory: **`dbfs:/databricks-datasets`**
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_32.png" alt="Hint"> You should see several datasets that come pre-installed in Databricks such as: **`COVID`**, **`adult`**, and **`airlines`**.

# COMMAND ----------

# ANSWER
# Note: since this answer relies on the %fs magic, you'll need to remove both comment lines before this cell will run!
%fs ls dbfs:/databricks-datasets

# COMMAND ----------

# MAGIC %md ### 2. List data files in DBFS using dbutils
# MAGIC - Use **`dbutils`** to get the files at the directory above and save it to the variable **`files`**
# MAGIC - Use the Databricks display() function to display the contents in **`files`**
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_32.png" alt="Hint"> You should see several datasets that come pre-installed in Databricks such as: **`COVID`**, **`adult`**, and **`airlines`**.

# COMMAND ----------

# ANSWER
files = dbutils.fs.ls("dbfs:/databricks-datasets")
display(files)

# COMMAND ----------

# MAGIC %md ### 3. Create tables below from files in DBFS
# MAGIC - Create the **`users`** table using the spark-context variable **`c.users_path`**
# MAGIC - Create the **`sales`** table using the spark-context variable **`c.sales_path`**
# MAGIC - Create the **`products`** table using the spark-context variable **`c.products_path`**
# MAGIC - Create the **`events`** table using the spark-context variable **`c.events_path`**
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_32.png"> Hint: We created the **`events`** table in the previous notebook but in a different database.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC CREATE TABLE IF NOT EXISTS users USING delta OPTIONS (path "${c.users_path}");
# MAGIC CREATE TABLE IF NOT EXISTS sales USING delta OPTIONS (path "${c.sales_path}");
# MAGIC CREATE TABLE IF NOT EXISTS products USING delta OPTIONS (path "${c.products_path}");
# MAGIC CREATE TABLE IF NOT EXISTS events USING delta OPTIONS (path "${c.events_path}");

# COMMAND ----------

# MAGIC %md Use the data tab of the workspace UI to confirm your tables were created.

# COMMAND ----------

# MAGIC %md ### 4. Execute SQL to explore BedBricks datasets
# MAGIC Run SQL queries on the **`products`**, **`sales`**, and **`events`** tables to answer the following questions. 
# MAGIC - What products are available for purchase at BedBricks?
# MAGIC - What is the average purchase revenue for a transaction at BedBricks?
# MAGIC - What types of events are recorded on the BedBricks website?
# MAGIC 
# MAGIC The schema of the relevant dataset is provided for each question in the cells below.

# COMMAND ----------

# MAGIC %md 
# MAGIC #### 4.1: What products are available for purchase at BedBricks?
# MAGIC 
# MAGIC The **`products`** dataset contains the ID, name, and price of products on the BedBricks retail site.
# MAGIC 
# MAGIC | field | type | description
# MAGIC | --- | --- | --- |
# MAGIC | item_id | string | unique item identifier |
# MAGIC | name | string | item name in plain text |
# MAGIC | price | double | price of item |
# MAGIC 
# MAGIC Execute a SQL query that selects all from the **`products`** table. 
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_32.png" alt="Hint"> You should see 12 products.

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- ANSWER
# MAGIC SELECT * FROM products

# COMMAND ----------

# MAGIC %md #### 4.2: What is the average purchase revenue for a transaction at BedBricks?
# MAGIC 
# MAGIC The **`sales`** dataset contains order information representing successfully processed sales.  
# MAGIC Most fields correspond directly with fields from the clickstream data associated with a sale finalization event.
# MAGIC 
# MAGIC | field | type | description|
# MAGIC | --- | --- | --- |
# MAGIC | order_id | long | unique identifier |
# MAGIC | email | string | the email address to which sales configuration was sent |
# MAGIC | transaction_timestamp | long | timestamp at which the order was processed, recorded in milliseconds since epoch |
# MAGIC | total_item_quantity | long | number of individual items in the order |
# MAGIC | purchase_revenue_in_usd | double | total revenue from order |
# MAGIC | unique_items | long | number of unique products in the order |
# MAGIC | items | array | provided as a list of JSON data, which is interpreted by Spark as an array of structs |
# MAGIC 
# MAGIC Execute a SQL query that computes the average **`purchase_revenue_in_usd`** from the **`sales`** table.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_32.png" alt="Hint"> The result should be **`1042.79`**.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC SELECT AVG(purchase_revenue_in_usd)
# MAGIC FROM sales

# COMMAND ----------

# MAGIC %md #### 4.3: What types of events are recorded on the BedBricks website?
# MAGIC 
# MAGIC The **`events`** dataset contains two weeks worth of parsed JSON records, created by consuming updates to an operational database.  
# MAGIC Records are received whenever: (1) a new user visits the site, (2) a user provides their email for the first time.
# MAGIC 
# MAGIC | field | type | description|
# MAGIC | --- | --- | --- |
# MAGIC | device | string | operating system of the user device |
# MAGIC | user_id | string | unique identifier for user/session |
# MAGIC | user_first_touch_timestamp | long | first time the user was seen in microseconds since epoch |
# MAGIC | traffic_source | string | referral source |
# MAGIC | geo (city, state) | struct | city and state information derived from IP address |
# MAGIC | event_timestamp | long | event time recorded as microseconds since epoch |
# MAGIC | event_previous_timestamp | long | time of previous event in microseconds since epoch |
# MAGIC | event_name | string | name of events as registered in clickstream tracker |
# MAGIC | items (item_id, item_name, price_in_usd, quantity, item_revenue in usd, coupon)| array | an array of structs for each unique item in the user’s cart |
# MAGIC | ecommerce (total_item_quantity, unique_items, purchase_revenue_in_usd)  |  struct  | purchase data (this field is only non-null in those events that correspond to a sales finalization) |
# MAGIC 
# MAGIC Execute a SQL query that selects distinct values in **`event_name`** from the **`events`** table
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_32.png" alt="Hint"> You should see 23 distinct **`event_name`** values.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC SELECT DISTINCT event_name
# MAGIC FROM events

# COMMAND ----------

# MAGIC %md ### Clean up classroom

# COMMAND ----------

classroom_cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
