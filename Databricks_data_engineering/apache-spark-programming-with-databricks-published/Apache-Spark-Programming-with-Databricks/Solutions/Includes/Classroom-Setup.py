# Databricks notebook source
# MAGIC %run ./Common-Notebooks/Common

# COMMAND ----------

sales_path = f"{datasets_dir}/sales/sales.delta"
spark.sql(f"SET c.sales_path = {sales_path}")

users_path = f"{datasets_dir}/users/users.delta"
spark.sql(f"SET c.users_path = {users_path}")

events_path = f"{datasets_dir}/events/events.delta"
spark.sql(f"SET c.events_path = {events_path}")

products_path = f"{datasets_dir}/products/products.delta"
spark.sql(f"SET c.products_path = {products_path}")

